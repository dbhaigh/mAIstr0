// Package taskmgr tracks submitted tasks through decomposition, assignment,
// dispatch, and completion so the GUI can poll for progress and results.
package taskmgr

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// SubtaskResult tracks one assignment's execution outcome.
type SubtaskResult struct {
	scheduler.Assignment
	Status Status `json:"status"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Task is a submitted job and everything derived from it.
type Task struct {
	ID          string          `json:"id"`
	Description string          `json:"description"`
	Status      Status          `json:"status"`
	Subtasks    []SubtaskResult `json:"subtasks"`
	Result      string          `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	ctx         context.Context
	cancel      context.CancelFunc
}

// Manager is a thread-safe in-memory store of tasks.
type Manager struct {
	mu    sync.RWMutex
	tasks map[string]*Task
	seq   int
}

func NewManager() *Manager {
	return &Manager{tasks: make(map[string]*Task)}
}

func (m *Manager) Create(description string, assignments []scheduler.Assignment) *Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	id := genID(m.seq)

	results := make([]SubtaskResult, 0, len(assignments))
	for _, a := range assignments {
		results = append(results, SubtaskResult{Assignment: a, Status: StatusPending})
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &Task{
		ID:          id,
		Description: description,
		Status:      StatusRunning,
		Subtasks:    results,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		ctx:         ctx,
		cancel:      cancel,
	}
	if len(results) == 0 {
		t.Status = StatusFailed
		t.Error = "task has no assigned subtasks"
	}
	m.tasks[id] = t
	return cloneTask(t)
}

func (m *Manager) Get(id string) (*Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, false
	}
	return cloneTask(t), true
}

func (m *Manager) Context(id string) (context.Context, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, false
	}
	return t.ctx, true
}

func (m *Manager) Cancel(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok || t.Status == StatusCompleted || t.Status == StatusFailed || t.Status == StatusCancelled {
		return false
	}
	t.cancel()
	t.Status = StatusCancelled
	t.Error = "task cancelled"
	for i := range t.Subtasks {
		if t.Subtasks[i].Status == StatusPending || t.Subtasks[i].Status == StatusRunning {
			t.Subtasks[i].Status = StatusCancelled
			t.Subtasks[i].Error = "task cancelled"
		}
	}
	t.UpdatedAt = time.Now()
	return true
}

func (m *Manager) All() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, cloneTask(t))
	}
	return out
}

// StartSubtask marks a dispatched assignment as actively running.
func (m *Manager) StartSubtask(taskID, subtaskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tasks[taskID]; ok {
		if t.Status == StatusCancelled {
			return
		}
		for i := range t.Subtasks {
			if t.Subtasks[i].Subtask.ID == subtaskID {
				t.Subtasks[i].Status = StatusRunning
				t.UpdatedAt = time.Now()
				return
			}
		}
	}
}

// CompleteSubtask records the outcome of a dispatched subtask and rolls the
// parent task's overall status up once every subtask has finished.
func (m *Manager) CompleteSubtask(taskID, subtaskID string, output string, taskErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return
	}
	if t.Status == StatusCancelled {
		return
	}
	found := false
	for i := range t.Subtasks {
		if t.Subtasks[i].Subtask.ID != subtaskID {
			continue
		}
		found = true
		if taskErr != nil {
			t.Subtasks[i].Status = StatusFailed
			t.Subtasks[i].Error = taskErr.Error()
		} else {
			t.Subtasks[i].Status = StatusCompleted
			t.Subtasks[i].Output = output
		}
		break
	}
	if !found {
		return
	}

	allDone := true
	anyFailed := false
	for _, s := range t.Subtasks {
		if s.Status == StatusPending || s.Status == StatusRunning {
			allDone = false
		}
		if s.Status == StatusFailed {
			anyFailed = true
		}
	}
	if allDone {
		if anyFailed {
			t.Status = StatusFailed
			t.Error = "one or more subtasks failed"
		} else {
			t.Status = StatusCompleted
		}
		t.Result = aggregateOutputs(t.Subtasks)
	}
	t.UpdatedAt = time.Now()
}

func cloneTask(t *Task) *Task {
	cloned := *t
	cloned.Subtasks = append([]SubtaskResult(nil), t.Subtasks...)
	for i := range cloned.Subtasks {
		cloned.Subtasks[i].Subtask.Tags = append([]string(nil), t.Subtasks[i].Subtask.Tags...)
	}
	return &cloned
}

func aggregateOutputs(subtasks []SubtaskResult) string {
	var result strings.Builder
	for _, subtask := range subtasks {
		if subtask.Status != StatusCompleted || strings.TrimSpace(subtask.Output) == "" {
			continue
		}
		if result.Len() > 0 {
			result.WriteString("\n\n")
		}
		fmt.Fprintf(&result, "## %s\n%s", subtask.Assignment.Subtask.Description, strings.TrimSpace(subtask.Output))
	}
	return result.String()
}

func genID(seq int) string {
	const letters = "0123456789abcdefghijklmnopqrstuvwxyz"
	n := seq
	if n == 0 {
		return "task-0"
	}
	var buf []byte
	for n > 0 {
		buf = append([]byte{letters[n%36]}, buf...)
		n /= 36
	}
	return "task-" + string(buf)
}
