// Package taskmgr tracks submitted tasks through decomposition, assignment,
// dispatch, and completion so the GUI can poll for progress and results.
package taskmgr

import (
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
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
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

	t := &Task{
		ID:          id,
		Description: description,
		Status:      StatusRunning,
		Subtasks:    results,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	m.tasks[id] = t
	return t
}

func (m *Manager) Get(id string) (*Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	return t, ok
}

func (m *Manager) All() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out
}

// StartSubtask marks a dispatched assignment as actively running.
func (m *Manager) StartSubtask(taskID, subtaskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tasks[taskID]; ok {
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
	for i := range t.Subtasks {
		if t.Subtasks[i].Subtask.ID != subtaskID {
			continue
		}
		if taskErr != nil {
			t.Subtasks[i].Status = StatusFailed
			t.Subtasks[i].Error = taskErr.Error()
		} else {
			t.Subtasks[i].Status = StatusCompleted
			t.Subtasks[i].Output = output
		}
		break
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
		} else {
			t.Status = StatusCompleted
		}
	}
	t.UpdatedAt = time.Now()
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
