// Package taskmgr tracks submitted tasks through decomposition, assignment,
// dispatch, and completion so the GUI can stream progress and show results.
// Durable writers acquire persistGate, per-task persistMu, then Manager.mu;
// they never wait for persistence while holding Manager.mu.
package taskmgr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/syncinterval"
)

type Status string

var ErrNotAwaitingApproval = errors.New("task is not awaiting approval")

// ErrNotResumable reports a resume request for a task that is not stopped in
// a failed or cancelled state.
var ErrNotResumable = errors.New("task is not resumable")

// ErrSubtaskFinished reports an attempt to start a subtask that already has a
// terminal outcome.
var ErrSubtaskFinished = errors.New("subtask has already finished")

const (
	StatusPending          Status = "pending"
	StatusRunning          Status = "running"
	StatusSynthesis        Status = "synthesizing"
	StatusAwaitingApproval Status = "awaiting_approval"
	StatusCompleted        Status = "completed"
	StatusFailed           Status = "failed"
	StatusCancelled        Status = "cancelled"
)

const interruptedTaskError = "orchestrator restarted before the task completed"

const coordinatorChangedTaskError = "coordinator changed before the task completed"

// SubtaskResult tracks one assignment's execution outcome.
type SubtaskResult struct {
	scheduler.Assignment
	Status   Status            `json:"status"`
	Output   string            `json:"output,omitempty"`
	Error    string            `json:"error,omitempty"`
	Attempts []DispatchAttempt `json:"attempts,omitempty"`
}

type DispatchAttemptStatus string

const (
	DispatchAttemptRunning   DispatchAttemptStatus = "running"
	DispatchAttemptSucceeded DispatchAttemptStatus = "succeeded"
	DispatchAttemptFailed    DispatchAttemptStatus = "failed"
)

type DispatchAttempt struct {
	ID           string                `json:"id"`
	RequestID    string                `json:"request_id,omitempty"`
	Number       int                   `json:"number"`
	NodeID       string                `json:"node_id"`
	Model        string                `json:"model"`
	Engine       string                `json:"engine,omitempty"`
	Status       DispatchAttemptStatus `json:"status"`
	FailureClass string                `json:"failure_class,omitempty"`
	HTTPStatus   int                   `json:"http_status,omitempty"`
	Error        string                `json:"error,omitempty"`
	StartedAt    time.Time             `json:"started_at"`
	FinishedAt   *time.Time            `json:"finished_at,omitempty"`
	DurationMs   int64                 `json:"duration_ms"`
}

// Task is a submitted job and everything derived from it.
type Task struct {
	ID              string               `json:"id"`
	Origin          string               `json:"origin,omitempty"`
	Sequence        uint64               `json:"sequence,omitempty"`
	Description     string               `json:"description"`
	Status          Status               `json:"status"`
	Subtasks        []SubtaskResult      `json:"subtasks"`
	Graph           *scheduler.TaskGraph `json:"graph,omitempty"`
	Resumable       bool                 `json:"resumable,omitempty"`
	LastCheckpoint  string               `json:"last_checkpoint,omitempty"`
	RequireApproval bool                 `json:"require_approval,omitempty"`
	Result          string               `json:"result,omitempty"`
	Error           string               `json:"error,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
	ctx             context.Context
	cancel          context.CancelFunc
	changed         chan struct{}
	// persistMu serializes durable updates to this task. Writers acquire
	// persistGate first, then this lock, then Manager.mu; never wait for it
	// while holding Manager.mu.
	persistMu *sync.Mutex
}

// Finalizer converts completed subtask outputs into the task's final result.
type Finalizer func(context.Context, *Task) (string, error)

type Store interface {
	Load() ([]*Task, error)
	Save(*Task) error
	SaveCheckpoint(*Checkpoint) error
	Checkpoints(taskID string) ([]Checkpoint, error)
	LatestCheckpoint(taskID string) (*Checkpoint, error)
	Close() error
}

// Manager is a thread-safe in-memory store of tasks. Writers acquire
// persistGate before per-task persistMu and mu. They may briefly use mu to
// look up a task, but release it before waiting for persistMu. State
// replacement takes persistGate exclusively before mu, and Close takes it
// exclusively before closing the store. Wait follows the same hierarchy when
// reading a snapshot so it cannot report a terminal state before persistence.
type Manager struct {
	// mu protects tasks and the recent-task cache; every task write invalidates the cache before unlocking.
	mu sync.RWMutex
	// persistGate is outermost: writers hold its read lock through persistence;
	// replacement and Close take it exclusively before touching mu or the store.
	persistGate sync.RWMutex
	tasks       map[string]*Task
	recentTasks []*Task
	// recentTasksValid is protected by mu and cleared on every task mutation.
	recentTasksValid bool
	seq              int
	syncOrigin       string
	syncSeq          map[string]uint64
	finalizer        Finalizer
	store            Store
	changes          syncinterval.Signal
}

func (m *Manager) lockTaskForUpdate(id string) (*Task, bool) {
	m.persistGate.RLock()
	for {
		m.mu.RLock()
		t, ok := m.tasks[id]
		m.mu.RUnlock()
		if !ok {
			m.persistGate.RUnlock()
			return nil, false
		}
		t.persistMu.Lock()
		m.mu.Lock()
		if m.tasks[id] == t {
			return t, true
		}
		m.mu.Unlock()
		t.persistMu.Unlock()
	}
}

func (m *Manager) unlockTaskForUpdate(t *Task) {
	m.mu.Unlock()
	t.persistMu.Unlock()
	m.persistGate.RUnlock()
}

func cloneTask(t *Task) *Task {
	cloned := *t
	cloned.persistMu = nil
	cloned.Subtasks = append([]SubtaskResult(nil), t.Subtasks...)
	for i := range cloned.Subtasks {
		cloned.Subtasks[i].Subtask = cloneSubtask(t.Subtasks[i].Subtask)
		cloned.Subtasks[i].Factors.MatchedCapabilities = append([]string(nil), t.Subtasks[i].Factors.MatchedCapabilities...)
		cloned.Subtasks[i].Attempts = append([]DispatchAttempt(nil), t.Subtasks[i].Attempts...)
		for j := range cloned.Subtasks[i].Attempts {
			if cloned.Subtasks[i].Attempts[j].RequestID == "" {
				cloned.Subtasks[i].Attempts[j].RequestID = DispatchRequestID(
					cloned.ID, cloned.Subtasks[i].Subtask.ID,
				)
			}
			if finishedAt := t.Subtasks[i].Attempts[j].FinishedAt; finishedAt != nil {
				clonedFinishedAt := *finishedAt
				cloned.Subtasks[i].Attempts[j].FinishedAt = &clonedFinishedAt
			}
		}
	}
	if t.Graph != nil {
		cloned.Graph = t.Graph.Clone()
	}
	return &cloned
}

func (m *Manager) invalidateRecentTasksLocked() {
	m.recentTasksValid = false
	m.recentTasks = nil
	m.changes.Notify()
}

// Changes returns a broadcast signal that closes when task history changes.
func (m *Manager) Changes() <-chan struct{} { return m.changes.Changes() }

func cloneSubtask(subtask scheduler.Subtask) scheduler.Subtask {
	subtask.Tags = append([]string(nil), subtask.Tags...)
	subtask.DependsOn = append([]string(nil), subtask.DependsOn...)
	subtask.RequiredFields = append([]string(nil), subtask.RequiredFields...)
	subtask.RequiredCapabilities = append([]string(nil), subtask.RequiredCapabilities...)
	subtask.PreferredCapabilities = append([]string(nil), subtask.PreferredCapabilities...)
	return subtask
}

func signalChanged(t *Task) {
	close(t.changed)
	t.changed = make(chan struct{})
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
