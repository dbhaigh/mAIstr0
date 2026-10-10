// Package taskmgr tracks submitted tasks through decomposition, assignment,
// dispatch, and completion so the GUI can stream progress and show results.
package taskmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
	bolt "go.etcd.io/bbolt"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSynthesis Status = "synthesizing"
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
	changed     chan struct{}
}

// Finalizer converts completed subtask outputs into the task's final result.
type Finalizer func(context.Context, *Task) (string, error)

// Manager is a thread-safe in-memory store of tasks.
type Manager struct {
	mu        sync.RWMutex
	tasks     map[string]*Task
	seq       int
	finalizer Finalizer
	db        *bolt.DB
}

func NewManager() *Manager {
	return &Manager{tasks: make(map[string]*Task)}
}

func NewManagerWithFinalizer(finalizer Finalizer) *Manager {
	return &Manager{tasks: make(map[string]*Task), finalizer: finalizer}
}

// SetFinalizer installs the result finalizer after a manager has been created.
func (m *Manager) SetFinalizer(finalizer Finalizer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finalizer = finalizer
}

// DefaultPath returns the per-user task history database path.
func DefaultPath(owner string) string {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		base = "."
	}
	return filepath.Join(base, "maistr0", "tasks-"+sanitize(owner)+".db")
}

func sanitize(value string) string {
	var out strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out.WriteRune(r)
		default:
			out.WriteByte('-')
		}
	}
	return out.String()
}

// OpenManager opens a persistent task store and marks interrupted work as
// failed rather than presenting an incomplete task as still running.
func OpenManager(path string, finalizer Finalizer) (*Manager, error) {
	if path == "" {
		return nil, fmt.Errorf("taskmgr: persistence path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("taskmgr: create data directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("taskmgr: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("tasks"))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("taskmgr: initialize database: %w", err)
	}

	manager := &Manager{tasks: make(map[string]*Task), finalizer: finalizer, db: db}
	if err := manager.load(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return manager, nil
}

func (m *Manager) load() error {
	var tasks []*Task
	interrupted := make(map[string]bool)
	if err := m.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("tasks"))
		return bucket.ForEach(func(key, value []byte) error {
			var task Task
			if err := json.Unmarshal(value, &task); err != nil {
				return fmt.Errorf("taskmgr: decode task %s: %w", key, err)
			}
			if task.ID == "" {
				return fmt.Errorf("taskmgr: stored task has no id")
			}
			ctx, cancel := context.WithCancel(context.Background())
			task.ctx = ctx
			task.cancel = cancel
			task.changed = make(chan struct{})
			if task.Status == StatusPending || task.Status == StatusRunning || task.Status == StatusSynthesis {
				task.Status = StatusFailed
				task.Error = "orchestrator restarted before the task completed"
				for i := range task.Subtasks {
					if task.Subtasks[i].Status == StatusPending || task.Subtasks[i].Status == StatusRunning {
						task.Subtasks[i].Status = StatusFailed
						task.Subtasks[i].Error = task.Error
					}
				}
				task.UpdatedAt = time.Now()
				interrupted[task.ID] = true
				cancel()
			} else {
				cancel()
			}
			tasks = append(tasks, &task)
			if sequence := taskSequence(task.ID); sequence > m.seq {
				m.seq = sequence
			}
			return nil
		})
	}); err != nil {
		return err
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("tasks"))
		for _, task := range tasks {
			m.tasks[task.ID] = task
			if interrupted[task.ID] {
				encoded, err := json.Marshal(task)
				if err != nil {
					return err
				}
				if err := bucket.Put([]byte(task.ID), encoded); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func taskSequence(id string) int {
	value, ok := strings.CutPrefix(id, "task-")
	if !ok {
		return 0
	}
	sequence, err := strconv.ParseInt(value, 36, 32)
	if err != nil {
		return 0
	}
	return int(sequence)
}

// Close flushes and releases the persistent task database.
func (m *Manager) Close() error {
	if m.db == nil {
		return nil
	}
	return m.db.Close()
}

func (m *Manager) Create(description string, assignments []scheduler.Assignment) *Task {
	task, err := m.CreateChecked(description, assignments)
	if err != nil {
		log.Printf("taskmgr: create task failed: %v", err)
		return nil
	}
	return task
}

// CreateChecked creates a task and reports persistence errors to the caller.
func (m *Manager) CreateChecked(description string, assignments []scheduler.Assignment) (*Task, error) {
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
		changed:     make(chan struct{}),
	}
	if len(results) == 0 {
		t.Status = StatusFailed
		t.Error = "task has no assigned subtasks"
	}
	if err := m.persistLocked(t); err != nil {
		cancel()
		return nil, err
	}
	m.tasks[id] = t
	return cloneTask(t), nil
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
	cancelled, err := m.CancelChecked(id)
	if err != nil {
		log.Printf("taskmgr: persist cancellation failed: %v", err)
	}
	return cancelled
}

// CancelChecked cancels a task and reports persistence errors.
func (m *Manager) CancelChecked(id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok || t.Status == StatusCompleted || t.Status == StatusFailed || t.Status == StatusCancelled {
		return false, nil
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
	signalChanged(t)
	return true, m.persistLocked(t)
}

func (m *Manager) All() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, cloneTask(t))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].ID > out[j].ID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out
}

// Recent returns at most limit tasks ordered by most recent update without
// cloning the entire persisted task history.
func (m *Manager) Recent(limit int) []*Task {
	if limit <= 0 {
		return []*Task{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	tasks := make([]*Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].UpdatedAt.Equal(tasks[j].UpdatedAt) {
			return tasks[i].ID > tasks[j].ID
		}
		return tasks[i].UpdatedAt.After(tasks[j].UpdatedAt)
	})
	if len(tasks) > limit {
		tasks = tasks[:limit]
	}
	out := make([]*Task, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, cloneTask(task))
	}
	return out
}

// StartSubtask marks a dispatched assignment as actively running.
func (m *Manager) StartSubtask(taskID, subtaskID string) {
	if err := m.StartSubtaskChecked(taskID, subtaskID); err != nil {
		log.Printf("taskmgr: persist subtask start failed: %v", err)
	}
}

// StartSubtaskChecked marks a dispatched assignment as running.
func (m *Manager) StartSubtaskChecked(taskID, subtaskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tasks[taskID]; ok {
		if t.Status == StatusCancelled {
			return nil
		}
		for i := range t.Subtasks {
			if t.Subtasks[i].Subtask.ID == subtaskID {
				t.Subtasks[i].Status = StatusRunning
				t.UpdatedAt = time.Now()
				signalChanged(t)
				return m.persistLocked(t)
			}
		}
	}
	return fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
}

// ReassignSubtaskChecked records a retry on a different node and model.
func (m *Manager) ReassignSubtaskChecked(taskID, subtaskID string, assignment scheduler.Assignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	for i := range t.Subtasks {
		if t.Subtasks[i].Subtask.ID != subtaskID {
			continue
		}
		if t.Subtasks[i].Status != StatusRunning {
			return fmt.Errorf("taskmgr: subtask %s in task %s is not running", subtaskID, taskID)
		}
		t.Subtasks[i].Assignment = assignment
		t.UpdatedAt = time.Now()
		signalChanged(t)
		return m.persistLocked(t)
	}
	return fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
}

// WaitForDependencies waits until every prerequisite has completed. It
// returns false if the task is cancelled or any prerequisite fails.
func (m *Manager) WaitForDependencies(ctx context.Context, taskID, subtaskID string) bool {
	for {
		m.mu.RLock()
		t, ok := m.tasks[taskID]
		if !ok || t.Status == StatusCancelled {
			m.mu.RUnlock()
			return false
		}
		var current *SubtaskResult
		results := make(map[string]Status, len(t.Subtasks))
		for i := range t.Subtasks {
			result := &t.Subtasks[i]
			results[result.Subtask.ID] = result.Status
			if result.Subtask.ID == subtaskID {
				current = result
			}
		}
		if current == nil {
			m.mu.RUnlock()
			return false
		}
		ready := true
		for _, dependency := range current.Subtask.DependsOn {
			switch results[dependency] {
			case StatusCompleted:
			case StatusFailed, StatusCancelled:
				m.mu.RUnlock()
				return false
			default:
				ready = false
			}
		}
		if ready {
			m.mu.RUnlock()
			return true
		}
		changed := t.changed
		m.mu.RUnlock()

		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

// InputForSubtask includes completed prerequisite outputs alongside the
// assigned subtask description.
func (m *Manager) InputForSubtask(taskID, subtaskID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return "", false
	}
	var current *SubtaskResult
	results := make(map[string]*SubtaskResult, len(t.Subtasks))
	for i := range t.Subtasks {
		result := &t.Subtasks[i]
		results[result.Subtask.ID] = result
		if result.Subtask.ID == subtaskID {
			current = result
		}
	}
	if current == nil {
		return "", false
	}
	var input strings.Builder
	for _, dependency := range current.Subtask.DependsOn {
		result, exists := results[dependency]
		if !exists || result.Status != StatusCompleted {
			return "", false
		}
		fmt.Fprintf(&input, "Prerequisite %s:\n%s\n\n", dependency, strings.TrimSpace(result.Output))
	}
	fmt.Fprintf(&input, "Task: %s\n\nSubtask: %s", t.Description, current.Subtask.Description)
	return input.String(), true
}

// CompleteSubtask records the outcome of a dispatched subtask and rolls the
// parent task's overall status up once every subtask has finished.
func (m *Manager) CompleteSubtask(taskID, subtaskID string, output string, taskErr error) {
	if err := m.CompleteSubtaskChecked(taskID, subtaskID, output, taskErr); err != nil {
		log.Printf("taskmgr: persist subtask completion failed: %v", err)
	}
}

// CompleteSubtaskChecked records a dispatched outcome and reports persistence
// failures to the caller.
func (m *Manager) CompleteSubtaskChecked(taskID, subtaskID string, output string, taskErr error) error {
	m.mu.Lock()
	t, ok := m.tasks[taskID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	if t.Status == StatusCancelled {
		m.mu.Unlock()
		return nil
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
		m.mu.Unlock()
		return fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
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
	var finalize Finalizer
	var snapshot *Task
	if allDone {
		t.Result = aggregateOutputs(t.Subtasks)
		if anyFailed {
			t.Status = StatusFailed
			t.Error = "one or more subtasks failed"
		} else if m.finalizer != nil {
			t.Status = StatusSynthesis
			finalize = m.finalizer
			snapshot = cloneTask(t)
		} else {
			t.Status = StatusCompleted
		}
	}
	t.UpdatedAt = time.Now()
	signalChanged(t)
	persistErr := m.persistLocked(t)
	taskCtx := t.ctx
	m.mu.Unlock()

	if finalize == nil {
		return persistErr
	}
	if persistErr != nil {
		return persistErr
	}
	result, err := finalize(taskCtx, snapshot)
	m.mu.Lock()
	t, ok = m.tasks[taskID]
	if !ok || t.Status != StatusSynthesis {
		m.mu.Unlock()
		return nil
	}
	if err != nil {
		t.Status = StatusFailed
		t.Error = "result synthesis failed: " + err.Error()
	} else if strings.TrimSpace(result) == "" {
		t.Status = StatusFailed
		t.Error = "result synthesis failed: model returned an empty result"
	} else {
		t.Result = strings.TrimSpace(result)
		t.Status = StatusCompleted
	}
	t.UpdatedAt = time.Now()
	signalChanged(t)
	persistErr = m.persistLocked(t)
	m.mu.Unlock()
	return persistErr
}

func (m *Manager) persistLocked(task *Task) error {
	if m.db == nil {
		return nil
	}
	encoded, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("taskmgr: encode task %s: %w", task.ID, err)
	}
	if err := m.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("tasks")).Put([]byte(task.ID), encoded)
	}); err != nil {
		return fmt.Errorf("taskmgr: persist task %s: %w", task.ID, err)
	}
	return nil
}

func cloneTask(t *Task) *Task {
	cloned := *t
	cloned.Subtasks = append([]SubtaskResult(nil), t.Subtasks...)
	for i := range cloned.Subtasks {
		cloned.Subtasks[i].Subtask.Tags = append([]string(nil), t.Subtasks[i].Subtask.Tags...)
		cloned.Subtasks[i].Subtask.DependsOn = append([]string(nil), t.Subtasks[i].Subtask.DependsOn...)
	}
	return &cloned
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

	var _ = sort.Slice
	var buf []byte
	for n > 0 {
		buf = append([]byte{letters[n%36]}, buf...)
		n /= 36
	}
	return "task-" + string(buf)
}
