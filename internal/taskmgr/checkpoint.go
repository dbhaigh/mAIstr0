package taskmgr

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
)

// Checkpoint stores a durable snapshot of task execution state at a consistent frontier.
type Checkpoint struct {
	ID        string               `json:"id"`
	TaskID    string               `json:"task_id"`
	Sequence  int                  `json:"sequence"`
	Status    Status               `json:"status"`
	Graph     *scheduler.TaskGraph `json:"graph,omitempty"`
	Subtasks  []SubtaskResult      `json:"subtasks"`
	Outputs   map[string]string    `json:"outputs"`
	Frontier  []string             `json:"frontier"`
	Error     string               `json:"error,omitempty"`
	Resumable bool                 `json:"resumable"`
	Timestamp time.Time            `json:"timestamp"`
}

func computeFrontier(t *Task) []string {
	completed := make(map[string]bool)
	for _, s := range t.Subtasks {
		if s.Status == StatusCompleted {
			completed[s.Assignment.Subtask.ID] = true
		}
	}
	if t.Graph != nil {
		readyNodes := t.Graph.ReadyNodes(completed)
		frontier := make([]string, 0, len(readyNodes))
		for _, node := range readyNodes {
			frontier = append(frontier, node.ID)
		}
		return frontier
	}
	var frontier []string
	for _, s := range t.Subtasks {
		if s.Status == StatusCompleted {
			continue
		}
		allDepsDone := true
		for _, dep := range s.Assignment.Subtask.DependsOn {
			if !completed[dep] {
				allDepsDone = false
				break
			}
		}
		if allDepsDone {
			frontier = append(frontier, s.Assignment.Subtask.ID)
		}
	}
	return frontier
}

func collectOutputs(t *Task) map[string]string {
	outputs := make(map[string]string)
	for _, s := range t.Subtasks {
		if s.Status == StatusCompleted && s.Output != "" {
			outputs[s.Assignment.Subtask.ID] = s.Output
		}
	}
	return outputs
}

func checkpointForTask(t *Task) *Checkpoint {
	outputs := collectOutputs(t)
	frontier := computeFrontier(t)
	seq := len(outputs)
	cpID := fmt.Sprintf("cp-%s-%d", t.ID, seq)
	resumable := t.Status != StatusCompleted

	cp := &Checkpoint{
		ID:        cpID,
		TaskID:    t.ID,
		Sequence:  seq,
		Status:    t.Status,
		Graph:     t.Graph,
		Subtasks:  append([]SubtaskResult(nil), t.Subtasks...),
		Outputs:   outputs,
		Frontier:  frontier,
		Error:     t.Error,
		Resumable: resumable,
		Timestamp: time.Now(),
	}

	t.LastCheckpoint = cpID
	t.Resumable = resumable

	return cp
}

func (m *Manager) saveCheckpointLocked(t *Task) (*Checkpoint, error) {
	if m.store == nil {
		return nil, nil
	}
	// Keep checkpoint mutation on a private snapshot while allowing unrelated
	// task reads and writes to proceed during the BoltDB transaction.
	snapshot := cloneTask(t)
	m.mu.Unlock()
	cp := checkpointForTask(snapshot)
	err := m.store.SaveCheckpoint(cp)
	m.mu.Lock()
	if err != nil {
		return nil, err
	}
	t.LastCheckpoint = snapshot.LastCheckpoint
	t.Resumable = snapshot.Resumable
	return cp, nil
}

// GetCheckpoints returns all checkpoints for a task ordered by timestamp.
func (m *Manager) GetCheckpoints(taskID string) ([]Checkpoint, error) {
	if m.store == nil {
		return nil, nil
	}
	out, err := m.store.Checkpoints(taskID)
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Sequence < out[j].Sequence
	})
	return out, nil
}

// GetLatestCheckpoint returns the most recent checkpoint for a task.
func (m *Manager) GetLatestCheckpoint(taskID string) (*Checkpoint, error) {
	if m.store == nil {
		return nil, nil
	}
	return m.store.LatestCheckpoint(taskID)
}

// ResumableTasks returns all tasks eligible for resumption.
func (m *Manager) ResumableTasks() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Task
	for _, t := range m.tasks {
		if t.Resumable && t.Status != StatusCompleted {
			out = append(out, cloneTask(t))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out
}

// ResumeTaskChecked restores an interrupted or failed task to running state
// from its last consistent checkpoint, leaving completed subtasks intact.
func (m *Manager) ResumeTaskChecked(taskID string) (*Task, error) {
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return nil, fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	defer m.unlockTaskForUpdate(t)
	if t.Status == StatusCompleted {
		return nil, fmt.Errorf("taskmgr: task %s is already completed", taskID)
	}
	if !t.Resumable {
		return nil, fmt.Errorf("taskmgr: task %s is not marked resumable", taskID)
	}

	// Reset task state back to running
	t.Status = StatusRunning
	t.Error = ""
	t.UpdatedAt = time.Now()

	// Reset incomplete subtasks back to pending so they can be dispatched
	for i := range t.Subtasks {
		if t.Subtasks[i].Status != StatusCompleted {
			t.Subtasks[i].Status = StatusPending
			t.Subtasks[i].Error = ""
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.ctx = ctx
	t.cancel = cancel

	signalChanged(t)
	if _, err := m.saveCheckpointLocked(t); err != nil {
		return nil, fmt.Errorf("taskmgr: checkpoint resumed task %s: %w", t.ID, err)
	}
	if err := m.persistLocked(t); err != nil {
		return nil, err
	}
	return cloneTask(t), nil
}
