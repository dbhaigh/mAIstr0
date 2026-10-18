package taskmgr

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

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

// Wait blocks until the task reaches a terminal state or ctx is cancelled.
func (m *Manager) Wait(ctx context.Context, id string) (*Task, error) {
	for {
		snapshot, changed, ok := m.waitSnapshot(id)
		if !ok {
			return nil, fmt.Errorf("taskmgr: task %s not found", id)
		}
		switch snapshot.Status {
		case StatusCompleted, StatusFailed, StatusCancelled:
			return snapshot, nil
		case StatusAwaitingApproval:
			return snapshot, errors.New("task is awaiting human approval")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// waitSnapshot serializes terminal-state reads with durable task updates so
// Wait cannot report completion before the corresponding checkpoint is saved.
func (m *Manager) waitSnapshot(id string) (*Task, <-chan struct{}, bool) {
	m.persistGate.RLock()
	defer m.persistGate.RUnlock()

	m.mu.RLock()
	t, ok := m.tasks[id]
	m.mu.RUnlock()
	if !ok {
		return nil, nil, false
	}

	t.persistMu.Lock()
	defer t.persistMu.Unlock()
	m.mu.RLock()
	snapshot := cloneTask(t)
	changed := t.changed
	m.mu.RUnlock()
	return snapshot, changed, true
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

// PendingAssignmentsByNode returns the number of waiting subtask assignments
// per node without cloning task outputs or other history.
func (m *Manager) PendingAssignmentsByNode() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	queued := make(map[string]int)
	for _, task := range m.tasks {
		for _, subtask := range task.Subtasks {
			if subtask.Status == StatusPending && subtask.NodeID != "" {
				queued[subtask.NodeID]++
			}
		}
	}
	return queued
}

// Recent returns at most limit tasks ordered by most recent update without
// cloning the entire persisted task history.
func (m *Manager) Recent(limit int) []*Task {
	if limit <= 0 {
		return []*Task{}
	}
	m.mu.RLock()
	if m.recentTasksValid {
		out := m.recentTasksLocked(limit)
		m.mu.RUnlock()
		return out
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.recentTasksValid {
		m.recentTasks = make([]*Task, 0, len(m.tasks))
		for _, task := range m.tasks {
			m.recentTasks = append(m.recentTasks, task)
		}
		sort.Slice(m.recentTasks, func(i, j int) bool {
			if m.recentTasks[i].UpdatedAt.Equal(m.recentTasks[j].UpdatedAt) {
				return m.recentTasks[i].ID > m.recentTasks[j].ID
			}
			return m.recentTasks[i].UpdatedAt.After(m.recentTasks[j].UpdatedAt)
		})
		m.recentTasksValid = true
	}
	return m.recentTasksLocked(limit)
}

func (m *Manager) recentTasksLocked(limit int) []*Task {
	count := len(m.recentTasks)
	if count > limit {
		count = limit
	}
	out := make([]*Task, 0, count)
	for _, task := range m.recentTasks[:count] {
		out = append(out, cloneTask(task))
	}
	return out
}
