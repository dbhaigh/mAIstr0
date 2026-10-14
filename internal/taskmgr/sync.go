package taskmgr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// MergeMissing imports tasks that are not already present. It is intended for
// collecting history from peers without allowing stale replicas to overwrite
// the current coordinator's task state.
func (m *Manager) MergeMissing(tasks []*Task) error {
	return m.MergeMissingFrom(tasks, "task:legacy")
}

func (m *Manager) MergeMissingFrom(tasks []*Task, sourceOrigin string) error {
	return m.syncTasks(tasks, false, false, sourceOrigin)
}

// MergeFromReplica imports missing tasks and newer peer updates while
// protecting coordinator state from a replica's restart-recovery status.
func (m *Manager) MergeFromReplica(tasks []*Task) error {
	return m.MergeFromReplicaFrom(tasks, "task:legacy")
}

func (m *Manager) MergeFromReplicaFrom(tasks []*Task, sourceOrigin string) error {
	return m.syncTasks(tasks, false, true, sourceOrigin)
}

// ReplaceFromSync applies an authoritative coordinator snapshot to this
// replica, even if its local copy has a newer timestamp from restart recovery.
func (m *Manager) ReplaceFromSync(tasks []*Task) error {
	return m.ReplaceFromSyncFrom(tasks, "task:legacy")
}

func (m *Manager) ReplaceFromSyncFrom(tasks []*Task, sourceOrigin string) error {
	return m.syncTasks(tasks, true, false, sourceOrigin)
}

func (m *Manager) syncTasks(tasks []*Task, replace, mergeReplicas bool, sourceOrigin string) error {
	if sourceOrigin == "" {
		sourceOrigin = "task:legacy"
	}
	m.persistGate.Lock()
	defer m.persistGate.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	var replicaAccepted []bool
	if mergeReplicas {
		current := make([]*Task, 0, len(m.tasks))
		for _, task := range m.tasks {
			current = append(current, task)
		}
		_, replicaAccepted = mergeReplicaTaskSlices(current, tasks)
	}

	for index, incoming := range tasks {
		if incoming == nil || incoming.ID == "" {
			return fmt.Errorf("taskmgr: synchronized task has no id")
		}
		if incoming.Origin == "" || incoming.Sequence == 0 {
			incoming.Origin = sourceOrigin
			incoming.Sequence = m.nextSyncSequenceLocked(sourceOrigin)
		} else if incoming.Sequence > m.syncSeq[incoming.Origin] {
			m.syncSeq[incoming.Origin] = incoming.Sequence
		}
		current, exists := m.tasks[incoming.ID]
		if exists && !replace {
			if !mergeReplicas || !replicaAccepted[index] {
				continue
			}
		}
		if exists && replace && incoming.Origin != "" && incoming.Origin == current.Origin &&
			incoming.Sequence < current.Sequence {
			continue
		}
		if exists && replace {
			currentJSON, err := json.Marshal(current)
			if err != nil {
				return fmt.Errorf("taskmgr: encode local task %s: %w", current.ID, err)
			}
			incomingJSON, err := json.Marshal(incoming)
			if err != nil {
				return fmt.Errorf("taskmgr: encode synchronized task %s: %w", incoming.ID, err)
			}
			if bytes.Equal(currentJSON, incomingJSON) {
				continue
			}
		}

		task := cloneTask(incoming)
		task.persistMu = &sync.Mutex{}
		orphaned := mergeReplicas && !exists &&
			(task.Status == StatusPending || task.Status == StatusRunning || task.Status == StatusSynthesis)
		if mergeReplicas && !exists {
			interruptOrphanedTask(task)
		}
		if orphaned {
			task.Origin = m.syncOrigin
			task.Sequence = m.nextSyncSequenceLocked(task.Origin)
		}
		task.ctx, task.cancel = context.WithCancel(context.Background())
		task.changed = make(chan struct{})
		if task.Status == StatusCompleted || task.Status == StatusFailed || task.Status == StatusCancelled {
			task.cancel()
		}
		if _, err := m.saveCheckpointLocked(task); err != nil {
			task.cancel()
			return fmt.Errorf("taskmgr: checkpoint synchronized task %s: %w", task.ID, err)
		}
		if err := m.persistReplicaLocked(task); err != nil {
			task.cancel()
			return err
		}
		if exists && current.cancel != nil {
			current.cancel()
		}
		m.tasks[task.ID] = task
		if sequence := taskSequence(task.ID); sequence > m.seq {
			m.seq = sequence
		}
	}
	return nil
}

func (m *Manager) persistReplicaLocked(task *Task) error {
	if m.store == nil {
		return nil
	}
	snapshot := cloneTask(task)
	m.mu.Unlock()
	err := m.store.Save(snapshot)
	m.mu.Lock()
	if err != nil {
		return fmt.Errorf("taskmgr: persist synchronized task %s: %w", task.ID, err)
	}
	return nil
}

func (m *Manager) nextSyncSequenceLocked(origin string) uint64 {
	m.syncSeq[origin]++
	return m.syncSeq[origin]
}

func (m *Manager) Watermarks() map[string]uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	watermarks := make(map[string]uint64)
	for _, task := range m.tasks {
		if task.Origin != "" && task.Sequence > watermarks[task.Origin] {
			watermarks[task.Origin] = task.Sequence
		}
	}
	return watermarks
}

func (m *Manager) SnapshotSince(watermarks map[string]uint64) []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tasks := make([]*Task, 0)
	for _, task := range m.tasks {
		if task.Origin == "" || task.Sequence == 0 || task.Sequence > watermarks[task.Origin] {
			tasks = append(tasks, cloneTask(task))
		}
	}
	return tasks
}

func (m *Manager) SnapshotForPeer(version int, watermarks map[string]uint64) []*Task {
	tasks, _ := m.SnapshotAndWatermarksForPeer(version, watermarks)
	return tasks
}

func (m *Manager) SnapshotAndWatermarksForPeer(version int, watermarks map[string]uint64) ([]*Task, map[string]uint64) {
	tasks := m.All()
	localWatermarks := SnapshotWatermarks(tasks)
	if version != 1 {
		return tasks, localWatermarks
	}
	for origin, sequence := range watermarks {
		if sequence > localWatermarks[origin] {
			return tasks, localWatermarks
		}
	}
	filtered := make([]*Task, 0)
	for _, task := range tasks {
		if task.Origin == "" || task.Sequence == 0 || task.Sequence > watermarks[task.Origin] {
			filtered = append(filtered, task)
		}
	}
	return filtered, localWatermarks
}

func SnapshotWatermarks(tasks []*Task) map[string]uint64 {
	watermarks := make(map[string]uint64)
	for _, task := range tasks {
		if task != nil && task.Origin != "" && task.Sequence > watermarks[task.Origin] {
			watermarks[task.Origin] = task.Sequence
		}
	}
	return watermarks
}

func FilterTaskWatermarks(watermarks map[string]uint64) map[string]uint64 {
	filtered := make(map[string]uint64)
	for origin, sequence := range watermarks {
		if strings.HasPrefix(origin, "task:") {
			filtered[origin] = sequence
		}
	}
	return filtered
}

func mergeReplicaTaskSlices(current, incoming []*Task) ([]*Task, []bool) {
	merged := make([]*Task, 0, len(current)+len(incoming))
	indexByID := make(map[string]int, len(current)+len(incoming))
	for _, task := range current {
		if task == nil || task.ID == "" {
			continue
		}
		indexByID[task.ID] = len(merged)
		merged = append(merged, cloneTask(task))
	}

	accepted := make([]bool, len(incoming))
	for index, task := range incoming {
		if task == nil || task.ID == "" {
			continue
		}
		existingIndex, exists := indexByID[task.ID]
		if exists && !replicaOutranks(merged[existingIndex], task) {
			continue
		}
		accepted[index] = true
		copyOf := cloneTask(task)
		if !exists {
			interruptOrphanedTask(copyOf)
			indexByID[task.ID] = len(merged)
			merged = append(merged, copyOf)
		} else {
			merged[existingIndex] = copyOf
		}
	}
	return merged, accepted
}

func replicaOutranks(current, incoming *Task) bool {
	if isInterruptedTask(incoming) {
		return false
	}
	if isInterruptedTask(current) {
		if incoming.Origin != "" && incoming.Origin == current.Origin &&
			incoming.Sequence < current.Sequence {
			return false
		}
		if incoming.Status == StatusCompleted || incoming.Status == StatusAwaitingApproval ||
			incoming.Status == StatusCancelled || incoming.Status == StatusFailed {
			return true
		}
		return completedSubtaskCount(incoming) > completedSubtaskCount(current)
	}
	if incoming.Origin != "" && incoming.Origin == current.Origin &&
		incoming.Sequence != current.Sequence {
		return incoming.Sequence > current.Sequence
	}
	if incoming.UpdatedAt.After(current.UpdatedAt) {
		return true
	}
	if incoming.UpdatedAt.Equal(current.UpdatedAt) {
		return completedSubtaskCount(incoming) > completedSubtaskCount(current) ||
			taskStatusRank(incoming.Status) > taskStatusRank(current.Status)
	}
	return false
}

func isInterruptedTask(task *Task) bool {
	return task.Error == interruptedTaskError || task.Error == coordinatorChangedTaskError
}

func interruptOrphanedTask(task *Task) {
	switch task.Status {
	case StatusPending, StatusRunning, StatusSynthesis:
		task.Status = StatusFailed
		task.Error = coordinatorChangedTaskError
		task.Resumable = true
		for i := range task.Subtasks {
			if task.Subtasks[i].Status == StatusPending || task.Subtasks[i].Status == StatusRunning {
				task.Subtasks[i].Status = StatusFailed
				task.Subtasks[i].Error = coordinatorChangedTaskError
			}
		}
		task.UpdatedAt = time.Now()
	}
}

func completedSubtaskCount(task *Task) int {
	completed := 0
	for _, subtask := range task.Subtasks {
		if subtask.Status == StatusCompleted {
			completed++
		}
	}
	return completed
}

func taskStatusRank(status Status) int {
	switch status {
	case StatusCompleted:
		return 6
	case StatusAwaitingApproval:
		return 5
	case StatusSynthesis:
		return 4
	case StatusFailed, StatusCancelled:
		return 3
	case StatusRunning:
		return 2
	case StatusPending:
		return 1
	default:
		return 0
	}
}
