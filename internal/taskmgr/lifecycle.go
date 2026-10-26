package taskmgr

import (
	"context"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
)

// SetFinalizer installs the result finalizer after a manager has been created.
func (m *Manager) SetFinalizer(finalizer Finalizer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finalizer = finalizer
}

func (m *Manager) Create(description string, assignments []scheduler.Assignment) *Task {
	task, err := m.CreateChecked(description, assignments)
	if err != nil {
		logging.Errorf("taskmgr", "taskmgr: create task failed: %v", err)
		return nil
	}
	return task
}

// CreateChecked creates a task and reports persistence errors to the caller.
func (m *Manager) CreateChecked(description string, assignments []scheduler.Assignment) (*Task, error) {
	return m.CreateWithOptionsChecked(description, assignments, false)
}

// CreateWithOptionsChecked creates a persisted task with an optional approval
// gate before final synthesis.
func (m *Manager) CreateWithOptionsChecked(description string, assignments []scheduler.Assignment, requireApproval bool) (*Task, error) {
	return m.CreateWithGraphAndOptionsChecked(description, nil, assignments, requireApproval)
}

// CreateWithGraphAndOptionsChecked creates a persisted task with an explicit task graph
// and optional approval gate.
func (m *Manager) CreateWithGraphAndOptionsChecked(description string, graph *scheduler.TaskGraph, assignments []scheduler.Assignment, requireApproval bool) (*Task, error) {
	m.persistGate.RLock()
	defer m.persistGate.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	id := genID(m.seq)

	results := make([]SubtaskResult, 0, len(assignments))
	subtasksForGraph := make([]scheduler.Subtask, 0, len(assignments))
	for _, a := range assignments {
		cloned := cloneSubtask(a.Subtask)
		a.Subtask = cloned
		subtasksForGraph = append(subtasksForGraph, cloned)
		results = append(results, SubtaskResult{Assignment: a, Status: StatusPending})
	}

	if graph == nil && len(subtasksForGraph) > 0 {
		graph, _ = scheduler.TaskGraphFromSubtasks(subtasksForGraph)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &Task{
		ID:              id,
		Description:     description,
		Status:          StatusRunning,
		Subtasks:        results,
		Graph:           graph,
		Resumable:       true,
		RequireApproval: requireApproval,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		ctx:             ctx,
		cancel:          cancel,
		changed:         make(chan struct{}),
		persistMu:       &sync.Mutex{},
	}
	if len(results) == 0 {
		t.Status = StatusFailed
		t.Error = "task has no assigned subtasks"
		t.Resumable = false
	}
	if _, err := m.persistTaskAndCheckpointLocked(t); err != nil {
		cancel()
		return nil, err
	}
	m.tasks[id] = t
	m.invalidateRecentTasksLocked()
	return cloneTask(t), nil
}

func (m *Manager) Cancel(id string) bool {
	cancelled, err := m.CancelChecked(id)
	if err != nil {
		logging.Errorf("taskmgr", "taskmgr: persist cancellation failed: %v", err)
	}
	return cancelled
}

// CancelChecked cancels a task and reports persistence errors.
func (m *Manager) CancelChecked(id string) (bool, error) {
	_, cancelled, err := m.CancelAndSnapshotChecked(id)
	return cancelled, err
}

// CancelAndSnapshotChecked cancels a task and returns the state at the point of
// cancellation, before concurrent dispatch completion can update its attempts.
func (m *Manager) CancelAndSnapshotChecked(id string) (*Task, bool, error) {
	t, ok := m.lockTaskForUpdate(id)
	if !ok {
		return nil, false, nil
	}
	defer m.unlockTaskForUpdate(t)
	if t.Status == StatusCompleted || t.Status == StatusFailed || t.Status == StatusCancelled {
		return nil, false, nil
	}
	t.cancel()
	t.Status = StatusCancelled
	t.Error = "task cancelled"
	hasCompleted := false
	for i := range t.Subtasks {
		switch t.Subtasks[i].Status {
		case StatusCompleted:
			hasCompleted = true
		case StatusPending, StatusRunning:
			t.Subtasks[i].Status = StatusCancelled
			t.Subtasks[i].Error = "task cancelled"
		}
	}
	t.Resumable = hasCompleted
	t.UpdatedAt = time.Now()
	signalChanged(t)
	_, persistErr := m.persistTaskAndCheckpointLocked(t)
	return cloneTask(t), true, persistErr
}

// StartSubtask marks a dispatched assignment as actively running.
func (m *Manager) StartSubtask(taskID, subtaskID string) {
	if err := m.StartSubtaskChecked(taskID, subtaskID); err != nil {
		logging.Errorf("taskmgr", "taskmgr: persist subtask start failed: %v", err)
	}
}

// StartSubtaskChecked marks a dispatched assignment as running.
func (m *Manager) StartSubtaskChecked(taskID, subtaskID string) error {
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
	}
	defer m.unlockTaskForUpdate(t)
	if t.Status == StatusCancelled {
		return nil
	}
	for i := range t.Subtasks {
		if t.Subtasks[i].Subtask.ID == subtaskID {
			t.Subtasks[i].Status = StatusRunning
			t.UpdatedAt = time.Now()
			signalChanged(t)
			if err := m.persistLocked(t); err != nil {
				return err
			}
			return nil
		}
	}
	return fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
}

// ReassignSubtaskChecked records a new assignment for pending or running work.
func (m *Manager) ReassignSubtaskChecked(taskID, subtaskID string, assignment scheduler.Assignment) error {
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	defer m.unlockTaskForUpdate(t)
	for i := range t.Subtasks {
		if t.Subtasks[i].Subtask.ID != subtaskID {
			continue
		}
		if t.Subtasks[i].Status != StatusRunning && t.Subtasks[i].Status != StatusPending {
			return fmt.Errorf("taskmgr: subtask %s in task %s is not pending or running", subtaskID, taskID)
		}
		assignment.Subtask = cloneSubtask(assignment.Subtask)
		t.Subtasks[i].Assignment = assignment
		t.UpdatedAt = time.Now()
		signalChanged(t)
		return m.persistLocked(t)
	}
	return fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
}

// CompleteSubtask records the outcome of a dispatched subtask and rolls the
// parent task's overall status up once every subtask has finished.
func (m *Manager) CompleteSubtask(taskID, subtaskID string, output string, taskErr error) {
	if err := m.CompleteSubtaskChecked(taskID, subtaskID, output, taskErr); err != nil {
		logging.Errorf("taskmgr", "taskmgr: persist subtask completion failed: %v", err)
	}
}

// CompleteSubtaskChecked records a dispatched outcome and reports persistence
// failures to the caller.
func (m *Manager) CompleteSubtaskChecked(taskID, subtaskID string, output string, taskErr error) error {
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	if t.Status == StatusCancelled {
		m.unlockTaskForUpdate(t)
		return nil
	}
	completion, err := m.completeSubtaskLocked(t, subtaskID, output, taskErr)
	if err != nil {
		m.unlockTaskForUpdate(t)
		return err
	}
	m.unlockTaskForUpdate(t)
	return m.finishSubtaskCompletion(taskID, completion)
}

type subtaskCompletion struct {
	finalize   Finalizer
	snapshot   *Task
	taskCtx    context.Context
	persistErr error
}

func (m *Manager) completeSubtaskLocked(t *Task, subtaskID, output string, taskErr error) (subtaskCompletion, error) {
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
		return subtaskCompletion{}, fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, t.ID)
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
			t.Resumable = true
		} else if t.RequireApproval {
			t.Status = StatusAwaitingApproval
			t.Resumable = true
		} else if finalize = m.finalizer; finalize != nil {
			t.Status = StatusSynthesis
			t.Resumable = true
			snapshot = cloneTask(t)
		} else {
			t.Status = StatusCompleted
			t.Resumable = false
		}
	} else {
		t.Resumable = true
	}
	t.UpdatedAt = time.Now()
	signalChanged(t)
	_, persistErr := m.persistTaskAndCheckpointLocked(t)
	return subtaskCompletion{
		finalize: finalize, snapshot: snapshot, taskCtx: t.ctx, persistErr: persistErr,
	}, nil
}

func (m *Manager) finishSubtaskCompletion(taskID string, completion subtaskCompletion) error {
	if completion.finalize == nil || completion.persistErr != nil {
		return completion.persistErr
	}
	return m.finishSynthesis(taskID, completion.taskCtx, completion.snapshot, completion.finalize, completion.persistErr)
}

// ApproveChecked releases a completed task's approval gate and starts
// synthesis using the configured finalizer.
func (m *Manager) ApproveChecked(taskID string) error {
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	if t.Status != StatusAwaitingApproval {
		m.unlockTaskForUpdate(t)
		return fmt.Errorf("taskmgr: task %s: %w", taskID, ErrNotAwaitingApproval)
	}
	finalize := m.finalizer
	if finalize == nil {
		t.Status = StatusCompleted
		t.Resumable = false
		t.UpdatedAt = time.Now()
		signalChanged(t)
		_, err := m.persistTaskAndCheckpointLocked(t)
		m.unlockTaskForUpdate(t)
		return err
	}
	t.Status = StatusSynthesis
	t.UpdatedAt = time.Now()
	signalChanged(t)
	taskCtx := t.ctx
	_, persistErr := m.persistTaskAndCheckpointLocked(t)
	snapshot := cloneTask(t)
	m.unlockTaskForUpdate(t)
	return m.finishSynthesis(taskID, taskCtx, snapshot, finalize, persistErr)
}

func (m *Manager) finishSynthesis(taskID string, taskCtx context.Context, snapshot *Task, finalize Finalizer, persistErr error) error {
	if finalize == nil || persistErr != nil {
		return persistErr
	}
	result, err := finalize(taskCtx, snapshot)
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok || t.Status != StatusSynthesis {
		if ok {
			m.unlockTaskForUpdate(t)
		}
		return nil
	}
	if err != nil {
		t.Status = StatusFailed
		t.Error = "result synthesis failed: " + err.Error()
		t.Resumable = true
	} else if strings.TrimSpace(result) == "" {
		t.Status = StatusFailed
		t.Error = "result synthesis failed: model returned an empty result"
		t.Resumable = true
	} else {
		t.Result = strings.TrimSpace(result)
		t.Status = StatusCompleted
		t.Resumable = false
	}
	t.UpdatedAt = time.Now()
	signalChanged(t)
	_, persistErr = m.persistTaskAndCheckpointLocked(t)
	m.unlockTaskForUpdate(t)
	return persistErr
}

func (m *Manager) persistTaskAndCheckpointLocked(task *Task) (*Checkpoint, error) {
	task.Origin = m.syncOrigin
	task.Sequence = m.nextSyncSequenceLocked(task.Origin)
	m.invalidateRecentTasksLocked()
	return m.saveTaskAndCheckpointLocked(task)
}

func (m *Manager) saveTaskAndCheckpointLocked(task *Task) (*Checkpoint, error) {
	if m.store == nil {
		return nil, nil
	}
	snapshot := cloneTask(task)
	checkpoint := checkpointForTask(snapshot)
	m.mu.Unlock()
	err := saveTaskAndCheckpoint(m.store, snapshot, checkpoint)
	m.mu.Lock()
	if err != nil {
		return nil, fmt.Errorf("taskmgr: persist task and checkpoint %s: %w", task.ID, err)
	}
	task.LastCheckpoint = snapshot.LastCheckpoint
	task.Resumable = snapshot.Resumable
	return checkpoint, nil
}

// persistLocked snapshots under the manager lock, then releases it for BoltDB
// I/O. Writers hold persistGate and the task's persistMu to preserve ordering.
func (m *Manager) persistLocked(task *Task) error {
	task.Origin = m.syncOrigin
	task.Sequence = m.nextSyncSequenceLocked(task.Origin)
	m.invalidateRecentTasksLocked()
	if m.store == nil {
		return nil
	}
	snapshot := cloneTask(task)
	m.mu.Unlock()
	err := m.store.Save(snapshot)
	m.mu.Lock()
	if err != nil {
		return fmt.Errorf("taskmgr: persist task %s: %w", task.ID, err)
	}
	return nil
}
