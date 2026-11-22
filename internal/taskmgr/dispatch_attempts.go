package taskmgr

import (
	"errors"
	"fmt"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
)

// DispatchAttemptID returns the stable identifier stored for a dispatch attempt.
func DispatchAttemptID(taskID, subtaskID string, number int) string {
	return fmt.Sprintf("%s:%s:%d", taskID, subtaskID, number)
}

func DispatchRequestID(taskID, subtaskID string) string {
	return taskID + ":" + subtaskID
}

func (m *Manager) BeginDispatchAttemptChecked(taskID, subtaskID string, assignment scheduler.Assignment) (int, error) {
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return 0, fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	defer m.unlockTaskForUpdate(t)
	for i := range t.Subtasks {
		result := &t.Subtasks[i]
		if result.Subtask.ID != subtaskID {
			continue
		}
		if result.Status != StatusRunning {
			return 0, fmt.Errorf("taskmgr: subtask %s in task %s is not running", subtaskID, taskID)
		}
		if subtaskHasUnknownDispatchOutcome(result) {
			return 0, fmt.Errorf("taskmgr: subtask %s in task %s: %w", subtaskID, taskID, ErrOutcomeUnknown)
		}
		for i := range result.Attempts {
			if result.Attempts[i].Status == DispatchAttemptRunning {
				return 0, fmt.Errorf("taskmgr: subtask %s in task %s: %w", subtaskID, taskID, ErrDispatchInProgress)
			}
		}
		number := len(result.Attempts) + 1
		now := time.Now()
		result.Attempts = append(result.Attempts, DispatchAttempt{
			ID:        DispatchAttemptID(taskID, subtaskID, number),
			RequestID: DispatchRequestID(taskID, subtaskID),
			Number:    number, NodeID: assignment.NodeID, Model: assignment.Model,
			Engine: assignment.Engine, Status: DispatchAttemptRunning, StartedAt: now,
		})
		t.UpdatedAt = now
		signalChanged(t)
		if err := m.persistLocked(t); err != nil {
			return 0, err
		}
		return number, nil
	}
	return 0, fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
}

func (m *Manager) FinishDispatchAttemptChecked(taskID, subtaskID string, number int, status DispatchAttemptStatus, failureClass string, httpStatus int, attemptErr error, duration time.Duration) error {
	if status != DispatchAttemptSucceeded && status != DispatchAttemptFailed {
		return fmt.Errorf("taskmgr: invalid dispatch attempt status %q", status)
	}
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	defer m.unlockTaskForUpdate(t)
	for i := range t.Subtasks {
		result := &t.Subtasks[i]
		if result.Subtask.ID != subtaskID {
			continue
		}
		for j := range result.Attempts {
			attempt := &result.Attempts[j]
			if attempt.Number != number {
				continue
			}
			if attempt.Status == DispatchAttemptFailed &&
				attempt.FailureClass == DispatchFailureClassOutcomeUnknown {
				return nil
			}
			if attempt.Status != DispatchAttemptRunning {
				return fmt.Errorf("taskmgr: dispatch attempt %d for subtask %s is already finished", number, subtaskID)
			}
			attempt.Status = status
			attempt.FailureClass = failureClass
			attempt.HTTPStatus = httpStatus
			if attemptErr != nil {
				attempt.Error = attemptErr.Error()
			}
			finishedAt := time.Now()
			attempt.FinishedAt = &finishedAt
			attempt.DurationMs = duration.Milliseconds()
			t.UpdatedAt = finishedAt
			if failureClass == DispatchFailureClassOutcomeUnknown {
				completion, err := m.completeSubtaskLocked(t, subtaskID, "", ErrOutcomeUnknown)
				if err != nil {
					return err
				}
				return completion.persistErr
			}
			signalChanged(t)
			return m.persistLocked(t)
		}
		return fmt.Errorf("taskmgr: dispatch attempt %d for subtask %s not found", number, subtaskID)
	}
	return fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
}

// CompleteDispatchAttemptChecked atomically persists a successful attempt and
// its subtask output in one task snapshot.
func (m *Manager) CompleteDispatchAttemptChecked(
	taskID, subtaskID string,
	number int,
	failureClass string,
	httpStatus int,
	duration time.Duration,
	output string,
) (bool, error) {
	return m.CompleteDispatchAttemptWithAssignmentChecked(
		taskID, subtaskID, number, nil, failureClass, httpStatus, duration, output, nil,
	)
}

// CompleteDispatchAttemptWithAssignmentChecked atomically records the
// successful attempt and subtask result before invoking the winner callback.
func (m *Manager) CompleteDispatchAttemptWithAssignmentChecked(
	taskID, subtaskID string,
	number int,
	assignment *scheduler.Assignment,
	failureClass string,
	httpStatus int,
	duration time.Duration,
	output string,
	onWinner func(),
) (bool, error) {
	t, ok := m.lockTaskForUpdate(taskID)
	if !ok {
		return false, fmt.Errorf("taskmgr: task %s not found", taskID)
	}
	var attempt *DispatchAttempt
	var subtaskResult *SubtaskResult
	foundSubtask := false
	for i := range t.Subtasks {
		result := &t.Subtasks[i]
		if result.Subtask.ID != subtaskID {
			continue
		}
		subtaskResult = result
		foundSubtask = true
		for j := range result.Attempts {
			if result.Attempts[j].Number != number {
				continue
			}
			attempt = &result.Attempts[j]
			break
		}
		break
	}
	if !foundSubtask {
		m.unlockTaskForUpdate(t)
		return false, fmt.Errorf("taskmgr: subtask %s in task %s not found", subtaskID, taskID)
	}
	if attempt == nil {
		m.unlockTaskForUpdate(t)
		return false, fmt.Errorf("taskmgr: dispatch attempt %d for subtask %s not found", number, subtaskID)
	}
	if attempt.Status != DispatchAttemptRunning {
		if attempt.Status == DispatchAttemptFailed &&
			attempt.FailureClass == DispatchFailureClassOutcomeUnknown {
			m.unlockTaskForUpdate(t)
			return false, nil
		}
		m.unlockTaskForUpdate(t)
		return false, fmt.Errorf("taskmgr: dispatch attempt %d for subtask %s is already finished", number, subtaskID)
	}
	finishedAt := time.Now()
	if t.Status == StatusCancelled || subtaskResult.Status != StatusRunning {
		markDispatchAttemptOutcomeUnknown(attempt, finishedAt)
		t.Error = ErrOutcomeUnknown.Error()
		t.Resumable = false
		if subtaskResult.Status != StatusCompleted {
			subtaskResult.Status = StatusFailed
			subtaskResult.Error = ErrOutcomeUnknown.Error()
		}
		signalChanged(t)
		err := m.persistLocked(t)
		m.unlockTaskForUpdate(t)
		return false, err
	}
	attempt.Status = DispatchAttemptSucceeded
	attempt.FailureClass = failureClass
	attempt.HTTPStatus = httpStatus
	attempt.FinishedAt = &finishedAt
	attempt.DurationMs = duration.Milliseconds()
	t.UpdatedAt = finishedAt
	if assignment != nil {
		assignment.Subtask = cloneSubtask(assignment.Subtask)
		subtaskResult.Assignment = *assignment
	}

	completion, err := m.completeSubtaskLocked(t, subtaskID, output, nil)
	if err != nil {
		m.unlockTaskForUpdate(t)
		return false, err
	}
	if completion.persistErr != nil {
		commitErr := completion.persistErr
		attempt.Status = DispatchAttemptRunning
		markDispatchAttemptOutcomeUnknown(attempt, time.Now())
		subtaskResult.Output = ""
		recovery, recoveryErr := m.completeSubtaskLocked(t, subtaskID, "", ErrOutcomeUnknown)
		if recoveryErr != nil {
			m.unlockTaskForUpdate(t)
			return false, errors.Join(
				fmt.Errorf("%w: completed dispatch result was not committed: %v", ErrOutcomeUnknown, commitErr),
				fmt.Errorf("record unknown dispatch outcome: %w", recoveryErr),
			)
		}
		m.unlockTaskForUpdate(t)
		unknownErr := fmt.Errorf("%w: completed dispatch result was not committed: %v", ErrOutcomeUnknown, commitErr)
		if recovery.persistErr != nil {
			return false, errors.Join(unknownErr, fmt.Errorf("persist unknown dispatch outcome: %w", recovery.persistErr))
		}
		return false, unknownErr
	}
	m.unlockTaskForUpdate(t)
	if onWinner != nil {
		onWinner()
	}
	return true, m.finishSubtaskCompletion(taskID, completion)
}
