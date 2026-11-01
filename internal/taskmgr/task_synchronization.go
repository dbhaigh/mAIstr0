package taskmgr

import (
	"errors"

	"github.com/maistr0/maistr0/internal/scheduler"
)

// TaskSynchronization binds side-specific dispatch and completion behavior.
type TaskSynchronization struct {
	Dispatch          func(string, scheduler.Assignment) error
	OnDispatchFailure func(scheduler.Assignment, error)
	Complete          func(string, string, string, error)
}

// DispatchAssignment completes a subtask with the admission error if dispatch fails.
func (s TaskSynchronization) DispatchAssignment(taskID string, assignment scheduler.Assignment) error {
	if s.Dispatch == nil || s.Complete == nil {
		return errors.New("task synchronization requires dispatch and complete callbacks")
	}
	err := s.Dispatch(taskID, assignment)
	if err != nil {
		if s.OnDispatchFailure != nil {
			s.OnDispatchFailure(assignment, err)
		}
		s.Complete(taskID, assignment.Subtask.ID, "", err)
	}
	return err
}
