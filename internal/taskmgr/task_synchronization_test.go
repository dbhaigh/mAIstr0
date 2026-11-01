package taskmgr

import (
	"errors"
	"testing"

	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestTaskSynchronizationCompletesFailedDispatch(t *testing.T) {
	dispatchErr := errors.New("dispatch rejected")
	assignment := scheduler.Assignment{Subtask: scheduler.Subtask{ID: "subtask"}}
	completed := false
	failureReported := false
	synchronization := TaskSynchronization{
		Dispatch: func(taskID string, got scheduler.Assignment) error {
			if taskID != "task" || got.Subtask.ID != assignment.Subtask.ID {
				t.Fatalf("dispatch received task %q and subtask %q", taskID, got.Subtask.ID)
			}
			return dispatchErr
		},
		OnDispatchFailure: func(got scheduler.Assignment, err error) {
			failureReported = true
			if got.Subtask.ID != assignment.Subtask.ID || !errors.Is(err, dispatchErr) {
				t.Fatalf("failure callback received subtask %q and error %v", got.Subtask.ID, err)
			}
		},
		Complete: func(taskID, subtaskID, output string, err error) {
			if !failureReported {
				t.Fatal("completion ran before dispatch failure was reported")
			}
			completed = true
			if taskID != "task" || subtaskID != assignment.Subtask.ID || output != "" || !errors.Is(err, dispatchErr) {
				t.Fatalf("completion received task %q, subtask %q, output %q, error %v",
					taskID, subtaskID, output, err)
			}
		},
	}

	if err := synchronization.DispatchAssignment("task", assignment); !errors.Is(err, dispatchErr) {
		t.Fatalf("dispatch error = %v, want %v", err, dispatchErr)
	}
	if !completed {
		t.Fatal("dispatch failure did not complete the subtask")
	}
}

func TestTaskSynchronizationDoesNotCompleteSuccessfulDispatch(t *testing.T) {
	completed := false
	synchronization := TaskSynchronization{
		Dispatch:          func(string, scheduler.Assignment) error { return nil },
		OnDispatchFailure: func(scheduler.Assignment, error) { t.Fatal("reported failure for successful dispatch") },
		Complete:          func(string, string, string, error) { completed = true },
	}

	if err := synchronization.DispatchAssignment("task", scheduler.Assignment{}); err != nil {
		t.Fatalf("dispatch error = %v, want nil", err)
	}
	if completed {
		t.Fatal("successful dispatch completed the subtask")
	}
}
