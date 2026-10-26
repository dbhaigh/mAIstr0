package taskmgr

import (
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestDispatchAttemptsPersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatch-attempts.db")
	manager, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	task, err := manager.CreateChecked("run inference", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", TaskType: "general"},
		NodeID:  "node-a", Model: "model-a", Engine: "ollama",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	number, err := manager.BeginDispatchAttemptChecked(task.ID, "work", task.Subtasks[0].Assignment)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.FinishDispatchAttemptChecked(task.ID, "work", number, DispatchAttemptFailed, "upstream_5xx", 503, nil, 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(reopened)
	persisted, ok := reopened.Get(task.ID)
	if !ok || len(persisted.Subtasks[0].Attempts) != 1 {
		t.Fatalf("persisted task attempts = %#v", persisted)
	}
	attempt := persisted.Subtasks[0].Attempts[0]
	if attempt.ID == "" || attempt.RequestID != DispatchRequestID(task.ID, "work") ||
		attempt.NodeID != "node-a" || attempt.Model != "model-a" ||
		attempt.Engine != "ollama" || attempt.Status != DispatchAttemptFailed ||
		attempt.FailureClass != "upstream_5xx" || attempt.HTTPStatus != 503 || attempt.DurationMs != 25 {
		t.Fatalf("persisted attempt metadata = %+v", attempt)
	}
}

func TestSuccessfulAttemptAndSubtaskOutputPersistTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatch-result.db")
	manager, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	task, err := manager.CreateChecked("persist result", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", TaskType: "general"},
		NodeID:  "node-a", Model: "model-a",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	number, err := manager.BeginDispatchAttemptChecked(task.ID, "work", task.Subtasks[0].Assignment)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := manager.CompleteDispatchAttemptChecked(
		task.ID, "work", number, "none", http.StatusOK, 10*time.Millisecond, "worker output",
	)
	if err != nil || !completed {
		t.Fatalf("complete successful attempt = %t, error %v", completed, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(reopened)
	restored, ok := reopened.Get(task.ID)
	if !ok || restored.Status != StatusCompleted || restored.Subtasks[0].Output != "worker output" ||
		len(restored.Subtasks[0].Attempts) != 1 ||
		restored.Subtasks[0].Attempts[0].Status != DispatchAttemptSucceeded ||
		restored.Subtasks[0].Attempts[0].RequestID != DispatchRequestID(task.ID, "work") {
		t.Fatalf("successful dispatch result was not durably restored: %+v", restored)
	}
}

func TestConcurrentSuccessfulAttemptsChooseOneWinner(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Close() })
	primary := scheduler.Assignment{
		Subtask: scheduler.Subtask{ID: "work", TaskType: "general"},
		NodeID:  "node-a", Model: "model-a",
	}
	hedge := primary
	hedge.NodeID = "node-b"
	hedge.Model = "model-b"
	task, err := manager.CreateChecked("hedged inference", []scheduler.Assignment{primary})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	attempts := make([]int, 2)
	for i, assignment := range []scheduler.Assignment{primary, hedge} {
		attempts[i], err = manager.BeginDispatchAttemptChecked(task.ID, "work", assignment)
		if err != nil {
			t.Fatal(err)
		}
	}

	var callbackCalls atomic.Int32
	results := make(chan bool, len(attempts))
	errorsFound := make(chan error, len(attempts))
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	done.Add(len(attempts))
	for i, assignment := range []scheduler.Assignment{primary, hedge} {
		go func(attempt int, candidate scheduler.Assignment) {
			defer done.Done()
			start.Wait()
			won, completeErr := manager.CompleteDispatchAttemptWithAssignmentChecked(
				task.ID, "work", attempts[attempt], &candidate, "none",
				http.StatusOK, 10*time.Millisecond, candidate.NodeID+" output",
				func() { callbackCalls.Add(1) },
			)
			results <- won
			errorsFound <- completeErr
		}(i, assignment)
	}
	start.Done()
	done.Wait()
	close(results)
	close(errorsFound)
	winnerCount := 0
	for won := range results {
		if won {
			winnerCount++
		}
	}
	for completeErr := range errorsFound {
		if completeErr != nil {
			t.Fatalf("complete concurrent attempt: %v", completeErr)
		}
	}
	if winnerCount != 1 || callbackCalls.Load() != 1 {
		t.Fatalf("winner count/callbacks = %d/%d, want 1/1", winnerCount, callbackCalls.Load())
	}
	completed, ok := manager.Get(task.ID)
	if !ok || completed.Status != StatusCompleted || len(completed.Subtasks[0].Attempts) != 2 {
		t.Fatalf("completed task = %+v", completed)
	}
	if completed.Subtasks[0].Output != completed.Subtasks[0].Assignment.NodeID+" output" {
		t.Fatalf("subtask output %q does not match winning assignment %+v",
			completed.Subtasks[0].Output, completed.Subtasks[0].Assignment)
	}
}

func TestRestartClosesRunningAttemptAsIndeterminate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatch-interrupted.db")
	manager, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	task, err := manager.CreateChecked("interrupted dispatch", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", TaskType: "general"},
		NodeID:  "node-a", Model: "model-a",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginDispatchAttemptChecked(task.ID, "work", task.Subtasks[0].Assignment); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(reopened)
	restored, ok := reopened.Get(task.ID)
	if !ok || restored.Status != StatusFailed || len(restored.Subtasks[0].Attempts) != 1 {
		t.Fatalf("interrupted dispatch was not restored: %+v", restored)
	}
	attempt := restored.Subtasks[0].Attempts[0]
	if attempt.Status != DispatchAttemptFailed || attempt.FailureClass != "orchestrator_restart" ||
		attempt.RequestID != DispatchRequestID(task.ID, "work") || attempt.FinishedAt == nil {
		t.Fatalf("running attempt was not closed with its uncertain outcome: %+v", attempt)
	}
}
