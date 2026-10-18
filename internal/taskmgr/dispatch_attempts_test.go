package taskmgr

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

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
	defer reopened.Close()
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
	defer reopened.Close()
	restored, ok := reopened.Get(task.ID)
	if !ok || restored.Status != StatusCompleted || restored.Subtasks[0].Output != "worker output" ||
		len(restored.Subtasks[0].Attempts) != 1 ||
		restored.Subtasks[0].Attempts[0].Status != DispatchAttemptSucceeded ||
		restored.Subtasks[0].Attempts[0].RequestID != DispatchRequestID(task.ID, "work") {
		t.Fatalf("successful dispatch result was not durably restored: %+v", restored)
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
	defer reopened.Close()
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
