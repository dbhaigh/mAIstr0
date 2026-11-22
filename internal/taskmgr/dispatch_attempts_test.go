package taskmgr

import (
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
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

func TestSingleActiveAttemptRejectsConcurrentHedge(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Close() })
	primary := scheduler.Assignment{
		Subtask: scheduler.Subtask{ID: "work", TaskType: "general"},
		NodeID:  "node-a", Model: "model-a",
	}
	hedge := primary
	hedge.NodeID = "node-b"
	hedge.Model = "model-b"
	task, err := manager.CreateChecked("single active dispatch", []scheduler.Assignment{primary})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	number, err := manager.BeginDispatchAttemptChecked(task.ID, "work", primary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginDispatchAttemptChecked(task.ID, "work", hedge); !errors.Is(err, ErrDispatchInProgress) {
		t.Fatalf("concurrent hedge admission = %v, want ErrDispatchInProgress", err)
	}
	won, err := manager.CompleteDispatchAttemptWithAssignmentChecked(
		task.ID, "work", number, &primary, "none", http.StatusOK,
		10*time.Millisecond, "primary output", nil,
	)
	if err != nil || !won {
		t.Fatalf("complete primary attempt = won %t, error %v", won, err)
	}
	completed, ok := manager.Get(task.ID)
	if !ok || completed.Status != StatusCompleted || len(completed.Subtasks[0].Attempts) != 1 {
		t.Fatalf("completed task = %+v", completed)
	}
	if completed.Subtasks[0].Output != "primary output" || completed.Subtasks[0].Assignment.NodeID != "node-a" {
		t.Fatalf("subtask result = %+v", completed.Subtasks[0])
	}
}

func TestUnknownAttemptFinishImmediatelyBlocksResume(t *testing.T) {
	manager := NewManager()
	task := manager.Create("unknown outcome", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work"}, NodeID: "node-a", Model: "model-a",
	}})
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	number, err := manager.BeginDispatchAttemptChecked(task.ID, "work", task.Subtasks[0].Assignment)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.FinishDispatchAttemptChecked(
		task.ID, "work", number, DispatchAttemptFailed, DispatchFailureClassOutcomeUnknown,
		0, ErrOutcomeUnknown, time.Millisecond,
	); err != nil {
		t.Fatal(err)
	}
	current, ok := manager.Get(task.ID)
	if !ok || current.Status != StatusFailed || current.Resumable ||
		current.Subtasks[0].Status != StatusFailed || current.Error != ErrOutcomeUnknown.Error() {
		t.Fatalf("finished unknown attempt state = %+v", current)
	}
	if _, err := manager.ResumeTaskChecked(task.ID); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("resume after unknown attempt = %v, want ErrOutcomeUnknown", err)
	}
}

type failOnceTaskStateStore struct {
	Store
	mu       sync.Mutex
	failNext bool
	err      error
}

func (s *failOnceTaskStateStore) SaveTaskAndCheckpoint(task *Task, checkpoint *Checkpoint) error {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	return saveTaskAndCheckpoint(s.Store, task, checkpoint)
}

func TestUncommittedSuccessfulDispatchBecomesUnknown(t *testing.T) {
	persistErr := errors.New("simulated task-store failure")
	store := &failOnceTaskStateStore{Store: NewMemoryStore(), err: persistErr}
	manager, err := NewManagerWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(manager)

	assignment := scheduler.Assignment{
		Subtask: scheduler.Subtask{ID: "work"}, NodeID: "node-a", Model: "model-a",
	}
	task, err := manager.CreateChecked("uncommitted result", []scheduler.Assignment{assignment})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	number, err := manager.BeginDispatchAttemptChecked(task.ID, "work", assignment)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.failNext = true
	store.mu.Unlock()

	won, err := manager.CompleteDispatchAttemptChecked(
		task.ID, "work", number, "none", http.StatusOK, time.Millisecond, "uncommitted output",
	)
	if won || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("completion after persistence failure = won %t, error %v; want unknown outcome", won, err)
	}
	current, ok := manager.Get(task.ID)
	if !ok || current.Status != StatusFailed || current.Resumable ||
		current.Subtasks[0].Output != "" ||
		current.Subtasks[0].Attempts[0].FailureClass != DispatchFailureClassOutcomeUnknown {
		t.Fatalf("in-memory failed commit state = %+v", current)
	}

	reopened, err := NewManagerWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(reopened)
	persisted, ok := reopened.Get(task.ID)
	if !ok || persisted.Status != StatusFailed || persisted.Resumable ||
		!persisted.HasUnknownDispatchOutcome() {
		t.Fatalf("persisted failed commit state = %+v", persisted)
	}
}

func TestRestartClosesRunningAttemptAsOutcomeUnknown(t *testing.T) {
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
	if attempt.Status != DispatchAttemptFailed || attempt.FailureClass != DispatchFailureClassOutcomeUnknown ||
		attempt.Error != ErrOutcomeUnknown.Error() ||
		attempt.RequestID != DispatchRequestID(task.ID, "work") || attempt.FinishedAt == nil ||
		restored.Resumable || !strings.Contains(restored.Error, "reconciliation required") {
		t.Fatalf("running attempt was not closed with its uncertain outcome: %+v", attempt)
	}
	if _, err := reopened.ResumeTaskChecked(task.ID); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("resume after restart = %v, want ErrOutcomeUnknown", err)
	}
}
