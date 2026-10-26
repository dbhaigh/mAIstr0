package taskmgr

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
)

func singleSubtask(id string) []scheduler.Assignment {
	return []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: id, Description: "Work " + id},
		NodeID:  "node-a", Address: "http://node-a", Model: "model-a",
	}}
}

// taskInState drives a fresh single-subtask task through public APIs into the
// requested state.
func taskInState(t *testing.T, manager *Manager, status Status) *Task {
	t.Helper()
	task, err := manager.CreateWithOptionsChecked("state "+string(status), singleSubtask("work"), status == StatusAwaitingApproval)
	if err != nil {
		t.Fatal(err)
	}
	switch status {
	case StatusRunning:
		if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
			t.Fatal(err)
		}
	case StatusCompleted, StatusAwaitingApproval:
		if err := manager.CompleteSubtaskChecked(task.ID, "work", "work output", nil); err != nil {
			t.Fatal(err)
		}
	case StatusFailed:
		if err := manager.CompleteSubtaskChecked(task.ID, "work", "", errors.New("worker failed")); err != nil {
			t.Fatal(err)
		}
	case StatusCancelled:
		if cancelled, err := manager.CancelChecked(task.ID); err != nil || !cancelled {
			t.Fatalf("cancel = %t, %v", cancelled, err)
		}
	default:
		t.Fatalf("unsupported state %q", status)
	}
	current, ok := manager.Get(task.ID)
	if !ok || current.Status != status {
		t.Fatalf("task state = %+v, want %s", current, status)
	}
	return current
}

type taskOutcome struct {
	status        Status
	subtaskStatus Status
	output        string
	subtaskError  string
	result        string
	resumable     bool
}

func outcomeOf(t *testing.T, manager *Manager, id string) taskOutcome {
	t.Helper()
	task, ok := manager.Get(id)
	if !ok {
		t.Fatalf("task %s not found", id)
	}
	return taskOutcome{
		status: task.Status, subtaskStatus: task.Subtasks[0].Status, output: task.Subtasks[0].Output,
		subtaskError: task.Subtasks[0].Error, result: task.Result, resumable: task.Resumable,
	}
}

func TestApproveRejectsTasksNotAwaitingApproval(t *testing.T) {
	for _, status := range []Status{StatusRunning, StatusCompleted, StatusFailed, StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			var finalizerCalls int
			manager := NewManager()
			task := taskInState(t, manager, status)
			manager.SetFinalizer(func(context.Context, *Task) (string, error) {
				finalizerCalls++
				return "synthesized", nil
			})
			before := outcomeOf(t, manager, task.ID)

			err := manager.ApproveChecked(task.ID)
			if !errors.Is(err, ErrNotAwaitingApproval) {
				t.Fatalf("approve %s task error = %v, want ErrNotAwaitingApproval", status, err)
			}
			if after := outcomeOf(t, manager, task.ID); after != before || finalizerCalls != 0 {
				t.Fatalf("rejected approval changed task: before %+v after %+v, finalizer calls %d", before, after, finalizerCalls)
			}
		})
	}
}

func TestApproveIsAcceptedOnlyOnceUnderConcurrency(t *testing.T) {
	release := make(chan struct{})
	var finalizerCalls int
	var finalizerMu sync.Mutex
	manager := NewManagerWithFinalizer(func(context.Context, *Task) (string, error) {
		finalizerMu.Lock()
		finalizerCalls++
		finalizerMu.Unlock()
		<-release
		return "approved result", nil
	})
	task := taskInState(t, manager, StatusAwaitingApproval)

	const callers = 8
	results := make(chan error, callers)
	var start sync.WaitGroup
	start.Add(1)
	for range callers {
		go func() {
			start.Wait()
			results <- manager.ApproveChecked(task.ID)
		}()
	}
	start.Done()
	// Every caller but the winner is rejected while the winner's synthesis is
	// still blocked, so rejections arrive before the release.
	for range callers - 1 {
		if err := <-results; !errors.Is(err, ErrNotAwaitingApproval) {
			t.Fatalf("concurrent approval error = %v, want ErrNotAwaitingApproval", err)
		}
	}
	close(release)
	if err := <-results; err != nil {
		t.Fatalf("winning approval failed: %v", err)
	}
	finalizerMu.Lock()
	defer finalizerMu.Unlock()
	if got := outcomeOf(t, manager, task.ID); got.status != StatusCompleted || got.result != "approved result" || finalizerCalls != 1 {
		t.Fatalf("approved task = %+v, finalizer calls %d; want one synthesis", got, finalizerCalls)
	}
}

func TestTerminalTasksIgnoreLateSubtaskUpdates(t *testing.T) {
	for _, status := range []Status{StatusCompleted, StatusFailed, StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			manager := NewManager()
			task := taskInState(t, manager, status)
			before := outcomeOf(t, manager, task.ID)

			if err := manager.CompleteSubtaskChecked(task.ID, "work", "", errors.New("late failure")); err != nil {
				t.Fatalf("late failure returned %v", err)
			}
			if err := manager.CompleteSubtaskChecked(task.ID, "work", "late output", nil); err != nil {
				t.Fatalf("late success returned %v", err)
			}
			startErr := manager.StartSubtaskChecked(task.ID, "work")
			if status == StatusCancelled {
				if startErr != nil {
					t.Fatalf("start after cancellation returned %v, want ignored", startErr)
				}
			} else if !errors.Is(startErr, ErrSubtaskFinished) {
				t.Fatalf("start of finished subtask returned %v, want ErrSubtaskFinished", startErr)
			}
			if cancelled, err := manager.CancelChecked(task.ID); cancelled || err != nil {
				t.Fatalf("cancel of terminal task = %t, %v; want rejected", cancelled, err)
			}
			if _, err := manager.BeginDispatchAttemptChecked(task.ID, "work", task.Subtasks[0].Assignment); err == nil {
				t.Fatal("dispatch attempt began on a terminal subtask")
			}
			if after := outcomeOf(t, manager, task.ID); after != before {
				t.Fatalf("terminal %s task changed: before %+v after %+v", status, before, after)
			}
		})
	}
}

func TestResumeIsLimitedToStoppedTasks(t *testing.T) {
	for _, test := range []struct {
		status  Status
		resumes bool
	}{
		{StatusRunning, false},
		{StatusAwaitingApproval, false},
		{StatusCompleted, false},
		{StatusFailed, true},
		// Cancellation before any subtask completed leaves nothing to resume.
		{StatusCancelled, false},
	} {
		t.Run(string(test.status), func(t *testing.T) {
			manager := NewManager()
			task := taskInState(t, manager, test.status)
			before := outcomeOf(t, manager, task.ID)
			ctx, ok := manager.Context(task.ID)
			if !ok {
				t.Fatal("task context missing")
			}
			liveBefore := ctx.Err() == nil

			resumed, err := manager.ResumeTaskChecked(task.ID)
			if !test.resumes {
				if !errors.Is(err, ErrNotResumable) || resumed != nil {
					t.Fatalf("resume %s = %+v, %v; want ErrNotResumable", test.status, resumed, err)
				}
				if after := outcomeOf(t, manager, task.ID); after != before {
					t.Fatalf("rejected resume changed task: before %+v after %+v", before, after)
				}
				if current, _ := manager.Context(task.ID); current != ctx || (ctx.Err() == nil) != liveBefore {
					t.Fatal("rejected resume replaced or cancelled the task context")
				}
				return
			}
			if err != nil {
				t.Fatalf("resume %s failed: %v", test.status, err)
			}
			if resumed.Status != StatusRunning || resumed.Subtasks[0].Status != StatusPending || resumed.Subtasks[0].Error != "" {
				t.Fatalf("resumed task = %+v", resumed)
			}
			if ctx.Err() == nil {
				t.Fatal("resume left the superseded task context live")
			}
			current, _ := manager.Context(task.ID)
			if current.Err() != nil {
				t.Fatal("resumed task context is already cancelled")
			}
			if cancelled, err := manager.CancelChecked(task.ID); !cancelled || err != nil {
				t.Fatalf("cancel resumed task = %t, %v", cancelled, err)
			}
			if current.Err() == nil {
				t.Fatal("cancel did not stop the resumed task context")
			}
		})
	}
}

func TestConcurrentResumeAdmitsExactlyOneCaller(t *testing.T) {
	manager := NewManager()
	task := taskInState(t, manager, StatusFailed)

	const callers = 8
	type result struct {
		task *Task
		err  error
	}
	results := make(chan result, callers)
	var start sync.WaitGroup
	start.Add(1)
	for range callers {
		go func() {
			start.Wait()
			resumed, err := manager.ResumeTaskChecked(task.ID)
			results <- result{resumed, err}
		}()
	}
	start.Done()
	var admitted int
	for range callers {
		got := <-results
		switch {
		case got.err == nil:
			admitted++
		case !errors.Is(got.err, ErrNotResumable):
			t.Fatalf("concurrent resume error = %v", got.err)
		}
	}
	if admitted != 1 {
		t.Fatalf("concurrent resumes admitted = %d, want exactly one", admitted)
	}
}

func TestFailedPrerequisiteReleasesDependentsWithoutInput(t *testing.T) {
	manager := NewManager()
	task := manager.Create("chain", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "root", Description: "Root"}},
		{Subtask: scheduler.Subtask{ID: "middle", Description: "Middle", DependsOn: []string{"root"}}},
		{Subtask: scheduler.Subtask{ID: "leaf", Description: "Leaf", DependsOn: []string{"middle"}}},
	})
	ctx, _ := manager.Context(task.ID)
	middleReady := make(chan bool, 1)
	leafReady := make(chan bool, 1)
	go func() { middleReady <- manager.WaitForDependencies(ctx, task.ID, "middle") }()
	go func() { leafReady <- manager.WaitForDependencies(ctx, task.ID, "leaf") }()

	if err := manager.StartSubtaskChecked(task.ID, "root"); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.InputForSubtask(task.ID, "middle"); ok {
		t.Fatal("dependent input was available while its prerequisite was running")
	}
	if err := manager.CompleteSubtaskChecked(task.ID, "root", "", errors.New("root failed")); err != nil {
		t.Fatal(err)
	}
	if ready := receiveWithin(t, middleReady, "middle dependency wait"); ready {
		t.Fatal("dependent became ready after its prerequisite failed")
	}
	// The dispatcher records the dependency failure; that must cascade.
	if err := manager.CompleteSubtaskChecked(task.ID, "middle", "", errors.New("a required subtask did not complete")); err != nil {
		t.Fatal(err)
	}
	if ready := receiveWithin(t, leafReady, "leaf dependency wait"); ready {
		t.Fatal("transitive dependent became ready after the chain failed")
	}
	if _, ok := manager.InputForSubtask(task.ID, "leaf"); ok {
		t.Fatal("transitive dependent received input from a failed chain")
	}
	if err := manager.CompleteSubtaskChecked(task.ID, "leaf", "", errors.New("a required subtask did not complete")); err != nil {
		t.Fatal(err)
	}
	final, _ := manager.Get(task.ID)
	if final.Status != StatusFailed || !final.Resumable {
		t.Fatalf("failed chain task = %s resumable=%t, want resumable failure", final.Status, final.Resumable)
	}
}

func TestDependentWaitsForEveryPrerequisiteAndReceivesOrderedOutputs(t *testing.T) {
	manager := NewManager()
	task := manager.Create("diamond", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "left", Description: "Left"}},
		{Subtask: scheduler.Subtask{ID: "right", Description: "Right"}},
		{Subtask: scheduler.Subtask{ID: "join", Description: "Join", DependsOn: []string{"right", "left"}}},
	})
	ctx, _ := manager.Context(task.ID)
	ready := make(chan bool, 1)
	go func() { ready <- manager.WaitForDependencies(ctx, task.ID, "join") }()

	if err := manager.CompleteSubtaskChecked(task.ID, "left", "left output", nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.InputForSubtask(task.ID, "join"); ok {
		t.Fatal("join input was available with one prerequisite outstanding")
	}
	select {
	case got := <-ready:
		t.Fatalf("join wait returned %t with one prerequisite outstanding", got)
	default:
	}
	if err := manager.CompleteSubtaskChecked(task.ID, "right", "right output", nil); err != nil {
		t.Fatal(err)
	}
	if !receiveWithin(t, ready, "join dependency wait") {
		t.Fatal("join did not become ready after all prerequisites completed")
	}
	input, ok := manager.InputForSubtask(task.ID, "join")
	if !ok {
		t.Fatal("join input unavailable after prerequisites completed")
	}
	right := strings.Index(input, "Prerequisite right:\nright output")
	left := strings.Index(input, "Prerequisite left:\nleft output")
	if right < 0 || left < 0 || right > left {
		t.Fatalf("join input does not list prerequisites in DependsOn order: %q", input)
	}
}

func TestRestartRecoveryThenResumeContinuesAttemptHistory(t *testing.T) {
	store := NewMemoryStore()
	manager, err := NewManagerWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	task, err := manager.CreateChecked("restart then resume", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "done", Description: "Done"}, NodeID: "node-a", Model: "model-a"},
		{Subtask: scheduler.Subtask{ID: "inflight", Description: "In flight", DependsOn: []string{"done"}}, NodeID: "node-a", Model: "model-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.CompleteSubtaskChecked(task.ID, "done", "kept output", nil); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(task.ID, "inflight"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginDispatchAttemptChecked(task.ID, "inflight", task.Subtasks[1].Assignment); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewManagerWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	recovered, _ := restarted.Get(task.ID)
	if recovered.Status != StatusFailed || !recovered.Resumable || recovered.Error != interruptedTaskError {
		t.Fatalf("recovered task = %s resumable=%t error=%q", recovered.Status, recovered.Resumable, recovered.Error)
	}
	if ctx, _ := restarted.Context(task.ID); ctx.Err() == nil {
		t.Fatal("recovered interrupted task kept a live context")
	}
	if recovered.Subtasks[0].Status != StatusCompleted || recovered.Subtasks[1].Status != StatusFailed {
		t.Fatalf("recovered subtasks = %+v", recovered.Subtasks)
	}
	if attempts := recovered.Subtasks[1].Attempts; len(attempts) != 1 ||
		attempts[0].Status != DispatchAttemptFailed || attempts[0].FailureClass != "orchestrator_restart" ||
		attempts[0].FinishedAt == nil {
		t.Fatalf("interrupted attempt = %+v, want indeterminate restart failure", attempts)
	}

	resumed, err := restarted.ResumeTaskChecked(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Subtasks[0].Status != StatusCompleted || resumed.Subtasks[0].Output != "kept output" ||
		resumed.Subtasks[1].Status != StatusPending || len(resumed.Subtasks[1].Attempts) != 1 {
		t.Fatalf("resumed subtasks = %+v", resumed.Subtasks)
	}
	if err := restarted.StartSubtaskChecked(task.ID, "inflight"); err != nil {
		t.Fatal(err)
	}
	number, err := restarted.BeginDispatchAttemptChecked(task.ID, "inflight", resumed.Subtasks[1].Assignment)
	if err != nil {
		t.Fatal(err)
	}
	if number != 2 {
		t.Fatalf("resumed attempt number = %d, want 2 after the interrupted attempt", number)
	}
	won, err := restarted.CompleteDispatchAttemptChecked(task.ID, "inflight", number, "none", 200, time.Millisecond, "resumed output")
	if err != nil || !won {
		t.Fatalf("complete resumed attempt = %t, %v", won, err)
	}

	reopened, err := NewManagerWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	final, _ := reopened.Get(task.ID)
	if final.Status != StatusCompleted || final.Resumable || final.Subtasks[1].Output != "resumed output" {
		t.Fatalf("persisted resumed task = %+v", final)
	}
	attempts := final.Subtasks[1].Attempts
	if len(attempts) != 2 || attempts[0].FailureClass != "orchestrator_restart" ||
		attempts[1].Status != DispatchAttemptSucceeded ||
		attempts[1].ID != DispatchAttemptID(task.ID, "inflight", 2) ||
		attempts[0].RequestID != attempts[1].RequestID {
		t.Fatalf("persisted attempt history = %+v", attempts)
	}
}

func TestFinishedDispatchAttemptCannotBeRecordedTwice(t *testing.T) {
	manager := NewManager()
	task := manager.Create("attempt", singleSubtask("work"))
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}
	number, err := manager.BeginDispatchAttemptChecked(task.ID, "work", task.Subtasks[0].Assignment)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.FinishDispatchAttemptChecked(task.ID, "work", number, DispatchAttemptFailed, "upstream_5xx", 503, errors.New("busy"), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	for name, record := range map[string]func() error{
		"finish": func() error {
			return manager.FinishDispatchAttemptChecked(task.ID, "work", number, DispatchAttemptSucceeded, "none", 200, nil, time.Millisecond)
		},
		"complete": func() error {
			_, err := manager.CompleteDispatchAttemptChecked(task.ID, "work", number, "none", 200, time.Millisecond, "late output")
			return err
		},
		"invalid status": func() error {
			return manager.FinishDispatchAttemptChecked(task.ID, "work", number, DispatchAttemptRunning, "", 0, nil, 0)
		},
	} {
		if err := record(); err == nil {
			t.Errorf("%s of a finished attempt succeeded", name)
		}
	}
	current, _ := manager.Get(task.ID)
	attempt := current.Subtasks[0].Attempts[0]
	if attempt.Status != DispatchAttemptFailed || attempt.FailureClass != "upstream_5xx" ||
		current.Subtasks[0].Status != StatusRunning || current.Subtasks[0].Output != "" {
		t.Fatalf("finished attempt was overwritten: attempt %+v subtask %+v", attempt, current.Subtasks[0])
	}
}

func receiveWithin(t *testing.T, ch <-chan bool, what string) bool {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return", what)
		return false
	}
}
