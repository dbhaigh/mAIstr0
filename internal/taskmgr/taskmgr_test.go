package taskmgr

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestTaskSnapshotsAndAggregateResult(t *testing.T) {
	manager := NewManager()
	created := manager.Create("Summarize two things", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "a", Description: "First", Tags: []string{"summarize"}}},
		{Subtask: scheduler.Subtask{ID: "b", Description: "Second"}},
	})
	created.Subtasks[0].Output = "caller mutation"
	created.Subtasks[0].Subtask.Tags[0] = "caller mutation"
	manager.CompleteSubtask(created.ID, "a", "First output", nil)
	manager.CompleteSubtask(created.ID, "b", "Second output", nil)

	task, ok := manager.Get(created.ID)
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed", task.Status)
	}
	if task.Result != "## First\nFirst output\n\n## Second\nSecond output" {
		t.Fatalf("unexpected aggregate result: %q", task.Result)
	}
	if task.Subtasks[0].Subtask.Tags[0] != "summarize" {
		t.Fatalf("snapshot was changed by caller: %+v", task.Subtasks[0].Subtask.Tags)
	}
	task.Subtasks[0].Output = "read mutation"
	fresh, _ := manager.Get(created.ID)
	if fresh.Subtasks[0].Output != "First output" {
		t.Fatalf("task manager leaked mutable task pointer: %q", fresh.Subtasks[0].Output)
	}
}

func TestRecentReturnsNewestBoundedSnapshot(t *testing.T) {
	manager := NewManager()
	first := manager.Create("first", []scheduler.Assignment{{Subtask: scheduler.Subtask{ID: "a"}}})
	second := manager.Create("second", []scheduler.Assignment{{Subtask: scheduler.Subtask{ID: "b"}}})

	recent := manager.Recent(1)
	if len(recent) != 1 || recent[0].ID != second.ID {
		t.Fatalf("Recent(1) = %#v, want newest task %q", recent, second.ID)
	}
	recent[0].Description = "caller mutation"
	stored, ok := manager.Get(second.ID)
	if !ok || stored.Description != "second" {
		t.Fatalf("recent task exposed mutable manager state: %#v", stored)
	}
	if got := manager.Recent(0); len(got) != 0 {
		t.Fatalf("Recent(0) = %#v, want empty result (first task %q)", got, first.ID)
	}
}

func TestEmptyTaskIsFailedAndCancellationStopsWork(t *testing.T) {
	manager := NewManager()
	empty := manager.Create("unassigned", nil)
	if empty.Status != StatusFailed {
		t.Fatalf("empty task status = %q, want failed", empty.Status)
	}

	task := manager.Create("long task", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "a"},
	}})
	ctx, ok := manager.Context(task.ID)
	if !ok {
		t.Fatal("task context not found")
	}
	if !manager.Cancel(task.ID) {
		t.Fatal("expected cancellation to succeed")
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("context error = %v, want canceled", err)
	}
	manager.StartSubtask(task.ID, "a")
	cancelled, _ := manager.Get(task.ID)
	if cancelled.Status != StatusCancelled || cancelled.Subtasks[0].Status != StatusCancelled {
		t.Fatalf("unexpected cancelled task state: %+v", cancelled)
	}
	if manager.Cancel(task.ID) {
		t.Fatal("second cancellation should fail")
	}
}

func TestDependenciesGateWorkAndPassPrerequisiteOutput(t *testing.T) {
	manager := NewManager()
	task := manager.Create("Research and summarize", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "research", Description: "Research"}},
		{Subtask: scheduler.Subtask{ID: "summary", Description: "Summarize", DependsOn: []string{"research"}}},
	})
	ctx, _ := manager.Context(task.ID)

	waiting := make(chan bool, 1)
	go func() {
		waiting <- manager.WaitForDependencies(ctx, task.ID, "summary")
	}()
	manager.CompleteSubtask(task.ID, "research", "Research findings", nil)
	if !<-waiting {
		t.Fatal("dependent subtask did not become ready after its prerequisite completed")
	}
	input, ok := manager.InputForSubtask(task.ID, "summary")
	if !ok || !strings.Contains(input, "Research findings") || !strings.Contains(input, "Summarize") {
		t.Fatalf("dependent input did not include prerequisite output: %q", input)
	}
}

func TestFinalizerSynthesizesCompletedTask(t *testing.T) {
	manager := NewManagerWithFinalizer(func(_ context.Context, task *Task) (string, error) {
		if len(task.Subtasks) != 2 || task.Subtasks[0].Output != "one" || task.Subtasks[1].Output != "two" {
			t.Fatalf("finalizer received incomplete task: %+v", task)
		}
		return "combined answer", nil
	})
	task := manager.Create("Combine results", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "a", Description: "First"}},
		{Subtask: scheduler.Subtask{ID: "b", Description: "Second"}},
	})
	manager.CompleteSubtask(task.ID, "a", "one", nil)
	manager.CompleteSubtask(task.ID, "b", "two", nil)

	completed, _ := manager.Get(task.ID)
	if completed.Status != StatusCompleted || completed.Result != "combined answer" {
		t.Fatalf("unexpected synthesized task: %+v", completed)
	}
}

func TestFinalizerFailureIsReported(t *testing.T) {
	manager := NewManagerWithFinalizer(func(context.Context, *Task) (string, error) {
		return "", errors.New("model unavailable")
	})
	task := manager.Create("Combine results", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "a", Description: "Only step"},
	}})
	manager.CompleteSubtask(task.ID, "a", "partial output", nil)

	failed, _ := manager.Get(task.ID)
	if failed.Status != StatusFailed || !strings.Contains(failed.Error, "model unavailable") {
		t.Fatalf("synthesis failure was not reported: %+v", failed)
	}
}

func TestPersistentManagerRestoresHistoryAndMarksInterruptedTasksFailed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	manager, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := manager.CreateChecked("Finished task", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "done", Description: "Finished"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	manager.CompleteSubtask(completed.ID, "done", "saved result", nil)

	interrupted, err := manager.CreateChecked("Interrupted task", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", Description: "In progress"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSubtaskChecked(interrupted.ID, "work"); err != nil {
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
	gotCompleted, ok := reopened.Get(completed.ID)
	if !ok || gotCompleted.Status != StatusCompleted || !strings.Contains(gotCompleted.Result, "saved result") {
		t.Fatalf("completed task was not restored: %+v", gotCompleted)
	}
	gotInterrupted, ok := reopened.Get(interrupted.ID)
	if !ok || gotInterrupted.Status != StatusFailed || !strings.Contains(gotInterrupted.Error, "restarted") {
		t.Fatalf("interrupted task was not marked failed: %+v", gotInterrupted)
	}
	next, err := reopened.CreateChecked("Next task", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "next", Description: "Next"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if next.ID == completed.ID || next.ID == interrupted.ID {
		t.Fatalf("task ID sequence was not restored: %q", next.ID)
	}
}
