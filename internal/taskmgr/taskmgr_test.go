package taskmgr

import (
	"context"
	"errors"
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
