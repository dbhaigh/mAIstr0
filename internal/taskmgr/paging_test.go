package taskmgr

import (
	"errors"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestPageRecentReturnsStablePagesAndFilters(t *testing.T) {
	manager := NewManager()
	defer manager.Close()
	for _, description := range []string{"alpha task", "beta task", "alpha follow-up"} {
		task, err := manager.CreateChecked(description, []scheduler.Assignment{{
			Subtask: scheduler.Subtask{ID: "work", Description: description},
			NodeID:  "node-a", Model: "model-a",
		}})
		if err != nil {
			t.Fatal(err)
		}
		if task.ID == "" {
			t.Fatal("created task has no ID")
		}
	}

	first, err := manager.PageRecent(nil, "alpha", "", 1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || !first.HasMore || first.NextCursor.ID == "" {
		t.Fatalf("unexpected first page: %+v", first)
	}
	last, err := manager.PageRecent(&first.NextCursor, "alpha", "", 1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(last.Items) != 1 || last.HasMore || last.Items[0].ID == first.Items[0].ID {
		t.Fatalf("unexpected last page: %+v", last)
	}

	empty, err := manager.PageRecent(nil, "not present", "", 2, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Items) != 0 || empty.HasMore {
		t.Fatalf("expected empty page, got %+v", empty)
	}
}

func TestPageRecentRejectsOversizedSingleTask(t *testing.T) {
	manager := NewManager()
	defer manager.Close()
	task, err := manager.CreateChecked("large", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work"},
		NodeID:  "node-a", Model: "model-a",
	}})
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.tasks[task.ID].Result = strings.Repeat("x", 128)
	manager.invalidateRecentTasksLocked()
	manager.mu.Unlock()

	_, err = manager.PageRecent(nil, "", "", 1, 64)
	if !errors.Is(err, ErrPageRecordTooLarge) {
		t.Fatalf("oversized task error = %v, want %v", err, ErrPageRecordTooLarge)
	}
}
