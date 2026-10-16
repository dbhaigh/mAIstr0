package taskmgr

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestRecentCacheInvalidatesAfterTaskWrite(t *testing.T) {
	manager := NewManager()
	makeTask := func(description string) *Task {
		t.Helper()
		task, err := manager.CreateChecked(description, []scheduler.Assignment{{
			Subtask: scheduler.Subtask{ID: "work", Description: description},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return task
	}

	first := makeTask("first")
	second := makeTask("second")
	if recent := manager.Recent(1); len(recent) != 1 || recent[0].ID != second.ID {
		t.Fatalf("initial recent task = %#v, want %s", recent, second.ID)
	}
	if !manager.recentTasksValid {
		t.Fatal("Recent did not populate the recent-task cache")
	}

	time.Sleep(time.Millisecond)
	if err := manager.StartSubtaskChecked(first.ID, "work"); err != nil {
		t.Fatal(err)
	}
	if manager.recentTasksValid {
		t.Fatal("task update did not invalidate the recent-task cache")
	}
	recent := manager.Recent(1)
	if len(recent) != 1 || recent[0].ID != first.ID {
		t.Fatalf("recent after task update = %#v, want %s", recent, first.ID)
	}
	recent[0].Description = "caller mutation"
	if stored, ok := manager.Get(first.ID); !ok || stored.Description != "first" {
		t.Fatalf("mutating cached result changed stored task: %#v", stored)
	}
}
