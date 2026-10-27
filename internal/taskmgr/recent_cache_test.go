package taskmgr

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestRecentAndPageRecentShareCacheOrdering(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		pageFirst bool
	}{
		{"Recent builds cache", false},
		{"PageRecent builds cache", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager()
			defer closeutil.Discard(manager)
			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			manager.tasks = map[string]*Task{
				"a": {ID: "a", UpdatedAt: base},
				"b": {ID: "b", UpdatedAt: base},
				"c": {ID: "c", UpdatedAt: base.Add(time.Second)},
			}
			check := func(want []string) {
				t.Helper()
				for _, paged := range []bool{test.pageFirst, !test.pageFirst} {
					var tasks []*Task
					if paged {
						page, err := manager.PageRecent(nil, "", "", 3, 0)
						if err != nil {
							t.Fatal(err)
						}
						tasks = page.Items
					} else {
						tasks = manager.Recent(3)
					}
					if len(tasks) != len(want) {
						t.Fatalf("paged=%t: got %d tasks, want %d", paged, len(tasks), len(want))
					}
					for i, id := range want {
						if tasks[i].ID != id {
							t.Fatalf("paged=%t: task %d = %s, want %s", paged, i, tasks[i].ID, id)
						}
					}
				}
			}
			check([]string{"c", "b", "a"})
			manager.mu.Lock()
			manager.tasks["a"].UpdatedAt = base.Add(time.Minute)
			manager.invalidateRecentTasksLocked()
			manager.mu.Unlock()
			check([]string{"a", "c", "b"})
		})
	}
}

func TestRecentCacheInvalidatesAfterTaskWrite(t *testing.T) {
	t.Parallel()
	manager := NewManager()
	defer closeutil.Discard(manager)
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
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	manager.mu.Lock()
	manager.tasks[first.ID].UpdatedAt = base
	manager.tasks[second.ID].UpdatedAt = base.Add(time.Second)
	manager.mu.Unlock()
	if recent := manager.Recent(1); len(recent) != 1 || recent[0].ID != second.ID {
		t.Fatalf("initial recent task = %#v, want %s", recent, second.ID)
	}
	if !manager.recentTasksValid {
		t.Fatal("Recent did not populate the recent-task cache")
	}

	if err := manager.StartSubtaskChecked(first.ID, "work"); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.tasks[first.ID].UpdatedAt = base.Add(2 * time.Second)
	manager.mu.Unlock()
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
