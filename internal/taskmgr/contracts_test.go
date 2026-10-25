package taskmgr

import (
	"flag"
	"path/filepath"
	"testing"

	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/testutil"
)

var updateTaskContracts = flag.Bool("update", false, "update task manager contract golden files")

func TestRestartRecoveryContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	manager, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	initial := make(map[string]Status)
	for _, test := range []struct {
		name          string
		status        Status
		subtaskStatus Status
	}{
		{"pending", StatusPending, StatusPending},
		{"running", StatusRunning, StatusRunning},
		{"synthesizing", StatusSynthesis, StatusCompleted},
	} {
		task, err := manager.CreateChecked(test.name, []scheduler.Assignment{{
			Subtask: scheduler.Subtask{ID: "work", Description: "Work"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		task, ok := manager.lockTaskForUpdate(task.ID)
		if !ok {
			t.Fatalf("task %s disappeared", test.name)
		}
		task.Status = test.status
		task.Subtasks[0].Status = test.subtaskStatus
		task.Subtasks[0].Output = "finished output"
		if err := manager.persistLocked(task); err != nil {
			manager.unlockTaskForUpdate(task)
			t.Fatal(err)
		}
		manager.unlockTaskForUpdate(task)
		initial[task.ID] = test.status
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	type recovery struct {
		InitialStatus Status   `json:"initial_status"`
		Status        Status   `json:"status"`
		Resumable     bool     `json:"resumable"`
		Subtasks      []Status `json:"subtasks"`
	}
	got := make(map[string]recovery, len(initial))
	for id, original := range initial {
		task, ok := reopened.Get(id)
		if !ok {
			t.Fatalf("recovered task %s not found", id)
		}
		subtasks := make([]Status, len(task.Subtasks))
		for i, subtask := range task.Subtasks {
			subtasks[i] = subtask.Status
		}
		got[string(original)] = recovery{
			InitialStatus: original, Status: task.Status, Resumable: task.Resumable, Subtasks: subtasks,
		}
	}
	assertTaskGolden(t, "restart-recovery.json", got)
}

func assertTaskGolden(t *testing.T, name string, value any) {
	t.Helper()
	path := filepath.Join("testdata", "contracts", name)
	testutil.AssertJSONGolden(t, path, value, *updateTaskContracts)
}
