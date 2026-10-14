package taskmgr

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestWriteLegacyTaskFixture(t *testing.T) {
	if os.Getenv("MAISTRO_WRITE_FIXTURES") != "1" {
		t.Skip("set MAISTRO_WRITE_FIXTURES=1 to regenerate the legacy fixture")
	}
	path := filepath.Join("testdata", "legacy-tasks.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	statuses := []Status{
		StatusPending, StatusRunning, StatusSynthesis, StatusAwaitingApproval,
		StatusCompleted, StatusFailed, StatusCancelled,
	}
	createdAt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := db.Update(func(tx *bolt.Tx) error {
		tasks, err := tx.CreateBucketIfNotExists([]byte("tasks"))
		if err != nil {
			return err
		}
		checkpoints, err := tx.CreateBucketIfNotExists([]byte("checkpoints"))
		if err != nil {
			return err
		}
		for i, status := range statuses {
			id := genID(i + 1)
			subtaskStatus := StatusCompleted
			switch status {
			case StatusPending:
				subtaskStatus = StatusPending
			case StatusRunning, StatusSynthesis:
				subtaskStatus = StatusRunning
			}
			task := &Task{
				ID: id, Description: "legacy task " + string(status), Status: status,
				Subtasks:  []SubtaskResult{{Status: subtaskStatus}},
				CreatedAt: createdAt, UpdatedAt: createdAt,
			}
			task.Subtasks[0].Assignment.Subtask.ID = "work"
			task.Subtasks[0].Assignment.Subtask.Description = "fixture work"
			encoded, err := json.Marshal(task)
			if err != nil {
				return err
			}
			if err := tasks.Put([]byte(id), encoded); err != nil {
				return err
			}

			checkpointID := "cp-" + id + "-0"
			checkpoint := &Checkpoint{
				ID: checkpointID, TaskID: id, Sequence: 0, Status: status,
				Subtasks: task.Subtasks, Outputs: map[string]string{}, Frontier: []string{"work"},
				Resumable: status != StatusCompleted, Timestamp: createdAt,
			}
			encoded, err = json.Marshal(checkpoint)
			if err != nil {
				return err
			}
			if err := checkpoints.Put([]byte(checkpointID), encoded); err != nil {
				return err
			}
			if err := checkpoints.Put([]byte("latest:"+id), encoded); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyTaskFixtureLoadsWithRestartRecovery(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "legacy-tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy-tasks.db")
	if err := os.WriteFile(path, fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	recovered := map[Status]bool{
		StatusPending: true, StatusRunning: true, StatusSynthesis: true,
	}
	for i, originalStatus := range []Status{
		StatusPending, StatusRunning, StatusSynthesis, StatusAwaitingApproval,
		StatusCompleted, StatusFailed, StatusCancelled,
	} {
		id := genID(i + 1)
		task, ok := manager.Get(id)
		if !ok {
			t.Fatalf("fixture task %s is missing", id)
		}
		if task.Description != "legacy task "+string(originalStatus) ||
			!task.CreatedAt.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) ||
			len(task.Subtasks) != 1 ||
			task.Subtasks[0].Assignment.Subtask.ID != "work" ||
			task.Subtasks[0].Assignment.Subtask.Description != "fixture work" {
			t.Fatalf("fixture task %s contents = %+v", id, task)
		}
		wantStatus := originalStatus
		if recovered[originalStatus] {
			wantStatus = StatusFailed
			if task.Error != interruptedTaskError || !task.Resumable {
				t.Fatalf("recovered task %s has error %q and resumable=%v", id, task.Error, task.Resumable)
			}
			if task.Subtasks[0].Status != StatusFailed {
				t.Fatalf("recovered subtask %s status = %q, want failed", id, task.Subtasks[0].Status)
			}
		} else if originalStatus == StatusAwaitingApproval || originalStatus == StatusFailed || originalStatus == StatusCancelled {
			if !task.Resumable {
				t.Fatalf("task %s status %q should be resumable", id, originalStatus)
			}
		}
		if task.Status != wantStatus {
			t.Fatalf("task %s status = %q, want %q", id, task.Status, wantStatus)
		}
		if task.Origin == "" || task.Sequence == 0 {
			t.Fatalf("task %s legacy sync metadata was not backfilled: %+v", id, task)
		}
		checkpoints, err := manager.GetCheckpoints(id)
		if err != nil {
			t.Fatal(err)
		}
		if len(checkpoints) == 0 {
			t.Fatalf("fixture task %s has no checkpoints", id)
		}
		latest, err := manager.GetLatestCheckpoint(id)
		if err != nil {
			t.Fatal(err)
		}
		if latest.TaskID != id || latest.Status != task.Status {
			t.Fatalf("latest checkpoint for %s = %+v, want status %q", id, latest, task.Status)
		}
		if latest.Subtasks[0].Assignment.Subtask.ID != "work" ||
			latest.Subtasks[0].Assignment.Subtask.Description != "fixture work" ||
			len(latest.Frontier) != 1 || latest.Frontier[0] != "work" {
			t.Fatalf("latest checkpoint for %s lost fixture contents: %+v", id, latest)
		}
	}
}

func TestMemoryStoreSupportsManagerPersistence(t *testing.T) {
	manager, err := NewManagerWithStore(NewMemoryStore(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	task, err := manager.CreateChecked("in-memory persistence", nil)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := manager.GetLatestCheckpoint(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latest.TaskID != task.ID || latest.Status != StatusFailed {
		t.Fatalf("in-memory checkpoint = %+v", latest)
	}
}
