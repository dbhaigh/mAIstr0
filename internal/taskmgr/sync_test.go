package taskmgr

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestMergeReplicaTaskSlices(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		current    []*Task
		incoming   []*Task
		wantApply  []bool
		wantStatus Status
		wantError  string
	}{
		{
			name:      "new in-progress task is marked orphaned",
			incoming:  []*Task{{ID: "new", Status: StatusRunning, UpdatedAt: base}},
			wantApply: []bool{true}, wantStatus: StatusFailed, wantError: coordinatorChangedTaskError,
		},
		{
			name:      "older update is ignored",
			current:   []*Task{{ID: "task", Status: StatusCompleted, UpdatedAt: base.Add(time.Hour)}},
			incoming:  []*Task{{ID: "task", Status: StatusRunning, UpdatedAt: base}},
			wantApply: []bool{false}, wantStatus: StatusCompleted,
		},
		{
			name:      "newer update is accepted",
			current:   []*Task{{ID: "task", Status: StatusRunning, UpdatedAt: base}},
			incoming:  []*Task{{ID: "task", Status: StatusCompleted, UpdatedAt: base.Add(time.Hour)}},
			wantApply: []bool{true}, wantStatus: StatusCompleted,
		},
		{
			name: "completed replica supersedes restart recovery",
			current: []*Task{{
				ID: "task", Status: StatusFailed, Error: interruptedTaskError, UpdatedAt: base.Add(time.Hour),
			}},
			incoming:  []*Task{{ID: "task", Status: StatusCompleted, UpdatedAt: base}},
			wantApply: []bool{true}, wantStatus: StatusCompleted,
		},
		{
			name:    "interrupted replica never overwrites live task",
			current: []*Task{{ID: "task", Status: StatusRunning, UpdatedAt: base}},
			incoming: []*Task{{
				ID: "task", Status: StatusFailed, Error: interruptedTaskError, UpdatedAt: base.Add(time.Hour),
			}},
			wantApply: []bool{false}, wantStatus: StatusRunning,
		},
		{
			name:    "duplicate updates are compared in sequence",
			current: []*Task{{ID: "task", Status: StatusRunning, UpdatedAt: base}},
			incoming: []*Task{
				{ID: "task", Status: StatusCompleted, UpdatedAt: base.Add(2 * time.Hour)},
				{ID: "task", Status: StatusFailed, UpdatedAt: base.Add(time.Hour)},
			},
			wantApply: []bool{true, false}, wantStatus: StatusCompleted,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			merged, applied := mergeReplicaTaskSlices(test.current, test.incoming)
			if len(applied) != len(test.wantApply) {
				t.Fatalf("applied decisions = %v, want %v", applied, test.wantApply)
			}
			for index := range applied {
				if applied[index] != test.wantApply[index] {
					t.Fatalf("applied decisions = %v, want %v", applied, test.wantApply)
				}
			}
			var got *Task
			for _, task := range merged {
				if task.ID == "task" || task.ID == "new" {
					got = task
					break
				}
			}
			if got == nil || got.Status != test.wantStatus || got.Error != test.wantError {
				t.Fatalf("merged task = %+v, want status %q and error %q", got, test.wantStatus, test.wantError)
			}
		})
	}
}

func TestIncrementalTaskSnapshotsAndOutOfOrderMerge(t *testing.T) {
	leader := NewManager()
	leader.SetSyncOrigin("task:leader")
	created, err := leader.CreateChecked("incremental", nil)
	if err != nil {
		t.Fatal(err)
	}
	newer := cloneTask(created)
	newer.Sequence = created.Sequence + 2
	newer.Status = StatusCompleted
	newer.UpdatedAt = time.Now().Add(time.Minute)
	older := cloneTask(created)
	older.Sequence = created.Sequence + 1
	older.Status = StatusRunning
	older.UpdatedAt = time.Now()

	replica := NewManager()
	if err := replica.MergeFromReplicaFrom([]*Task{newer}, "task:leader"); err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(replica.All())
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.MergeFromReplicaFrom([]*Task{newer}, "task:leader"); err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(replica.All())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("replaying task batch changed state:\nbefore: %s\nafter:  %s", before, after)
	}
	if err := replica.MergeFromReplicaFrom([]*Task{older}, "task:leader"); err != nil {
		t.Fatal(err)
	}
	got, ok := replica.Get(created.ID)
	if !ok || got.Status != StatusCompleted || got.Sequence != newer.Sequence {
		t.Fatalf("out-of-order task update replaced newer state: %+v", got)
	}
	if delta := replica.SnapshotSince(map[string]uint64{"task:leader": older.Sequence}); len(delta) != 1 {
		t.Fatalf("incremental task snapshot contains %d tasks, want 1", len(delta))
	}
	if full := replica.SnapshotForPeer(0, map[string]uint64{"task:leader": older.Sequence}); len(full) != 1 {
		t.Fatalf("legacy peer received %d tasks, want full snapshot", len(full))
	}
	if incremental := replica.SnapshotForPeer(1, map[string]uint64{"task:leader": older.Sequence}); len(incremental) != 1 {
		t.Fatalf("version 1 peer received %d tasks, want the incremental update", len(incremental))
	}
	if marks := replica.Watermarks(); marks["task:leader"] != newer.Sequence {
		t.Fatalf("task watermarks = %v, want task:leader=%d", marks, newer.Sequence)
	}
}

func TestLegacyTaskSyncGetsSourceMetadata(t *testing.T) {
	manager := NewManager()
	legacy := &Task{ID: "task-0001", Status: StatusCompleted, UpdatedAt: time.Now()}
	if err := manager.MergeFromReplicaFrom([]*Task{legacy}, "task:old-peer"); err != nil {
		t.Fatal(err)
	}
	got, ok := manager.Get(legacy.ID)
	if !ok || got.Origin != "task:old-peer" || got.Sequence == 0 {
		t.Fatalf("legacy task metadata = %+v", got)
	}
}

func TestTaskWatermarksPersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	manager, err := OpenManagerWithOrigin(path, nil, "task:node-a")
	if err != nil {
		t.Fatal(err)
	}
	task := manager.Create("persist metadata", nil)
	if task.Origin != "task:node-a" || task.Sequence == 0 {
		t.Fatalf("created task metadata = %+v", task)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenManagerWithOrigin(path, nil, "task:node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if marks := reopened.Watermarks(); marks["task:node-a"] != task.Sequence {
		t.Fatalf("task watermarks after restart = %v, want task:node-a=%d", marks, task.Sequence)
	}
	stored, ok := reopened.Get(task.ID)
	if !ok || stored.Origin != task.Origin || stored.Sequence != task.Sequence {
		t.Fatalf("task metadata after restart = %+v", stored)
	}
}
