package taskmgr

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func benchmarkSyncTasks(count int) []*Task {
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	tasks := make([]*Task, count)
	for i := range tasks {
		tasks[i] = &Task{
			ID: "task-" + strconv.Itoa(i), Origin: "task:benchmark", Sequence: uint64(i + 1),
			Description: "synchronized benchmark task", Status: StatusCompleted,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}
	}
	return tasks
}

func BenchmarkTaskSyncMerge10K(b *testing.B) {
	tasks := benchmarkSyncTasks(10_000)
	b.ReportAllocs()
	b.ReportMetric(float64(len(tasks)), "tasks/op")
	b.ResetTimer()
	for range b.N {
		manager := NewManager()
		if err := manager.MergeFromReplicaFrom(tasks, "task:benchmark"); err != nil {
			b.Fatal(err)
		}
		if err := manager.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBboltTaskSave(b *testing.B) {
	store, err := openBoltStore(filepath.Join(b.TempDir(), "tasks.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark task store: %v", err)
		}
	})
	task := &Task{
		ID: "benchmark-task", Description: "bbolt task write benchmark",
		Status: StatusCompleted, CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
		UpdatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := store.Save(task); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBboltTaskSaveParallel(b *testing.B) {
	store, err := openBoltStore(filepath.Join(b.TempDir(), "tasks.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark task store: %v", err)
		}
	})
	task := &Task{
		ID: "benchmark-task", Description: "parallel bbolt task write benchmark",
		Status: StatusCompleted, CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
		UpdatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := store.Save(task); err != nil {
				b.Errorf("save benchmark task: %v", err)
				return
			}
		}
	})
}

func BenchmarkBboltCheckpointSave(b *testing.B) {
	store, err := openBoltStore(filepath.Join(b.TempDir(), "tasks.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark task store: %v", err)
		}
	})
	checkpoint := &Checkpoint{
		ID: "cp-benchmark-task-1", TaskID: "benchmark-task", Sequence: 1,
		Status: StatusRunning, Outputs: map[string]string{}, Frontier: []string{"subtask"},
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := store.SaveCheckpoint(checkpoint); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBboltCheckpointSaveParallel(b *testing.B) {
	store, err := openBoltStore(filepath.Join(b.TempDir(), "tasks.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark task store: %v", err)
		}
	})
	checkpoint := &Checkpoint{
		ID: "cp-benchmark-task-1", TaskID: "benchmark-task", Sequence: 1,
		Status: StatusRunning, Outputs: map[string]string{}, Frontier: []string{"subtask"},
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := store.SaveCheckpoint(checkpoint); err != nil {
				b.Errorf("save benchmark checkpoint: %v", err)
				return
			}
		}
	})
}

func BenchmarkBboltTaskStateSave(b *testing.B) {
	store, err := openBoltStore(filepath.Join(b.TempDir(), "tasks.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark task store: %v", err)
		}
	})
	task := &Task{
		ID: "benchmark-task", Description: "atomic task state benchmark",
		Status: StatusCompleted, CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
		UpdatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	checkpoint := &Checkpoint{
		ID: "cp-benchmark-task-1", TaskID: task.ID, Sequence: 1,
		Status: task.Status, Outputs: map[string]string{}, Frontier: []string{"subtask"},
		Timestamp: task.UpdatedAt,
	}

	b.Run("SeparateTransactions", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := store.SaveCheckpoint(checkpoint); err != nil {
				b.Fatal(err)
			}
			if err := store.Save(task); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("AtomicTransaction", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := store.SaveTaskAndCheckpoint(task, checkpoint); err != nil {
				b.Fatal(err)
			}
		}
	})
}
