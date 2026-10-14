package taskmgr

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
	bolt "go.etcd.io/bbolt"
)

func managerBoltDB(t *testing.T, manager *Manager) *bolt.DB {
	t.Helper()
	store, ok := manager.store.(*boltStore)
	if !ok {
		t.Fatal("manager does not use a Bolt store")
	}
	return store.db
}

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

func TestTaskHistorySyncMergesMissingAndReplacesReplicaState(t *testing.T) {
	leader, err := OpenManager(filepath.Join(t.TempDir(), "leader.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	followerPath := filepath.Join(t.TempDir(), "follower.db")
	follower, err := OpenManager(followerPath, nil)
	if err != nil {
		t.Fatal(err)
	}

	created, err := leader.CreateChecked("Replicated task", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", Description: "Complete work"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	leader.CompleteSubtask(created.ID, "work", "saved output", nil)
	snapshot := leader.All()

	if err := follower.MergeMissing(snapshot); err != nil {
		t.Fatal(err)
	}
	replica, ok := follower.Get(created.ID)
	if !ok || replica.Subtasks[0].Output != "saved output" {
		t.Fatalf("task history was not imported: %#v", replica)
	}

	replica.Description = "stale local state"
	if err := follower.ReplaceFromSync([]*Task{replica}); err != nil {
		t.Fatal(err)
	}
	replica.Description = "authoritative state"
	replica.UpdatedAt = replica.UpdatedAt.Add(-time.Hour)
	if err := follower.ReplaceFromSync([]*Task{replica}); err != nil {
		t.Fatal(err)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenManager(followerPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	stored, _ := restarted.Get(created.ID)
	if stored.Description != "authoritative state" {
		t.Fatalf("authoritative task replica was not durable: %q", stored.Description)
	}
}

func TestReplicaSyncRecoversProgressWithoutImportingRestartArtifacts(t *testing.T) {
	leader := NewManager()
	created := leader.Create("Recover peer progress", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work"},
	}})
	running, _ := leader.Get(created.ID)

	activeReplica := NewManager()
	if err := activeReplica.MergeMissing([]*Task{running}); err != nil {
		t.Fatal(err)
	}
	restartArtifact := cloneTask(running)
	restartArtifact.Status = StatusFailed
	restartArtifact.Error = interruptedTaskError
	restartArtifact.UpdatedAt = restartArtifact.UpdatedAt.Add(time.Hour)
	restartArtifact.Subtasks[0].Status = StatusFailed
	if err := activeReplica.MergeFromReplica([]*Task{restartArtifact}); err != nil {
		t.Fatal(err)
	}
	stillRunning, _ := activeReplica.Get(created.ID)
	if stillRunning.Status != StatusRunning {
		t.Fatalf("replica restart artifact overwrote active coordinator state: %q", stillRunning.Status)
	}

	leader.CompleteSubtask(created.ID, "work", "peer completed output", nil)
	if err := activeReplica.MergeFromReplica(leader.All()); err != nil {
		t.Fatal(err)
	}
	recovered, _ := activeReplica.Get(created.ID)
	if recovered.Status != StatusCompleted || !strings.Contains(recovered.Result, "peer completed output") {
		t.Fatalf("completed peer progress did not recover interrupted state: %#v", recovered)
	}

	orphanReplica := NewManager()
	if err := orphanReplica.MergeFromReplica([]*Task{running}); err != nil {
		t.Fatal(err)
	}
	orphaned, _ := orphanReplica.Get(created.ID)
	if orphaned.Status != StatusFailed || orphaned.Error != coordinatorChangedTaskError || !orphaned.Resumable {
		t.Fatalf("orphaned running task was not made resumable: %#v", orphaned)
	}
	if err := orphanReplica.MergeFromReplica(leader.All()); err != nil {
		t.Fatal(err)
	}
	recovered, _ = orphanReplica.Get(created.ID)
	if recovered.Status != StatusCompleted || !strings.Contains(recovered.Result, "peer completed output") {
		t.Fatalf("peer completion did not replace coordinator-change recovery: %#v", recovered)
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

func TestWaitForDependenciesReturnsOnContextCancellation(t *testing.T) {
	manager := NewManager()
	task := manager.Create("Wait for prerequisite", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "root"}},
		{Subtask: scheduler.Subtask{ID: "dependent", DependsOn: []string{"root"}}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		done <- manager.WaitForDependencies(ctx, task.ID, "dependent")
	}()
	cancel()
	select {
	case ready := <-done:
		if ready {
			t.Fatal("cancelled dependency wait reported ready")
		}
	case <-time.After(time.Second):
		t.Fatal("dependency wait did not unblock after cancellation")
	}
}

func TestTaskReadsProceedWhilePersistenceIsBlocked(t *testing.T) {
	manager, err := OpenManager(filepath.Join(t.TempDir(), "tasks.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close task manager: %v", err)
		}
	})
	task, err := manager.CreateChecked("Persist outside manager lock", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "work"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	db := managerBoltDB(t, manager)

	dbLocked := make(chan struct{})
	releaseDB := make(chan struct{})
	dbErr := make(chan error, 1)
	go func() {
		dbErr <- db.Update(func(*bolt.Tx) error {
			close(dbLocked)
			<-releaseDB
			return nil
		})
	}()
	<-dbLocked

	current, ok := manager.Get(task.ID)
	if !ok {
		t.Fatal("created task disappeared")
	}
	started := make(chan error, 1)
	go func() {
		started <- manager.StartSubtaskChecked(task.ID, "work")
	}()
	<-current.changed

	deadline := time.Now().Add(time.Second)
	lockAvailable := false
	for time.Now().Before(deadline) {
		if manager.mu.TryRLock() {
			lockAvailable = true
			manager.mu.RUnlock()
			break
		}
		runtime.Gosched()
	}
	close(releaseDB)
	if err := <-dbErr; err != nil {
		t.Fatal(err)
	}
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if !lockAvailable {
		t.Fatal("manager map lock remained held while BoltDB persistence was blocked")
	}
}

func TestSameTaskPersistenceRemainsOrdered(t *testing.T) {
	manager, err := OpenManager(filepath.Join(t.TempDir(), "tasks.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close task manager: %v", err)
		}
	})
	task, err := manager.CreateChecked("Serialize task writes", []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "work"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	db := managerBoltDB(t, manager)

	dbLocked := make(chan struct{})
	releaseDB := make(chan struct{})
	dbErr := make(chan error, 1)
	go func() {
		dbErr <- db.Update(func(*bolt.Tx) error {
			close(dbLocked)
			<-releaseDB
			return nil
		})
	}()
	<-dbLocked
	current, _ := manager.Get(task.ID)
	started := make(chan error, 1)
	go func() { started <- manager.StartSubtaskChecked(task.ID, "work") }()
	<-current.changed

	completed := make(chan error, 1)
	go func() { completed <- manager.CompleteSubtaskChecked(task.ID, "work", "done", nil) }()
	close(releaseDB)
	if err := <-dbErr; err != nil {
		t.Fatal(err)
	}
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if err := <-completed; err != nil {
		t.Fatal(err)
	}

	var persisted Task
	if err := db.View(func(tx *bolt.Tx) error {
		return json.Unmarshal(tx.Bucket([]byte("tasks")).Get([]byte(task.ID)), &persisted)
	}); err != nil {
		t.Fatal(err)
	}
	if persisted.Status != StatusCompleted || persisted.Subtasks[0].Status != StatusCompleted {
		t.Fatalf("persisted task lost the later completion: %+v", persisted)
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

func TestApprovalGateDefersSynthesisUntilApproved(t *testing.T) {
	finalizerCalls := 0
	manager := NewManagerWithFinalizer(func(context.Context, *Task) (string, error) {
		finalizerCalls++
		return "approved result", nil
	})
	task, err := manager.CreateWithOptionsChecked("Review this", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", Description: "Do the work"},
	}}, true)
	if err != nil {
		t.Fatal(err)
	}
	manager.CompleteSubtask(task.ID, "work", "worker result", nil)
	waiting, _ := manager.Get(task.ID)
	if waiting.Status != StatusAwaitingApproval || finalizerCalls != 0 {
		t.Fatalf("approval gate state = %q, finalizer calls = %d", waiting.Status, finalizerCalls)
	}
	if err := manager.ApproveChecked(task.ID); err != nil {
		t.Fatal(err)
	}
	completed, _ := manager.Get(task.ID)
	if completed.Status != StatusCompleted || completed.Result != "approved result" || finalizerCalls != 1 {
		t.Fatalf("approval did not resume synthesis: %+v, finalizer calls = %d", completed, finalizerCalls)
	}
}

func TestApprovalGateSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "approval.db")
	manager, err := OpenManager(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	task, err := manager.CreateWithOptionsChecked("Review this", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", Description: "Do the work"},
	}}, true)
	if err != nil {
		t.Fatal(err)
	}
	manager.CompleteSubtask(task.ID, "work", "saved output", nil)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenManager(path, func(_ context.Context, task *Task) (string, error) {
		if task.Subtasks[0].Output != "saved output" {
			t.Fatalf("restored output = %q", task.Subtasks[0].Output)
		}
		return "resumed result", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.ApproveChecked(task.ID); err != nil {
		t.Fatal(err)
	}
	result, _ := reopened.Get(task.ID)
	if result.Status != StatusCompleted || result.Result != "resumed result" {
		t.Fatalf("approval state did not resume after restart: %+v", result)
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
