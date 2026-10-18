package taskmgr

import (
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/scheduler"
)

type blockingCheckpointStore struct {
	Store
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func (s *blockingCheckpointStore) unblock() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *blockingCheckpointStore) SaveCheckpoint(checkpoint *Checkpoint) error {
	if checkpoint.Status == StatusCompleted {
		close(s.entered)
		<-s.release
	}
	return s.Store.SaveCheckpoint(checkpoint)
}

func TestCheckpointSaveAndRetrieve(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tasks.db")
	mgr, err := OpenManager(dbPath, nil)
	if err != nil {
		t.Fatalf("OpenManager failed: %v", err)
	}
	defer mgr.Close()

	graph := scheduler.NewTaskGraph()
	_ = graph.AddNode(scheduler.Subtask{ID: "node-1", Description: "Step 1"})
	_ = graph.AddNode(scheduler.Subtask{ID: "node-2", Description: "Step 2", DependsOn: []string{"node-1"}})
	_ = graph.AddNode(scheduler.Subtask{ID: "node-3", Description: "Step 3"})
	_ = graph.AddEdge("node-1", "node-2")

	assignments := []scheduler.Assignment{
		{Subtask: graph.Nodes["node-1"], NodeID: "worker-1", Model: "llama"},
		{Subtask: graph.Nodes["node-2"], NodeID: "worker-1", Model: "llama"},
		{Subtask: graph.Nodes["node-3"], NodeID: "worker-2", Model: "qwen"},
	}

	task, err := mgr.CreateWithGraphAndOptionsChecked("Multi-step DAG task", graph, assignments, false)
	if err != nil {
		t.Fatalf("CreateWithGraphAndOptionsChecked failed: %v", err)
	}

	// Verify initial checkpoint
	cps, err := mgr.GetCheckpoints(task.ID)
	if err != nil {
		t.Fatalf("GetCheckpoints failed: %v", err)
	}
	if len(cps) == 0 {
		t.Fatalf("expected initial checkpoint to be created")
	}
	latest, err := mgr.GetLatestCheckpoint(task.ID)
	if err != nil || latest == nil {
		t.Fatalf("expected latest checkpoint, got %v (err: %v)", latest, err)
	}
	if len(latest.Frontier) != 2 { // node-1 and node-3 should be ready
		t.Fatalf("expected initial frontier [node-1 node-3], got %v", latest.Frontier)
	}

	// Complete node-1
	mgr.StartSubtask(task.ID, "node-1")
	if err := mgr.CompleteSubtaskChecked(task.ID, "node-1", "result-1", nil); err != nil {
		t.Fatalf("CompleteSubtaskChecked failed: %v", err)
	}

	latest, err = mgr.GetLatestCheckpoint(task.ID)
	if err != nil || latest == nil {
		t.Fatalf("failed to get latest checkpoint: %v", err)
	}
	if latest.Outputs["node-1"] != "result-1" {
		t.Fatalf("checkpoint output mismatch: %v", latest.Outputs)
	}
	// Now node-2 and node-3 should be ready in frontier
	frontierMap := make(map[string]bool)
	for _, f := range latest.Frontier {
		frontierMap[f] = true
	}
	if !frontierMap["node-2"] || !frontierMap["node-3"] {
		t.Fatalf("expected node-2 and node-3 in frontier, got %v", latest.Frontier)
	}
}

func TestWaitDoesNotReturnBeforeTerminalCheckpointIsPersisted(t *testing.T) {
	store := &blockingCheckpointStore{
		Store:   NewMemoryStore(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager, err := NewManagerWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.unblock()
		if err := manager.Close(); err != nil {
			t.Errorf("close manager: %v", err)
		}
	})

	task := manager.Create("wait for checkpoint persistence", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", Description: "Work"},
	}})
	if err := manager.StartSubtaskChecked(task.ID, "work"); err != nil {
		t.Fatal(err)
	}

	completion := make(chan error, 1)
	go func() {
		completion <- manager.CompleteSubtaskChecked(task.ID, "work", "done", nil)
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("completion did not enter checkpoint persistence")
	}

	type waitResult struct {
		task *Task
		err  error
	}
	waiting := make(chan struct{})
	result := make(chan waitResult, 1)
	go func() {
		close(waiting)
		completed, err := manager.Wait(context.Background(), task.ID)
		result <- waitResult{task: completed, err: err}
	}()
	<-waiting
	runtime.Gosched()
	select {
	case got := <-result:
		t.Fatalf("Wait returned before checkpoint persistence: task=%+v err=%v", got.task, got.err)
	case <-time.After(20 * time.Millisecond):
	}

	store.unblock()
	if err := <-completion; err != nil {
		t.Fatalf("complete subtask: %v", err)
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("Wait returned error: %v", got.err)
		}
		if got.task == nil || got.task.Status != StatusCompleted {
			t.Fatalf("Wait returned task %#v, want completed", got.task)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after checkpoint persistence")
	}
}

func TestResumableTasksOnInterruption(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tasks.db")
	mgr, err := OpenManager(dbPath, nil)
	if err != nil {
		t.Fatalf("OpenManager failed: %v", err)
	}

	assignments := []scheduler.Assignment{
		{Subtask: scheduler.Subtask{ID: "sub-1", Description: "Part 1"}},
		{Subtask: scheduler.Subtask{ID: "sub-2", Description: "Part 2", DependsOn: []string{"sub-1"}}},
	}

	task, err := mgr.CreateChecked("Interrupted task", assignments)
	if err != nil {
		t.Fatalf("CreateChecked failed: %v", err)
	}
	mgr.StartSubtask(task.ID, "sub-1")
	if err := mgr.CompleteSubtaskChecked(task.ID, "sub-1", "completed part 1", nil); err != nil {
		t.Fatalf("CompleteSubtaskChecked failed: %v", err)
	}

	// Close to simulate crash/restart while sub-2 was pending
	mgr.Close()

	mgr2, err := OpenManager(dbPath, nil)
	if err != nil {
		t.Fatalf("OpenManager restart failed: %v", err)
	}
	defer mgr2.Close()

	resumable := mgr2.ResumableTasks()
	found := false
	for _, r := range resumable {
		if r.ID == task.ID {
			found = true
			if !r.Resumable {
				t.Fatalf("expected task %s to be marked resumable", task.ID)
			}
		}
	}
	if !found {
		t.Fatalf("interrupted task not found in ResumableTasks: %+v", resumable)
	}

	// Resume the task
	resumed, err := mgr2.ResumeTaskChecked(task.ID)
	if err != nil {
		t.Fatalf("ResumeTaskChecked failed: %v", err)
	}
	if resumed.Status != StatusRunning {
		t.Fatalf("expected resumed status %s, got %s", StatusRunning, resumed.Status)
	}
	// Check that sub-1 output is still intact and sub-2 is pending
	if resumed.Subtasks[0].Status != StatusCompleted || resumed.Subtasks[0].Output != "completed part 1" {
		t.Fatalf("sub-1 outcome lost on resume: %+v", resumed.Subtasks[0])
	}
	if resumed.Subtasks[1].Status != StatusPending {
		t.Fatalf("sub-2 status expected pending, got %s", resumed.Subtasks[1].Status)
	}
}
