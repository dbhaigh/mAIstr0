package orchestrator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestSpeculativeHedgeCancelsLoserAndPersistsWinner(t *testing.T) {
	primaryStarted := make(chan struct{})
	primaryCancelled := make(chan struct{})
	primaryRelease := make(chan struct{})
	var releaseOnce sync.Once
	releasePrimary := func() { releaseOnce.Do(func() { close(primaryRelease) }) }
	var primaryCalls, hedgeCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		primaryCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		close(primaryStarted)
		select {
		case <-request.Context().Done():
			close(primaryCancelled)
		case <-primaryRelease:
		}
	}))
	defer primary.Close()
	defer releasePrimary()
	hedge := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		hedgeCalls.Add(1)
		_ = json.NewEncoder(response).Encode(map[string]string{"output": "hedge output"})
	}))
	defer hedge.Close()

	srv := newTestServer(t)
	srv.dispatcher.configureSpeculativeHedging(true, 1)
	for _, duration := range []int64{10, 20, 30, 40, 50} {
		if _, err := srv.Memory().Record(memory.Experience{
			Kind: "subtask", NodeID: "node-a", Model: "shared-model", TaskType: "general",
			Output: strings.Repeat("x", 128), Success: true, DurationMs: duration,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range []cluster.NodeStatus{
		{
			ID: "node-a", Address: primary.URL, Healthy: true, MaxConcurrentTasks: 2, AvailableCapacity: 2,
			Models: []engine.Model{{Name: "shared-model", Engine: "ollama", Tags: []string{"general"}}},
		},
		{
			ID: "node-b", Address: hedge.URL, Healthy: true, MaxConcurrentTasks: 2, AvailableCapacity: 2,
			Models: []engine.Model{{Name: "shared-model", Engine: "ollama", Tags: []string{"general"}}},
		},
	} {
		srv.registry.Upsert(node)
	}
	task, err := srv.tasks.CreateChecked("hedge one subtask", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{
			ID: "work", Description: "Run the work", TaskType: "general", Tags: []string{"general"},
		},
		NodeID: "node-a", Address: primary.URL, Model: "shared-model", Engine: "ollama",
	}})
	if err != nil {
		t.Fatal(err)
	}

	dispatchDone := make(chan struct{})
	go func() {
		srv.dispatcher.dispatchWithContext(context.Background(), task.ID, task.Subtasks[0].Assignment)
		close(dispatchDone)
	}()
	select {
	case <-primaryStarted:
	case <-time.After(2 * time.Second):
		current, _ := srv.tasks.Get(task.ID)
		t.Fatalf("primary dispatch did not start; task state: %+v", current)
	}
	select {
	case <-dispatchDone:
	case <-time.After(3 * time.Second):
		t.Fatal("hedged dispatch did not finish")
	}
	select {
	case <-primaryCancelled:
	case <-time.After(time.Second):
		t.Fatal("losing primary dispatch was not cancelled")
	}

	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusCompleted ||
		completed.Subtasks[0].Output != "hedge output" ||
		completed.Subtasks[0].Assignment.NodeID != "node-b" {
		t.Fatalf("hedge winner was not persisted: %+v", completed)
	}
	attempts := completed.Subtasks[0].Attempts
	if len(attempts) != 2 {
		t.Fatalf("dispatch attempts = %+v, want primary and hedge", attempts)
	}
	var winner, loser bool
	for _, attempt := range attempts {
		switch {
		case attempt.NodeID == "node-b" && attempt.Status == taskmgr.DispatchAttemptSucceeded:
			winner = true
		case attempt.NodeID == "node-a" && attempt.Status == taskmgr.DispatchAttemptFailed &&
			attempt.FailureClass == "hedge_loser":
			loser = true
		}
	}
	if !winner || !loser {
		t.Fatalf("attempt outcomes = %+v, want hedge winner and cancelled primary loser", attempts)
	}
	if primaryCalls.Load() != 1 || hedgeCalls.Load() != 1 {
		t.Fatalf("worker calls = primary %d, hedge %d; want one each", primaryCalls.Load(), hedgeCalls.Load())
	}
	for _, nodeID := range []string{"node-a", "node-b"} {
		node, found := srv.registry.Get(nodeID)
		if !found || node.ActiveTasks != 0 {
			t.Fatalf("node %s load after hedge = %+v, found=%t; want zero active tasks", nodeID, node, found)
		}
	}
	experiences := srv.Memory().Experiences(100)
	var taskExperiences int
	for _, experience := range experiences {
		if experience.Description == "Run the work" {
			taskExperiences++
			if !experience.Success || experience.NodeID != "node-b" || experience.Output != "hedge output" {
				t.Errorf("unexpected hedge task experience: %+v", experience)
			}
		}
	}
	if taskExperiences != 1 {
		t.Fatalf("task experiences = %d, want winner only", taskExperiences)
	}
}
