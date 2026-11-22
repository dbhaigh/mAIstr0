package nodeagent

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestPeerDispatchLostResponseRequiresReconciliation(t *testing.T) {
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("test worker does not support connection hijacking")
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack worker response: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer worker.Close()

	a := newTestAgent(t, "peer-dispatch")
	a.agentState.mu.Lock()
	a.agentState.peers = []cluster.NodeStatus{{
		ID: "worker", Address: worker.URL, Healthy: true,
	}}
	a.agentState.mu.Unlock()
	assignment := scheduler.Assignment{
		Subtask: scheduler.Subtask{ID: "work", Description: "Run work", TaskType: "general"},
		NodeID:  "worker", Address: worker.URL, Model: "model-a", Engine: "ollama",
	}
	task, err := a.orchestratorLink.tasks.CreateChecked("peer dispatch", []scheduler.Assignment{assignment})
	if err != nil {
		t.Fatal(err)
	}
	ctx, ok := a.orchestratorLink.tasks.Context(task.ID)
	if !ok {
		t.Fatal("task context not found")
	}

	a.peerComponent.dispatchToPeer(ctx, task.ID, assignment)

	current, ok := a.orchestratorLink.tasks.Get(task.ID)
	if !ok || current.Status != taskmgr.StatusFailed || current.Resumable ||
		!current.HasUnknownDispatchOutcome() || calls.Load() != 1 {
		t.Fatalf("lost peer response state = %+v after %d calls", current, calls.Load())
	}
	attempt := current.Subtasks[0].Attempts[0]
	if attempt.Status != taskmgr.DispatchAttemptFailed ||
		attempt.FailureClass != taskmgr.DispatchFailureClassOutcomeUnknown ||
		!strings.Contains(attempt.Error, "reconciliation required") ||
		!strings.Contains(current.Subtasks[0].Error, "outcome unknown") {
		t.Fatalf("peer dispatch attempt = %+v, subtask error %q", attempt, current.Subtasks[0].Error)
	}
	if _, err := a.orchestratorLink.tasks.ResumeTaskChecked(task.ID); !errors.Is(err, taskmgr.ErrOutcomeUnknown) {
		t.Fatalf("peer task resume error = %v, want ErrOutcomeUnknown", err)
	}
}
