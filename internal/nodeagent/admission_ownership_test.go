package nodeagent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/workpool"
)

func TestPeerSubmissionRejectsSaturatedQueueAndPersistsFailure(t *testing.T) {
	a := newTestAgent(t, "admission-node")
	a.peerComponent.dispatchPool.Close()
	started := make(chan struct{})
	a.peerComponent.dispatchPool = workpool.New[peerDispatchJob](1, 1, func(ctx context.Context, _ peerDispatchJob) {
		close(started)
		<-ctx.Done()
	})
	if err := a.peerComponent.dispatchPool.Submit(context.Background(), peerDispatchJob{}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := a.peerComponent.dispatchPool.Submit(context.Background(), peerDispatchJob{}); err != nil {
		t.Fatal(err)
	}
	a.agentState.mu.Lock()
	a.agentState.peers = []cluster.NodeStatus{{
		ID: "worker", Address: "http://worker.invalid", Healthy: true,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	}}
	a.agentState.mu.Unlock()
	rec := httptest.NewRecorder()
	a.handleSubmit(rec, httptest.NewRequest(http.MethodPost, "/submit",
		strings.NewReader(`{"description":"one operation"}`)))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), workpool.ErrFull.Error()) {
		t.Fatalf("peer overload = %d %s", rec.Code, rec.Body.String())
	}
	tasks := a.orchestratorLink.tasks.All()
	if len(tasks) != 1 || tasks[0].Status != taskmgr.StatusFailed ||
		!strings.Contains(rec.Body.String(), tasks[0].ID) {
		t.Fatalf("rejected peer dispatch was not recorded: %+v", tasks)
	}
	t.Run("canceled before persistence", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/submit",
			strings.NewReader(`{"description":"already canceled"}`)).WithContext(ctx)
		a.handleSubmit(rec, req)
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), context.Canceled.Error()) {
			t.Fatalf("canceled peer submission = %d %s", rec.Code, rec.Body.String())
		}
		if after := a.orchestratorLink.tasks.All(); len(after) != len(tasks) {
			t.Fatalf("canceled peer submission persisted work: %+v", after)
		}
	})
}
