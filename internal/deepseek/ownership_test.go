package deepseek

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

type ownershipTransport func(*http.Request) (*http.Response, error)

func (f ownershipTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestParallelDispatchBoundsWorkersAndCancelsWaitingSubmissions(t *testing.T) {
	started := make(chan struct{}, 4)
	client := &http.Client{Transport: ownershipTransport(func(r *http.Request) (*http.Response, error) {
		started <- struct{}{}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{
		ID: "worker", Address: "http://worker.invalid", Healthy: true, MaxConcurrentTasks: 1,
		Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
	})
	h := New(registry, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		output any
		err    error
	}
	done := make(chan result, 1)
	go func() {
		output, _, _, _, err := (&ClusterParallelDispatchTool{}).Execute(ctx, h, &Session{}, map[string]any{
			"subtasks": []any{
				map[string]any{"description": "one"}, map[string]any{"description": "two"},
				map[string]any{"description": "three"}, map[string]any{"description": "four"},
			},
		})
		done <- result{output, err}
	}()
	<-started
	select {
	case <-started:
		t.Error("parallel tool exceeded its advertised worker budget")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		results := got.output.(map[string]any)["results"].([]parallelResult)
		if len(results) != 4 {
			t.Fatalf("results = %d, want every input", len(results))
		}
		for _, result := range results {
			if !strings.Contains(result.Error, context.Canceled.Error()) {
				t.Errorf("canceled result lacks error: %+v", result)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not release dispatch and waiting submissions")
	}
	node, _ := registry.Get("worker")
	if node.ActiveTasks != 0 {
		t.Fatalf("parallel dispatch retained load: %d", node.ActiveTasks)
	}
}
