package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestMemoryAPIRoundTrip(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.Mux()

	// Remember a fact.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/memory/facts",
		strings.NewReader(`{"key":"gpu-node","value":"node-a has the only GPU"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST fact returned %d: %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/memory/facts", nil))
	var facts []memory.Fact
	if err := json.NewDecoder(rec.Body).Decode(&facts); err != nil {
		t.Fatalf("decode facts: %v", err)
	}
	if len(facts) != 1 || facts[0].Key != "gpu-node" {
		t.Fatalf("unexpected facts: %+v", facts)
	}

	// Record experience directly, then confirm it is searchable over HTTP.
	_, _ = srv.mem.Record(memory.Experience{
		TaskType: "code", NodeID: "node-a", Model: "coder",
		Description: "generate a websocket handler", Success: true, DurationMs: 700,
	})

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/memory/experiences?q=websocket", nil))
	var found []memory.Experience
	if err := json.NewDecoder(rec.Body).Decode(&found); err != nil {
		t.Fatalf("decode experiences: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("expected 1 recalled experience, got %d", len(found))
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/memory/insights", nil))
	var insights memory.Insights
	if err := json.NewDecoder(rec.Body).Decode(&insights); err != nil {
		t.Fatalf("decode insights: %v", err)
	}
	if insights.TotalExperiences != 1 || insights.Facts != 1 {
		t.Errorf("unexpected insights: %+v", insights)
	}

	// Forget the fact again.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/memory/facts/gpu-node", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE fact returned %d", rec.Code)
	}
}

func TestMemorySyncMergesNodeLearning(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.Mux()

	payload := `{"node_id":"node-a","performances":[{"node_id":"node-a","model":"coder","task_type":"code","attempts":40,"successes":38,"success_rate":0.95,"avg_duration_ms":900}]}`
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/memory/sync", strings.NewReader(payload)))
	if rec.Code != http.StatusOK {
		t.Fatalf("sync returned %d: %s", rec.Code, rec.Body.String())
	}

	if bias := srv.mem.LearnedBias("node-a", "coder", "code"); bias <= 0 {
		t.Errorf("expected node-reported success history to bias routing positively, got %.2f", bias)
	}
}

func TestSchedulerLearnsFromDispatchOutcomes(t *testing.T) {
	srv := newTestServer(t)

	nodes := []cluster.NodeStatus{
		{
			ID: "reliable", Address: "http://reliable", Healthy: true,
			Hardware: hardware.Info{CPUCores: 8, Score: 50},
			Models:   []engine.Model{{Name: "coder", Tags: []string{"code"}}},
		},
		{
			ID: "flaky", Address: "http://flaky", Healthy: true,
			Hardware: hardware.Info{CPUCores: 8, Score: 50},
			Models:   []engine.Model{{Name: "coder", Tags: []string{"code"}}},
		},
	}
	subtask := scheduler.Subtask{ID: "s1", Description: "write code", TaskType: "code"}

	// Teach the cluster that "flaky" keeps failing this kind of work.
	for i := 0; i < 10; i++ {
		srv.recordDispatch(scheduler.Assignment{
			Subtask: subtask, NodeID: "reliable", Model: "coder",
		}, "ok", nil, 800*time.Millisecond)
		srv.recordDispatch(scheduler.Assignment{
			Subtask: subtask, NodeID: "flaky", Model: "coder",
		}, "", errTimeout{}, 40*time.Second)
	}

	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{subtask}, nodes, srv.experience())
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	if assignments[0].NodeID != "reliable" {
		t.Errorf("scheduler ignored learned history, picked %s", assignments[0].NodeID)
	}
}

type errTimeout struct{}

func (errTimeout) Error() string { return "node timed out" }
