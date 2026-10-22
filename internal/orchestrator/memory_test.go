package orchestrator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
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

func TestMemorySyncRequestUsesConfiguredBodyLimit(t *testing.T) {
	srv := newTestServer(t)
	srv.syncMaxBytes = 4
	request := httptest.NewRequest(http.MethodPost, "/api/memory/sync", strings.NewReader(`{"node_id":"too-large"}`))
	response := httptest.NewRecorder()
	srv.handleMemorySync(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("memory sync returned %d, want 413: %s", response.Code, response.Body.String())
	}
}

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

func TestMemorySyncReplicatesNodeKnowledgeAndExperience(t *testing.T) {
	srv := newTestServer(t)
	now := time.Now()
	snapshot := memory.Snapshot{
		Experiences: []memory.Experience{
			{Kind: "generate", TaskType: "code", Description: "write code", NodeID: "node-a", Model: "coder", Success: true, DurationMs: 100, CreatedAt: now},
			{Kind: "generate", TaskType: "code", Description: "write code", NodeID: "node-a", Model: "coder", Success: true, DurationMs: 100, CreatedAt: now.Add(time.Second)},
			{Kind: "generate", TaskType: "code", Description: "write code", NodeID: "node-a", Model: "coder", Success: true, DurationMs: 100, CreatedAt: now.Add(2 * time.Second)},
		},
		Facts: []memory.Fact{{
			Key: "node-capability", Value: "node-a is reliable for code", Scope: "cluster",
			Source: "node-a", CreatedAt: now, UpdatedAt: now,
		}},
		Performances: []memory.Performance{{
			NodeID: "node-a", Model: "coder", TaskType: "code", Attempts: 3,
			Successes: 3, TotalDuration: 300, AvgDurationMs: 100,
			SuccessRate: 1, LastUsed: now.Add(2 * time.Second),
		}},
		Dialogues: []memory.DialogueRecord{{
			ID: "cluster-dialogue", Participants: []string{"node-a", "orchestrator"},
			Turns:     []memory.Turn{{Speaker: "node-a", Content: "use the code model", At: now}},
			CreatedAt: now, UpdatedAt: now,
		}},
	}
	body, err := json.Marshal(memorySyncRequest{NodeID: "node-a", Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/memory/sync", bytes.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("memory sync returned %d: %s", response.Code, response.Body.String())
	}
	if bias := srv.mem.LearnedBias("node-a", "coder", "code"); bias <= 0 {
		t.Fatalf("orchestrator did not learn from replicated experience, bias = %.2f", bias)
	}

	response = httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/memory/sync", nil))
	var merged memory.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&merged); err != nil {
		t.Fatalf("decode merged memory: %v", err)
	}
	if len(merged.Experiences) != 3 || len(merged.Facts) != 1 || len(merged.Dialogues) != 1 {
		t.Fatalf("orchestrator snapshot did not retain all replicated memory: %+v", merged)
	}
}

func TestMemorySyncGetReturnsIncrementalSnapshotForCurrentPeer(t *testing.T) {
	srv := newTestServer(t)
	for range 3 {
		if _, err := srv.mem.Record(memory.Experience{Description: "record"}); err != nil {
			t.Fatal(err)
		}
	}
	full := srv.mem.Snapshot()
	origin := full.Experiences[0].Origin
	request := httptest.NewRequest(http.MethodGet, "/api/memory/sync", nil)
	if err := memory.SetSyncRequestHeaders(request, map[string]uint64{origin: 2}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("incremental memory sync returned %d: %s", response.Code, response.Body.String())
	}
	var incremental memory.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&incremental); err != nil {
		t.Fatal(err)
	}
	if incremental.SyncVersion != memory.SyncProtocolVersion || len(incremental.Experiences) != 1 {
		t.Fatalf("incremental snapshot = version %d, experiences %d", incremental.SyncVersion, len(incremental.Experiences))
	}
	if got := srv.metrics.snapshot().syncPages; got != 1 {
		t.Fatalf("served sync pages = %d, want 1", got)
	}

	legacyResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(legacyResponse, httptest.NewRequest(http.MethodGet, "/api/memory/sync", nil))
	var legacy memory.Snapshot
	if err := json.NewDecoder(legacyResponse.Body).Decode(&legacy); err != nil {
		t.Fatal(err)
	}
	if len(legacy.Experiences) != 3 {
		t.Fatalf("legacy peer received %d experiences, want full snapshot", len(legacy.Experiences))
	}
	if got := srv.metrics.snapshot().syncPages; got != 1 {
		t.Fatalf("legacy request changed served sync page count to %d", got)
	}
}

func TestMemorySyncGetNegotiatesEmptyAndGzip(t *testing.T) {
	srv := newTestServer(t)
	for _, test := range []struct {
		version int
		status  int
	}{
		{version: memory.SyncPageProtocolVersion, status: http.StatusOK},
		{version: memory.SyncProtocolVersion, status: http.StatusNoContent},
	} {
		request := httptest.NewRequest(http.MethodGet, "/api/memory/sync", nil)
		if err := memory.SetSyncHeadersVersion(request.Header, nil, test.version); err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		srv.Mux().ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("protocol %d empty memory status = %d, want %d", test.version, response.Code, test.status)
		}
		version, _, err := memory.ReadSyncHeaders(response.Header())
		if err != nil {
			t.Fatal(err)
		}
		if version != test.version {
			t.Fatalf("protocol %d response version = %d", test.version, version)
		}
	}

	if _, err := srv.mem.Record(memory.Experience{Description: strings.Repeat("memory-data ", 200)}); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/memory/sync", nil)
	if err := memory.SetSyncHeadersVersion(request.Header, nil, memory.SyncProtocolVersion); err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", "gzip")
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if got := response.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("memory sync Content-Encoding = %q, want gzip", got)
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	var snapshot memory.Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Experiences) != 1 {
		t.Fatalf("decoded memory experience count = %d, want 1", len(snapshot.Experiences))
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
