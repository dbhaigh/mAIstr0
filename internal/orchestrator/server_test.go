package orchestrator

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/deepseek"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/piagent"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"sync"
	"sync/atomic"
)

// newTestServer builds an orchestrator with its memory database isolated to
// the test's temp directory so runs never touch the real user store.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	srv, err := NewWithMemoryAndHarness(filepath.Join(t.TempDir(), "orch.db"), "deepseek")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = srv.Close()
	})
	return srv
}

func TestNewWithConfigNormalizesAndValidates(t *testing.T) {
	cfg := config.OrchestratorConfig{
		Harness:                             " DEEPSEEK ",
		DiscoveryEnabled:                    true,
		DispatchTimeoutSeconds:              90,
		SyncMaxBytes:                        2 << 20,
		SyncTimeoutSeconds:                  37,
		RouterTimeToFirstByteTimeoutSeconds: 17,
		RouterIdleStreamTimeoutSeconds:      8,
		RouterRequestsPerMinute:             12,
		RouterMaxConcurrentPerClient:        3,
		MemoryPath:                          filepath.Join(t.TempDir(), "orchestrator.db"),
	}
	srv, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("close test server: %v", err)
		}
	})
	if srv.HarnessName() != "deepseek" {
		t.Fatalf("harness = %q, want deepseek", srv.HarnessName())
	}
	if srv.dispatcher.limit != config.DefaultDispatchConcurrency {
		t.Fatalf("dispatch limit = %d, want %d", srv.dispatcher.limit, config.DefaultDispatchConcurrency)
	}
	if srv.dispatcher.dispatchTimeout != 90*time.Second {
		t.Fatalf("dispatch timeout = %s, want 90s", srv.dispatcher.dispatchTimeout)
	}
	if srv.maxSyncBytes() != 2<<20 || srv.syncTimeout != 37*time.Second ||
		srv.nodeClient.sync.Timeout != 37*time.Second {
		t.Fatalf("sync config = %d bytes/%s client timeout %s, want %d bytes/37s",
			srv.maxSyncBytes(), srv.syncTimeout, srv.nodeClient.sync.Timeout, 2<<20)
	}
	if srv.routerTTFBTimeout != 17*time.Second || srv.routerIdleTimeout != 8*time.Second ||
		srv.routerClients.requestsPerMinute != 12 || srv.routerClients.maxConcurrent != 3 {
		t.Fatalf("router config = %s TTFB/%s idle/%d requests/min/%d concurrent",
			srv.routerTTFBTimeout, srv.routerIdleTimeout,
			srv.routerClients.requestsPerMinute, srv.routerClients.maxConcurrent)
	}

	cfg.DiscoveryEnabled = false
	if _, err := NewWithConfig(cfg); err == nil || !strings.Contains(err.Error(), "discovery_enabled") {
		t.Fatalf("invalid config error = %v, want discovery_enabled validation error", err)
	}
}

func TestMinimalModeOmitsOptionalRoutesAndKeepsClusterRouting(t *testing.T) {
	cfg := config.DefaultOrchestratorConfig()
	cfg.Mode = config.OrchestratorModeMinimal
	cfg.MemoryPath = filepath.Join(t.TempDir(), "minimal.db")
	srv, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("close minimal-mode server: %v", err)
		}
	})
	if srv.harness != nil {
		t.Fatal("minimal mode initialized an agent harness")
	}
	if srv.Memory() != nil {
		t.Fatal("minimal mode opened the learning memory store")
	}

	mux := srv.Mux()
	for _, path := range []string{
		"/api/agent/sessions",
		"/api/agent/backend",
		"/api/memory/experiences",
		"/api/memory/sync",
	} {
		_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil))
		if pattern != "/" {
			t.Errorf("optional route %s is still registered as %q", path, pattern)
		}
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Errorf("optional route %s returned %d, want 404", path, response.Code)
		}
	}
	for _, route := range []struct {
		method, path, wantPattern string
	}{
		{http.MethodGet, "/api/status", "GET /api/status"},
		{http.MethodGet, "/api/metrics", "GET /api/metrics"},
		{http.MethodGet, "/metrics", "GET /metrics"},
		{http.MethodGet, "/api/cluster/health", "GET /api/cluster/health"},
		{http.MethodPost, "/router/v1/chat/completions", "POST /router/{path...}"},
		{http.MethodGet, "/v1/models", "GET /v1/models"},
	} {
		_, pattern := mux.Handler(httptest.NewRequest(route.method, route.path, nil))
		if pattern != route.wantPattern {
			t.Errorf("%s %s matched %q, want %q", route.method, route.path, pattern, route.wantPattern)
		}
	}

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	var status struct {
		Persistence struct {
			Available bool   `json:"available"`
			Warning   string `json:"warning"`
		} `json:"persistence"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Persistence.Available || status.Persistence.Warning != "" {
		t.Fatalf("minimal-mode persistence status = %+v, want disabled without an unavailable warning", status.Persistence)
	}
}

func TestYieldedDashboardPreservesLocalPairingAccess(t *testing.T) {
	srv := newTestServer(t)
	if err := srv.SetLocalNodeDashboard("http://localhost:7451"); err != nil {
		t.Fatal(err)
	}
	handler := peeridentity.LoopbackOnly(http.HandlerFunc(srv.handleYieldedDashboard))
	for _, test := range []struct {
		method, path, remote string
		code                 int
	}{
		{http.MethodGet, "/", "127.0.0.1:1234", http.StatusTemporaryRedirect},
		{http.MethodGet, "/api/pairing/status", "127.0.0.1:1234", http.StatusOK},
		{http.MethodPost, "/api/nodes/register", "127.0.0.1:1234", http.StatusServiceUnavailable},
		{http.MethodPost, "/api/pairing/join", "127.0.0.1:1234", http.StatusServiceUnavailable},
		{http.MethodGet, "/", "192.0.2.1:1234", http.StatusForbidden},
		{http.MethodGet, "/api/pairing/status", "192.0.2.1:1234", http.StatusForbidden},
	} {
		t.Run(test.method+test.path+test.remote, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Host = "localhost"
			request.RemoteAddr = test.remote
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.code {
				t.Fatalf("returned %d, want %d: %s", response.Code, test.code, response.Body.String())
			}
			if test.code == http.StatusTemporaryRedirect && response.Header().Get("Location") != "http://localhost:7451/" {
				t.Fatalf("redirect = %q", response.Header().Get("Location"))
			}
			if test.code == http.StatusOK {
				var status map[string]string
				if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
					t.Fatal(err)
				}
				if status["role"] != "node" || status["dashboard_url"] != "http://localhost:7451" {
					t.Fatalf("handoff status = %#v", status)
				}
			}
		})
	}
}

func TestLocalNodeDashboardRejectsNonLoopbackTargets(t *testing.T) {
	srv := newTestServer(t)
	for _, address := range []string{
		"https://localhost:7451", "http://192.0.2.1:7451", "http://example.com:7451",
		"http://user@localhost:7451", "http://localhost:7451/path", "http://localhost:7451?query=1",
	} {
		if err := srv.SetLocalNodeDashboard(address); err == nil {
			t.Errorf("accepted dashboard target %q", address)
		}
	}
	response := httptest.NewRecorder()
	srv.handleYieldedDashboard(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured handoff returned %d", response.Code)
	}
}

func TestNodeRegistrationPreservesGPUHistory(t *testing.T) {
	srv := newTestServer(t)
	sample := hardware.GPUUsageSample{
		Timestamp: time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC),
		Available: true, Utilization: 73,
	}
	body, err := json.Marshal(cluster.NodeStatus{
		ID: "telemetry-node", Address: "https://127.0.0.1:7451",
		GPUHistory: []hardware.GPUUsageSample{sample},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/nodes/register", bytes.NewReader(body)))
	if response.Code != http.StatusNoContent {
		t.Fatalf("registration returned %d: %s", response.Code, response.Body.String())
	}
	registered, ok := srv.registry.Get("telemetry-node")
	if !ok || len(registered.GPUHistory) != 1 || registered.GPUHistory[0] != sample {
		t.Fatalf("registered GPU history = %#v", registered.GPUHistory)
	}
}

func TestNodeRegistrationPreservesNPUHistory(t *testing.T) {
	srv := newTestServer(t)
	sample := hardware.NPUUsageSample{
		Timestamp: time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC),
		Available: true, Utilization: 63,
	}
	body, err := json.Marshal(cluster.NodeStatus{
		ID: "npu-telemetry-node", Address: "https://127.0.0.1:7451",
		NPUHistory: []hardware.NPUUsageSample{sample},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/nodes/register", bytes.NewReader(body)))
	if response.Code != http.StatusNoContent {
		t.Fatalf("registration returned %d: %s", response.Code, response.Body.String())
	}
	registered, ok := srv.registry.Get("npu-telemetry-node")
	if !ok || len(registered.NPUHistory) != 1 || registered.NPUHistory[0] != sample {
		t.Fatalf("registered NPU history = %#v", registered.NPUHistory)
	}
}

func TestNodeRegistrationPreservesSystemHistory(t *testing.T) {
	srv := newTestServer(t)
	sample := hardware.SystemUsageSample{
		Timestamp:    time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC),
		CPUAvailable: true, CPUUtilization: 42, MemoryAvailable: true, MemoryUtilization: 67,
	}
	body, err := json.Marshal(cluster.NodeStatus{
		ID: "system-history-node", Address: "https://127.0.0.1:7452",
		SystemHistory: []hardware.SystemUsageSample{sample},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/nodes/register", bytes.NewReader(body)))
	if response.Code != http.StatusNoContent {
		t.Fatalf("registration returned %d: %s", response.Code, response.Body.String())
	}
	registered, ok := srv.registry.Get("system-history-node")
	if !ok || len(registered.SystemHistory) != 1 || registered.SystemHistory[0] != sample {
		t.Fatalf("registered system history = %#v", registered.SystemHistory)
	}
}

func TestClusterViewsRetainOfflineMembers(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{ID: "online", Address: "https://online"})
	srv.registry.Upsert(cluster.NodeStatus{ID: "offline", Address: "https://offline"})
	srv.registry.MarkUnhealthy("offline")

	activeResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(activeResponse, httptest.NewRequest(http.MethodGet, "/api/nodes", nil))
	if activeResponse.Code != http.StatusOK {
		t.Fatalf("GET /api/nodes returned %d: %s", activeResponse.Code, activeResponse.Body.String())
	}
	var active []cluster.NodeStatus
	if err := json.Unmarshal(activeResponse.Body.Bytes(), &active); err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != "online" {
		t.Fatalf("active nodes = %#v, want only the online member", active)
	}

	allResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(allResponse, httptest.NewRequest(http.MethodGet, "/api/nodes/all", nil))
	if allResponse.Code != http.StatusOK {
		t.Fatalf("GET /api/nodes/all returned %d: %s", allResponse.Code, allResponse.Body.String())
	}
	var all []cluster.NodeStatus
	if err := json.Unmarshal(allResponse.Body.Bytes(), &all); err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all nodes = %#v, want online and offline members", all)
	}
	foundOffline := false
	for _, member := range all {
		if member.ID == "offline" && !member.Healthy {
			foundOffline = true
		}
	}
	if !foundOffline {
		t.Fatalf("offline member missing or marked healthy: %#v", all)
	}

	snapshot := srv.snapshot()
	if snapshot.MemberCount != 1 || len(snapshot.Nodes) != 2 {
		t.Fatalf("snapshot has %d members and %d nodes, want 1 online member and 2 known nodes", snapshot.MemberCount, len(snapshot.Nodes))
	}
}

func TestHarnessSelection(t *testing.T) {
	defaultAgent, err := NewWithMemory(filepath.Join(t.TempDir(), "default.db"))
	if err != nil {
		t.Fatal(err)
	}
	if defaultAgent.HarnessName() != "pi" {
		t.Fatalf("default backend = %q, want pi", defaultAgent.HarnessName())
	}
	defer defaultAgent.Close()

	deepseek, err := NewWithMemoryAndHarness(filepath.Join(t.TempDir(), "deepseek.db"), "deepseek")
	if err != nil {
		t.Fatal(err)
	}
	if deepseek.HarnessName() != "deepseek" {
		t.Fatalf("expected deepseek harness, got %q", deepseek.HarnessName())
	}
	defer deepseek.Close()

	pi, err := NewWithMemoryAndHarness(filepath.Join(t.TempDir(), "pi.db"), "pi")
	if err != nil {
		t.Fatal(err)
	}
	if pi.HarnessName() != "pi" {
		t.Fatalf("expected pi backend, got %q", pi.HarnessName())
	}
	defer pi.Close()

	if _, err := NewWithMemoryAndHarness(filepath.Join(t.TempDir(), "unknown.db"), "unknown"); err == nil {
		t.Fatal("expected unknown backend to return an error")
	}
}

func TestPiProviderRoutesToLocalClusterAndParsesTools(t *testing.T) {
	toolOutput := `<tool_call>{"name":"cluster_status","arguments":{}}</tool_call>`
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate" || r.URL.Query().Get("stream") != "true" {
			http.NotFound(w, r)
			return
		}
		var request generateAPIRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if request.Model != "local-model" {
			http.Error(w, "unexpected model "+request.Model, http.StatusBadRequest)
			return
		}
		payload, _ := json.Marshal(map[string]string{"output": toolOutput})
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
	}))
	defer worker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "local-worker", Address: worker.URL, Healthy: true, FastScore: 100,
		Models:       []engine.Model{{Name: "local-model", Tags: []string{"general"}}},
		DefaultModel: "local-model",
	})
	provider := &localPiProvider{generate: srv}
	var streamed strings.Builder
	completion, err := provider.Complete(context.Background(), piagent.CompletionRequest{
		Model: "local-model",
		Messages: []piagent.ChatMessage{
			{Role: "system", Content: "Coordinate local cluster work."},
			{Role: "user", Content: "Check cluster status."},
		},
		Tools: []agent.ToolDef{{
			Name: "cluster_status", Description: "Inspect cluster health",
			Parameters: map[string]any{"type": "object", "properties": map[string]any{}},
		}},
	}, func(delta string) { streamed.WriteString(delta) })
	if err != nil {
		t.Fatalf("complete with local cluster: %v", err)
	}
	if streamed.String() != toolOutput {
		t.Fatalf("streamed output = %q, want %q", streamed.String(), toolOutput)
	}
	if completion.Content != "" || len(completion.ToolCalls) != 1 ||
		completion.ToolCalls[0].Function.Name != "cluster_status" {
		t.Fatalf("local completion = %+v", completion)
	}
}

func TestPiExecutesClusterToolsAndReceivesResults(t *testing.T) {
	var generation int
	var toolNames []string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate" {
			http.NotFound(w, r)
			return
		}
		var request generateAPIRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		generation++
		output := `<tool_call>{"name":"cluster_status","arguments":{}}</tool_call>`
		if generation == 1 {
			for _, name := range toolNames {
				if !strings.Contains(request.Prompt, name) {
					t.Errorf("Pi prompt omitted registered tool %q", name)
				}
				if !strings.Contains(request.Prompt, fmt.Sprintf("- `%s`:", name)) {
					t.Errorf("Pi prompt omitted executable guidance for tool %q", name)
				}
			}
			for _, guidance := range []string{
				"`cluster_status`:", "`node_converse`:", "`cluster_parallel_dispatch`:",
				"`recall_memory`:", "`remember_fact`:",
			} {
				if !strings.Contains(request.Prompt, guidance) {
					t.Errorf("Pi prompt omitted tool usage guidance %q", guidance)
				}
			}
			if !strings.Contains(request.Prompt, "if the user asks you to run all available tools, call every tool listed below") {
				t.Error("Pi prompt does not require running every tool when explicitly instructed")
			}
		} else {
			if !strings.Contains(request.Prompt, "<tool_response>") ||
				!strings.Contains(request.Prompt, "Tool call ID: call_") ||
				!strings.Contains(request.Prompt, `"total_nodes":1`) {
				t.Errorf("Pi follow-up prompt omitted tool result: %s", request.Prompt)
			}
			output = "The cluster has one available worker."
		}
		payload, err := json.Marshal(map[string]string{"output": output})
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
	}))
	defer worker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "local-worker", Address: worker.URL, Healthy: true, FastScore: 100,
		Models:       []engine.Model{{Name: "local-model", Tags: []string{"general"}}},
		DefaultModel: "local-model",
	})
	backend := srv.harness.backend("pi")
	toolSet := make(map[string]bool)
	for _, tool := range backend.ListTools() {
		toolNames = append(toolNames, tool.Name)
		toolSet[tool.Name] = true
	}
	for _, name := range []string{
		"cluster_status", "list_node_models", "node_converse", "node_collaborate",
		"cluster_converse", "node_llm_query", "cluster_llm_query", "cluster_parallel_dispatch",
		"test_node_model", "list_node_dialogues", "get_node_dialogue", "eval_expression",
		"fetch_web", "session_memory", "recall_memory", "remember_fact", "memory_insights",
		"node_to_node_conversation",
	} {
		if !toolSet[name] {
			t.Errorf("Pi tool catalog is missing %q", name)
		}
	}
	if piTools, deepSeekTools := backend.ListTools(), srv.harness.backend("deepseek").ListTools(); !reflect.DeepEqual(piTools, deepSeekTools) {
		t.Fatalf("Pi tools differ from DeepSeek tools:\nPi: %#v\nDeepSeek: %#v", piTools, deepSeekTools)
	}
	if piTools, deepSeekTools := backend.ListTools(), srv.harness.backend("deepseek").ListTools(); !reflect.DeepEqual(piTools, deepSeekTools) {
		t.Fatalf("Pi tools differ from DeepSeek tools:\nPi: %#v\nDeepSeek: %#v", piTools, deepSeekTools)
	}
	session, err := backend.CreateSession("tool access", agent.SessionConfig{CoordinatorModel: "local-model"})
	if err != nil {
		t.Fatal(err)
	}
	final, err := backend.SendMessage(context.Background(), session.ID, "Check the cluster status.", nil)
	if err != nil {
		t.Fatal(err)
	}
	if final.Content != "The cluster has one available worker." || generation != 2 {
		t.Fatalf("Pi final response = %q after %d generations", final.Content, generation)
	}
}

func TestAgentSessionHistoryRestoredAfterRestart(t *testing.T) {
	for _, harness := range []string{"pi", "deepseek"} {
		t.Run(harness, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "orch.db")
			first, err := NewWithMemoryAndHarness(path, harness)
			if err != nil {
				t.Fatal(err)
			}
			firstOpen := true
			t.Cleanup(func() {
				if firstOpen {
					_ = first.Close()
				}
			})

			request := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(`{"title":"Persist me"}`))
			response := httptest.NewRecorder()
			first.Mux().ServeHTTP(response, request)
			if response.Code != http.StatusCreated {
				t.Fatalf("create session returned %d: %s", response.Code, response.Body.String())
			}
			var created agent.Session
			if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
				t.Fatalf("decode created session: %v", err)
			}
			created.Messages = []agent.Message{
				{Role: agent.RoleUser, Content: "remember this"},
				{Role: agent.RoleAssistant, Content: "I will"},
			}
			if err := first.persistAgentSession(&created); err != nil {
				t.Fatalf("persist transcript: %v", err)
			}
			if err := first.Close(); err != nil {
				t.Fatalf("close first orchestrator: %v", err)
			}
			firstOpen = false

			restarted, err := NewWithMemoryAndHarness(path, harness)
			if err != nil {
				t.Fatalf("restart orchestrator: %v", err)
			}
			defer restarted.Close()
			request = httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+created.ID, nil)
			response = httptest.NewRecorder()
			restarted.Mux().ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("get restored session returned %d: %s", response.Code, response.Body.String())
			}
			var restored agent.Session
			if err := json.Unmarshal(response.Body.Bytes(), &restored); err != nil {
				t.Fatalf("decode restored session: %v", err)
			}
			if restored.Harness != harness || len(restored.Messages) != 2 || restored.Messages[0].Content != "remember this" {
				t.Fatalf("restored session = %+v", restored)
			}
		})
	}
}

func TestOllamaAndAnthropicMessagesCompatibility(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "compatibility response"})
	}))
	defer worker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "compat-worker", Address: worker.URL, Healthy: true,
		Models:       []engine.Model{{Name: "compat-model", Tags: []string{"general"}}},
		DefaultModel: "compat-model",
	})
	mux := srv.Mux()

	tests := []struct {
		path string
		body string
		want string
	}{
		{
			path: "/api/chat",
			body: `{"model":"compat-model","messages":[{"role":"user","content":"hello"}],"stream":false}`,
			want: `"message":{"role":"assistant","content":"compatibility response"}`,
		},
		{
			path: "/api/generate",
			body: `{"model":"compat-model","prompt":"hello","stream":false}`,
			want: `"response":"compatibility response"`,
		},
		{
			path: "/v1/messages",
			body: `{"model":"compat-model","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`,
			want: `"type":"message"`,
		},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("%s returned %d: %s", test.path, response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), test.want) {
				t.Fatalf("%s response %q does not contain %q", test.path, response.Body.String(), test.want)
			}
		})
	}

	streamTests := []struct {
		path string
		body string
		want string
	}{
		{
			path: "/api/chat",
			body: `{"model":"compat-model","messages":[{"role":"user","content":"hello"}],"stream":true}`,
			want: `"done":false`,
		},
		{
			path: "/v1/messages",
			body: `{"model":"compat-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hello"}]}`,
			want: "event: content_block_delta",
		},
	}
	for _, test := range streamTests {
		request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), test.want) {
			t.Fatalf("streaming %s returned %d: %s", test.path, response.Code, response.Body.String())
		}
		if test.path == "/v1/messages" {
			for _, event := range []string{"event: message_start", "event: content_block_delta", "event: content_block_stop", "event: message_stop"} {
				if !strings.Contains(response.Body.String(), event) {
					t.Errorf("Anthropic stream omitted %q: %s", event, response.Body.String())
				}
			}
		}
	}

	tags := httptest.NewRecorder()
	mux.ServeHTTP(tags, httptest.NewRequest(http.MethodGet, "/api/tags", nil))
	if tags.Code != http.StatusOK || !strings.Contains(tags.Body.String(), `"name":"compat-model"`) {
		t.Fatalf("GET /api/tags returned %d: %s", tags.Code, tags.Body.String())
	}
}

func TestModelCatalogReturnsBundledEngineSpecificEntries(t *testing.T) {
	srv := newTestServer(t)
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/models/catalog?engine=ollama&q=code", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("catalog returned %d: %s", response.Code, response.Body.String())
	}
	var models []engine.CatalogModel
	if err := json.Unmarshal(response.Body.Bytes(), &models); err != nil {
		t.Fatal(err)
	}
	if len(models) == 0 {
		t.Fatal("expected local catalog entries for Ollama code models")
	}
	for _, model := range models {
		if model.Engine != "ollama" || !strings.Contains(strings.ToLower(model.Name+" "+model.Description+" "+strings.Join(model.Tags, " ")), "code") {
			t.Fatalf("catalog did not honor engine/query filters: %+v", model)
		}
	}
}

func TestSchedulerNodesIncludeQueuedWorkflowAssignments(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "queue-worker", Address: "http://queue-worker", ActiveTasks: 2,
		Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "idle-worker", Address: "http://idle-worker",
		Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
	})
	_, err := srv.tasks.CreateChecked("queued work", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "waiting", TaskType: "general"},
		NodeID:  "queue-worker",
		Model:   "model",
	}})
	if err != nil {
		t.Fatal(err)
	}
	nodes := srv.schedulerNodes()
	if len(nodes) != 2 || nodes[0].ID != "idle-worker" || nodes[1].ID != "queue-worker" {
		t.Fatalf("scheduler nodes = %+v", nodes)
	}
	if nodes[1].ActiveTasks != 2 || nodes[1].QueuedTasks != 1 || nodes[0].QueuedTasks != 0 {
		t.Fatalf("active and queued load not combined: %+v", nodes)
	}
}

func TestAnthropicTextBlocksAndRequestValidation(t *testing.T) {
	var received struct {
		Prompt   string `json:"prompt"`
		MaxToken int    `json:"max_tokens"`
	}
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode worker request: %v", err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "ok"})
	}))
	defer worker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "anthropic-block-worker", Address: worker.URL, Healthy: true,
		Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
	})
	body := `{"model":"model","max_tokens":80,"system":[{"type":"text","text":"Be concise."}],"messages":[{"role":"user","content":[{"type":"text","text":"hello "},{"type":"text","text":"world"}]}]}`
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("valid Anthropic text blocks returned %d: %s", response.Code, response.Body.String())
	}
	for _, part := range []string{"Be concise.", "hello world"} {
		if !strings.Contains(received.Prompt, part) {
			t.Errorf("worker prompt omitted %q: %s", part, received.Prompt)
		}
	}
	if received.MaxToken != 80 {
		t.Fatalf("max_tokens = %d, want 80", received.MaxToken)
	}

	invalid := httptest.NewRecorder()
	srv.Mux().ServeHTTP(invalid, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(
		`{"model":"model","max_tokens":80,"messages":[{"role":"user","content":[{"type":"image","source":{}}]}]}`,
	)))
	if invalid.Code != http.StatusBadRequest ||
		!strings.Contains(invalid.Body.String(), `"type":"error"`) ||
		!strings.Contains(invalid.Body.String(), `"type":"invalid_request_error"`) {
		t.Fatalf("unsupported Anthropic content returned %d: %s", invalid.Code, invalid.Body.String())
	}
}

func TestPairingJoinPinsCertificateAndRotatesPIN(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	statusResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil))
	var pairingStatus struct {
		Role string `json:"role"`
		PIN  string `json:"pin"`
	}
	if statusResponse.Code != http.StatusOK ||
		json.Unmarshal(statusResponse.Body.Bytes(), &pairingStatus) != nil ||
		pairingStatus.Role != "orchestrator" || pairingStatus.PIN != pin {
		t.Fatalf("pairing status returned %d: %s", statusResponse.Code, statusResponse.Body.String())
	}
	node, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "pairing-node")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(pairingJoinRequest{
		ID: node.ID(), Role: "node", PIN: pin, CertificatePEM: node.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
	request.TLS = &tls.ConnectionState{}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("pairing returned %d: %s", response.Code, response.Body.String())
	}
	if !identity.IsTrustedID(node.ID()) {
		t.Fatal("node certificate was not pinned by orchestrator")
	}
	var joined pairingJoinResponse
	if err := json.Unmarshal(response.Body.Bytes(), &joined); err != nil {
		t.Fatal(err)
	}
	if joined.ID != srv.SelfID() || joined.Peers[node.ID()] != node.CertificatePEM() {
		t.Fatalf("pairing response did not return the trusted roster: %#v", joined)
	}

	replay := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
	replay.TLS = &tls.ConnectionState{}
	replayResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(replayResponse, replay)
	if replayResponse.Code != http.StatusUnauthorized {
		t.Fatalf("replayed PIN returned %d, want 401", replayResponse.Code)
	}

	plaintext := httptest.NewRecorder()
	srv.Mux().ServeHTTP(plaintext, httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body)))
	if plaintext.Code != http.StatusUpgradeRequired {
		t.Fatalf("plaintext pairing returned %d, want 426", plaintext.Code)
	}
}

func TestPairingJoinRejectsWrongPINFromRemotePeer(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	node, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "wrong-pin-node")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(pairingJoinRequest{
		ID: node.ID(), Role: "node", PIN: "000000", CertificatePEM: node.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
	request.RemoteAddr = "203.0.113.10:1234"
	request.TLS = &tls.ConnectionState{}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-PIN pairing returned %d, want 401: %s", response.Code, response.Body.String())
	}
	if identity.IsTrustedID(node.ID()) || !identity.VerifyPairingPIN(pin) {
		t.Fatal("wrong-PIN pairing changed the trust roster or consumed the active PIN")
	}
}

func TestAutoJoinOnSameSubnetWithoutPIN(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	activePIN, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}

	node, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "subnet-node")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(pairingJoinRequest{
		ID: node.ID(), Role: "node", PIN: "", CertificatePEM: node.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
	request.TLS = &tls.ConnectionState{}
	request.RemoteAddr = "127.0.0.1:54321" // loopback is on the same subnet
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("auto-join on same subnet returned %d: %s", response.Code, response.Body.String())
	}
	if !identity.IsTrustedID(node.ID()) {
		t.Fatal("node certificate was not pinned by orchestrator")
	}

	// The active pairing PIN must remain untouched so off-subnet nodes can still use it
	if !identity.VerifyPairingPIN(activePIN) {
		t.Fatal("active pairing PIN should not have been consumed by auto-join on same subnet")
	}
}

func TestAutoJoinOnSameSubnetWithInvalidPIN(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	activePIN, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}

	node, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "subnet-node-wrong-pin")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(pairingJoinRequest{
		ID: node.ID(), Role: "node", PIN: "000000", CertificatePEM: node.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
	request.TLS = &tls.ConnectionState{}
	request.RemoteAddr = "127.0.0.1:54321" // loopback is on the same subnet
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("auto-join with wrong PIN on same subnet returned %d: %s", response.Code, response.Body.String())
	}
	if !identity.IsTrustedID(node.ID()) {
		t.Fatal("node certificate was not pinned by orchestrator")
	}

	// Active PIN must still be valid
	if !identity.VerifyPairingPIN(activePIN) {
		t.Fatal("active pairing PIN should still be valid")
	}
}

func TestPairingFromDifferentSubnetRequiresPIN(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}

	node, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "remote-subnet-node")
	if err != nil {
		t.Fatal(err)
	}

	// Attempt 1: No PIN from remote subnet (192.0.2.1) -> must be rejected with 401
	bodyNoPIN, err := json.Marshal(pairingJoinRequest{
		ID: node.ID(), Role: "node", PIN: "", CertificatePEM: node.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	reqNoPIN := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(bodyNoPIN))
	reqNoPIN.TLS = &tls.ConnectionState{}
	reqNoPIN.RemoteAddr = "192.0.2.1:54321"
	respNoPIN := httptest.NewRecorder()
	srv.Mux().ServeHTTP(respNoPIN, reqNoPIN)
	if respNoPIN.Code != http.StatusUnauthorized {
		t.Fatalf("remote subnet join without PIN returned %d, want 401", respNoPIN.Code)
	}

	// Attempt 2: Valid PIN from remote subnet -> must succeed with 200
	bodyWithPIN, err := json.Marshal(pairingJoinRequest{
		ID: node.ID(), Role: "node", PIN: pin, CertificatePEM: node.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	reqWithPIN := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(bodyWithPIN))
	reqWithPIN.TLS = &tls.ConnectionState{}
	reqWithPIN.RemoteAddr = "192.0.2.1:54321"
	respWithPIN := httptest.NewRecorder()
	var pairingLogs bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&pairingLogs)
	defer log.SetOutput(previousOutput)
	srv.Mux().ServeHTTP(respWithPIN, reqWithPIN)
	if respWithPIN.Code != http.StatusOK {
		t.Fatalf("remote subnet join with valid PIN returned %d: %s", respWithPIN.Code, respWithPIN.Body.String())
	}
	if strings.Contains(pairingLogs.String(), pin) {
		t.Fatalf("pairing log leaked PIN %q: %s", pin, pairingLogs.String())
	}
	if !identity.IsTrustedID(node.ID()) {
		t.Fatal("node certificate was not pinned after valid PIN pairing")
	}
}

func TestSnapshotAndIdentityLogsDoNotExposePairingPIN(t *testing.T) {
	srv := newTestServer(t)
	var logs bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previousOutput)

	if err := srv.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	pin, _, active := srv.identity.ActivePairingPIN()
	if !active || pin == "" {
		t.Fatal("identity did not create an active pairing PIN")
	}
	if strings.Contains(logs.String(), pin) {
		t.Fatalf("identity initialization log leaked PIN %q: %s", pin, logs.String())
	}

	snapshot, err := json.Marshal(srv.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(snapshot), pin) {
		t.Fatalf("cluster snapshot leaked pairing PIN %q: %s", pin, snapshot)
	}
}

func TestPairingPINCanBeRegeneratedFromLoopback(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	consumedPIN, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	if !identity.ConsumePairingPIN(consumedPIN) {
		t.Fatal("could not consume initial pairing PIN")
	}

	request := httptest.NewRequest(http.MethodPost, "/api/pairing/pin", nil)
	request.Host = "localhost"
	request.RemoteAddr = "127.0.0.1:1234"
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("loopback PIN regeneration returned %d: %s", response.Code, response.Body.String())
	}
	var result struct {
		PIN       string    `json:"pin"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.PIN == "" || result.ExpiresAt.IsZero() || !identity.VerifyPairingPIN(result.PIN) {
		t.Fatalf("regenerated PIN is not active: %#v", result)
	}

	remoteRequest := httptest.NewRequest(http.MethodPost, "/api/pairing/pin", nil)
	remoteRequest.Host = "localhost"
	remoteRequest.RemoteAddr = "192.0.2.10:1234"
	remoteResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(remoteResponse, remoteRequest)
	if remoteResponse.Code != http.StatusForbidden {
		t.Fatalf("remote PIN regeneration returned %d, want 403", remoteResponse.Code)
	}
}

func TestOrchestratorPairingIsRejected(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := peeridentity.Open(filepath.Join(t.TempDir(), "other-orchestrator.json"), "other-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(pairingJoinRequest{
		ID: peer.ID(), Role: "orchestrator", PIN: pin, CertificatePEM: peer.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
	request.TLS = &tls.ConnectionState{}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("orchestrator join returned %d, want 400: %s", response.Code, response.Body.String())
	}
	if identity.IsTrustedID(peer.ID()) || !identity.VerifyPairingPIN(pin) {
		t.Fatal("rejected orchestrator pairing changed the trust roster or consumed the PIN")
	}

	connectResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(connectResponse, httptest.NewRequest(http.MethodPost, "/api/pairing/connect", nil))
	if connectResponse.Code != http.StatusForbidden {
		t.Fatalf("POST /api/pairing/connect returned %d, want 403", connectResponse.Code)
	}
}

func TestDiscoveredNodeStatusAllowsSlowPeerResponse(t *testing.T) {
	srv := newTestServer(t)
	srv.election.selfID = "orchestrator-slow-node-test"
	if err := srv.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	peer, err := peeridentity.Open(filepath.Join(t.TempDir(), "slow-node.json"), "slow-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.identity.TrustPeerDetails(peer.ID(), peer.CertificatePEM(), "node", ""); err != nil {
		t.Fatal(err)
	}
	if err := peer.TrustPeer(srv.SelfID(), srv.identity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}

	peerMux := http.NewServeMux()
	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	peerMux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-releaseResponse:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"id":"slow-node"}`))
	})
	peerServer := httptest.NewUnstartedServer(peer.RequireTrustedPeer(peerMux))
	peerServer.TLS = peer.ServerTLSConfig()
	peerServer.StartTLS()
	defer peerServer.Close()

	type statusResult struct {
		id  string
		err error
	}
	resultCh := make(chan statusResult, 1)
	go func() {
		status, err := srv.discoveredNodeStatus(peerServer.URL)
		if err != nil {
			resultCh <- statusResult{err: err}
			return
		}
		resultCh <- statusResult{id: status.ID}
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("slow paired node did not receive the status request")
	}
	close(releaseResponse)

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("fetch from a slow paired node failed: %v", result.err)
		}
		if result.id != peer.ID() {
			t.Fatalf("fetched node status ID = %q, want %q", result.id, peer.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("fetch from a slow paired node did not complete")
	}
}

func TestHarnessCanChangeWithoutMovingExistingSessions(t *testing.T) {
	srv := newTestServer(t)
	mux := srv.Mux()

	create := func(title string) map[string]any {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"title": title})
		request := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", bytes.NewReader(body))
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("create session returned %d: %s", response.Code, response.Body.String())
		}
		var session map[string]any
		if err := json.NewDecoder(response.Body).Decode(&session); err != nil {
			t.Fatal(err)
		}
		return session
	}
	original := create("DeepSeek session")
	if original["harness"] != "deepseek" {
		t.Fatalf("new session harness = %#v, want deepseek", original["harness"])
	}

	request := httptest.NewRequest(http.MethodPatch, "/api/agent/backend", strings.NewReader(`{"harness":"pi"}`))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("switch harness returned %d: %s", response.Code, response.Body.String())
	}
	var backend struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(response.Body).Decode(&backend); err != nil {
		t.Fatal(err)
	}
	if backend.Name != "pi" || srv.HarnessName() != "pi" {
		t.Fatalf("active harness = %q, response = %#v", srv.HarnessName(), backend)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+original["id"].(string), nil)
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("get existing session returned %d", response.Code)
	}
	var existing map[string]any
	if err := json.NewDecoder(response.Body).Decode(&existing); err != nil {
		t.Fatal(err)
	}
	if existing["harness"] != "deepseek" {
		t.Fatalf("existing session moved harnesses: %#v", existing["harness"])
	}

	newSession := create("Pi session")
	if newSession["harness"] != "pi" {
		t.Fatalf("new session after switch uses %v, want pi", newSession["harness"])
	}
}

func TestOrchestratorAgentAPI(t *testing.T) {
	// Mock worker node
	mockWorker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"output": "Worker processed your request successfully.",
		})
	}))
	defer mockWorker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID:      "test-worker-1",
		Address: mockWorker.URL,
		Hardware: hardware.Info{
			CPUCores:   8,
			TotalRAMMB: 32768,
			Score:      60,
		},
		Models: []engine.Model{
			{Name: "qwen-coder", Tags: []string{"code", "chat"}},
		},
		DefaultModel: "qwen-coder",
		Healthy:      true,
	})

	mux := srv.Mux()

	// 1. Test GET /api/agent/tools
	req := httptest.NewRequest(http.MethodGet, "/api/agent/tools", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/agent/tools returned %d", rec.Code)
	}
	var tools []deepseek.ToolDef
	if err := json.NewDecoder(rec.Body).Decode(&tools); err != nil {
		t.Fatalf("failed to decode tools: %v", err)
	}
	if len(tools) == 0 {
		t.Errorf("expected tools list, got empty")
	}

	req = httptest.NewRequest(http.MethodGet, "/api/agent/backend", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/agent/backend returned %d", rec.Code)
	}
	var backendInfo struct {
		Name                     string `json:"name"`
		SupportsCoordinatorModel bool   `json:"supports_coordinator_model"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&backendInfo); err != nil {
		t.Fatalf("failed to decode backend info: %v", err)
	}
	if backendInfo.Name != "deepseek" || !backendInfo.SupportsCoordinatorModel {
		t.Fatalf("unexpected backend info: %#v", backendInfo)
	}

	// 2. Test GET /api/agent/models
	req = httptest.NewRequest(http.MethodGet, "/api/agent/models", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/agent/models returned %d", rec.Code)
	}
	var models []deepseek.ModelInfo
	if err := json.NewDecoder(rec.Body).Decode(&models); err != nil {
		t.Fatalf("failed to decode models: %v", err)
	}
	if len(models) != 1 || models[0].Name != "qwen-coder" {
		t.Errorf("expected qwen-coder model, got %v", models)
	}

	// 3. Test POST /api/agent/sessions
	createBody, _ := json.Marshal(map[string]any{
		"title":             "Test Session",
		"coordinator_model": "qwen-coder",
		"max_steps":         6,
	})
	req = httptest.NewRequest(http.MethodPost, "/api/agent/sessions", bytes.NewReader(createBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/agent/sessions returned %d", rec.Code)
	}
	var createdSession deepseek.Session
	if err := json.NewDecoder(rec.Body).Decode(&createdSession); err != nil {
		t.Fatalf("failed to decode session: %v", err)
	}
	if createdSession.ID == "" || createdSession.Title != "Test Session" {
		t.Errorf("unexpected session created: id=%q title=%q", createdSession.ID, createdSession.Title)
	}

	// 4. Test POST /api/agent/sessions/{id}/messages
	msgBody, _ := json.Marshal(map[string]string{
		"content": "Hello cluster",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+createdSession.ID+"/messages", bytes.NewReader(msgBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST message returned %d: %s", rec.Code, rec.Body.String())
	}
	var respMsg deepseek.Message
	if err := json.NewDecoder(rec.Body).Decode(&respMsg); err != nil {
		t.Fatalf("failed to decode msg response: %v", err)
	}
	if !strings.Contains(respMsg.Content, "Worker processed your request successfully") {
		t.Errorf("unexpected response message: %q", respMsg.Content)
	}

	// 5. Test GET /api/agent/sessions/{id}
	req = httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+createdSession.ID, nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET session returned %d", rec.Code)
	}

	// 6. Test DELETE /api/agent/sessions/{id}
	req = httptest.NewRequest(http.MethodDelete, "/api/agent/sessions/"+createdSession.ID, nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE session returned %d", rec.Code)
	}
}

func TestDirectNodeLLMModelAPI(t *testing.T) {
	mockWorker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"output": "Output from model " + req["model"] + ": " + req["prompt"],
		})
	}))
	defer mockWorker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID:      "worker-alpha",
		Address: mockWorker.URL,
		Hardware: hardware.Info{
			CPUCores:   8,
			TotalRAMMB: 32768,
			Score:      60,
		},
		Models: []engine.Model{
			{Name: "llama3-8b", Tags: []string{"chat", "general"}},
			{Name: "codellama-7b", Tags: []string{"code"}},
		},
		DefaultModel: "llama3-8b",
		Healthy:      true,
	})

	mux := srv.Mux()

	// 1. Test GET /api/models
	req := httptest.NewRequest(http.MethodGet, "/api/models", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/models returned %d", rec.Code)
	}

	var models []deepseek.ModelInfo
	if err := json.NewDecoder(rec.Body).Decode(&models); err != nil {
		t.Fatalf("decode models failed: %v", err)
	}
	if len(models) != 2 {
		t.Errorf("expected 2 models, got %d", len(models))
	}

	// 2. Test POST /api/generate
	genBody, _ := json.Marshal(map[string]string{
		"model":  "codellama-7b",
		"prompt": "write a function",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/generate", bytes.NewReader(genBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/generate returned %d: %s", rec.Code, rec.Body.String())
	}
	var genResp generateAPIResponse
	if err := json.NewDecoder(rec.Body).Decode(&genResp); err != nil {
		t.Fatalf("decode genResp failed: %v", err)
	}
	if genResp.NodeID != "worker-alpha" || genResp.Model != "codellama-7b" {
		t.Errorf("unexpected generate response: %+v", genResp)
	}

	// 3. Test POST /api/nodes/{id}/generate
	nodeGenBody, _ := json.Marshal(map[string]string{
		"prompt": "hello alpha",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/nodes/worker-alpha/generate", bytes.NewReader(nodeGenBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/nodes/worker-alpha/generate returned %d", rec.Code)
	}

	// 4. Test POST /api/nodes/{id}/models/{model}/test
	req = httptest.NewRequest(http.MethodPost, "/api/nodes/worker-alpha/models/llama3-8b/test", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST model test returned %d", rec.Code)
	}
	var testResult map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&testResult)
	if testResult["healthy"] != true {
		t.Errorf("expected healthy true, got %v", testResult)
	}

	// 5. Test OpenAI compatibility /v1/models & /v1/chat/completions
	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/models returned %d", rec.Code)
	}

	chatBody, _ := json.Marshal(map[string]any{
		"model": "codellama-7b",
		"messages": []map[string]string{
			{"role": "user", "content": "hello OpenAI"},
		},
	})
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(chatBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/chat/completions returned %d: %s", rec.Code, rec.Body.String())
	}

	// 6. Test POST /api/nodes/{id}/chat (direct multi-turn conversation proxy)
	nodeChatBody, _ := json.Marshal(map[string]any{
		"model": "llama3-8b",
		"messages": []map[string]string{
			{"role": "user", "content": "hello node chat"},
		},
	})
	req = httptest.NewRequest(http.MethodPost, "/api/nodes/worker-alpha/chat", bytes.NewReader(nodeChatBody))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/nodes/worker-alpha/chat returned %d: %s", rec.Code, rec.Body.String())
	}
}

func TestOpenAIStreamingPreservesOptionsAndMessageRoles(t *testing.T) {
	type generatePayload struct {
		Prompt      string  `json:"prompt"`
		Temperature float64 `json:"temperature"`
		MaxTokens   int     `json:"max_tokens"`
	}
	var received generatePayload
	mockWorker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate" || r.URL.Query().Get("stream") != "true" {
			t.Errorf("unexpected worker request: %s %s", r.Method, r.URL.String())
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode node request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"output\":\"Hello \"}\n\n"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write([]byte("data: {\"output\":\"world\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"output\":\"\",\"done\":true}\n\n"))
	}))
	defer mockWorker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "stream-worker", Address: mockWorker.URL, Healthy: true,
		Models:       []engine.Model{{Name: "general-model", Tags: []string{"general"}}},
		DefaultModel: "general-model",
	})
	requestBody, err := json.Marshal(map[string]any{
		"model": "general-model", "stream": true, "temperature": 0.35, "max_tokens": 64,
		"messages": []map[string]string{
			{"role": "system", "content": "Follow the instructions"},
			{"role": "user", "content": "Say hello"},
			{"role": "assistant", "content": "I will use a tool"},
			{"role": "tool", "content": "Tool result"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(requestBody)))
	if rec.Code != http.StatusOK {
		t.Fatalf("stream endpoint returned %d: %s", rec.Code, rec.Body.String())
	}
	if received.Temperature != 0.35 || received.MaxTokens != 64 {
		t.Fatalf("generation options not forwarded: %+v", received)
	}
	for _, expected := range []string{`"content":"Hello "`, `"content":"world"`, "data: [DONE]"} {
		if !strings.Contains(rec.Body.String(), expected) {
			t.Errorf("stream response is missing %q: %s", expected, rec.Body.String())
		}
	}
	for _, expected := range []string{"<|im_start|>assistant\nI will use a tool<|im_end|>", "<|im_start|>tool\nTool result<|im_end|>"} {
		if !strings.Contains(received.Prompt, expected) {
			t.Errorf("forwarded prompt is missing %q: %s", expected, received.Prompt)
		}
	}
}

func TestCreateTaskRejectsPartiallyAssignableWork(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "code-worker", Address: "http://worker", Healthy: true,
		Models: []engine.Model{{Name: "code-model", Tags: []string{"code"}}},
	})
	body := bytes.NewBufferString(`{"description":"Write code. Inspect this image."}`)
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/tasks", body))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("task creation returned %d, want %d: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if tasks := srv.tasks.All(); len(tasks) != 0 {
		t.Fatalf("task created despite unassignable work: %+v", tasks)
	}
}

func TestDispatchRetriesOnAnotherHealthyNode(t *testing.T) {
	// These counters are updated by concurrent test-server handlers.
	var firstCalls, secondCalls atomic.Int32
	type dispatchIDs struct{ request, attempt string }
	attemptIDs := make(chan dispatchIDs, 2)
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		if r.Header.Get(inferroute.HeaderRequestID) == "" || r.Header.Get(inferroute.HeaderAttemptID) == "" {
			t.Errorf("dispatch request IDs missing: request=%q attempt=%q",
				r.Header.Get(inferroute.HeaderRequestID), r.Header.Get(inferroute.HeaderAttemptID))
		}
		if got, want := r.Header.Get(httpx.RequestIDHeader), r.Header.Get(inferroute.HeaderRequestID); got != want {
			t.Errorf("standard dispatch request ID = %q, want attempt request ID %q", got, want)
		}
		attemptIDs <- dispatchIDs{request: r.Header.Get(inferroute.HeaderRequestID), attempt: r.Header.Get(inferroute.HeaderAttemptID)}
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		var request struct {
			Model  string `json:"model"`
			Engine string `json:"engine"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Engine != "ollama" || request.Model != "other-model" {
			t.Errorf("dispatch model/engine = %q/%q, error=%v", request.Model, request.Engine, err)
		}
		if r.Header.Get(inferroute.HeaderRequestID) == "" || r.Header.Get(inferroute.HeaderAttemptID) == "" {
			t.Errorf("retry request IDs missing: request=%q attempt=%q",
				r.Header.Get(inferroute.HeaderRequestID), r.Header.Get(inferroute.HeaderAttemptID))
		}
		if got, want := r.Header.Get(httpx.RequestIDHeader), r.Header.Get(inferroute.HeaderRequestID); got != want {
			t.Errorf("standard retry request ID = %q, want attempt request ID %q", got, want)
		}
		attemptIDs <- dispatchIDs{request: r.Header.Get(inferroute.HeaderRequestID), attempt: r.Header.Get(inferroute.HeaderAttemptID)}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "retry succeeded"})
	}))
	defer second.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: first.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Engine: "ollama", Tags: []string{"general"}}},
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-b", Address: second.URL, Healthy: true,
		Models: []engine.Model{{Name: "other-model", Engine: "ollama", Tags: []string{"general"}}},
	})
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "sub-a", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-a", Address: first.URL, Model: "general", Engine: "ollama",
	}})
	if err != nil {
		t.Fatal(err)
	}

	srv.dispatch(task.ID, task.Subtasks[0].Assignment)
	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusCompleted {
		t.Fatalf("retried task did not complete: %+v", completed)
	}
	if completed.Subtasks[0].NodeID != "node-b" || completed.Subtasks[0].Output != "retry succeeded" {
		t.Fatalf("retry placement/output not recorded: %+v", completed.Subtasks[0])
	}
	if attempts := completed.Subtasks[0].Attempts; len(attempts) != 2 ||
		attempts[0].Status != taskmgr.DispatchAttemptFailed ||
		attempts[0].FailureClass != "upstream_5xx" ||
		attempts[1].Status != taskmgr.DispatchAttemptSucceeded ||
		attempts[0].NodeID == attempts[1].NodeID ||
		attempts[0].Model == attempts[1].Model ||
		attempts[0].ID != taskmgr.DispatchAttemptID(task.ID, "sub-a", 1) ||
		attempts[1].ID != taskmgr.DispatchAttemptID(task.ID, "sub-a", 2) ||
		attempts[0].RequestID != taskmgr.DispatchRequestID(task.ID, "sub-a") ||
		attempts[1].RequestID != attempts[0].RequestID {
		t.Fatalf("dispatch attempt history = %+v", attempts)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("dispatch attempts = (%d, %d), want (1, 1)", firstCalls.Load(), secondCalls.Load())
	}
	for _, expected := range []dispatchIDs{
		{request: task.ID + ":sub-a", attempt: taskmgr.DispatchAttemptID(task.ID, "sub-a", 1)},
		{request: task.ID + ":sub-a", attempt: taskmgr.DispatchAttemptID(task.ID, "sub-a", 2)},
	} {
		if got := <-attemptIDs; got != expected {
			t.Errorf("worker dispatch IDs = %+v, want %+v", got, expected)
		}
	}
	historyResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(historyResponse, httptest.NewRequest(http.MethodGet, "/api/tasks/"+task.ID, nil))
	if historyResponse.Code != http.StatusOK {
		t.Fatalf("task history API returned %d: %s", historyResponse.Code, historyResponse.Body.String())
	}
	var history taskmgr.Task
	if err := json.Unmarshal(historyResponse.Body.Bytes(), &history); err != nil {
		t.Fatalf("decode task history response: %v", err)
	}
	if len(history.Subtasks) != 1 || len(history.Subtasks[0].Attempts) != 2 ||
		history.Subtasks[0].Attempts[0].RequestID != task.ID+":sub-a" ||
		history.Subtasks[0].Attempts[1].ID != taskmgr.DispatchAttemptID(task.ID, "sub-a", 2) {
		t.Fatalf("task API did not expose full attempt history: %+v", history.Subtasks)
	}
	metrics := srv.metrics.snapshot()
	if metrics.dispatches != 2 || metrics.retries != 1 || metrics.capacityReservations != 2 {
		t.Fatalf("dispatch metrics = %+v, want two attempts, one retry, and two reservations", metrics)
	}
}

func TestDispatchPermanentFailureDoesNotRetry(t *testing.T) {
	var retryCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid request", http.StatusBadRequest)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		retryCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "should not run"})
	}))
	defer second.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: first.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Engine: "ollama", Tags: []string{"general"}}},
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-b", Address: second.URL, Healthy: true,
		Models: []engine.Model{{Name: "other", Engine: "ollama", Tags: []string{"general"}}},
	})
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "permanent", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-a", Address: first.URL, Model: "general", Engine: "ollama",
	}})
	if err != nil {
		t.Fatal(err)
	}

	srv.dispatch(task.ID, task.Subtasks[0].Assignment)
	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusFailed {
		t.Fatalf("permanent failure task = %+v", completed)
	}
	attempts := completed.Subtasks[0].Attempts
	if len(attempts) != 1 || attempts[0].FailureClass != "client_error" ||
		attempts[0].Status != taskmgr.DispatchAttemptFailed {
		t.Fatalf("permanent failure attempts = %+v, want one client_error failure", attempts)
	}
	if retryCalls.Load() != 0 {
		t.Fatalf("permanent failure triggered %d retries", retryCalls.Load())
	}
}

func TestDispatchCancellationDuringExecutionFailsWithoutRetry(t *testing.T) {
	entered := make(chan struct{})
	releaseWorker := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-releaseWorker
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "late response"})
	}))
	defer first.Close()
	defer release()
	var retryCalls atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		retryCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "should not run"})
	}))
	defer second.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: first.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Engine: "ollama", Tags: []string{"general"}}},
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-b", Address: second.URL, Healthy: true,
		Models: []engine.Model{{Name: "other", Engine: "ollama", Tags: []string{"general"}}},
	})
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "cancelled", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-a", Address: first.URL, Model: "general", Engine: "ollama",
	}})
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.dispatcher.dispatchWithContext(parent, task.ID, task.Subtasks[0].Assignment)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("dispatch did not reach the worker")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not stop after cancellation")
	}
	release()

	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusFailed {
		t.Fatalf("cancelled dispatch task = %+v", completed)
	}
	attempts := completed.Subtasks[0].Attempts
	if len(attempts) != 1 || attempts[0].FailureClass != "cancelled" ||
		attempts[0].Status != taskmgr.DispatchAttemptFailed {
		t.Fatalf("cancelled dispatch attempts = %+v", attempts)
	}
	if retryCalls.Load() != 0 {
		t.Fatalf("cancelled dispatch triggered %d retries", retryCalls.Load())
	}
}

type loseFirstDispatchResponse struct {
	base      dispatchHTTPClient
	completed <-chan struct{}
	drop      atomic.Bool
}

func (c *loseFirstDispatchResponse) Do(request *http.Request) (*http.Response, error) {
	response, err := c.base.Do(request)
	if err != nil {
		return response, err
	}
	if c.drop.CompareAndSwap(false, true) {
		<-c.completed
		_ = response.Body.Close()
		return nil, errors.New("simulated lost response after worker completion")
	}
	return response, nil
}

func TestDispatchRetriesWhenCompletedResponseIsLost(t *testing.T) {
	var firstCompleted, secondCalls atomic.Int32
	workerCompleted := make(chan struct{})
	type dispatchIDs struct{ request, attempt string }
	firstIDs := make(chan dispatchIDs, 1)
	secondIDs := make(chan dispatchIDs, 1)
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCompleted.Add(1)
		if got, want := r.Header.Get(httpx.RequestIDHeader), r.Header.Get(inferroute.HeaderRequestID); got != want {
			t.Errorf("standard lost-response request ID = %q, want attempt request ID %q", got, want)
		}
		firstIDs <- dispatchIDs{
			request: r.Header.Get(inferroute.HeaderRequestID),
			attempt: r.Header.Get(inferroute.HeaderAttemptID),
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "completed but response lost"})
		close(workerCompleted)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		if got, want := r.Header.Get(httpx.RequestIDHeader), r.Header.Get(inferroute.HeaderRequestID); got != want {
			t.Errorf("standard retry request ID = %q, want attempt request ID %q", got, want)
		}
		secondIDs <- dispatchIDs{
			request: r.Header.Get(inferroute.HeaderRequestID),
			attempt: r.Header.Get(inferroute.HeaderAttemptID),
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "retry completed"})
	}))
	defer second.Close()

	srv := newTestServer(t)
	srv.dispatcher.executeClient = &loseFirstDispatchResponse{
		base: http.DefaultClient, completed: workerCompleted,
	}
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: first.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Engine: "ollama", Tags: []string{"general"}}},
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-b", Address: second.URL, Healthy: true,
		Models: []engine.Model{{Name: "other", Engine: "ollama", Tags: []string{"general"}}},
	})
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "lost-response", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-a", Address: first.URL, Model: "general", Engine: "ollama",
	}})
	if err != nil {
		t.Fatal(err)
	}

	srv.dispatch(task.ID, task.Subtasks[0].Assignment)
	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusCompleted {
		t.Fatalf("lost-response retry task = %+v", completed)
	}
	attempts := completed.Subtasks[0].Attempts
	if firstCompleted.Load() != 1 || secondCalls.Load() != 1 || len(attempts) != 2 ||
		attempts[0].FailureClass != "transport" ||
		attempts[0].Status != taskmgr.DispatchAttemptFailed ||
		attempts[1].Status != taskmgr.DispatchAttemptSucceeded ||
		attempts[0].RequestID != taskmgr.DispatchRequestID(task.ID, "lost-response") ||
		attempts[1].RequestID != attempts[0].RequestID {
		t.Fatalf("lost-response execution = first %d, retry %d, attempts %+v",
			firstCompleted.Load(), secondCalls.Load(), attempts)
	}
	if got, want := <-firstIDs, (dispatchIDs{
		request: taskmgr.DispatchRequestID(task.ID, "lost-response"),
		attempt: taskmgr.DispatchAttemptID(task.ID, "lost-response", 1),
	}); got != want {
		t.Fatalf("first lost-response attempt IDs = %+v, want %+v", got, want)
	}
	if got, want := <-secondIDs, (dispatchIDs{
		request: taskmgr.DispatchRequestID(task.ID, "lost-response"),
		attempt: taskmgr.DispatchAttemptID(task.ID, "lost-response", 2),
	}); got != want {
		t.Fatalf("retry attempt IDs = %+v, want %+v", got, want)
	}
}

func TestDispatchRetriesWhenAssignedNodeDisappears(t *testing.T) {
	disappeared := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	disappearedAddress := disappeared.URL
	disappeared.Close()
	available := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "recovered on another node"})
	}))
	defer available.Close()

	srv := newTestServer(t)
	model := []engine.Model{{Name: "general", Engine: "ollama", Tags: []string{"general"}}}
	srv.registry.Upsert(cluster.NodeStatus{ID: "node-a", Address: disappearedAddress, Healthy: true, Models: model})
	srv.registry.Upsert(cluster.NodeStatus{ID: "node-b", Address: available.URL, Healthy: true, Models: model})
	task, err := srv.tasks.CreateChecked("Run after node loss", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "node-loss", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-a", Address: disappearedAddress, Model: "general", Engine: "ollama",
	}})
	if err != nil {
		t.Fatal(err)
	}

	srv.dispatch(task.ID, task.Subtasks[0].Assignment)
	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusCompleted {
		t.Fatalf("node-loss retry task = %+v", completed)
	}
	attempts := completed.Subtasks[0].Attempts
	if len(attempts) != 2 || attempts[0].FailureClass != "transport" ||
		attempts[0].Status != taskmgr.DispatchAttemptFailed ||
		attempts[1].Status != taskmgr.DispatchAttemptSucceeded ||
		completed.Subtasks[0].NodeID != "node-b" ||
		completed.Subtasks[0].Output != "recovered on another node" {
		t.Fatalf("node-loss dispatch result = task %+v, attempts %+v", completed.Subtasks[0], attempts)
	}
}

type dropFirstHTTPResponseRoundTripper struct {
	base    http.RoundTripper
	dropped atomic.Bool
}

func (t *dropFirstHTTPResponseRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return response, err
	}
	if t.dropped.CompareAndSwap(false, true) {
		_, copyErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		return nil, errors.New("simulated dropped inference response")
	}
	return response, nil
}

func TestGenerateForwardsIdempotencyKeyWithoutFallbackAfterLostResponse(t *testing.T) {
	var generateCalls, executeCalls atomic.Int32
	workerKeys := make(chan string, 3)
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/generate":
			generateCalls.Add(1)
			workerKeys <- r.Header.Get(inferroute.HeaderIdempotencyKey)
			_ = json.NewEncoder(w).Encode(map[string]string{"output": "generated"})
		case "/execute":
			executeCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"output": "fallback"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer worker.Close()

	srv := newTestServer(t)
	srv.nodeClient.dispatcher.Transport = &dropFirstHTTPResponseRoundTripper{base: http.DefaultTransport}
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: worker.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Engine: "ollama", Tags: []string{"general"}}},
	})
	makeRequest := func() *http.Request {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/api/nodes/node-a/generate", strings.NewReader(
			`{"model":"general","prompt":"repeat me"}`,
		))
		request.Header.Set(inferroute.HeaderIdempotencyKey, "request-key")
		return request
	}
	first := httptest.NewRecorder()
	srv.Mux().ServeHTTP(first, makeRequest())
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("lost worker response returned %d, want gateway error: %s", first.Code, first.Body.String())
	}
	retry := httptest.NewRecorder()
	srv.Mux().ServeHTTP(retry, makeRequest())
	if retry.Code != http.StatusOK || generateCalls.Load() != 2 || executeCalls.Load() != 0 {
		t.Fatalf("keyed retry returned %d after %d generate and %d execute calls: %s",
			retry.Code, generateCalls.Load(), executeCalls.Load(), retry.Body.String())
	}
	for range 2 {
		if key := <-workerKeys; key != "request-key" {
			t.Fatalf("worker received idempotency key %q, want request-key", key)
		}
	}
	compatRequest := httptest.NewRequest(http.MethodPost, "/api/generate", strings.NewReader(
		`{"model":"general","prompt":"compat request","stream":false}`,
	))
	compatRequest.Header.Set(inferroute.HeaderIdempotencyKey, "compat-key")
	compatResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(compatResponse, compatRequest)
	if compatResponse.Code != http.StatusOK {
		t.Fatalf("compat generate returned %d: %s", compatResponse.Code, compatResponse.Body.String())
	}
	if key := <-workerKeys; key != "compat-key" {
		t.Fatalf("compat generate forwarded key %q, want compat-key", key)
	}
}

func TestDispatchDeadlineDuringRetryStopsFurtherAttempts(t *testing.T) {
	var thirdCalls atomic.Int32
	retryStarted := make(chan struct{})
	releaseRetry := make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(retryStarted)
		select {
		case <-r.Context().Done():
		case <-releaseRetry:
		}
	}))
	defer second.Close()
	defer close(releaseRetry)
	third := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		thirdCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "must not run"})
	}))
	defer third.Close()

	srv := newTestServer(t)
	srv.dispatcher.retryPolicy.maxAttempts = 3
	srv.dispatcher.retryPolicy.backoff = time.Millisecond
	srv.dispatcher.retryPolicy.maxBackoff = time.Millisecond
	for _, node := range []struct {
		id      string
		address string
		score   float64
	}{
		{id: "node-a", address: first.URL, score: 100},
		{id: "node-b", address: second.URL, score: 80},
		{id: "node-c", address: third.URL, score: 20},
	} {
		srv.registry.Upsert(cluster.NodeStatus{
			ID: node.id, Address: node.address, Healthy: true,
			Hardware: hardware.Info{Score: node.score},
			Models:   []engine.Model{{Name: "general", Tags: []string{"general"}}},
		})
	}
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "deadline", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-a", Address: first.URL, Model: "general",
	}})
	if err != nil {
		t.Fatal(err)
	}

	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		srv.dispatcher.dispatchWithContext(parent, task.ID, task.Subtasks[0].Assignment)
		close(done)
	}()
	select {
	case <-retryStarted:
	case <-time.After(time.Second):
		t.Fatal("retry did not reach the second worker")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not stop after the parent deadline")
	}

	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusFailed {
		t.Fatalf("deadline retry task = %+v", completed)
	}
	attempts := completed.Subtasks[0].Attempts
	if len(attempts) != 2 || attempts[0].FailureClass != "upstream_5xx" ||
		attempts[1].FailureClass != "timeout" || attempts[1].Status != taskmgr.DispatchAttemptFailed ||
		!strings.Contains(completed.Subtasks[0].Error, "deadline exceeded") || thirdCalls.Load() != 0 {
		t.Fatalf("deadline retry history = %+v, task error %q, third worker calls %d",
			attempts, completed.Subtasks[0].Error, thirdCalls.Load())
	}
}

func TestDispatchRetryClassificationExcludesClientErrors(t *testing.T) {
	for _, test := range []struct {
		status    int
		body      string
		retryable bool
		class     string
	}{
		{http.StatusRequestTimeout, "", true, "timeout"},
		{http.StatusTooManyRequests, "", true, "rate_limit"},
		{http.StatusServiceUnavailable, "", true, "upstream_5xx"},
		{http.StatusNotImplemented, "", false, "client_error"},
		{http.StatusBadRequest, "", false, "client_error"},
		{http.StatusUnauthorized, "", false, "client_error"},
		{http.StatusNotFound, "model not found", true, "stale_model"},
	} {
		class, retryable := classifyDispatchStatus(test.status, []byte(test.body))
		if retryable != test.retryable || class != test.class {
			t.Errorf("classifyDispatchStatus(%d, %q) = (%q, %t), want (%q, %t)",
				test.status, test.body, class, retryable, test.class, test.retryable)
		}

	}
}

func TestDispatcherUsesConfiguredTimeout(t *testing.T) {
	releaseWorker := make(chan struct{})
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-releaseWorker
		w.WriteHeader(http.StatusOK)
	}))
	defer worker.Close()
	defer close(releaseWorker)

	srv := newTestServer(t)
	srv.dispatcher.dispatchTimeout = 50 * time.Millisecond
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "slow-worker", Address: worker.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	})
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{
			ID: "slow", Description: "Run it", TaskType: "general", Tags: []string{"general"},
		},
		NodeID: "slow-worker", Address: worker.URL, Model: "general",
	}})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		srv.dispatcher.dispatchWithContext(context.Background(), task.ID, task.Subtasks[0].Assignment)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not stop at the configured timeout")
	}

	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusFailed {
		t.Fatalf("timed-out task = %+v", completed)
	}
	result := completed.Subtasks[0]
	if !strings.Contains(result.Error, "subtask dispatch deadline exceeded") ||
		len(result.Attempts) != 1 || result.Attempts[0].FailureClass != "timeout" {
		t.Fatalf("timed-out subtask result = %+v", result)
	}
}

func TestDispatchRetriesAcrossMoreThanTwoNodes(t *testing.T) {
	// Per-node atomics safely count calls from parallel dispatch handlers.
	var calls [3]atomic.Int32
	nodes := make([]*httptest.Server, 3)
	for index := range nodes {
		nodeIndex := index
		nodes[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls[nodeIndex].Add(1)
			if nodeIndex < 2 {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"output": "third node succeeded"})
		}))
		defer nodes[index].Close()
	}

	srv := newTestServer(t)
	for i, node := range nodes {
		srv.registry.Upsert(cluster.NodeStatus{
			ID: fmt.Sprintf("node-%d", i), Address: node.URL, Healthy: true,
			Hardware: hardware.Info{Score: float64(100 - i*40)},
			Models:   []engine.Model{{Name: "general", Tags: []string{"general"}}},
		})
	}
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "sub-a", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-0", Address: nodes[0].URL, Model: "general",
	}})
	if err != nil {
		t.Fatal(err)
	}

	srv.dispatch(task.ID, task.Subtasks[0].Assignment)
	completed, ok := srv.tasks.Get(task.ID)
	if !ok || completed.Status != taskmgr.StatusCompleted {
		t.Fatalf("dispatch did not complete on third node: %+v", completed)
	}
	if completed.Subtasks[0].NodeID != "node-2" || len(completed.Subtasks[0].Attempts) != 3 {
		t.Fatalf("retry assignment/attempt history = %+v", completed.Subtasks[0])
	}
	for i := range calls {
		if calls[i].Load() != 1 {
			t.Fatalf("node-%d calls = %d, want 1", i, calls[i].Load())
		}
	}
}

func TestIndependentSubtasksDispatchConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	nodes := make([]*httptest.Server, 0, 2)
	for _, id := range []string{"node-a", "node-b"} {
		nodeID := id
		node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			started <- nodeID
			<-release
			_ = json.NewEncoder(w).Encode(map[string]string{"output": nodeID + " output"})
		}))
		nodes = append(nodes, node)
		defer node.Close()
	}

	srv := newTestServer(t)
	if err := srv.tasks.Close(); err != nil {
		t.Fatal(err)
	}
	srv.tasks = taskmgr.NewManagerWithFinalizer(func(context.Context, *taskmgr.Task) (string, error) {
		return "combined output", nil
	})
	srv.dispatcher.tasks = srv.tasks
	srv.planner.tasks = srv.tasks
	assignments := make([]scheduler.Assignment, 0, len(nodes))
	for i, node := range nodes {
		id := []string{"node-a", "node-b"}[i]
		srv.registry.Upsert(cluster.NodeStatus{
			ID: id, Address: node.URL, Healthy: true,
			MaxConcurrentTasks: 1, AvailableCapacity: 1,
		})
		assignments = append(assignments, scheduler.Assignment{
			Subtask: scheduler.Subtask{ID: id, Description: "independent work"},
			NodeID:  id, Address: node.URL, Model: "test",
		})
	}
	task, err := srv.tasks.CreateChecked("Parallel dispatch", assignments)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range assignments {
		if err := srv.submitDispatch(task.ID, assignment); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for range 2 {
		select {
		case id := <-started:
			seen[id] = true
		case <-time.After(time.Second):
			close(release)
			t.Fatal("independent subtasks were not dispatched concurrently")
		}
	}
	close(release)
	if len(seen) != 2 {
		t.Fatalf("dispatched nodes = %v, want both nodes", seen)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	completed, err := srv.tasks.Wait(waitCtx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != taskmgr.StatusCompleted {
		t.Fatalf("task status = %s, want completed: %+v", completed.Status, completed)
	}
}

func TestAgentSessionInterrupt(t *testing.T) {
	srv := newTestServer(t)

	rec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(`{"title":"test"}`))
	srv.handleAgentSessionsCreate(rec, createReq)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create session returned %d: %s", rec.Code, rec.Body.String())
	}
	var session agent.Session
	if err := json.NewDecoder(rec.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}

	interrupted := srv.InterruptSession(session.ID)
	if interrupted {
		t.Error("expected InterruptSession to return false when no turn is in flight")
	}

	rec = httptest.NewRecorder()
	interruptReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+session.ID+"/interrupt", nil)
	interruptReq.SetPathValue("id", session.ID)
	srv.handleAgentSessionInterrupt(rec, interruptReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("interrupt endpoint returned %d: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp["session_id"] != session.ID {
		t.Fatalf("expected session_id %s, got %v", session.ID, resp["session_id"])
	}
}

func TestAgentSessionActiveTurnInterrupt(t *testing.T) {
	var once sync.Once
	started := make(chan struct{})
	stopServer := make(chan struct{})

	mockCoord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-stopServer:
		case <-r.Context().Done():
		}
	}))
	defer mockCoord.Close()
	defer close(stopServer)

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID:      "coord-node",
		Address: mockCoord.URL,
		Hardware: hardware.Info{
			CPUCores:   16,
			TotalRAMMB: 65536,
			Score:      100,
		},
		Models: []engine.Model{
			{Name: "deepseek-test", Tags: []string{"chat"}},
		},
		DefaultModel: "deepseek-test",
		Healthy:      true,
		Leader:       true,
	})

	rec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(`{"title":"test-active","coordinator_model":"deepseek-test"}`))
	srv.handleAgentSessionsCreate(rec, createReq)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create session returned %d: %s", rec.Code, rec.Body.String())
	}
	var session agent.Session
	if err := json.NewDecoder(rec.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		msgRec := httptest.NewRecorder()
		msgReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+session.ID+"/messages?stream=true", strings.NewReader(`{"content":"hello"}`))
		msgReq.SetPathValue("id", session.ID)
		srv.handleAgentMessagePost(msgRec, msgReq)
		close(done)
	}()

	<-started

	rec = httptest.NewRecorder()
	interruptReq := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+session.ID+"/interrupt", nil)
	interruptReq.SetPathValue("id", session.ID)
	srv.handleAgentSessionInterrupt(rec, interruptReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("interrupt returned %d: %s", rec.Code, rec.Body.String())
	}

	<-done

	_, backend, exists := srv.agentForSession(session.ID)
	if !exists {
		t.Fatal("session not found")
	}
	stored, ok := backend.GetSession(session.ID)
	if !ok {
		t.Fatal("session not found in backend")
	}
	if stored.Active {
		t.Fatal("expected session Active to be false after interrupt")
	}
}
