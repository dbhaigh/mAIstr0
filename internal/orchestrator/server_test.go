package orchestrator

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/deepseek"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/piagent"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
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
	provider := &localPiProvider{server: srv}
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
			}
			for _, guidance := range []string{
				"`cluster_status`:", "`node_converse`:", "`cluster_parallel_dispatch`:",
				"`recall_memory`:", "`remember_fact`:",
			} {
				if !strings.Contains(request.Prompt, guidance) {
					t.Errorf("Pi prompt omitted tool usage guidance %q", guidance)
				}
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
	backend := srv.agents["pi"]
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
	if piTools, deepSeekTools := backend.ListTools(), srv.agents["deepseek"].ListTools(); !reflect.DeepEqual(piTools, deepSeekTools) {
		t.Fatalf("Pi tools differ from DeepSeek tools:\nPi: %#v\nDeepSeek: %#v", piTools, deepSeekTools)
	}
	if piTools, deepSeekTools := backend.ListTools(), srv.agents["deepseek"].ListTools(); !reflect.DeepEqual(piTools, deepSeekTools) {
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
	}

	tags := httptest.NewRecorder()
	mux.ServeHTTP(tags, httptest.NewRequest(http.MethodGet, "/api/tags", nil))
	if tags.Code != http.StatusOK || !strings.Contains(tags.Body.String(), `"name":"compat-model"`) {
		t.Fatalf("GET /api/tags returned %d: %s", tags.Code, tags.Body.String())
	}
}

func TestPairingJoinPinsCertificateAndRotatesPIN(t *testing.T) {
	srv := newTestServer(t)
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), srv.selfID)
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
	if joined.ID != srv.selfID || joined.Peers[node.ID()] != node.CertificatePEM() {
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

func TestOrchestratorsPairAndAuthenticateFederationRequests(t *testing.T) {
	left := newTestServer(t)
	right := newTestServer(t)
	left.selfID = "orchestrator-left"
	right.selfID = "orchestrator-right"
	if err := left.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	if err := right.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	node, err := peeridentity.Open(filepath.Join(t.TempDir(), "federated-node.json"), "federated-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := left.identity.TrustPeer(node.ID(), node.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := left.SetAdvertiseAddress("https://left.example:7450"); err != nil {
		t.Fatal(err)
	}
	rightPIN, err := right.identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}

	rightMux := right.Mux()
	rightServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pairing/join" {
			rightMux.ServeHTTP(w, r)
			return
		}
		right.identity.RequireTrustedPeer(rightMux).ServeHTTP(w, r)
	}))
	rightServer.TLS = right.identity.ServerTLSConfig()
	rightServer.StartTLS()
	defer rightServer.Close()

	connectBody, err := json.Marshal(map[string]string{"address": rightServer.URL, "pin": rightPIN})
	if err != nil {
		t.Fatal(err)
	}
	connectResponse := httptest.NewRecorder()
	left.Mux().ServeHTTP(connectResponse, httptest.NewRequest(http.MethodPost, "/api/pairing/connect", bytes.NewReader(connectBody)))
	if connectResponse.Code != http.StatusOK {
		t.Fatalf("POST /api/pairing/connect returned %d: %s", connectResponse.Code, connectResponse.Body.String())
	}
	var connectResult map[string]string
	if err := json.Unmarshal(connectResponse.Body.Bytes(), &connectResult); err != nil {
		t.Fatal(err)
	}
	if connectResult["paired_orchestrator"] != rightServer.URL {
		t.Fatalf("paired address = %q, want %q", connectResult["paired_orchestrator"], rightServer.URL)
	}
	if !left.identity.IsTrustedID(right.selfID) || !right.identity.IsTrustedID(left.selfID) {
		t.Fatal("pairing did not establish reciprocal orchestrator trust")
	}
	if !right.identity.IsTrustedID(node.ID()) {
		t.Fatal("federation did not transfer the joining orchestrator's trusted node certificate")
	}
	if right.identity.PairedOrchestratorAddresses()[left.selfID] != "https://left.example:7450" {
		t.Fatal("receiving orchestrator did not persist the joining orchestrator address")
	}
	if err := node.TrustPeer(right.selfID, right.identity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	nodeClient := &http.Client{Timeout: 3 * time.Second, Transport: node.ClientTransport()}
	rosterResponse, err := nodeClient.Get(rightServer.URL + "/api/pairing/roster")
	if err != nil {
		t.Fatalf("federated node mTLS request failed: %v", err)
	}
	rosterResponse.Body.Close()
	if rosterResponse.StatusCode != http.StatusOK {
		t.Fatalf("federated node mTLS request returned %s, want 200", rosterResponse.Status)
	}
	right.registry.Upsert(cluster.NodeStatus{
		ID: "federated-worker", Address: "https://worker.example:7451", Healthy: true,
		Models: []engine.Model{{Name: "shared-model", Tags: []string{"general"}}},
	})
	left.syncDiscoveredPeers()
	if node, ok := left.registry.Get("federated-worker"); !ok || node.Address != "https://worker.example:7451" {
		t.Fatalf("paired orchestrator cluster state was not synchronized: %#v, found=%v", node, ok)
	}
	left.peerMu.RLock()
	pairedAddress := left.pairedOrchestrators[right.selfID]
	left.peerMu.RUnlock()
	if pairedAddress != rightServer.URL {
		t.Fatalf("paired address = %q, want %q", pairedAddress, rightServer.URL)
	}
	reloadedLeft, err := peeridentity.Open(left.identityPath, left.selfID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedLeft.PairedOrchestratorAddresses()[right.selfID] != rightServer.URL {
		t.Fatal("initiating orchestrator address did not survive identity reload")
	}

	client := &http.Client{Timeout: 3 * time.Second, Transport: left.identity.ClientTransport()}
	response, err := client.Get(rightServer.URL + "/api/pairing/roster")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("authenticated federation request returned %d, want 200", response.StatusCode)
	}

	unpaired, err := peeridentity.Open(filepath.Join(t.TempDir(), "unpaired.json"), "unpaired-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	unpairedClient := &http.Client{Timeout: 3 * time.Second, Transport: unpaired.BootstrapTransport()}
	response, err = unpairedClient.Get(rightServer.URL + "/api/pairing/roster")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unpaired federation request returned %d, want 403", response.StatusCode)
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
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "retry succeeded"})
	}))
	defer second.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: first.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-b", Address: second.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	})
	task, err := srv.tasks.CreateChecked("Run one step", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "sub-a", Description: "Run it", TaskType: "general", Tags: []string{"general"}},
		NodeID:  "node-a", Address: first.URL, Model: "general",
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
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("dispatch attempts = (%d, %d), want (1, 1)", firstCalls.Load(), secondCalls.Load())
	}
}
