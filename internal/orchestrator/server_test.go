package orchestrator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hermes"
)

// newTestServer builds an orchestrator with its memory database isolated to
// the test's temp directory so runs never touch the real user store.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	srv := NewWithMemory(filepath.Join(t.TempDir(), "orch.db"))
	t.Cleanup(func() {
		if srv.mem != nil {
			_ = srv.mem.Close()
		}
	})
	return srv
}

func TestHarnessSelection(t *testing.T) {
	deepseek := NewWithMemoryAndHarness(filepath.Join(t.TempDir(), "deepseek.db"), "deepseek")
	if deepseek.HarnessName() != "deepseek" {
		t.Fatalf("expected deepseek harness, got %q", deepseek.HarnessName())
	}
	if deepseek.mem != nil {
		defer deepseek.mem.Close()
	}

	unknown := NewWithMemoryAndHarness(filepath.Join(t.TempDir(), "unknown.db"), "unknown")
	if unknown.HarnessName() != "deepseek" {
		t.Fatalf("expected unknown harness to fall back to deepseek, got %q", unknown.HarnessName())
	}
	if unknown.mem != nil {
		defer unknown.mem.Close()
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
	var tools []hermes.ToolDef
	if err := json.NewDecoder(rec.Body).Decode(&tools); err != nil {
		t.Fatalf("failed to decode tools: %v", err)
	}
	if len(tools) == 0 {
		t.Errorf("expected tools list, got empty")
	}

	// 2. Test GET /api/agent/models
	req = httptest.NewRequest(http.MethodGet, "/api/agent/models", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/agent/models returned %d", rec.Code)
	}
	var models []hermes.ModelInfo
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
	var createdSession hermes.Session
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
	var respMsg hermes.Message
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
	var models []hermes.ModelInfo
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
