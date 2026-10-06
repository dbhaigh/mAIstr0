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
	"github.com/maistr0/maistr0/internal/deepseek"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
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
