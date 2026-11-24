package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/config"
)

type namedControlledHarness struct {
	*controlledHarnessBackend
	name           string
	interruptCalls atomic.Int32
	deleteFails    bool
}

func (b *namedControlledHarness) Name() string { return b.name }

func (b *namedControlledHarness) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	session := &agent.Session{
		ID: b.name + "-session", Title: title, Harness: b.name,
		Messages: []agent.Message{}, Memory: map[string]string{}, Config: cfg,
	}
	b.mu.Lock()
	b.sessions[session.ID] = session
	b.mu.Unlock()
	copy := *session
	return &copy, nil
}

func (b *namedControlledHarness) InterruptSession(string) bool {
	b.interruptCalls.Add(1)
	return false
}

func (b *namedControlledHarness) DeleteSession(id string) bool {
	if b.deleteFails {
		return false
	}
	return b.controlledHarnessBackend.DeleteSession(id)
}

func TestOptionalBackendSelectionHonorsDeclaredCapabilities(t *testing.T) {
	srv := newTestServer(t)
	backend := &namedControlledHarness{
		controlledHarnessBackend: newControlledHarnessBackend("external-session"),
		name:                     "external",
	}
	capabilities := agent.Capabilities{SessionResume: false}
	execution := agent.ExecutionProfile{
		Location: agent.ExecutionRemote, Workspace: "/srv/approved",
		Isolation:           "Workspace-scoped; not an OS sandbox.",
		ApprovalLimitations: "No approval interaction.",
	}
	if err := srv.harness.registry.Register(agent.BackendRegistration{
		Backend: backend, Capabilities: capabilities, Execution: execution,
	}); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPatch, "/api/agent/backend", strings.NewReader(`{"harness":"external"}`))
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("select optional backend returned %d: %s", response.Code, response.Body.String())
	}
	var status struct {
		Name         string             `json:"name"`
		Available    bool               `json:"available"`
		Capabilities agent.Capabilities `json:"capabilities"`
		Backends     []agentBackendView `json:"backends"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Name != "external" || !status.Available || status.Capabilities.SessionResume ||
		status.Capabilities.ModelSelection || status.Capabilities.Streaming ||
		status.Capabilities.Cancellation || status.Capabilities.Approvals ||
		status.Capabilities.WorkspaceIsolation || status.Capabilities.ToolAccess {
		t.Fatalf("backend status misreported capabilities: %+v", status)
	}
	foundExecution := false
	for _, registered := range status.Backends {
		if registered.Name == "external" {
			foundExecution = registered.Available && registered.Execution == execution
			break
		}
	}
	if !foundExecution || strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("backend execution profile was missing or exposed sensitive configuration: %s", response.Body.String())
	}

	for _, path := range []string{"/api/agent/tools", "/api/agent/models"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		srv.Mux().ServeHTTP(response, request)
		if response.Code != http.StatusNotImplemented {
			t.Errorf("unsupported endpoint %s returned %d: %s", path, response.Code, response.Body.String())
		}
	}

	request = httptest.NewRequest(http.MethodPost, "/api/agent/sessions",
		strings.NewReader(`{"title":"not silently dropped","coordinator_model":"model-x"}`))
	response = httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusNotImplemented || !strings.Contains(response.Body.String(), "does not support model selection") {
		t.Fatalf("unsupported model selection returned %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(`{"title":"supported session"}`))
	response = httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create optional-backend session returned %d: %s", response.Code, response.Body.String())
	}
	var session agent.Session
	if err := json.Unmarshal(response.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	if session.Harness != "external" || !strings.Contains(response.Body.String(), `"available":true`) {
		t.Fatalf("session ownership/availability was not reported: %s", response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+session.ID+"/messages?stream=true",
		strings.NewReader(`{"content":"hello"}`))
	request.SetPathValue("id", session.ID)
	response = httptest.NewRecorder()
	srv.handleAgentMessagePost(response, request)
	if response.Code != http.StatusNotImplemented || !strings.Contains(response.Body.String(), "does not support streaming") {
		t.Fatalf("unsupported streaming returned %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+session.ID+"/messages",
		strings.NewReader(`{"content":"hello"}`))
	request.SetPathValue("id", session.ID)
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleAgentMessagePost(httptest.NewRecorder(), request)
	}()
	turn := awaitHarnessTurn(t, backend.controlledHarnessBackend)
	interrupt := httptest.NewRecorder()
	interruptRequest := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+session.ID+"/interrupt", nil)
	interruptRequest.SetPathValue("id", session.ID)
	srv.handleAgentSessionInterrupt(interrupt, interruptRequest)
	if !strings.Contains(interrupt.Body.String(), `"interrupted":false`) {
		t.Fatalf("unsupported cancellation reported success: %s", interrupt.Body.String())
	}
	if turn.ctx.Err() != nil || backend.interruptCalls.Load() != 0 {
		t.Fatal("backend received a cancellation despite declaring cancellation unsupported")
	}
	turn.finish <- nil
	awaitHandler(t, done)
}

func TestBackendDeleteFailurePreservesPersistedSession(t *testing.T) {
	srv := newTestServer(t)
	backend := &namedControlledHarness{
		controlledHarnessBackend: newControlledHarnessBackend("external-session"),
		name:                     "external",
		deleteFails:              true,
	}
	if err := srv.harness.registry.Register(agent.BackendRegistration{Backend: backend}); err != nil {
		t.Fatal(err)
	}
	selectBackend := httptest.NewRecorder()
	srv.Mux().ServeHTTP(selectBackend, httptest.NewRequest(http.MethodPatch, "/api/agent/backend",
		strings.NewReader(`{"harness":"external"}`)))
	if selectBackend.Code != http.StatusOK {
		t.Fatalf("select optional backend returned %d: %s", selectBackend.Code, selectBackend.Body.String())
	}

	create := httptest.NewRecorder()
	srv.Mux().ServeHTTP(create, httptest.NewRequest(http.MethodPost, "/api/agent/sessions",
		strings.NewReader(`{"title":"keep history"}`)))
	if create.Code != http.StatusCreated {
		t.Fatalf("create optional-backend session returned %d: %s", create.Code, create.Body.String())
	}
	var session agent.Session
	if err := json.Unmarshal(create.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	session.Messages = []agent.Message{{Role: agent.RoleUser, Content: "preserve this transcript"}}
	backend.mu.Lock()
	backend.sessions[session.ID].Messages = session.Messages
	backend.mu.Unlock()
	if err := srv.persistAgentSession(&session); err != nil {
		t.Fatal(err)
	}

	deleted := httptest.NewRecorder()
	srv.Mux().ServeHTTP(deleted, httptest.NewRequest(http.MethodDelete,
		"/api/agent/sessions/"+session.ID, nil))
	if deleted.Code == http.StatusNoContent {
		t.Fatalf("backend refused deletion but API returned success: %s", deleted.Body.String())
	}
	saved, exists := srv.agentSession(session.ID)
	if !exists || saved.Harness != "external" || len(saved.Messages) != 1 ||
		saved.Messages[0].Content != "preserve this transcript" {
		t.Fatalf("backend deletion failure lost persisted session history: %+v", saved)
	}
}

func TestMissingOptionalBackendRetainsReadableSessionAndOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "orchestrator.db")
	cfg := config.DefaultOrchestratorConfig()
	cfg.Harness = "external"
	cfg.MemoryPath = path
	backend := &namedControlledHarness{
		controlledHarnessBackend: newControlledHarnessBackend("external-session"),
		name:                     "external",
	}
	session := &agent.Session{
		ID: "external-session", Harness: "external", Title: "kept history",
		Messages: []agent.Message{{Role: agent.RoleUser, Content: "preserve this history"}},
		Memory:   map[string]string{},
	}
	backend.mu.Lock()
	backend.sessions[session.ID] = session
	backend.mu.Unlock()
	first, err := NewWithConfigAndBackends(cfg, agent.BackendRegistration{Backend: backend})
	if err != nil {
		t.Fatalf("start with optional backend: %v", err)
	}
	if err := first.persistAgentSession(session); err != nil {
		t.Fatalf("persist optional-backend session: %v", err)
	}
	first.harness.setSessionHarness(session.ID, session.Harness)
	if err := first.Close(); err != nil {
		t.Fatalf("close first orchestrator: %v", err)
	}

	restarted, err := NewWithMemoryAndHarness(path, "pi")
	if err != nil {
		t.Fatalf("missing optional backend prevented startup: %v", err)
	}
	defer func() {
		if err := restarted.Close(); err != nil {
			t.Errorf("close restarted orchestrator: %v", err)
		}
	}()

	list := httptest.NewRecorder()
	restarted.Mux().ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/agent/sessions", nil))
	var sessions []agentSessionView
	if err := json.Unmarshal(list.Body.Bytes(), &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].Available || sessions[0].Harness != "external" ||
		sessions[0].UnavailableReason != "backend_unavailable" ||
		sessions[0].Messages[0].Content != "preserve this history" {
		t.Fatalf("unavailable session was not retained: %+v", sessions)
	}

	get := httptest.NewRecorder()
	getRequest := httptest.NewRequest(http.MethodGet, "/api/agent/sessions/"+session.ID, nil)
	getRequest.SetPathValue("id", session.ID)
	restarted.handleAgentSessionGet(get, getRequest)
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), "preserve this history") ||
		!strings.Contains(get.Body.String(), `"available":false`) {
		t.Fatalf("stored session cannot be read: %d %s", get.Code, get.Body.String())
	}

	message := httptest.NewRecorder()
	messageRequest := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/"+session.ID+"/messages",
		strings.NewReader(`{"content":"do not reroute"}`))
	messageRequest.SetPathValue("id", session.ID)
	restarted.handleAgentMessagePost(message, messageRequest)
	if message.Code != http.StatusServiceUnavailable || !strings.Contains(message.Body.String(), "history is preserved") {
		t.Fatalf("unavailable session did not fail without rerouting: %d %s", message.Code, message.Body.String())
	}

	backends := httptest.NewRecorder()
	restarted.Mux().ServeHTTP(backends, httptest.NewRequest(http.MethodGet, "/api/agent/backend", nil))
	if !strings.Contains(backends.Body.String(), `"name":"external","available":false`) {
		t.Fatalf("backend status did not expose missing optional backend: %s", backends.Body.String())
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/agent/sessions/"+session.ID, nil)
	deleteRequest.SetPathValue("id", session.ID)
	deleted := httptest.NewRecorder()
	restarted.handleAgentSessionDelete(deleted, deleteRequest)
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete unavailable session returned %d: %s", deleted.Code, deleted.Body.String())
	}
}

func TestBackendWithoutResumeRetainsSessionAsUnavailable(t *testing.T) {
	cfg := config.DefaultOrchestratorConfig()
	cfg.Harness = "external"
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	session := &agent.Session{
		ID: "external-session", Harness: "external", Title: "non-resumable history",
		Messages: []agent.Message{{Role: agent.RoleUser, Content: "keep this history"}},
		Memory:   map[string]string{},
	}

	firstBackend := &namedControlledHarness{
		controlledHarnessBackend: newControlledHarnessBackend(session.ID),
		name:                     "external",
	}
	first, err := NewWithConfigAndBackends(cfg, agent.BackendRegistration{
		Backend: firstBackend, Capabilities: agent.Capabilities{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.persistAgentSession(session); err != nil {
		t.Fatal(err)
	}
	first.harness.setSessionHarness(session.ID, session.Harness)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restartedBackend := &namedControlledHarness{
		controlledHarnessBackend: newControlledHarnessBackend(session.ID),
		name:                     "external",
	}
	restarted, err := NewWithConfigAndBackends(cfg, agent.BackendRegistration{
		Backend: restartedBackend, Capabilities: agent.Capabilities{},
	})
	if err != nil {
		t.Fatalf("backend without session resume prevented startup: %v", err)
	}
	defer func() {
		if err := restarted.Close(); err != nil {
			t.Errorf("close restarted orchestrator: %v", err)
		}
	}()

	saved, exists := restarted.agentSession(session.ID)
	if !exists || saved.Available || saved.UnavailableReason != "session_resume_unsupported" ||
		saved.Messages[0].Content != "keep this history" {
		t.Fatalf("non-resumable session was not retained as unavailable: %+v", saved)
	}
	if _, _, available := restarted.agentForSession(session.ID); available {
		t.Fatal("non-resumable session was exposed for sending")
	}
}

func TestClaudeCodeEnvironmentOptInFailureIsReported(t *testing.T) {
	t.Setenv("MAISTR0_CLAUDE_CODE_EXECUTABLE", "")
	t.Setenv("MAISTR0_CLAUDE_CODE_WORKSPACE", "")
	t.Setenv("MAISTR0_CODEX_EXECUTABLE", "")
	t.Setenv("MAISTR0_CODEX_WORKSPACE", "")
	t.Setenv("MAISTR0_CODEX_HOME", "")
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	if _, err := NewWithConfig(cfg); err == nil ||
		!strings.Contains(err.Error(), "configure Claude Code backend") ||
		!strings.Contains(err.Error(), "requires both MAISTR0_CLAUDE_CODE_EXECUTABLE") {
		t.Fatalf("invalid Claude Code opt-in error = %v", err)
	}
}

func TestCodexEnvironmentOptInFailureIsReported(t *testing.T) {
	unsetEnvironmentVariable(t, "MAISTR0_CLAUDE_CODE_EXECUTABLE")
	unsetEnvironmentVariable(t, "MAISTR0_CLAUDE_CODE_WORKSPACE")
	t.Setenv("MAISTR0_CODEX_EXECUTABLE", "configured")
	t.Setenv("MAISTR0_CODEX_WORKSPACE", "")
	t.Setenv("MAISTR0_CODEX_HOME", "")
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	if _, err := NewWithConfig(cfg); err == nil ||
		!strings.Contains(err.Error(), "configure Codex backend") ||
		!strings.Contains(err.Error(), "requires MAISTR0_CODEX_EXECUTABLE") {
		t.Fatalf("invalid Codex opt-in error = %v", err)
	}
}

func TestOpenCodeEnvironmentOptInFailureIsReported(t *testing.T) {
	for _, name := range []string{
		"MAISTR0_CLAUDE_CODE_EXECUTABLE", "MAISTR0_CLAUDE_CODE_WORKSPACE",
		"MAISTR0_CODEX_EXECUTABLE", "MAISTR0_CODEX_WORKSPACE", "MAISTR0_CODEX_HOME",
	} {
		unsetEnvironmentVariable(t, name)
	}
	t.Setenv("MAISTR0_OPENCODE_MODE", "connect")
	for _, name := range []string{
		"MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_WORKSPACE", "MAISTR0_OPENCODE_HOME",
		"MAISTR0_OPENCODE_SERVER_URL", "MAISTR0_OPENCODE_SERVER_PASSWORD", "MAISTR0_OPENCODE_SESSION_DIR",
	} {
		unsetEnvironmentVariable(t, name)
	}
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	if _, err := NewWithConfig(cfg); err == nil ||
		!strings.Contains(err.Error(), "configure OpenCode backend") ||
		!strings.Contains(err.Error(), "MAISTR0_OPENCODE_SERVER_URL") {
		t.Fatalf("invalid OpenCode opt-in error = %v", err)
	}
}

func TestOpenCodeBackendIsRegisteredWithOnlySupportedCapabilities(t *testing.T) {
	const password = "test-only-opencode-password"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user, got, ok := r.BasicAuth(); !ok || user != "opencode" || got != password {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/info":
			_ = json.NewEncoder(w).Encode(map[string]string{"version": "2.3.2"})
		case "/api/plugin", "/api/mcp":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	for _, name := range []string{
		"MAISTR0_CLAUDE_CODE_EXECUTABLE", "MAISTR0_CLAUDE_CODE_WORKSPACE",
		"MAISTR0_CODEX_EXECUTABLE", "MAISTR0_CODEX_WORKSPACE", "MAISTR0_CODEX_HOME",
	} {
		unsetEnvironmentVariable(t, name)
	}
	for _, name := range []string{
		"MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_HOME",
	} {
		t.Setenv(name, "")
	}
	workspace := t.TempDir()
	t.Setenv("MAISTR0_OPENCODE_MODE", "connect")
	t.Setenv("MAISTR0_OPENCODE_WORKSPACE", workspace)
	t.Setenv("MAISTR0_OPENCODE_SERVER_URL", server.URL)
	t.Setenv("MAISTR0_OPENCODE_SERVER_PASSWORD", password)
	t.Setenv("MAISTR0_OPENCODE_SESSION_DIR", filepath.Join(t.TempDir(), "sessions"))
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	srv, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatalf("start orchestrator with OpenCode backend: %v", err)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			t.Errorf("close orchestrator: %v", err)
		}
	}()

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/agent/backend", nil))
	var status struct {
		Backends []struct {
			Name         string             `json:"name"`
			Available    bool               `json:"available"`
			Capabilities agent.Capabilities `json:"capabilities"`
		} `json:"backends"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &status) != nil {
		t.Fatalf("backend status response = %d %s", response.Code, response.Body.String())
	}
	for _, backend := range status.Backends {
		if backend.Name == "opencode" {
			capabilities := backend.Capabilities
			if !backend.Available || !capabilities.SessionResume || !capabilities.Cancellation ||
				capabilities.Streaming || capabilities.Approvals || capabilities.WorkspaceIsolation ||
				capabilities.ModelSelection || capabilities.ToolAccess {
				t.Fatalf("OpenCode backend capabilities = %+v, available=%t", capabilities, backend.Available)
			}
			return
		}
	}
	t.Fatal("OpenCode backend was not registered")
}

func TestOpenHandsEnvironmentOptInFailureIsReported(t *testing.T) {
	for _, name := range []string{
		"MAISTR0_CLAUDE_CODE_EXECUTABLE", "MAISTR0_CLAUDE_CODE_WORKSPACE",
		"MAISTR0_CODEX_EXECUTABLE", "MAISTR0_CODEX_WORKSPACE", "MAISTR0_CODEX_HOME",
		"MAISTR0_OPENCODE_MODE", "MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_WORKSPACE",
		"MAISTR0_OPENCODE_HOME", "MAISTR0_OPENCODE_SERVER_URL",
		"MAISTR0_OPENCODE_SERVER_PASSWORD", "MAISTR0_OPENCODE_SESSION_DIR",
		"MAISTR0_GEMINI_EXECUTABLE", "MAISTR0_GEMINI_WORKSPACE", "MAISTR0_GEMINI_HOME",
		"MAISTR0_GEMINI_SANDBOX", "MAISTR0_GEMINI_AUTH_MODE",
		"MAISTR0_HERMES_SERVER_URL", "MAISTR0_HERMES_API_KEY", "MAISTR0_HERMES_SESSION_DIR",
	} {
		unsetEnvironmentVariable(t, name)
	}
	t.Setenv("MAISTR0_OPENHANDS_SERVER_URL", "https://openhands.example")
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	if _, err := NewWithConfig(cfg); err == nil ||
		!strings.Contains(err.Error(), "configure OpenHands backend") ||
		!strings.Contains(err.Error(), "MAISTR0_OPENHANDS_SESSION_API_KEY") {
		t.Fatalf("invalid OpenHands opt-in error = %v", err)
	}
}

func TestOpenHandsBackendIsRegisteredWithDeclaredCapabilities(t *testing.T) {
	const sessionKey = "test-openhands-key"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/server_info":
			_ = json.NewEncoder(w).Encode(map[string]string{"version": "1.63.1"})
		case "/api/conversations/count":
			if r.Header.Get("X-Session-API-Key") == sessionKey {
				_ = json.NewEncoder(w).Encode(map[string]int{"count": 0})
				return
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	for _, name := range []string{
		"MAISTR0_CLAUDE_CODE_EXECUTABLE", "MAISTR0_CLAUDE_CODE_WORKSPACE",
		"MAISTR0_CODEX_EXECUTABLE", "MAISTR0_CODEX_WORKSPACE", "MAISTR0_CODEX_HOME",
		"MAISTR0_OPENCODE_MODE", "MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_WORKSPACE",
		"MAISTR0_OPENCODE_HOME", "MAISTR0_OPENCODE_SERVER_URL",
		"MAISTR0_OPENCODE_SERVER_PASSWORD", "MAISTR0_OPENCODE_SESSION_DIR",
		"MAISTR0_GEMINI_EXECUTABLE", "MAISTR0_GEMINI_WORKSPACE", "MAISTR0_GEMINI_HOME",
		"MAISTR0_GEMINI_SANDBOX", "MAISTR0_GEMINI_AUTH_MODE",
		"MAISTR0_HERMES_SERVER_URL", "MAISTR0_HERMES_API_KEY", "MAISTR0_HERMES_SESSION_DIR",
	} {
		unsetEnvironmentVariable(t, name)
	}
	t.Setenv("MAISTR0_OPENHANDS_SERVER_URL", server.URL)
	t.Setenv("MAISTR0_OPENHANDS_SESSION_API_KEY", sessionKey)
	t.Setenv("MAISTR0_OPENHANDS_WORKSPACE", t.TempDir())
	t.Setenv("MAISTR0_OPENHANDS_MODEL", "test/model")
	t.Setenv("MAISTR0_OPENHANDS_SESSION_DIR", filepath.Join(t.TempDir(), "sessions"))
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	srv, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatalf("start orchestrator with OpenHands backend: %v", err)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			t.Errorf("close orchestrator: %v", err)
		}
	}()

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/agent/backend", nil))
	var status struct {
		Backends []struct {
			Name         string             `json:"name"`
			Available    bool               `json:"available"`
			Capabilities agent.Capabilities `json:"capabilities"`
		} `json:"backends"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &status) != nil {
		t.Fatalf("backend status response = %d %s", response.Code, response.Body.String())
	}
	for _, backend := range status.Backends {
		if backend.Name == "openhands" {
			capabilities := backend.Capabilities
			if !backend.Available || !capabilities.SessionResume || !capabilities.Streaming ||
				!capabilities.Cancellation || capabilities.Approvals ||
				capabilities.WorkspaceIsolation || capabilities.ModelSelection || capabilities.ToolAccess {
				t.Fatalf("OpenHands capabilities = %+v, available=%t", capabilities, backend.Available)
			}
			return
		}
	}
	t.Fatal("OpenHands backend was not registered")
}

func TestHermesEnvironmentOptInFailureIsReported(t *testing.T) {
	for _, name := range []string{
		"MAISTR0_CLAUDE_CODE_EXECUTABLE", "MAISTR0_CLAUDE_CODE_WORKSPACE",
		"MAISTR0_CODEX_EXECUTABLE", "MAISTR0_CODEX_WORKSPACE", "MAISTR0_CODEX_HOME",
		"MAISTR0_OPENCODE_MODE", "MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_WORKSPACE",
		"MAISTR0_OPENCODE_HOME", "MAISTR0_OPENCODE_SERVER_URL",
		"MAISTR0_OPENCODE_SERVER_PASSWORD", "MAISTR0_OPENCODE_SESSION_DIR",
		"MAISTR0_GEMINI_EXECUTABLE", "MAISTR0_GEMINI_WORKSPACE", "MAISTR0_GEMINI_HOME",
		"MAISTR0_GEMINI_SANDBOX", "MAISTR0_GEMINI_AUTH_MODE",
		"MAISTR0_OPENHANDS_SERVER_URL", "MAISTR0_OPENHANDS_SESSION_API_KEY",
		"MAISTR0_OPENHANDS_WORKSPACE", "MAISTR0_OPENHANDS_MODEL", "MAISTR0_OPENHANDS_SESSION_DIR",
		"MAISTR0_HERMES_API_KEY", "MAISTR0_HERMES_SESSION_DIR",
	} {
		unsetEnvironmentVariable(t, name)
	}
	t.Setenv("MAISTR0_HERMES_SERVER_URL", "https://hermes.example")
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	if _, err := NewWithConfig(cfg); err == nil ||
		!strings.Contains(err.Error(), "configure Hermes backend") ||
		!strings.Contains(err.Error(), "requires MAISTR0_HERMES_API_KEY") {
		t.Fatalf("invalid Hermes opt-in error = %v", err)
	}
}

func TestHermesBackendIsRegisteredWithDeclaredCapabilities(t *testing.T) {
	const apiKey = "test-hermes-api-key"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "ok", "platform": "hermes-agent", "version": "0.23.3",
			})
		case "/v1/capabilities":
			if r.Header.Get("Authorization") != "Bearer "+apiKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"object": "hermes.api_server.capabilities", "platform": "hermes-agent",
				"auth": map[string]any{"type": "bearer", "required": true},
				"features": map[string]any{
					"run_submission": true, "run_status": true, "run_events_sse": true, "run_stop": true,
					"runs_idempotency": map[string]any{"supported": true, "durable": true},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	for _, name := range []string{
		"MAISTR0_CLAUDE_CODE_EXECUTABLE", "MAISTR0_CLAUDE_CODE_WORKSPACE",
		"MAISTR0_CODEX_EXECUTABLE", "MAISTR0_CODEX_WORKSPACE", "MAISTR0_CODEX_HOME",
		"MAISTR0_OPENCODE_MODE", "MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_WORKSPACE",
		"MAISTR0_OPENCODE_HOME", "MAISTR0_OPENCODE_SERVER_URL",
		"MAISTR0_OPENCODE_SERVER_PASSWORD", "MAISTR0_OPENCODE_SESSION_DIR",
		"MAISTR0_GEMINI_EXECUTABLE", "MAISTR0_GEMINI_WORKSPACE", "MAISTR0_GEMINI_HOME",
		"MAISTR0_GEMINI_SANDBOX", "MAISTR0_GEMINI_AUTH_MODE",
		"MAISTR0_OPENHANDS_SERVER_URL", "MAISTR0_OPENHANDS_SESSION_API_KEY",
		"MAISTR0_OPENHANDS_WORKSPACE", "MAISTR0_OPENHANDS_MODEL", "MAISTR0_OPENHANDS_SESSION_DIR",
	} {
		unsetEnvironmentVariable(t, name)
	}
	t.Setenv("MAISTR0_HERMES_SERVER_URL", server.URL)
	t.Setenv("MAISTR0_HERMES_API_KEY", apiKey)
	t.Setenv("MAISTR0_HERMES_SESSION_DIR", filepath.Join(t.TempDir(), "sessions"))
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	srv, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatalf("start orchestrator with Hermes backend: %v", err)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			t.Errorf("close orchestrator: %v", err)
		}
	}()

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/agent/backend", nil))
	var status struct {
		Backends []struct {
			Name         string             `json:"name"`
			Available    bool               `json:"available"`
			Capabilities agent.Capabilities `json:"capabilities"`
		} `json:"backends"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &status) != nil {
		t.Fatalf("backend status response = %d %s", response.Code, response.Body.String())
	}
	for _, backend := range status.Backends {
		if backend.Name == "hermes" {
			capabilities := backend.Capabilities
			if !backend.Available || !capabilities.SessionResume || !capabilities.Streaming ||
				!capabilities.Cancellation || capabilities.Approvals ||
				capabilities.WorkspaceIsolation || !capabilities.ModelSelection || capabilities.ToolAccess {
				t.Fatalf("Hermes capabilities = %+v, available=%t", capabilities, backend.Available)
			}
			return
		}
	}
	t.Fatal("Hermes backend was not registered")
}

func unsetEnvironmentVariable(t *testing.T, name string) {
	t.Helper()
	value, exists := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if exists {
			_ = os.Setenv(name, value)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}

var _ agent.Backend = (*namedControlledHarness)(nil)
