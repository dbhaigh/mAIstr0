package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestOptionalBackendSelectionHonorsDeclaredCapabilities(t *testing.T) {
	srv := newTestServer(t)
	backend := &namedControlledHarness{
		controlledHarnessBackend: newControlledHarnessBackend("external-session"),
		name:                     "external",
	}
	capabilities := agent.Capabilities{SessionResume: false}
	if err := srv.harness.registry.Register(agent.BackendRegistration{
		Backend: backend, Capabilities: capabilities,
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

var _ agent.Backend = (*namedControlledHarness)(nil)
