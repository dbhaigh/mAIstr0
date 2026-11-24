package openhands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/maistr0/maistr0/internal/agent"
)

const (
	fakeSessionKey    = "test-session-key"
	fakeConversation  = "123e4567-e89b-12d3-a456-426614174000"
	fakeAssistantText = "OpenHands test answer"
)

type fakeOpenHandsServer struct {
	server    *httptest.Server
	workspace string

	mu              sync.Mutex
	version         string
	mode            string
	status          string
	finalResponse   string
	lastSeq         int64
	socketCount     int
	promptCount     int
	createdRequest  map[string]any
	authDisabled    bool
	wrongWorkspace  bool
	ignoreInterrupt bool
	interrupted     chan struct{}
	interruptOnce   sync.Once
}

func newFakeOpenHandsServer(t *testing.T, workspace string) *fakeOpenHandsServer {
	t.Helper()
	fake := &fakeOpenHandsServer{
		workspace: workspace, version: SupportedVersion, status: "idle", lastSeq: -1,
		finalResponse: fakeAssistantText, interrupted: make(chan struct{}),
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeOpenHandsServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/server_info" {
		f.mu.Lock()
		version := f.version
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"version": version})
		return
	}
	if r.URL.Path == "/api/conversations/count" {
		f.mu.Lock()
		authDisabled := f.authDisabled
		f.mu.Unlock()
		if !authDisabled && r.Header.Get("X-Session-API-Key") != fakeSessionKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"count": 0})
		return
	}
	if r.URL.Path == "/sockets/session/"+fakeConversation {
		f.serveSocket(w, r)
		return
	}
	if r.Header.Get("X-Session-API-Key") != fakeSessionKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/conversations":
		f.createConversation(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/conversations/"+fakeConversation:
		f.conversationInfo(w)
	case r.Method == http.MethodGet && r.URL.Path == "/api/conversations/"+fakeConversation+"/agent_final_response":
		f.mu.Lock()
		response := f.finalResponse
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"response": response})
	case r.Method == http.MethodPost && r.URL.Path == "/api/conversations/"+fakeConversation+"/interrupt":
		f.mu.Lock()
		ignore := f.ignoreInterrupt
		if !ignore {
			f.status = "paused"
		}
		f.mu.Unlock()
		if !ignore {
			f.interruptOnce.Do(func() { close(f.interrupted) })
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete && r.URL.Path == "/api/conversations/"+fakeConversation:
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeOpenHandsServer) createConversation(w http.ResponseWriter, r *http.Request) {
	var request map[string]any
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.createdRequest = request
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": fakeConversation, "execution_status": "idle",
		"workspace": map[string]string{
			"kind": "LocalWorkspace", "working_dir": f.workspace,
		},
	})
}

func (f *fakeOpenHandsServer) conversationInfo(w http.ResponseWriter) {
	f.mu.Lock()
	status := f.status
	workspace := f.workspace
	if f.wrongWorkspace {
		workspace = workspace + "-unexpected"
	}
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": fakeConversation, "execution_status": status,
		"workspace": map[string]string{
			"kind": "LocalWorkspace", "working_dir": workspace,
		},
	})
}

func (f *fakeOpenHandsServer) serveSocket(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Session-API-Key") != fakeSessionKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	authCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(authCtx)
	if err != nil {
		return
	}
	var auth struct {
		Type string `json:"type"`
		Key  string `json:"session_api_key"`
	}
	if json.Unmarshal(data, &auth) != nil || auth.Type != "auth" || auth.Key != fakeSessionKey {
		return
	}

	afterSeq, err := strconv.ParseInt(r.URL.Query().Get("after_seq"), 10, 64)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.socketCount++
	socketNumber := f.socketCount
	mode := f.mode
	throughSeq := f.lastSeq
	if mode == "reconnect" && socketNumber == 2 {
		throughSeq = 1
		f.status = "finished"
		f.finalResponse = fakeAssistantText
		f.lastSeq = 1
	}
	f.mu.Unlock()
	if err := writeSocketJSON(r.Context(), conn, map[string]any{
		"type": "sync", "from_seq": afterSeq, "through_seq": throughSeq,
	}); err != nil {
		return
	}

	if mode == "reconnect" && socketNumber == 2 {
		_ = writeDurable(r.Context(), conn, 1, "assistant-reconnected", "assistant", fakeAssistantText)
		return
	}
	if mode == "reconnect" && socketNumber > 2 {
		return
	}

	_, prompt, err := conn.Read(r.Context())
	if err != nil {
		return
	}
	var message struct {
		Role    string `json:"role"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(prompt, &message) != nil || message.Role != "user" || len(message.Content) != 1 {
		return
	}
	f.mu.Lock()
	f.promptCount++
	f.status = "running"
	if mode != "block" && mode != "malformed" && mode != "reconnect" && mode != "failed" {
		f.status = "finished"
		f.finalResponse = fakeAssistantText
		f.lastSeq = 1
	}
	if mode == "failed" {
		f.status = "error"
	}
	if mode == "reconnect" {
		f.status = "running"
		f.lastSeq = 0
	}
	f.mu.Unlock()

	switch mode {
	case "block":
		select {
		case <-f.interrupted:
		case <-r.Context().Done():
		}
	case "malformed":
		_ = writeSocketJSON(r.Context(), conn, map[string]any{
			"type": "error", "content": "sensitive-output-must-not-leak",
		})
	case "failed":
		return
	case "reconnect":
		_ = writeDurable(r.Context(), conn, 0, "user-reconnect", "user", message.Content[0].Text)
	case "continuation":
		f.mu.Lock()
		f.status = "finished"
		f.finalResponse = fakeAssistantText
		f.lastSeq = 1
		f.mu.Unlock()
		_ = writeDurable(r.Context(), conn, 0, "user-new", "user", message.Content[0].Text)
		_ = writeDurable(r.Context(), conn, 1, "assistant-new", "assistant", fakeAssistantText)
	default:
		_ = writeDurable(r.Context(), conn, 0, "user-normal", "user", message.Content[0].Text)
		_ = writeDurable(r.Context(), conn, 1, "assistant-normal", "assistant", fakeAssistantText)
	}
}

func writeSocketJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func writeDurable(
	ctx context.Context,
	conn *websocket.Conn,
	sequence int64,
	id, role, content string,
) error {
	event := map[string]any{
		"id": id, "kind": "MessageEvent", "timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"llm_message": map[string]any{
			"role":    role,
			"content": []map[string]string{{"type": "text", "text": content}},
		},
	}
	return writeSocketJSON(ctx, conn, map[string]any{
		"type": "durable", "seq": sequence, "event": event,
	})
}

func newFakeBackend(t *testing.T, fake *fakeOpenHandsServer) (*Backend, *agent.Session) {
	t.Helper()
	backend, err := New(Config{
		ServerURL: fake.server.URL, SessionKey: fakeSessionKey, Workspace: fake.workspace,
		Model: "test/model", SessionDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	backend.pollInterval = 10 * time.Millisecond
	backend.disconnectWait = 500 * time.Millisecond
	backend.cancelWait = 200 * time.Millisecond
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close OpenHands backend: %v", err)
		}
	})
	session, err := backend.CreateSession("test", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return backend, session
}

func TestOpenHandsAuthenticationAndExplicitWorkspacePolicy(t *testing.T) {
	workspace := t.TempDir()
	fake := newFakeOpenHandsServer(t, workspace)
	fake.version = "1.63.2"
	if _, err := New(Config{
		ServerURL: fake.server.URL, SessionKey: fakeSessionKey, Workspace: workspace,
		Model: "test/model", SessionDir: t.TempDir(),
	}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("unsupported server version error = %v", err)
	}
	fake.version = SupportedVersion
	if _, err := New(Config{
		ServerURL: fake.server.URL, SessionKey: "wrong-key", Workspace: workspace,
		Model: "test/model", SessionDir: t.TempDir(),
	}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("invalid session key error = %v", err)
	}

	backend, _ := newFakeBackend(t, fake)
	fake.mu.Lock()
	request := fake.createdRequest
	fake.mu.Unlock()
	workspaceRequest := nestedMap(request, "workspace")
	agentRequest := nestedMap(request, "agent")
	mcpConfig := nestedMap(agentRequest, "mcp_config")
	if workspaceRequest["kind"] != "LocalWorkspace" || workspaceRequest["working_dir"] != workspace {
		t.Fatalf("conversation workspace policy = %#v", workspaceRequest)
	}
	if request["conversation_runtime"] != "local" ||
		nestedMap(request, "confirmation_policy")["kind"] != "NeverConfirm" ||
		len(nestedSlice(agentRequest, "tools")) != 2 ||
		len(nestedMap(mcpConfig, "mcpServers")) != 0 ||
		len(nestedSlice(request, "plugins")) != 0 ||
		request["hook_config"] != nil {
		t.Fatalf("conversation execution policy = %#v", request)
	}
	if backend.Version() != SupportedVersion {
		t.Fatalf("backend version = %q", backend.Version())
	}
	if _, err := backend.CreateSession("unsupported", agent.SessionConfig{SystemPrompt: "not supported"}); !errors.Is(err, ErrUnsupportedConfiguration) {
		t.Fatalf("unsupported session configuration error = %v", err)
	}
}

func TestOpenHandsMapsDurableMessagesAndFinalResult(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	backend, session := newFakeBackend(t, fake)
	stream := make(chan agent.StreamEvent, 4)
	message, err := backend.SendMessage(context.Background(), session.ID, "safe test prompt", stream)
	if err != nil {
		t.Fatal(err)
	}
	if message == nil || message.Role != agent.RoleAssistant || message.Content != fakeAssistantText {
		t.Fatalf("final message = %+v", message)
	}
	saved, ok := backend.GetSession(session.ID)
	if !ok || len(saved.Messages) != 2 || saved.Messages[0].Role != agent.RoleUser ||
		saved.Messages[0].Content != "safe test prompt" || saved.Messages[1].Content != fakeAssistantText {
		t.Fatalf("reconciled session = %+v", saved)
	}
	seenFinal, seenDone := false, false
	for len(stream) > 0 {
		event := <-stream
		seenFinal = seenFinal || event.Type == agent.EventFinalMessage
		seenDone = seenDone || event.Type == agent.EventDone
	}
	if !seenFinal || !seenDone {
		t.Fatalf("terminal stream events missing: final=%t done=%t", seenFinal, seenDone)
	}
	fake.mu.Lock()
	promptCount := fake.promptCount
	fake.mu.Unlock()
	if promptCount != 1 {
		t.Fatalf("server received %d prompts, want 1", promptCount)
	}
}

func TestOpenHandsReconnectReplaysAcceptedEvents(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	fake.mode = "reconnect"
	backend, session := newFakeBackend(t, fake)
	message, err := backend.SendMessage(context.Background(), session.ID, "accepted once", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != fakeAssistantText {
		t.Fatalf("final message = %+v", message)
	}
	saved, _ := backend.GetSession(session.ID)
	if len(saved.Messages) != 2 || saved.Messages[0].Content != "accepted once" ||
		saved.Messages[1].Content != fakeAssistantText {
		t.Fatalf("replayed session messages = %+v", saved.Messages)
	}
	fake.mu.Lock()
	socketCount, promptCount := fake.socketCount, fake.promptCount
	fake.mu.Unlock()
	if socketCount != 2 || promptCount != 1 {
		t.Fatalf("connections=%d prompts=%d, want reconnect without resubmission", socketCount, promptCount)
	}
}

func TestOpenHandsCancellationRequiresServerConfirmation(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	fake.mode = "block"
	backend, session := newFakeBackend(t, fake)
	sendDone := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "cancel me", nil)
		sendDone <- err
	}()
	waitForCondition(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.promptCount == 1
	})
	if !backend.InterruptSession(session.ID) {
		t.Fatal("cancellation was not confirmed")
	}
	select {
	case err := <-sendDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled send error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send did not return after confirmed cancellation")
	}

	fake.mu.Lock()
	fake.status = "running"
	fake.ignoreInterrupt = true
	fake.mu.Unlock()
	backend.mu.RLock()
	state := backend.sessions[session.ID]
	backend.mu.RUnlock()
	if err := backend.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = true
		mapping.PendingSince = time.Now()
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.interruptAndConfirm(state); !errors.Is(err, ErrCancellationUnconfirmed) {
		t.Fatalf("unconfirmed cancellation error = %v", err)
	}
	saved, _ := backend.GetSession(session.ID)
	if !saved.Active {
		t.Fatal("unconfirmed remote execution was not retained as active")
	}
	fake.mu.Lock()
	fake.ignoreInterrupt = false
	fake.status = "paused"
	fake.mu.Unlock()
	if err := backend.clearPending(state); err != nil {
		t.Fatal(err)
	}
}

func TestOpenHandsDeleteWaitsForActiveTurnToStop(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	fake.mode = "block"
	backend, session := newFakeBackend(t, fake)
	sendDone := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "delete active turn", nil)
		sendDone <- err
	}()
	waitForCondition(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.promptCount == 1
	})
	if !backend.DeleteSession(session.ID) {
		t.Fatal("backend deleted a session without confirming the active turn stopped")
	}
	select {
	case err := <-sendDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("deleted turn error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active send did not stop before session deletion")
	}
	if _, ok := backend.GetSession(session.ID); ok {
		t.Fatal("deleted session remained available")
	}
}

func TestOpenHandsTerminalFailureIsReported(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	fake.mode = "failed"
	backend, session := newFakeBackend(t, fake)
	_, err := backend.SendMessage(context.Background(), session.ID, "fail safely", nil)
	if !errors.Is(err, ErrTurnFailed) {
		t.Fatalf("terminal failure error = %v", err)
	}
	saved, ok := backend.GetSession(session.ID)
	if !ok || saved.Active {
		t.Fatalf("terminal failure left an active session: %+v", saved)
	}
}

func TestOpenHandsRestoredPendingTurnContinuesWithoutResubmission(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	first, session := newFakeBackend(t, fake)
	first.mu.RLock()
	state := first.sessions[session.ID]
	first.mu.RUnlock()
	if err := first.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = true
		mapping.PendingStartSeq = -1
		mapping.PendingSince = time.Now()
	}); err != nil {
		t.Fatal(err)
	}
	session.Messages = append(session.Messages, agent.Message{
		ID: "persisted-user", Role: agent.RoleUser, Content: "previous prompt", Timestamp: time.Now(),
	})
	fake.mu.Lock()
	fake.status = "finished"
	fake.finalResponse = "continued answer"
	fake.mu.Unlock()

	restored, err := New(Config{
		ServerURL: fake.server.URL, SessionKey: fakeSessionKey, Workspace: fake.workspace,
		Model: "test/model", SessionDir: first.mappingDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	restored.pollInterval = 10 * time.Millisecond
	restored.disconnectWait = 500 * time.Millisecond
	restored.cancelWait = 200 * time.Millisecond
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close restored backend: %v", err)
		}
	})
	if err := restored.RestoreSession(session); err != nil {
		t.Fatal(err)
	}
	message, err := restored.SendMessage(context.Background(), session.ID, "next prompt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != fakeAssistantText {
		t.Fatalf("new turn final message = %+v", message)
	}
	fake.mu.Lock()
	promptCount := fake.promptCount
	fake.mu.Unlock()
	if promptCount != 1 {
		t.Fatalf("prompt count = %d, prior accepted prompt was resent", promptCount)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close original backend: %v", err)
	}
}

func TestOpenHandsMalformedAndSensitiveSocketOutputIsRedacted(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	fake.mode = "malformed"
	backend, session := newFakeBackend(t, fake)
	const sensitive = "sensitive-output-must-not-leak"
	_, err := backend.SendMessage(context.Background(), session.ID, "prompt", nil)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("malformed frame error = %v", err)
	}
	if strings.Contains(fmt.Sprint(err), sensitive) {
		t.Fatalf("protocol error exposed raw server output: %v", err)
	}
}

func TestOpenHandsAuthenticationMustBeEnforcedAndWorkspaceMustMatch(t *testing.T) {
	workspace := t.TempDir()
	fake := newFakeOpenHandsServer(t, workspace)
	fake.authDisabled = true
	if _, err := New(Config{
		ServerURL: fake.server.URL, SessionKey: fakeSessionKey, Workspace: workspace,
		Model: "test/model", SessionDir: t.TempDir(),
	}); !errors.Is(err, ErrAuthenticationRequired) {
		t.Fatalf("server without authentication error = %v", err)
	}

	fake.authDisabled = false
	backend, session := newFakeBackend(t, fake)
	mapping, err := loadMapping(backend.mappingDir, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	mapping.Workspace = t.TempDir()
	if err := saveMapping(backend.mappingDir, mapping); err != nil {
		t.Fatal(err)
	}
	if err := backend.RestoreSession(session); !errors.Is(err, ErrSessionOwnership) {
		t.Fatalf("workspace mismatch error = %v", err)
	}
}

func TestOpenHandsRejectsRemoteWorkspaceChangeBeforeSending(t *testing.T) {
	fake := newFakeOpenHandsServer(t, t.TempDir())
	backend, session := newFakeBackend(t, fake)
	fake.mu.Lock()
	fake.wrongWorkspace = true
	fake.mu.Unlock()
	if _, err := backend.SendMessage(context.Background(), session.ID, "do not send", nil); !errors.Is(err, ErrSessionOwnership) {
		t.Fatalf("remote workspace mismatch error = %v", err)
	}
	fake.mu.Lock()
	promptCount := fake.promptCount
	fake.mu.Unlock()
	if promptCount != 0 {
		t.Fatalf("server received %d prompts after workspace changed", promptCount)
	}
}

func TestValidateWorkspaceAcceptsServerAbsolutePaths(t *testing.T) {
	for _, path := range []string{"/srv/openhands/workspace", `C:\work\workspace`} {
		if got, err := validateWorkspace(path); err != nil || got != path {
			t.Errorf("validateWorkspace(%q) = %q, %v", path, got, err)
		}
	}
	for _, path := range []string{"relative/workspace", "/", `C:\`, "/tmp/../etc"} {
		if _, err := validateWorkspace(path); err == nil {
			t.Errorf("validateWorkspace(%q) unexpectedly succeeded", path)
		}
	}
}

func TestRealOpenHandsSmoke(t *testing.T) {
	if strings.TrimSpace(os.Getenv("MAISTR0_OPENHANDS_REAL_SMOKE")) != "1" {
		t.Skip("set MAISTR0_OPENHANDS_REAL_SMOKE=1 to run against an OpenHands Agent Server")
	}
	backend, enabled, err := NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Fatal("OpenHands smoke test requires the backend environment configuration")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close OpenHands backend: %v", err)
		}
	}()
	t.Logf("OpenHands Agent Server version: %s", backend.Version())
	session, err := backend.CreateSession("mAIstr0 OpenHands smoke test", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !backend.DeleteSession(session.ID) {
			t.Errorf("delete OpenHands smoke-test conversation")
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	message, err := backend.SendMessage(ctx, session.ID, "Reply with a brief smoke-test confirmation.", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message == nil || message.Role != agent.RoleAssistant || strings.TrimSpace(message.Content) == "" {
		t.Fatalf("OpenHands smoke-test result was empty or invalid")
	}
}

func nestedMap(value map[string]any, key string) map[string]any {
	if nested, ok := value[key].(map[string]any); ok {
		return nested
	}
	return map[string]any{}
}

func nestedSlice(value map[string]any, key string) []any {
	if nested, ok := value[key].([]any); ok {
		return nested
	}
	return nil
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before deadline")
}
