package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

func TestSupportedOpenCodeServerVersionRange(t *testing.T) {
	for _, version := range []string{"2.3.2", "v2.3.2", "2.3.99"} {
		if got, err := supportedVersion(version); err != nil || got == "" {
			t.Errorf("supportedVersion(%q) = %q, %v", version, got, err)
		}
	}
	for _, version := range []string{"2.3.1", "2.4.0", "3.0.0", "2.3.2-dev", "unknown"} {
		if _, err := supportedVersion(version); !errors.Is(err, ErrUnsupportedVersion) {
			t.Errorf("supportedVersion(%q) error = %v", version, err)
		}
	}
}

func TestOpenCodeTurnUsesScopedPermissionsAndConfirmedWait(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	backend, session := newFakeBackend(t, server)
	message, err := backend.SendMessage(context.Background(), session.ID, "safe test prompt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != "answer:safe test prompt" || message.Role != agent.RoleAssistant {
		t.Fatalf("final message = %+v", message)
	}
	saved, ok := backend.GetSession(session.ID)
	if !ok || len(saved.Messages) != 2 || saved.Messages[0].Role != agent.RoleUser ||
		saved.Messages[1].Role != agent.RoleAssistant || saved.Messages[0].Content != "safe test prompt" {
		t.Fatalf("session messages were not reconciled: %+v", saved)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.promptCount != 1 || server.waitCount < 2 || server.authFailures != 0 ||
		server.permissionFailures != 0 || server.policyQueryFailures != 0 {
		t.Fatalf("API contract counters: prompts=%d waits=%d auth=%d permissions=%d policy-query=%d",
			server.promptCount, server.waitCount, server.authFailures, server.permissionFailures, server.policyQueryFailures)
	}
}

func TestOpenCodeRestartReconcilesAcceptedTurnBeforeNewPrompt(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	first, session := newFakeBackend(t, server)
	first.mu.RLock()
	state := first.sessions[session.ID]
	first.mu.RUnlock()
	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	const priorPromptID = "msg_prior-restart"
	server.mu.Lock()
	remote := server.sessions[mapping.NativeID]
	remote.messages = []nativeMessage{
		{ID: priorPromptID, Type: "user", Text: "prior accepted prompt"},
		{ID: "msg_prior-answer", Type: "assistant", Content: []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{{Type: "text", Text: "prior answer"}}},
		{ID: "msg_prior-idle", Type: "idle", Outcome: "succeeded"},
	}
	server.mu.Unlock()
	mapping.PendingID = priorPromptID
	if err := saveMapping(first.mappingDir, mapping); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first backend: %v", err)
	}

	second, err := New(Config{
		Mode: ModeConnect, Workspace: first.workspace, ServerURL: server.server.URL,
		ServerPassword: fakeServerPassword, SessionDir: first.mappingDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("close second backend: %v", err)
		}
	})
	if err := second.RestoreSession(session); err != nil {
		t.Fatal(err)
	}
	if _, err := second.SendMessage(context.Background(), session.ID, "next prompt", nil); err != nil {
		t.Fatal(err)
	}
	restored, ok := second.GetSession(session.ID)
	if !ok {
		t.Fatal("restored session disappeared")
	}
	want := []string{"prior accepted prompt", "prior answer", "next prompt", "answer:next prompt"}
	if len(restored.Messages) != len(want) {
		t.Fatalf("reconciled messages = %+v", restored.Messages)
	}
	for index, content := range want {
		if restored.Messages[index].Content != content {
			t.Fatalf("message %d = %q, want %q", index, restored.Messages[index].Content, content)
		}
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.promptCount != 1 {
		t.Fatalf("reconciliation resent the prior prompt: prompt count %d", server.promptCount)
	}
}

func TestOpenCodeInterruptWaitsForServerConfirmation(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	server.blockPromptWait = true
	backend, session := newFakeBackend(t, server)
	sendDone := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "cancel me", nil)
		sendDone <- err
	}()
	select {
	case <-server.promptAccepted:
	case <-time.After(5 * time.Second):
		t.Fatal("OpenCode did not durably accept the prompt")
	}
	select {
	case <-server.promptWaitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("OpenCode wait did not start")
	}
	if !backend.InterruptSession(session.ID) {
		t.Fatal("backend reported an unconfirmed interruption")
	}
	select {
	case err := <-sendDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled turn error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled turn did not finish")
	}
	saved, ok := backend.GetSession(session.ID)
	if !ok || saved.Active {
		t.Fatalf("session is active after confirmed cancellation: %+v", saved)
	}
}

func TestOpenCodeUnconfirmedCancellationIsAnError(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	server.blockPromptWait = true
	server.failWaitAfterInterrupt = true
	backend, session := newFakeBackend(t, server)
	sendDone := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "unconfirmed", nil)
		sendDone <- err
	}()
	select {
	case <-server.promptAccepted:
	case <-time.After(5 * time.Second):
		t.Fatal("OpenCode did not durably accept the prompt")
	}
	select {
	case <-server.promptWaitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("OpenCode wait did not start")
	}
	if backend.InterruptSession(session.ID) {
		t.Fatal("backend reported cancellation without an idle confirmation")
	}
	select {
	case err := <-sendDone:
		if !errors.Is(err, ErrCancellationUnconfirmed) {
			t.Fatalf("turn error = %v, want cancellation-unconfirmed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unconfirmed turn did not return")
	}
	saved, ok := backend.GetSession(session.ID)
	if !ok || !saved.Active {
		t.Fatalf("uncertain remote execution was not retained as active: %+v", saved)
	}
	if err := backend.Close(); !errors.Is(err, ErrCancellationUnconfirmed) {
		t.Fatalf("close with unconfirmed cancellation error = %v", err)
	}
	server.mu.Lock()
	server.failWaitAfterInterrupt = false
	server.mu.Unlock()
	if err := backend.Close(); err != nil {
		t.Fatalf("retry close after server confirms idle: %v", err)
	}
}

func TestOpenCodeUnavailableServerAndConnectedClose(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	backend, _ := newFakeBackend(t, server)
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := server.server.Client().Get(server.server.URL + "/api/info")
	if err != nil {
		t.Fatalf("connected backend stopped its server: %v", err)
	}
	_ = response.Body.Close()

	unavailable := newFakeOpenCodeServer(t)
	second, secondSession := newFakeBackend(t, unavailable)
	unavailable.failSessionGet = true
	if _, err := second.SendMessage(context.Background(), secondSession.ID, "not sent", nil); !errors.Is(err, ErrServerUnavailable) {
		t.Fatalf("unavailable server error = %v", err)
	}
	unavailable.failSessionGet = false
}

func TestOpenCodeDisconnectDoesNotConfirmDeletion(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	backend, err := New(fakeBackendConfig(t, server))
	if err != nil {
		t.Fatal(err)
	}
	session, err := backend.CreateSession("disconnect test", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	server.server.Close()
	if _, err := backend.SendMessage(context.Background(), session.ID, "not sent", nil); !errors.Is(err, ErrServerUnavailable) {
		t.Fatalf("disconnected server error = %v", err)
	}
	if backend.DeleteSession(session.ID) {
		t.Fatal("backend reported deletion without reaching the server")
	}
	if err := backend.Close(); !errors.Is(err, ErrCancellationUnconfirmed) {
		t.Fatalf("close after disconnect error = %v", err)
	}
	backend.client.CloseIdleConnections()
}

func TestOpenCodeRejectsMappedSessionOutsideWorkspace(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	backend, session := newFakeBackend(t, server)
	state, exists := backend.getState(session.ID)
	if !exists {
		t.Fatal("created session is missing")
	}
	state.mu.Lock()
	nativeID := state.mapping.NativeID
	state.mu.Unlock()
	server.mu.Lock()
	remote := server.sessions[nativeID]
	remote.directory = t.TempDir()
	server.mu.Unlock()

	if _, err := backend.SendMessage(context.Background(), session.ID, "must not send", nil); !errors.Is(err, ErrSessionOwnership) {
		t.Fatalf("send for another workspace returned %v", err)
	}
	if backend.DeleteSession(session.ID) {
		t.Fatal("backend deleted a session outside its configured workspace")
	}
	server.mu.Lock()
	if server.promptCount != 0 || server.interruptCount != 0 || server.deleteCount != 0 {
		t.Fatalf("foreign session was touched: prompts=%d interrupts=%d deletes=%d",
			server.promptCount, server.interruptCount, server.deleteCount)
	}
	remote.directory = backend.workspace
	server.mu.Unlock()
	if err := backend.Close(); err != nil {
		t.Fatalf("close after restoring owned session: %v", err)
	}
}

func TestOpenCodeRejectsLoadedPluginsAndMCP(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	server.plugins = []json.RawMessage{json.RawMessage(`{"name":"unrelated"}`)}
	config := fakeBackendConfig(t, server)
	if _, err := New(config); !errors.Is(err, ErrPolicyUnavailable) {
		t.Fatalf("configured plugin error = %v", err)
	}
}

func TestOpenCodeDoesNotExposeRawServerErrorBody(t *testing.T) {
	server := newFakeOpenCodeServer(t)
	server.errorBody = "sensitive server output must not escape"
	backend, session := newFakeBackend(t, server)
	_, err := backend.SendMessage(context.Background(), session.ID, "private prompt", nil)
	if err == nil || strings.Contains(err.Error(), "sensitive server output") ||
		strings.Contains(err.Error(), "private prompt") {
		t.Fatalf("server error leaked sensitive data: %v", err)
	}
}

func TestOpenCodeEnvironmentOptInIsExplicit(t *testing.T) {
	unsetOpenCodeEnvironment(t)
	backend, enabled, err := NewFromEnvironment()
	if backend != nil || enabled || err != nil {
		t.Fatalf("unset OpenCode opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
	t.Setenv("MAISTR0_OPENCODE_MODE", ModeConnect)
	backend, enabled, err = NewFromEnvironment()
	if backend != nil || !enabled || err == nil ||
		!strings.Contains(err.Error(), "MAISTR0_OPENCODE_SERVER_URL") {
		t.Fatalf("partial OpenCode opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
}

func TestRealOpenCodeSmoke(t *testing.T) {
	if os.Getenv("MAISTR0_OPENCODE_REAL_SMOKE") != "1" {
		t.Skip("set MAISTR0_OPENCODE_REAL_SMOKE=1 to run against an installed OpenCode server")
	}
	backend, enabled, err := NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || backend == nil {
		t.Fatal("OpenCode smoke test requires explicit backend configuration")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close OpenCode backend: %v", err)
		}
	}()
	t.Logf("OpenCode server version: %s", backend.Version())
	session, err := backend.CreateSession("mAIstr0 OpenCode smoke", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	message, err := backend.SendMessage(context.Background(), session.ID,
		"Reply with exactly: mAIstr0 OpenCode smoke passed", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message.Content, "mAIstr0 OpenCode smoke passed") {
		t.Fatalf("unexpected smoke response: %q", message.Content)
	}
}

const fakeServerPassword = "test-only-opencode-password"

type fakeOpenCodeSession struct {
	id        string
	directory string
	messages  []nativeMessage
	active    bool
	gate      chan struct{}
}

type fakeOpenCodeServer struct {
	server                 *httptest.Server
	mu                     sync.Mutex
	sessions               map[string]*fakeOpenCodeSession
	nextSession            int
	promptCount            int
	waitCount              int
	interruptCount         int
	deleteCount            int
	authFailures           int
	permissionFailures     int
	policyQueryFailures    int
	plugins                []json.RawMessage
	mcps                   []json.RawMessage
	errorBody              string
	blockPromptWait        bool
	failWaitAfterInterrupt bool
	failSessionGet         bool
	promptAccepted         chan struct{}
	promptWaitStarted      chan struct{}
}

func newFakeOpenCodeServer(t *testing.T) *fakeOpenCodeServer {
	t.Helper()
	fake := &fakeOpenCodeServer{
		sessions:          make(map[string]*fakeOpenCodeSession),
		plugins:           []json.RawMessage{},
		mcps:              []json.RawMessage{},
		promptAccepted:    make(chan struct{}, 10),
		promptWaitStarted: make(chan struct{}, 10),
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (s *fakeOpenCodeServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	username, password, ok := r.BasicAuth()
	if !ok || username != "opencode" || password != fakeServerPassword {
		s.mu.Lock()
		s.authFailures++
		s.mu.Unlock()
		http.Error(w, "sensitive server error body", http.StatusUnauthorized)
		return
	}
	if s.errorBody != "" && strings.HasSuffix(r.URL.Path, "/message") && r.Method == http.MethodGet {
		http.Error(w, s.errorBody, http.StatusInternalServerError)
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/info":
		writeFakeJSON(w, map[string]string{"version": "2.3.2"})
	case r.Method == http.MethodGet && (r.URL.Path == "/api/plugin" || r.URL.Path == "/api/mcp"):
		s.mu.Lock()
		data := s.mcps
		if r.URL.Path == "/api/plugin" {
			data = s.plugins
		}
		if r.URL.Query().Get("location[directory]") == "" {
			s.policyQueryFailures++
		}
		s.mu.Unlock()
		writeFakeJSON(w, map[string]any{"location": map[string]string{"directory": r.URL.Query().Get("location[directory]")}, "data": data})
	case r.Method == http.MethodPost && r.URL.Path == "/api/session":
		var request struct {
			Location struct {
				Directory string `json:"directory"`
			} `json:"location"`
			Permissions []permissionRule `json:"permissions"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || !hasRequiredPermissions(request.Permissions, request.Location.Directory) {
			s.mu.Lock()
			s.permissionFailures++
			s.mu.Unlock()
			http.Error(w, "permission policy missing", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.nextSession++
		id := fmt.Sprintf("ses_test-%d", s.nextSession)
		session := &fakeOpenCodeSession{id: id, directory: request.Location.Directory}
		s.sessions[id] = session
		s.mu.Unlock()
		writeFakeJSON(w, map[string]any{"data": map[string]any{
			"id": id, "location": map[string]string{"directory": request.Location.Directory},
		}})
	case strings.HasPrefix(r.URL.Path, "/api/experimental/session/"):
		clone := r.Clone(r.Context())
		clone.URL = new(url.URL)
		*clone.URL = *r.URL
		clone.URL.Path = strings.Replace(r.URL.Path, "/api/experimental/session/", "/api/session/", 1)
		s.handleSession(w, clone)
	case strings.HasPrefix(r.URL.Path, "/api/session/"):
		s.handleSession(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *fakeOpenCodeServer) handleSession(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.NotFound(w, r)
		return
	}
	id := parts[2]
	s.mu.Lock()
	session := s.sessions[id]
	s.mu.Unlock()
	if session == nil {
		http.NotFound(w, r)
		return
	}
	switch {
	case len(parts) == 3 && r.Method == http.MethodGet:
		s.mu.Lock()
		failSessionGet := s.failSessionGet
		s.mu.Unlock()
		if failSessionGet {
			http.Error(w, "sensitive unavailable body", http.StatusServiceUnavailable)
			return
		}
		writeFakeJSON(w, map[string]any{"data": map[string]any{
			"id": id, "location": map[string]string{"directory": session.directory},
		}})
	case len(parts) == 3 && r.Method == http.MethodPatch:
		var request struct {
			Permissions []permissionRule `json:"permissions"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || !hasRequiredPermissions(request.Permissions, session.directory) {
			s.mu.Lock()
			s.permissionFailures++
			s.mu.Unlock()
			http.Error(w, "permission policy missing", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case len(parts) == 3 && r.Method == http.MethodDelete:
		s.mu.Lock()
		s.deleteCount++
		delete(s.sessions, id)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case len(parts) == 4 && parts[3] == "prompt" && r.Method == http.MethodPost:
		var request struct {
			ID   string `json:"id"`
			Text string `json:"text"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || !validNativeMessageID(request.ID) {
			http.Error(w, "invalid prompt", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		session.messages = append(session.messages, nativeMessage{ID: request.ID, Type: "user", Text: request.Text})
		session.active = true
		if s.blockPromptWait {
			session.gate = make(chan struct{})
		}
		s.promptCount++
		s.mu.Unlock()
		s.promptAccepted <- struct{}{}
		writeFakeJSON(w, map[string]any{"data": map[string]any{"id": request.ID}})
	case len(parts) == 4 && parts[3] == "wait" && r.Method == http.MethodPost:
		s.mu.Lock()
		s.waitCount++
		active := session.active
		gate := session.gate
		block := active && s.blockPromptWait
		s.mu.Unlock()
		if block {
			s.promptWaitStarted <- struct{}{}
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		} else if active {
			s.finishSession(session, "succeeded")
		}
		s.mu.Lock()
		interrupted := len(session.messages) > 0 && session.messages[len(session.messages)-1].Type == "idle" &&
			session.messages[len(session.messages)-1].Outcome == "interrupted"
		failAfterInterrupt := s.failWaitAfterInterrupt && interrupted
		s.mu.Unlock()
		if failAfterInterrupt {
			http.Error(w, "sensitive failure", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case len(parts) == 4 && parts[3] == "interrupt" && r.Method == http.MethodPost:
		s.mu.Lock()
		s.interruptCount++
		wasActive := session.active
		if wasActive {
			session.active = false
			session.messages = append(session.messages, nativeMessage{
				ID: fmt.Sprintf("msg_interrupted-%d", s.promptCount), Type: "idle", Outcome: "interrupted",
			})
			if session.gate != nil {
				close(session.gate)
			}
		}
		s.mu.Unlock()
		writeFakeJSON(w, map[string]bool{"interrupted": wasActive})
	case len(parts) == 4 && parts[3] == "message" && r.Method == http.MethodGet:
		s.mu.Lock()
		messages := append([]nativeMessage(nil), session.messages...)
		s.mu.Unlock()
		for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
			messages[left], messages[right] = messages[right], messages[left]
		}
		writeFakeJSON(w, map[string]any{"data": messages, "cursor": map[string]string{}})
	default:
		http.NotFound(w, r)
	}
}

func (s *fakeOpenCodeServer) finishSession(session *fakeOpenCodeSession, outcome string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !session.active {
		return
	}
	user := ""
	for index := len(session.messages) - 1; index >= 0; index-- {
		if session.messages[index].Type == "user" {
			user = session.messages[index].Text
			break
		}
	}
	session.messages = append(session.messages,
		nativeMessage{
			ID: fmt.Sprintf("msg_answer-%d", len(session.messages)), Type: "assistant",
			Content: []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}{{Type: "text", Text: "answer:" + user}},
		},
		nativeMessage{ID: fmt.Sprintf("msg_idle-%d", len(session.messages)+1), Type: "idle", Outcome: outcome},
	)
	session.active = false
	if session.gate != nil {
		select {
		case <-session.gate:
		default:
			close(session.gate)
		}
	}
}

func writeFakeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func hasRequiredPermissions(permissions []permissionRule, directory string) bool {
	seen := map[string]string{}
	for _, permission := range permissions {
		if permission.Action == "*" && permission.Effect == "deny" {
			seen["*"] = permission.Effect
		}
		if permission.Action == "shell" && permission.Effect == "deny" {
			seen["shell"] = permission.Effect
		}
		if permission.Action == "external_directory" && permission.Effect == "deny" {
			seen["external_directory"] = permission.Effect
		}
		if permission.Action == "edit" && permission.Effect == "allow" &&
			permission.Resource == filepath.ToSlash(filepath.Clean(directory))+"/**" {
			seen["edit"] = permission.Effect
		}
	}
	return len(seen) == 4
}

func newFakeBackend(t *testing.T, server *fakeOpenCodeServer) (*Backend, *agent.Session) {
	t.Helper()
	backend, err := New(fakeBackendConfig(t, server))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close fake OpenCode backend: %v", err)
		}
	})
	session, err := backend.CreateSession("test session", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return backend, session
}

func fakeBackendConfig(t *testing.T, server *fakeOpenCodeServer) Config {
	t.Helper()
	return Config{
		Mode: ModeConnect, Workspace: t.TempDir(), ServerURL: server.server.URL,
		ServerPassword: fakeServerPassword, SessionDir: t.TempDir(),
	}
}

func unsetOpenCodeEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"MAISTR0_OPENCODE_MODE", "MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_WORKSPACE",
		"MAISTR0_OPENCODE_HOME", "MAISTR0_OPENCODE_SERVER_URL", "MAISTR0_OPENCODE_SERVER_PASSWORD",
		"MAISTR0_OPENCODE_SESSION_DIR",
	} {
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
}

func TestOpenCodeSessionMappingRejectsSymlinksAndPreservesOnlyIDs(t *testing.T) {
	directory := t.TempDir()
	mapping := sessionMapping{
		Version: mappingVersion, SessionID: "test-session", NativeID: "ses_native",
		PendingID: "msg_pending",
	}
	if err := saveMapping(directory, mapping); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(directory, mappingFilename(mapping.SessionID)))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "prompt") || strings.Contains(string(data), "secret") {
		t.Fatalf("mapping unexpectedly includes prompt data: %s", data)
	}
	info, err := os.Stat(filepath.Join(directory, mappingFilename(mapping.SessionID)))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("mapping permissions = %o", info.Mode().Perm())
	}
	loaded, err := loadMapping(directory, mapping.SessionID)
	if err != nil || loaded != mapping {
		t.Fatalf("loaded mapping = %+v, %v", loaded, err)
	}
}
