package hermes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

const (
	fakeAPIKey       = "test-hermes-api-key"
	fakeFinalAnswer  = "Hermes test answer"
	fakeSensitiveErr = "private-gateway-error-output"
)

type fakeRun struct {
	id        string
	sessionID string
	status    string
	output    string
	mode      string
}

type fakeHermesServer struct {
	server *httptest.Server

	mu               sync.Mutex
	version          string
	authRequired     bool
	durable          bool
	ignoreStop       bool
	mode             string
	runs             map[string]*fakeRun
	postCount        int
	stopCount        int
	deleteCount      int
	eventConnections map[string]int
	lastEventID      []string
	lastRequest      map[string]any
}

func newFakeHermesServer(t *testing.T, mode string) *fakeHermesServer {
	t.Helper()
	fake := &fakeHermesServer{
		version: SupportedVersion, authRequired: true, durable: true, mode: mode,
		runs: make(map[string]*fakeRun), eventConnections: make(map[string]int),
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeHermesServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		f.mu.Lock()
		version := f.version
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "ok", "platform": "hermes-agent", "version": version,
		})
		return
	}
	f.mu.Lock()
	authRequired := f.authRequired
	f.mu.Unlock()
	if authRequired && r.Header.Get("Authorization") != "Bearer "+fakeAPIKey {
		http.Error(w, "private auth response", http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/capabilities":
		f.serveCapabilities(w)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/runs":
		f.createRun(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/runs/") &&
		!strings.HasSuffix(r.URL.Path, "/events"):
		f.getRun(w, r, strings.TrimPrefix(r.URL.Path, "/v1/runs/"))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/runs/") &&
		strings.HasSuffix(r.URL.Path, "/events"):
		runID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/runs/"), "/events")
		f.streamEvents(w, r, runID)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/runs/") &&
		strings.HasSuffix(r.URL.Path, "/stop"):
		runID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/runs/"), "/stop")
		f.stopRun(w, r, runID)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/sessions/"):
		f.mu.Lock()
		f.deleteCount++
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeHermesServer) serveCapabilities(w http.ResponseWriter) {
	f.mu.Lock()
	required, durable := f.authRequired, f.durable
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"object": "hermes.api_server.capabilities", "platform": "hermes-agent",
		"auth": map[string]any{"type": "bearer", "required": required},
		"features": map[string]any{
			"run_submission": true, "run_status": true, "run_events_sse": true, "run_stop": true,
			"runs_idempotency": map[string]any{"supported": true, "durable": durable},
		},
	})
}

func (f *fakeHermesServer) createRun(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Idempotency-Key") == "" {
		http.Error(w, "missing idempotency", http.StatusBadRequest)
		return
	}
	var body map[string]any
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	sessionID, _ := body["session_id"].(string)
	if !validSessionID(sessionID) {
		http.Error(w, "invalid session id", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.postCount++
	f.lastRequest = body
	runID := fmt.Sprintf("run_%d", f.postCount)
	run := &fakeRun{id: runID, sessionID: sessionID, output: fakeFinalAnswer, mode: f.mode}
	switch f.mode {
	case "stream", "disconnect", "cancel", "ignore-stop", "malformed":
		run.status = "running"
	case "failed":
		run.status = "failed"
	case "interrupted":
		run.status = "interrupted"
	default:
		run.status = "completed"
	}
	if f.mode == "failed" {
		run.output = ""
	}
	f.runs[runID] = run
	f.mu.Unlock()
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(runSubmission{RunID: runID, Status: "started"})
}

func (f *fakeHermesServer) getRun(w http.ResponseWriter, r *http.Request, runID string) {
	f.mu.Lock()
	run := f.runs[runID]
	if run != nil && run.mode == "cancel" && run.status == "stopping" {
		run.status = "cancelled"
	}
	if run != nil && run.mode == "ignore-stop" && run.status == "stopping" {
		run.status = "running"
	}
	var payload runStatus
	if run != nil {
		payload = runStatus{
			Object: "hermes.run", RunID: run.id, Status: run.status,
			SessionID: run.sessionID, Output: run.output,
		}
	}
	f.mu.Unlock()
	if run == nil {
		http.NotFound(w, r)
		return
	}
	response := map[string]any{
		"object": payload.Object, "run_id": payload.RunID, "status": payload.Status,
		"session_id": payload.SessionID, "output": payload.Output,
	}
	if payload.Status == "failed" {
		response["error"] = fakeSensitiveErr
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (f *fakeHermesServer) streamEvents(w http.ResponseWriter, r *http.Request, runID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	f.mu.Lock()
	run := f.runs[runID]
	if run == nil {
		f.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	f.eventConnections[runID]++
	connection := f.eventConnections[runID]
	f.lastEventID = append(f.lastEventID, r.Header.Get("Last-Event-ID"))
	mode := run.mode
	f.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	switch mode {
	case "cancel", "ignore-stop":
		<-r.Context().Done()
	case "disconnect":
		if connection == 1 {
			writeSSEFragmented(w, flusher, 0, map[string]any{
				"event": "message.delta", "run_id": runID, "delta": "Hel",
			})
			return
		}
		writeSSEFragmented(w, flusher, 1, map[string]any{
			"event": "message.delta", "run_id": runID, "delta": "lo",
		})
		f.completeRun(runID)
	case "malformed":
		_, _ = fmt.Fprint(w, "id: 0\ndata: {\"event\":\n\n")
		flusher.Flush()
	case "stream":
		writeSSEFragmented(w, flusher, 0, map[string]any{
			"event": "message.delta", "run_id": runID, "delta": "Hello ",
		})
		writeSSEFragmented(w, flusher, 1, map[string]any{
			"event": "tool.started", "run_id": runID, "tool": "terminal", "tool_call_id": "call-1",
		})
		writeSSEFragmented(w, flusher, 2, map[string]any{
			"event": "tool.completed", "run_id": runID, "tool": "terminal",
			"tool_call_id": "call-1", "error": false, "preview": fakeSensitiveErr,
		})
		f.completeRun(runID)
	default:
		f.completeRun(runID)
	}
}

func writeSSEFragmented(w http.ResponseWriter, flusher http.Flusher, seq int, event map[string]any) {
	event["seq"] = seq
	data, _ := json.Marshal(event)
	frame := fmt.Sprintf("id: %d\ndata: %s\n\n", seq, data)
	middle := len(frame) / 2
	_, _ = fmt.Fprint(w, frame[:middle])
	flusher.Flush()
	_, _ = fmt.Fprint(w, frame[middle:])
	flusher.Flush()
}

func (f *fakeHermesServer) completeRun(runID string) {
	f.mu.Lock()
	if run := f.runs[runID]; run != nil {
		run.status = "completed"
	}
	f.mu.Unlock()
}

func (f *fakeHermesServer) stopRun(w http.ResponseWriter, r *http.Request, runID string) {
	f.mu.Lock()
	f.stopCount++
	run := f.runs[runID]
	if run != nil && !f.ignoreStop {
		run.status = "stopping"
	}
	var payload runStatus
	if run != nil {
		payload = runStatus{
			Object: "hermes.run", RunID: run.id, Status: "stopping",
			SessionID: run.sessionID,
		}
	}
	f.mu.Unlock()
	if run == nil {
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func newTestBackend(t *testing.T, fake *fakeHermesServer) (*Backend, *agent.Session) {
	t.Helper()
	backend, err := New(Config{
		ServerURL: fake.server.URL, APIKey: fakeAPIKey, SessionDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	backend.pollInterval = 5 * time.Millisecond
	backend.cancelWait = 100 * time.Millisecond
	backend.serverLossWait = 150 * time.Millisecond
	backend.turnTimeout = 2 * time.Second
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Hermes backend: %v", err)
		}
	})
	session, err := backend.CreateSession("test", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return backend, session
}

func TestHermesRequiresPinnedAuthenticatedDurableRunsAPI(t *testing.T) {
	fake := newFakeHermesServer(t, "complete")
	fake.version = "0.23.4"
	if _, err := New(Config{ServerURL: fake.server.URL, APIKey: fakeAPIKey, SessionDir: t.TempDir()}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("unsupported version error = %v", err)
	}
	fake.version = SupportedVersion
	fake.durable = false
	if _, err := New(Config{ServerURL: fake.server.URL, APIKey: fakeAPIKey, SessionDir: t.TempDir()}); !errors.Is(err, ErrUnsupportedCapabilities) {
		t.Fatalf("non-durable run ledger error = %v", err)
	}
	fake.durable = true
	fake.authRequired = false
	if _, err := New(Config{ServerURL: fake.server.URL, APIKey: fakeAPIKey, SessionDir: t.TempDir()}); !errors.Is(err, ErrUnsupportedCapabilities) {
		t.Fatalf("server without required authentication error = %v", err)
	}
	fake.authRequired = true
	if _, err := New(Config{ServerURL: fake.server.URL, APIKey: "wrong-key", SessionDir: t.TempDir()}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong API key error = %v", err)
	}
}

func TestHermesSessionMapsFragmentedEventsAndRedactsToolPreviews(t *testing.T) {
	fake := newFakeHermesServer(t, "stream")
	backend, session := newTestBackend(t, fake)
	if _, err := backend.CreateSession("unsupported", agent.SessionConfig{MaxSteps: 1}); !errors.Is(err, ErrUnsupportedConfiguration) {
		t.Fatalf("unsupported session options error = %v", err)
	}
	session, err := backend.CreateSession("configured", agent.SessionConfig{
		SystemPrompt: "operator instructions", CoordinatorModel: "gateway-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	stream := make(chan agent.StreamEvent, 16)
	message, err := backend.SendMessage(context.Background(), session.ID, "safe prompt", stream)
	if err != nil {
		t.Fatal(err)
	}
	if message == nil || message.Role != agent.RoleAssistant || message.Content != fakeFinalAnswer {
		t.Fatalf("final message = %+v", message)
	}
	fake.mu.Lock()
	body := fake.lastRequest
	fake.mu.Unlock()
	if body["session_id"] != session.ID || body["input"] != "safe prompt" ||
		body["instructions"] != "operator instructions" || body["model"] != "gateway-model" {
		t.Fatalf("submitted run body = %#v", body)
	}
	for _, field := range []string{"tools", "toolsets", "skills", "hooks", "mcp"} {
		if _, exists := body[field]; exists {
			t.Fatalf("backend unexpectedly configured Hermes field %q", field)
		}
	}
	saved, ok := backend.GetSession(session.ID)
	if !ok || len(saved.Messages) != 2 || saved.Messages[0].Role != agent.RoleUser ||
		saved.Messages[1].Role != agent.RoleAssistant {
		t.Fatalf("saved session = %+v", saved)
	}
	seenDelta, seenToolStart, seenToolResult, seenFinal, seenDone := false, false, false, false, false
	for len(stream) > 0 {
		event := <-stream
		switch event.Type {
		case agent.EventTextDelta:
			seenDelta = seenDelta || event.Content == "Hello "
		case agent.EventToolCall:
			seenToolStart = seenToolStart || event.ToolCall != nil && event.ToolCall.Name == "terminal"
		case agent.EventToolResponse:
			seenToolResult = seenToolResult || event.ToolResp != nil &&
				event.ToolResp.Name == "terminal" && event.ToolResp.Output == ""
			if strings.Contains(fmt.Sprint(event.ToolResp), fakeSensitiveErr) {
				t.Fatal("tool preview leaked into streamed output")
			}
		case agent.EventFinalMessage:
			seenFinal = true
		case agent.EventDone:
			seenDone = true
		}
	}
	if !seenDelta || !seenToolStart || !seenToolResult || !seenFinal || !seenDone {
		t.Fatalf("stream mapping delta=%t tool-start=%t tool-result=%t final=%t done=%t",
			seenDelta, seenToolStart, seenToolResult, seenFinal, seenDone)
	}
}

func TestHermesReconnectsAndResumesWithoutResubmitting(t *testing.T) {
	fake := newFakeHermesServer(t, "disconnect")
	backend, session := newTestBackend(t, fake)
	message, err := backend.SendMessage(context.Background(), session.ID, "accepted once", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != fakeFinalAnswer {
		t.Fatalf("final message = %+v", message)
	}
	fake.mu.Lock()
	postCount, eventConnections := fake.postCount, fake.eventConnections["run_1"]
	lastEventIDs := append([]string(nil), fake.lastEventID...)
	fake.mu.Unlock()
	if postCount != 1 || eventConnections != 2 || len(lastEventIDs) != 2 ||
		lastEventIDs[0] != "" || lastEventIDs[1] != "0" {
		t.Fatalf("posts=%d event connections=%d last IDs=%v", postCount, eventConnections, lastEventIDs)
	}
}

func TestHermesCancellationRequiresTerminalAcknowledgement(t *testing.T) {
	fake := newFakeHermesServer(t, "cancel")
	backend, session := newTestBackend(t, fake)
	result := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "cancel me", nil)
		result <- err
	}()
	waitForHermes(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.postCount == 1 && fake.eventConnections["run_1"] > 0
	})
	if !backend.InterruptSession(session.ID) {
		t.Fatal("server cancellation was not confirmed")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted send error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send did not return after cancellation")
	}
	fake.mu.Lock()
	stopCount := fake.stopCount
	fake.mu.Unlock()
	if stopCount != 1 {
		t.Fatalf("stop requests = %d, want one", stopCount)
	}
}

func TestHermesUnconfirmedCancellationAndGatewayRestartRemainUnknown(t *testing.T) {
	fake := newFakeHermesServer(t, "ignore-stop")
	backend, session := newTestBackend(t, fake)
	result := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "do not retry", nil)
		result <- err
	}()
	waitForHermes(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.postCount == 1 && fake.eventConnections["run_1"] > 0
	})
	if backend.InterruptSession(session.ID) {
		t.Fatal("unconfirmed stop was reported as cancellation")
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrCancellationUnconfirmed) {
			t.Fatalf("unconfirmed send error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send did not return after cancellation timeout")
	}

	fake.mu.Lock()
	fake.runs["run_1"].status = "interrupted"
	fake.mu.Unlock()
	if _, err := backend.SendMessage(context.Background(), session.ID, "new prompt must not run", nil); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("gateway-interrupted run error = %v", err)
	}
	fake.mu.Lock()
	postCount := fake.postCount
	fake.mu.Unlock()
	if postCount != 1 {
		t.Fatalf("unknown run was resubmitted %d times", postCount-1)
	}
}

func TestHermesRestoresGatewayOwnedPendingRun(t *testing.T) {
	fake := newFakeHermesServer(t, "complete")
	dir := t.TempDir()
	first, err := New(Config{ServerURL: fake.server.URL, APIKey: fakeAPIKey, SessionDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	first.pollInterval = 5 * time.Millisecond
	session, err := first.CreateSession("restored", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := first.getState(session.ID)
	if err := first.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = true
		mapping.RunID = "run_existing"
		mapping.IdempotencyKey = "maistr0-existing-key"
	}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.runs["run_existing"] = &fakeRun{
		id: "run_existing", sessionID: session.ID, status: "completed", output: "previous result",
	}
	fake.mu.Unlock()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restored, err := New(Config{ServerURL: fake.server.URL, APIKey: fakeAPIKey, SessionDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	restored.pollInterval = 5 * time.Millisecond
	restored.cancelWait = 100 * time.Millisecond
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close restored Hermes backend: %v", err)
		}
	})
	if err := restored.RestoreSession(session); err != nil {
		t.Fatal(err)
	}
	message, err := restored.SendMessage(context.Background(), session.ID, "next prompt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != fakeFinalAnswer {
		t.Fatalf("new run result = %+v", message)
	}
	fake.mu.Lock()
	postCount := fake.postCount
	lastRequest := fake.lastRequest
	fake.mu.Unlock()
	if postCount != 1 || lastRequest["session_id"] != session.ID || lastRequest["input"] != "next prompt" {
		t.Fatalf("restored run was resubmitted or session changed: posts=%d body=%#v", postCount, lastRequest)
	}
}

func TestHermesReconciliationCancellationDoesNotStopGatewayRun(t *testing.T) {
	fake := newFakeHermesServer(t, "cancel")
	backend, session := newTestBackend(t, fake)
	state, _ := backend.getState(session.ID)
	if err := backend.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = true
		mapping.RunID = "run_existing"
		mapping.IdempotencyKey = "maistr0-existing-key"
	}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.runs["run_existing"] = &fakeRun{
		id: "run_existing", sessionID: session.ID, status: "running", mode: "cancel",
	}
	fake.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(ctx, session.ID, "do not stop prior work", nil)
		result <- err
	}()
	waitForHermes(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.eventConnections["run_existing"] > 0
	})
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("reconciliation cancellation error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconciliation did not detach after cancellation")
	}
	fake.mu.Lock()
	stopCount := fake.stopCount
	fake.runs["run_existing"].status = "completed"
	fake.runs["run_existing"].output = "previous result"
	fake.mode = "complete"
	fake.mu.Unlock()
	state.mu.Lock()
	pending := state.mapping.Pending
	state.mu.Unlock()
	if stopCount != 0 || !pending {
		t.Fatalf("reconciliation cancellation changed remote work: stops=%d pending=%t", stopCount, pending)
	}
	if _, err := backend.SendMessage(context.Background(), session.ID, "continue after reconciliation", nil); err != nil {
		t.Fatalf("reconcile completed run and continue: %v", err)
	}
	fake.mu.Lock()
	postCount := fake.postCount
	fake.mu.Unlock()
	if postCount != 1 {
		t.Fatalf("new run count after reconciliation = %d, want 1", postCount)
	}
}

func TestHermesShutdownDetachesWithoutStoppingGatewayRun(t *testing.T) {
	fake := newFakeHermesServer(t, "cancel")
	backend, session := newTestBackend(t, fake)
	result := make(chan error, 1)
	go func() {
		_, err := backend.SendMessage(context.Background(), session.ID, "continue after disconnect", nil)
		result <- err
	}()
	waitForHermes(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.postCount == 1 && fake.eventConnections["run_1"] > 0
	})
	if err := backend.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("detached send result = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("local watcher did not detach during shutdown")
	}
	fake.mu.Lock()
	stopCount, status := fake.stopCount, fake.runs["run_1"].status
	fake.mu.Unlock()
	if stopCount != 0 || status != "running" {
		t.Fatalf("shutdown stopped gateway-owned work: stop requests=%d status=%q", stopCount, status)
	}
}

func TestHermesMalformedAndFailedResponsesDoNotExposeGatewayOutput(t *testing.T) {
	fake := newFakeHermesServer(t, "failed")
	backend, session := newTestBackend(t, fake)
	_, err := backend.SendMessage(context.Background(), session.ID, "fail safely", nil)
	if !errors.Is(err, ErrTurnFailed) || strings.Contains(fmt.Sprint(err), fakeSensitiveErr) {
		t.Fatalf("terminal error exposed gateway output: %v", err)
	}

	fake.mu.Lock()
	fake.mode = "malformed"
	fake.mu.Unlock()
	session2, err := backend.CreateSession("malformed", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = backend.SendMessage(context.Background(), session2.ID, "malformed stream", nil)
	if !errors.Is(err, ErrProtocol) || strings.Contains(fmt.Sprint(err), fakeSensitiveErr) {
		t.Fatalf("malformed stream error exposed gateway output: %v", err)
	}
	state, _ := backend.getState(session2.ID)
	state.mu.Lock()
	pending := state.mapping.Pending
	state.mu.Unlock()
	if !pending {
		t.Fatal("malformed stream cleared the unresolved run mapping")
	}
}

func TestHermesDeleteRemovesOnlyMappedGatewaySession(t *testing.T) {
	fake := newFakeHermesServer(t, "complete")
	backend, session := newTestBackend(t, fake)
	if !backend.DeleteSession(session.ID) {
		t.Fatal("delete mapped session failed")
	}
	if _, ok := backend.GetSession(session.ID); ok {
		t.Fatal("deleted session remained available")
	}
	if _, err := os.Stat(filepath.Join(backend.mappingDir, mappingFilename(session.ID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mapping file remains after deletion: %v", err)
	}
	fake.mu.Lock()
	deletes := fake.deleteCount
	fake.mu.Unlock()
	if deletes != 1 {
		t.Fatalf("remote session deletions = %d", deletes)
	}
}

func TestHermesEnvironmentOptInIsExplicit(t *testing.T) {
	names := []string{
		"MAISTR0_HERMES_SERVER_URL", "MAISTR0_HERMES_API_KEY", "MAISTR0_HERMES_SESSION_DIR",
	}
	previous := make(map[string]string, len(names))
	wasSet := make(map[string]bool, len(names))
	for _, name := range names {
		previous[name], wasSet[name] = os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, name := range names {
			if wasSet[name] {
				_ = os.Setenv(name, previous[name])
			} else {
				_ = os.Unsetenv(name)
			}
		}
	})
	if backend, enabled, err := NewFromEnvironment(); err != nil || enabled || backend != nil {
		t.Fatalf("unset opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
	t.Setenv("MAISTR0_HERMES_SERVER_URL", "http://example.test")
	if backend, enabled, err := NewFromEnvironment(); err == nil || !enabled || backend != nil {
		t.Fatalf("partial opt-in = backend %v, enabled %t, error %v", backend, enabled, err)
	}
}

func TestRealHermesSmoke(t *testing.T) {
	if os.Getenv("MAISTR0_HERMES_REAL_SMOKE") != "1" {
		t.Skip("set MAISTR0_HERMES_REAL_SMOKE=1 to run against an installed Hermes API Server")
	}
	backend, enabled, err := NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || backend == nil {
		t.Fatal("real smoke requires the complete Hermes backend configuration")
	}
	t.Logf("Hermes API Server version %s", backend.Version())
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close Hermes backend: %v", err)
		}
	}()
	session, err := backend.CreateSession("mAIstr0 Hermes smoke test", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	message, err := backend.SendMessage(context.Background(), session.ID, "Reply with a short smoke-test confirmation.", nil)
	if err != nil {
		t.Fatal(err)
	}
	if message == nil || strings.TrimSpace(message.Content) == "" {
		t.Fatal("Hermes returned an empty smoke-test response")
	}
	if !backend.DeleteSession(session.ID) {
		t.Error("delete Hermes smoke-test session was not confirmed")
	}
}

func waitForHermes(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Hermes fake server did not reach the expected state")
}
