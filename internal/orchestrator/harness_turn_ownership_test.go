package orchestrator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

type failingHarnessWriter struct{ *httptest.ResponseRecorder }

func (failingHarnessWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type cancellationStreamingBackend struct {
	*controlledHarnessBackend
	finished chan struct{}
	event    agent.StreamEvent
}

func (b *cancellationStreamingBackend) SendMessage(ctx context.Context, _ string, _ string, stream chan<- agent.StreamEvent) (*agent.Message, error) {
	defer close(b.finished)
	stream <- b.event
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHarnessWriteFailureCancelsAndJoinsBackend(t *testing.T) {
	for _, tt := range []struct {
		name         string
		event        agent.StreamEvent
		writeFailure bool
	}{
		{name: "write failure", event: agent.StreamEvent{Type: agent.EventDone}, writeFailure: true},
		{name: "encoding failure", event: agent.StreamEvent{Type: agent.EventDone, ToolResp: &agent.ToolResponse{Output: make(chan struct{})}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t)
			base := installControlledHarness(t, srv, "session")
			backend := &cancellationStreamingBackend{controlledHarnessBackend: base, finished: make(chan struct{}), event: tt.event}
			srv.harness.mu.Lock()
			srv.harness.agents["controlled"] = backend
			srv.harness.mu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			req := httptest.NewRequest(http.MethodPost, "/api/agent/sessions/session/messages?stream=true",
				strings.NewReader(`{"content":"hello"}`)).WithContext(ctx)
			req.SetPathValue("id", "session")
			done := make(chan struct{})
			rec := httptest.NewRecorder()
			var writer http.ResponseWriter = rec
			if tt.writeFailure {
				writer = failingHarnessWriter{rec}
			}
			go func() {
				defer close(done)
				srv.handleAgentMessagePost(writer, req)
			}()
			t.Cleanup(func() { cancel(); awaitHandler(t, done) })
			awaitHandler(t, done)
			select {
			case <-backend.finished:
			default:
				t.Error("handler returned before backend cleanup")
			}
			if ctx.Err() != nil {
				t.Error("test request was canceled externally instead of by the write failure")
			}
			if srv.InterruptSession("session") {
				t.Error("write failure retained a turn cancellation handle")
			}
			if !tt.writeFailure && (!strings.Contains(rec.Body.String(), `"type":"error"`) || !strings.Contains(rec.Body.String(), "could not encode agent stream event") || strings.Contains(rec.Body.String(), `"type":"done"`)) {
				t.Fatalf("encoding failure did not produce only a terminal error: %s", rec.Body.String())
			}
		})
	}
}

// harnessTurn is one in-flight SendMessage call controlled by the test.
type harnessTurn struct {
	ctx    context.Context
	finish chan error
}

// controlledHarnessBackend hands each turn to the test, which decides how
// and when it ends; turns also end when their context is canceled.
type controlledHarnessBackend struct {
	mu       sync.Mutex
	sessions map[string]*agent.Session
	turns    chan harnessTurn
}

func newControlledHarnessBackend(sessionID string) *controlledHarnessBackend {
	return &controlledHarnessBackend{
		sessions: map[string]*agent.Session{sessionID: {ID: sessionID, Title: "controlled"}},
		turns:    make(chan harnessTurn),
	}
}

func (b *controlledHarnessBackend) Name() string { return "controlled" }
func (b *controlledHarnessBackend) CreateSession(string, agent.SessionConfig) (*agent.Session, error) {
	return nil, errors.New("unused")
}
func (b *controlledHarnessBackend) GetSession(id string) (*agent.Session, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	session, ok := b.sessions[id]
	if !ok {
		return nil, false
	}
	copied := *session
	return &copied, true
}
func (b *controlledHarnessBackend) ListSessions() []*agent.Session { return nil }
func (b *controlledHarnessBackend) DeleteSession(string) bool      { return false }
func (b *controlledHarnessBackend) SendMessage(ctx context.Context, _ string, _ string, stream chan<- agent.StreamEvent) (*agent.Message, error) {
	turn := harnessTurn{ctx: ctx, finish: make(chan error, 1)}
	select {
	case b.turns <- turn:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var err error
	select {
	case err = <-turn.finish:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	if stream != nil {
		stream <- agent.StreamEvent{Type: agent.EventDone}
	}
	return &agent.Message{Role: "assistant", Content: "done"}, nil
}
func (b *controlledHarnessBackend) InterruptSession(string) bool         { return false }
func (b *controlledHarnessBackend) ListTools() []agent.ToolDef           { return nil }
func (b *controlledHarnessBackend) ListClusterModels() []agent.ModelInfo { return nil }
func (b *controlledHarnessBackend) Stats() agent.AgentStats              { return agent.AgentStats{} }
func (b *controlledHarnessBackend) Close() error                         { return nil }
func (b *controlledHarnessBackend) ExecuteTool(context.Context, string, string, map[string]any) (agent.ToolResult, error) {
	return agent.ToolResult{}, errors.New("unused")
}

func installControlledHarness(t *testing.T, srv *Server, sessionID string) *controlledHarnessBackend {
	t.Helper()
	backend := newControlledHarnessBackend(sessionID)
	srv.harness.mu.Lock()
	srv.harness.agents["controlled"] = backend
	srv.harness.mu.Unlock()
	srv.harness.setSessionHarness(sessionID, "controlled")
	return backend
}

func postHarnessMessage(srv *Server, ctx context.Context, sessionID string, stream bool) (*httptest.ResponseRecorder, <-chan struct{}) {
	target := "/api/agent/sessions/" + sessionID + "/messages"
	if stream {
		target += "?stream=true"
	}
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"content":"hello"}`)).WithContext(ctx)
	req.SetPathValue("id", sessionID)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.handleAgentMessagePost(rec, req)
	}()
	return rec, done
}

func awaitHarnessTurn(t *testing.T, backend *controlledHarnessBackend) harnessTurn {
	t.Helper()
	select {
	case turn := <-backend.turns:
		return turn
	case <-time.After(5 * time.Second):
		t.Fatal("agent turn did not reach the backend")
		return harnessTurn{}
	}
}

func awaitHandler(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("agent message handler did not return")
	}
}

func TestOverlappingHarnessTurnsKeepTheirOwnCancellation(t *testing.T) {
	srv := newTestServer(t)
	backend := installControlledHarness(t, srv, "session")

	_, firstDone := postHarnessMessage(srv, context.Background(), "session", false)
	first := awaitHarnessTurn(t, backend)
	secondRec, secondDone := postHarnessMessage(srv, context.Background(), "session", false)
	second := awaitHarnessTurn(t, backend)

	first.finish <- nil
	awaitHandler(t, firstDone)

	// The first turn's cleanup must not discard the second turn's handle.
	if !srv.InterruptSession("session") {
		t.Fatal("in-flight second turn could not be interrupted after the first completed")
	}
	awaitHandler(t, secondDone)
	if !errors.Is(second.ctx.Err(), context.Canceled) {
		t.Fatalf("second turn context error = %v, want canceled", second.ctx.Err())
	}
	if !strings.Contains(secondRec.Body.String(), `"interrupted"`) {
		t.Fatalf("second turn response = %d %s, want interrupted", secondRec.Code, secondRec.Body.String())
	}
	if srv.InterruptSession("session") {
		t.Fatal("cancellation handles leaked after both turns finished")
	}
}

func TestHarnessStreamTerminalEventsAndCleanup(t *testing.T) {
	tests := []struct {
		name       string
		end        func(turn harnessTurn, cancelClient context.CancelFunc)
		wantEvent  string
		forbidText string
	}{
		{
			name:      "completed turn ends with done",
			end:       func(turn harnessTurn, _ context.CancelFunc) { turn.finish <- nil },
			wantEvent: `"type":"done"`,
		},
		{
			name:      "backend failure ends with error event",
			end:       func(turn harnessTurn, _ context.CancelFunc) { turn.finish <- errors.New("upstream exploded") },
			wantEvent: `"error":"upstream exploded"`,
		},
		{
			name:       "client disconnect cancels the turn without an error event",
			end:        func(_ harnessTurn, cancelClient context.CancelFunc) { cancelClient() },
			forbidText: `"type":"error"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t)
			backend := installControlledHarness(t, srv, "session")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			rec, done := postHarnessMessage(srv, ctx, "session", true)
			turn := awaitHarnessTurn(t, backend)
			tt.end(turn, cancel)
			awaitHandler(t, done)

			if turn.ctx.Err() == nil {
				t.Error("turn context was not released when the handler returned")
			}
			if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
				t.Errorf("content type = %q", got)
			}
			body := rec.Body.String()
			if tt.wantEvent != "" && !strings.Contains(body, tt.wantEvent) {
				t.Errorf("stream body %q lacks terminal event %s", body, tt.wantEvent)
			}
			if tt.forbidText != "" && strings.Contains(body, tt.forbidText) {
				t.Errorf("stream body %q unexpectedly contains %s", body, tt.forbidText)
			}
			if srv.InterruptSession("session") {
				t.Error("turn cancellation handle leaked after the stream ended")
			}
		})
	}
}
