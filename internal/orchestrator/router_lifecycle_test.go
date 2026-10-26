package orchestrator

import (
	"bytes"
	"context"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRouterLifecycleAndPrivateLogs(t *testing.T) {
	for _, test := range []struct {
		name, request string
		status        int
		terminal      string
	}{
		{"success", `{"model":"demo","messages":[{"role":"user","content":"private-prompt"}]}`, 200, "completed"},
		{"invalid", `{"model":false}`, 200, "failed"},
		{"upstream rejection", `{"model":"demo"}`, 400, "failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			oldWriter, oldFlags := log.Writer(), log.Flags()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() {
				slog.SetDefault(old)
				log.SetOutput(oldWriter)
				log.SetFlags(oldFlags)
			})
			server := newTestServer(t)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, `{"private-response":"secret-output"}`)
			}))
			defer upstream.Close()
			server.registry.Upsert(routerTestNode("private-node", upstream.URL, 1))
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions?key=secret-query", strings.NewReader(test.request))
			req.Header.Set("Authorization", "Bearer secret-token")
			server.Mux().ServeHTTP(recorder, req)
			requestID := recorder.Header().Get("X-Request-ID")
			events := server.routerEvents.snapshot()
			if len(events) < 2 || events[0].Phase != "accepted" || events[len(events)-1].Phase != test.terminal {
				t.Fatalf("lifecycle terminal = %+v", events)
			}
			terminalCount := 0
			for _, event := range events {
				if event.RequestID != requestID || event.Timestamp.IsZero() {
					t.Fatalf("uncorrelated event: %+v", event)
				}
				if event.Phase == "completed" || event.Phase == "failed" {
					terminalCount++
				}
			}
			if terminalCount != 1 {
				t.Fatalf("terminal events = %d", terminalCount)
			}
			if test.terminal == "completed" {
				want := []string{"accepted", "scheduled", "queued", "attempt_started", "dispatched", "upstream_response",
					"first_upstream_byte", "attempt_succeeded", "completed"}
				if len(events) != len(want) {
					t.Fatalf("event count = %d: %+v", len(events), events)
				}
				for i, phase := range want {
					if events[i].Phase != phase {
						t.Fatalf("event %d = %s, want %s", i, events[i].Phase, phase)
					}
				}
			}
			for _, secret := range []string{"private-node", "private-prompt", "secret-output", "secret-query", "secret-token", `"model":"demo"`} {
				if strings.Contains(logs.String(), secret) {
					t.Errorf("logs exposed %q", secret)
				}
			}
			assertRouterResourcesReleased(t, server, "private-node")
		})
	}
}

func TestRouterLifecycleCancellationHasTerminalEvent(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions", strings.NewReader(`{"model":"demo"}`)).WithContext(ctx)
	server.Mux().ServeHTTP(httptest.NewRecorder(), req)
	events := server.routerEvents.snapshot()
	if len(events) < 2 || events[len(events)-1].Phase != "failed" || events[len(events)-1].FailureClass != "cancelled" {
		t.Fatalf("cancellation lifecycle = %+v", events)
	}
}
