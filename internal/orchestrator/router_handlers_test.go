package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/inferroute"
)

func TestRouterRetriesTransientFailureBeforeForwardingContent(t *testing.T) {
	srv := newTestServer(t)
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		if r.URL.Path != "/proxy/v1/chat/completions" || r.URL.RawQuery != "stream=true" {
			t.Errorf("first node received URL %q", r.URL.String())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer client-secret" {
			t.Errorf("authorization = %q", got)
		}
		if got := r.Header.Get("X-Request-ID"); got != "client-trace" {
			t.Errorf("provider request ID = %q", got)
		}
		if got := r.Header.Get(inferroute.HeaderRequestID); got == "" {
			t.Error("request ID was not forwarded internally")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read first request: %v", err)
		}
		if got := string(body); got != `{"model":"demo","stream":true,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function"}]}` {
			t.Errorf("provider request changed: %s", got)
		}
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer client-secret" {
			t.Errorf("authorization = %q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: routed\n\n")
	}))
	defer second.Close()

	srv.registry.Upsert(routerTestNode("a-first", first.URL, 100))
	srv.registry.Upsert(routerTestNode("b-second", second.URL, 1))
	req := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions?stream=true",
		strings.NewReader(`{"model":"demo","stream":true,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function"}]}`))
	req.Header.Set("Authorization", "Bearer client-secret")
	req.Header.Set("X-Request-ID", "client-trace")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != "data: routed\n\n" {
		t.Fatalf("router response = %d %q", rec.Code, rec.Body.String())
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("node calls = first:%d second:%d; want one each", firstCalls.Load(), secondCalls.Load())
	}
	requestID := rec.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("response omitted request ID")
	}
	events := srv.routerEvents.snapshot()
	var sawRetry, sawSuccess bool
	for _, event := range events {
		if event.RequestID != requestID {
			continue
		}
		if event.Phase == "attempt_failed" && event.Status == http.StatusServiceUnavailable && event.FailureClass == "upstream_5xx" {
			sawRetry = event.AttemptID != ""
		}
		if event.Phase == "attempt_succeeded" && event.Status == http.StatusOK && event.NodeID == "b-second" {
			sawSuccess = event.AttemptID != "" && event.TimeToFirstByteMs >= 0
		}
	}
	if !sawRetry || !sawSuccess {
		t.Fatalf("request-level attempt events missing retry/success: %+v", events)
	}
}

func TestRouterDoesNotRetryClientErrors(t *testing.T) {
	srv := newTestServer(t)
	var secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer second.Close()
	srv.registry.Upsert(routerTestNode("a-first", first.URL, 100))
	srv.registry.Upsert(routerTestNode("b-second", second.URL, 1))

	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions",
		strings.NewReader(`{"model":"demo","messages":[]}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("response status = %d, want 401: %s", rec.Code, rec.Body.String())
	}
	if secondCalls.Load() != 0 {
		t.Fatalf("client error was retried on another node %d times", secondCalls.Load())
	}
}

func TestRouterRetryClassificationIsLimitedToTransientFailures(t *testing.T) {
	for _, test := range []struct {
		status    int
		body      string
		retryable bool
		class     string
	}{
		{http.StatusRequestTimeout, "", true, "timeout"},
		{http.StatusTooManyRequests, "", true, "rate_limit"},
		{http.StatusBadGateway, "", true, "upstream_5xx"},
		{http.StatusNotImplemented, "", false, "client_error"},
		{http.StatusUnauthorized, "", false, "client_error"},
		{http.StatusBadRequest, "", false, "client_error"},
		{http.StatusNotFound, `{"error":"model not found"}`, true, "stale_model"},
		{http.StatusNotFound, `{"error":"route not found"}`, false, "client_error"},
	} {
		class, retryable := classifyRouterStatus(test.status, []byte(test.body))
		if retryable != test.retryable || class != test.class {
			t.Errorf("classifyRouterStatus(%d, %q) = (%q, %t), want (%q, %t)",
				test.status, test.body, class, retryable, test.class, test.retryable)
		}
	}
}

func TestRouterNeverSwitchesNodesAfterContentIsCommitted(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(routerTestNode("a-first", "http://node-a", 100))
	srv.registry.Upsert(routerTestNode("b-second", "http://node-b", 1))
	var firstCalls, secondCalls atomic.Int32
	srv.nodeClient.dispatcher.Transport = routerRoundTripper(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Host {
		case "node-a":
			firstCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}},
				Body: io.NopCloser(&failAfterFirstByteReader{}), Request: r,
			}, nil
		case "node-b":
			secondCalls.Add(1)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("should not be used")), Request: r}, nil
		default:
			return nil, errors.New("unexpected node")
		}
	})

	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions",
		strings.NewReader(`{"model":"demo"}`)))
	if rec.Code != http.StatusOK || rec.Body.String() != "x" {
		t.Fatalf("committed stream response = %d %q", rec.Code, rec.Body.String())
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 0 {
		t.Fatalf("nodes were switched after content: first=%d second=%d", firstCalls.Load(), secondCalls.Load())
	}
	requestID := rec.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Fatal("response omitted request ID")
	}
	foundFailure := false
	for _, event := range srv.routerEvents.snapshot() {
		if event.RequestID == requestID && event.Phase == "attempt_failed" &&
			event.FailureClass == "stream_interrupted" && event.Status == http.StatusOK {
			foundFailure = true
		}
	}
	if !foundFailure {
		t.Fatal("interrupted committed stream was not reported as a failed attempt")
	}
}

func TestRouterPropagatesClientCancellation(t *testing.T) {
	srv := newTestServer(t)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	srv.registry.Upsert(routerTestNode("a-first", "http://node-a", 100))
	srv.nodeClient.dispatcher.Transport = routerRoundTripper(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		close(cancelled)
		return nil, r.Context().Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions", strings.NewReader(`{"model":"demo"}`)).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		srv.Mux().ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request did not start")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("client cancellation was not propagated upstream")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("router handler did not return after cancellation")
	}
}

type routerRoundTripper func(*http.Request) (*http.Response, error)

func (f routerRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failAfterFirstByteReader struct{ sent bool }

func (r *failAfterFirstByteReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		p[0] = 'x'
		return 1, nil
	}
	return 0, errors.New("stream interrupted")
}

func routerTestNode(id, address string, hardwareScore float64) cluster.NodeStatus {
	return cluster.NodeStatus{
		ID: id, Address: address, Healthy: true,
		Hardware:           hardware.Info{Score: hardwareScore},
		MaxConcurrentTasks: 1, AvailableCapacity: 1,
		Engines: []engine.Status{{Name: "vllm", Ready: true, Protocols: []string{"openai"}}},
		Models: []engine.Model{{
			Name: "demo", Engine: "vllm", Tags: []string{"general"}, Protocols: []string{"openai"},
		}},
	}
}

func TestRouterAddsChosenModelWhenOmitted(t *testing.T) {
	srv := newTestServer(t)
	var got map[string]json.RawMessage
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode routed body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer node.Close()
	srv.registry.Upsert(routerTestNode("node", node.URL, 1))

	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions",
		strings.NewReader(`{"messages":[{"role":"user","content":"hello"}]}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("response status = %d: %s", rec.Code, rec.Body.String())
	}
	var model string
	if err := json.Unmarshal(got["model"], &model); err != nil || model != "demo" {
		t.Fatalf("routed model = %q, error=%v", model, err)
	}
}
