package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestRouterRetriesInjectedFailureWithSimulatedEngineAndKeepsTelemetryPromptFree(t *testing.T) {
	srv := newTestServer(t)
	simulated := engine.NewSimulated()
	var calls atomic.Int32
	srv.nodeClient.dispatcher.Transport = routerRoundTripper(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("injected temporary failure")),
				Request:    request,
			}, nil
		}
		var payload struct {
			Prompt string `json:"prompt"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			return nil, err
		}
		output, err := simulated.GenerateWithOptions(request.Context(), "demo", payload.Prompt, engine.GenerationOptions{})
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/plain"}},
			Body:       io.NopCloser(strings.NewReader(output)),
			Request:    request,
		}, nil
	})
	srv.registry.Upsert(simulatedRouterTestNode("a-failing", "http://node-a", 100))
	srv.registry.Upsert(simulatedRouterTestNode("b-simulated", "http://node-b", 1))

	const prompt = "ROUTER_PROMPT_MUST_STAY_PRIVATE"
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "/router/v1/completions",
		strings.NewReader(`{"model":"demo","prompt":"`+prompt+`"}`),
	))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "simulated response") {
		t.Fatalf("simulated router response = %d %q", response.Code, response.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("simulated upstream calls = %d, want one injected failure and one retry", calls.Load())
	}

	for name, body := range map[string]string{
		"router events":      routerEventsJSON(t, srv),
		"SSE snapshot":       string(mustJSON(t, srv.snapshot())),
		"Prometheus metrics": metricsBody(t, srv),
	} {
		if strings.Contains(body, prompt) {
			t.Errorf("%s exposed prompt content", name)
		}
	}
}

func TestRouterReportsInjectedSimulatedMidStreamFailure(t *testing.T) {
	srv := newTestServer(t)
	simulated := engine.NewSimulated()
	output, err := simulated.GenerateWithOptions(context.Background(), "demo", "private test prompt", engine.GenerationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv.nodeClient.dispatcher.Transport = routerRoundTripper(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       &simulatedFailureBody{remaining: []byte("data: " + output + "\n\n")},
			Request:    request,
		}, nil
	})
	srv.registry.Upsert(simulatedRouterTestNode("simulated-node", "http://node", 1))

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "/router/v1/chat/completions", strings.NewReader(`{"model":"demo","stream":true}`),
	))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), output) {
		t.Fatalf("partial simulated stream = %d %q", response.Code, response.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("mid-stream failure triggered %d upstream attempts, want 1", calls.Load())
	}
	requestID := response.Header().Get("X-Request-ID")
	if !routerHasEvent(srv.routerEvents.snapshot(), requestID, "attempt_failed", "stream_interrupted") {
		t.Fatalf("mid-stream failure event missing: %+v", srv.routerEvents.snapshot())
	}
	if got := srv.metrics.routerActiveStreams.Load(); got != 0 {
		t.Fatalf("active stream gauge = %d after failure, want 0", got)
	}
}

func TestRouterTimeToFirstByteTimeoutIsSeparateFromTotalDeadline(t *testing.T) {
	srv := newTestServer(t)
	srv.routerTTFBTimeout = 80 * time.Millisecond
	srv.routerIdleTimeout = time.Second
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "late")
	}))
	defer node.Close()
	srv.registry.Upsert(simulatedRouterTestNode("slow-node", node.URL, 1))

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "/router/v1/completions", strings.NewReader(`{"model":"demo","prompt":"test"}`),
	))
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("time-to-first-byte timeout status = %d, want 504: %s", response.Code, response.Body.String())
	}
	if !routerHasEvent(srv.routerEvents.snapshot(), response.Header().Get("X-Request-ID"),
		"attempt_failed", "time_to_first_byte_timeout") {
		t.Fatalf("first-byte timeout event missing: %+v", srv.routerEvents.snapshot())
	}
}

func TestRouterReturnsGatewayTimeoutWhenFinalAttemptMissesFirstByteDeadline(t *testing.T) {
	srv := newTestServer(t)
	srv.routerTTFBTimeout = 25 * time.Millisecond
	srv.routerIdleTimeout = time.Second
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer node.Close()
	for i := range routerMaxAttempts {
		srv.registry.Upsert(routerTestNode(
			fmt.Sprintf("slow-node-%d", i), node.URL, float64(routerMaxAttempts-i),
		))
	}

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "/router/v1/completions", strings.NewReader(`{"model":"demo","prompt":"test"}`),
	))
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("final first-byte timeout status = %d, want 504: %s", response.Code, response.Body.String())
	}
	var timeouts int
	for _, event := range srv.routerEvents.snapshot() {
		if event.Phase == "attempt_failed" && event.FailureClass == "time_to_first_byte_timeout" {
			timeouts++
		}
	}
	if timeouts != routerMaxAttempts {
		t.Fatalf("first-byte timeout attempts = %d, want %d", timeouts, routerMaxAttempts)
	}
}

func TestRouterIdleStreamTimeoutCancelsUpstreamAfterFirstByte(t *testing.T) {
	srv := newTestServer(t)
	srv.routerTTFBTimeout = time.Second
	srv.routerIdleTimeout = 80 * time.Millisecond
	upstreamCancelled := make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(upstreamCancelled)
	}))
	defer node.Close()
	srv.registry.Upsert(simulatedRouterTestNode("idle-node", node.URL, 1))

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "/router/v1/chat/completions", strings.NewReader(`{"model":"demo","stream":true}`),
	))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "data: first") {
		t.Fatalf("idle stream response = %d %q", response.Code, response.Body.String())
	}
	select {
	case <-upstreamCancelled:
	case <-time.After(time.Second):
		t.Fatal("idle timeout did not cancel the upstream stream")
	}
	if !routerHasEvent(srv.routerEvents.snapshot(), response.Header().Get("X-Request-ID"),
		"attempt_failed", "idle_stream_timeout") {
		t.Fatalf("idle timeout event missing: %+v", srv.routerEvents.snapshot())
	}
	if got := srv.metrics.routerActiveStreams.Load(); got != 0 {
		t.Fatalf("active stream gauge = %d after timeout, want 0", got)
	}
}

func TestRouterCircuitBreakerIsExposedInRequestsAndSSE(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(simulatedRouterTestNode("breaker-node", "http://node", 1))
	for range routerBreakerFailureThreshold {
		srv.routerBreakers.recordFailure("breaker-node", "demo")
	}

	var response struct {
		Events          []routerRequestEvent           `json:"events"`
		CircuitBreakers []routerCircuitBreakerSnapshot `json:"circuit_breakers"`
	}
	encoded := routerEventsJSON(t, srv)
	if err := json.Unmarshal([]byte(encoded), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.CircuitBreakers) != 1 || response.CircuitBreakers[0].State != "open" {
		t.Fatalf("router request breaker state = %+v", response.CircuitBreakers)
	}

	snapshot := srv.snapshot()
	if len(snapshot.RouterCircuitBreakers) != 1 || snapshot.RouterCircuitBreakers[0].State != "open" {
		t.Fatalf("SSE snapshot breaker state = %+v", snapshot.RouterCircuitBreakers)
	}
	encodedSnapshot := string(mustJSON(t, snapshot))
	if !strings.Contains(encodedSnapshot, `"router_circuit_breakers"`) {
		t.Fatalf("SSE snapshot omitted router circuit breakers: %s", encodedSnapshot)
	}
}

func TestRouterClientRateLimitIsAppliedBeforeDispatch(t *testing.T) {
	srv := newTestServer(t)
	srv.routerClients = newRouterClientLimiter(1, 0)
	makeRequest := func() *http.Request {
		request := httptest.NewRequest(http.MethodPost, "/router/v1/completions",
			strings.NewReader(`{"model":"demo","prompt":"private"}`))
		request.RemoteAddr = "192.0.2.30:4321"
		request.Header.Set("Authorization", "Bearer secret-client-key")
		return request
	}

	first := httptest.NewRecorder()
	srv.Mux().ServeHTTP(first, makeRequest())
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first request status = %d, want no-node 503", first.Code)
	}
	second := httptest.NewRecorder()
	srv.Mux().ServeHTTP(second, makeRequest())
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("limited request status = %d, want 429", second.Code)
	}
	if strings.Contains(routerEventsJSON(t, srv), "secret-client-key") {
		t.Fatal("router request events exposed an API key")
	}
}

func simulatedRouterTestNode(id, address string, score float64) cluster.NodeStatus {
	node := routerTestNode(id, address, score)
	node.Engines[0].Name = "simulated"
	node.Models[0].Engine = "simulated"
	return node
}

func routerHasEvent(events []routerRequestEvent, requestID, phase, failureClass string) bool {
	for _, event := range events {
		if event.RequestID == requestID && event.Phase == phase && event.FailureClass == failureClass {
			return true
		}
	}
	return false
}

func routerEventsJSON(t *testing.T, srv *Server) string {
	t.Helper()
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/router/requests", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("router events status = %d", response.Code)
	}
	return response.Body.String()
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func metricsBody(t *testing.T, srv *Server) string {
	t.Helper()
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}
	return response.Body.String()
}

type simulatedFailureBody struct {
	remaining []byte
	failed    bool
}

func (b *simulatedFailureBody) Read(buffer []byte) (int, error) {
	if len(b.remaining) > 0 {
		n := copy(buffer, b.remaining)
		b.remaining = b.remaining[n:]
		return n, nil
	}
	if !b.failed {
		b.failed = true
		return 0, errors.New("injected simulated mid-stream failure")
	}
	return 0, io.EOF
}

func (b *simulatedFailureBody) Close() error { return nil }
