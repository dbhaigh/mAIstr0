package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/httpx"
)

func serveNodeHandlerRequest(srv *Server, method, path, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
	return response
}

func TestNodeRegistrationAndEjectEdgeCases(t *testing.T) {
	srv := newTestServer(t)
	for name, body := range map[string]string{
		"invalid JSON":   `{"id":`,
		"missing fields": `{"id":"node-only"}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/register", body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("registration returned %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body.String())
			}
		})
	}

	srv.registry.Upsert(cluster.NodeStatus{ID: "current", Address: "http://current", Version: "2.0.0"})
	older, err := json.Marshal(cluster.NodeStatus{ID: "older", Address: "http://older", Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	response := serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/register", string(older))
	if response.Code != http.StatusConflict {
		t.Fatalf("older-version registration returned %d, want %d: %s", response.Code, http.StatusConflict, response.Body.String())
	}
	if _, ok := srv.registry.Get("older"); !ok {
		t.Fatal("older-version node was not recorded")
	}

	srv.registry.Upsert(cluster.NodeStatus{ID: "ejected", Address: "http://ejected"})
	response = serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/ejected/eject", "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("eject returned %d, want %d: %s", response.Code, http.StatusNoContent, response.Body.String())
	}
	if _, ok := srv.registry.Get("ejected"); ok {
		t.Fatal("ejected node remains registered")
	}

	missingID := httptest.NewRecorder()
	srv.handleEject(missingID, httptest.NewRequest(http.MethodPost, "/api/nodes/eject", nil))
	if missingID.Code != http.StatusBadRequest {
		t.Fatalf("eject without a path ID returned %d, want %d", missingID.Code, http.StatusBadRequest)
	}
}

func TestRefreshAllNodeModelsReportsEachOutcome(t *testing.T) {
	success := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer success.Close()
	failure := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer failure.Close()
	unreachable := httptest.NewServer(http.NotFoundHandler())
	unreachableURL := unreachable.URL
	unreachable.Close()

	srv := newTestServer(t)
	for _, node := range []cluster.NodeStatus{
		{ID: "success", Address: success.URL},
		{ID: "failure", Address: failure.URL},
		{ID: "unreachable", Address: unreachableURL},
		{ID: "invalid-url", Address: "not a URL"},
	} {
		srv.registry.Upsert(node)
	}

	response := serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/models/refresh", "")
	if response.Code != http.StatusOK {
		t.Fatalf("refresh-all returned %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var results map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}
	if results["success"] != "refreshed" {
		t.Errorf("success result = %q", results["success"])
	}
	if !strings.HasPrefix(results["failure"], "503") {
		t.Errorf("failure result = %q, want upstream status", results["failure"])
	}
	if !strings.HasPrefix(results["unreachable"], "unreachable: ") {
		t.Errorf("unreachable result = %q", results["unreachable"])
	}
	if results["invalid-url"] == "" {
		t.Error("invalid URL result is empty")
	}
}

func TestNodeModelHandlersProxyResponsesAndRejectInvalidRequests(t *testing.T) {
	var gotMethod, gotPath, gotBody, gotContentType string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotContentType = r.Method, r.URL.Path, r.Header.Get("Content-Type")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request", http.StatusBadRequest)
			return
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "node")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"models":[]}`)
	}))
	defer upstream.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{ID: "models", Address: upstream.URL})
	response := serveNodeHandlerRequest(srv, http.MethodGet, "/api/nodes/models/models", "")
	if response.Code != http.StatusAccepted || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("models GET response = %d, headers %v", response.Code, response.Header())
	}
	if gotMethod != http.MethodGet || gotPath != "/models/all" || response.Body.String() != `{"models":[]}` {
		t.Fatalf("models GET forwarded %s %s and returned %q", gotMethod, gotPath, response.Body.String())
	}

	response = serveNodeHandlerRequest(srv, http.MethodPatch, "/api/nodes/models/models", `{"disabled":["model-a"]}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("models PATCH response = %d, headers %v", response.Code, response.Header())
	}
	if gotMethod != http.MethodPatch || gotPath != "/models" || gotBody != `{"disabled":["model-a"]}` || gotContentType != "application/json" {
		t.Fatalf("models PATCH forwarded method=%q path=%q content-type=%q body=%q",
			gotMethod, gotPath, gotContentType, gotBody)
	}

	response = serveNodeHandlerRequest(srv, http.MethodGet, "/api/nodes/missing/models", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing node GET returned %d, want %d", response.Code, http.StatusNotFound)
	}
	response = serveNodeHandlerRequest(srv, http.MethodPatch, "/api/nodes/missing/models", "{}")
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing node PATCH returned %d, want %d", response.Code, http.StatusNotFound)
	}

	oversized := strings.Repeat("x", int(httpx.ControlJSONBodyBytes)+1)
	response = serveNodeHandlerRequest(srv, http.MethodPatch, "/api/nodes/models/models", oversized)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("oversized models PATCH returned %d, want %d", response.Code, http.StatusBadRequest)
	}

	srv.registry.Upsert(cluster.NodeStatus{ID: "bad-url", Address: "http://bad host"})
	response = serveNodeHandlerRequest(srv, http.MethodGet, "/api/nodes/bad-url/models", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("invalid upstream URL returned %d, want %d", response.Code, http.StatusInternalServerError)
	}

	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	srv.registry.Upsert(cluster.NodeStatus{ID: "unreachable", Address: closedURL})
	for _, method := range []string{http.MethodGet, http.MethodPatch} {
		response = serveNodeHandlerRequest(srv, method, "/api/nodes/unreachable/models", "{}")
		if response.Code != http.StatusBadGateway {
			t.Errorf("%s to unreachable node returned %d, want %d", method, response.Code, http.StatusBadGateway)
		}
	}
}

func TestNodeControlMapsProxyPathsAndPreservesRequestMetadata(t *testing.T) {
	type observedRequest struct {
		method, path, query, body string
		headers                   []string
	}
	var observed observedRequest
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request", http.StatusBadRequest)
			return
		}
		observed = observedRequest{
			method: r.Method, path: r.URL.EscapedPath(), query: r.URL.RawQuery,
			body: string(body), headers: r.Header.Values("X-Trace"),
		}
		w.Header().Set("X-Upstream", "node")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "proxied")
	}))
	defer upstream.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{ID: "route-node", Address: upstream.URL})
	mux := srv.Mux()
	for _, test := range []struct {
		name, method, path, upstreamPath string
	}{
		{"properties", http.MethodGet, "/api/nodes/route-node/properties", "/properties"},
		{"engines", http.MethodGet, "/api/nodes/route-node/engines", "/engine"},
		{"managed engines", http.MethodGet, "/api/nodes/route-node/engines/managed", "/api/engines/managed"},
		{"head models", http.MethodGet, "/api/nodes/route-node/engines/managed/head-models", "/api/engines/managed/head-models"},
		{"configure head", http.MethodPost, "/api/nodes/route-node/engines/managed/configure-head", "/api/engines/managed/configure-head"},
		{"install worker", http.MethodPost, "/api/nodes/route-node/engines/managed/install-worker", "/api/engines/managed/install-worker"},
		{"download model", http.MethodPost, "/api/nodes/route-node/engines/managed/download-model", "/api/engines/managed/download-model"},
		{"escaped engine ID", http.MethodGet, "/api/nodes/route-node/engines/managed/engine%2Fname/profile", "/api/engines/managed/engine%2Fname/profile"},
		{"model pull", http.MethodPost, "/api/nodes/route-node/models/pull", "/models/pull"},
		{"model refresh", http.MethodPost, "/api/nodes/route-node/models/refresh", "/models/refresh"},
		{"pairing connect", http.MethodPost, "/api/nodes/route-node/pairing/connect", "/api/pairing/connect"},
		{"runtime settings", http.MethodPatch, "/api/nodes/route-node/runtime-settings", "/api/runtime-settings"},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed = observedRequest{}
			body := ""
			if test.method != http.MethodGet {
				body = `{"setting":"value"}`
			}
			request := httptest.NewRequest(test.method, test.path+"?trace=1", strings.NewReader(body))
			request.Header.Add("X-Trace", "first")
			request.Header.Add("X-Trace", "second")
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != http.StatusAccepted || response.Header().Get("X-Upstream") != "node" {
				t.Fatalf("proxy returned %d, headers %v, body %q", response.Code, response.Header(), response.Body.String())
			}
			if observed.method != test.method || observed.path != test.upstreamPath || observed.query != "trace=1" {
				t.Fatalf("forwarded request = %+v, want method=%s path=%s query=trace=1",
					observed, test.method, test.upstreamPath)
			}
			if len(observed.headers) != 2 || observed.headers[0] != "first" || observed.headers[1] != "second" {
				t.Errorf("forwarded X-Trace values = %v", observed.headers)
			}
			if observed.body != body {
				t.Errorf("forwarded body = %q, want %q", observed.body, body)
			}
		})
	}
}

func TestRuntimeSettingsUseServiceControlAddress(t *testing.T) {
	var observedPath, observedQuery, observedMethod, observedBody string
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedPath, observedQuery, observedMethod = r.URL.Path, r.URL.RawQuery, r.Method
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request", http.StatusBadRequest)
			return
		}
		observedBody = string(body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "settings updated")
	}))
	defer upstream.Close()

	srv := newTestServer(t)
	srv.nodeClient.dispatcher = upstream.Client()
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "offline", Address: "http://unreachable", ControlAddress: upstream.URL,
	})
	srv.registry.MarkUnhealthy("offline")
	response := serveNodeHandlerRequest(srv, http.MethodPatch, "/api/nodes/offline/runtime-settings?scope=worker", `{"threads":4}`)
	if response.Code != http.StatusAccepted || response.Body.String() != "settings updated" {
		t.Fatalf("runtime settings response = %d %q", response.Code, response.Body.String())
	}
	if observedMethod != http.MethodPatch || observedPath != "/api/runtime-settings" ||
		observedQuery != "scope=worker" || observedBody != `{"threads":4}` {
		t.Fatalf("service controller received method=%q path=%q query=%q body=%q",
			observedMethod, observedPath, observedQuery, observedBody)
	}

	srv.registry.Upsert(cluster.NodeStatus{
		ID: "bad-control", Address: "http://unused", ControlAddress: "http://insecure",
	})
	srv.registry.MarkUnhealthy("bad-control")
	response = serveNodeHandlerRequest(srv, http.MethodGet, "/api/nodes/bad-control/runtime-settings", "")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("invalid service control address returned %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestNodeControlRejectsInvalidTargetAndWorkerInstallResults(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{ID: "bad-target", Address: "http://bad host"})
	response := serveNodeHandlerRequest(srv, http.MethodGet, "/api/nodes/bad-target/properties", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("invalid control target returned %d, want %d", response.Code, http.StatusInternalServerError)
	}

	t.Run("invalid worker response", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "not JSON")
		}))
		defer upstream.Close()
		srv := newTestServer(t)
		srv.registry.Upsert(cluster.NodeStatus{ID: "worker", Address: upstream.URL})
		response := serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/worker/engines/managed/install-worker", `{}`)
		if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "invalid worker installation response") {
			t.Fatalf("invalid installer response = %d %q", response.Code, response.Body.String())
		}
	})

	t.Run("node changes during installation", func(t *testing.T) {
		var srv *Server
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			node, ok := srv.registry.Get("worker")
			if !ok {
				t.Error("worker disappeared during installation")
			} else {
				node.Address = "http://replacement"
				srv.registry.Upsert(node)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"devices":[{"kind":"gpu","free_memory_mb":6000}]}`)
		}))
		defer upstream.Close()
		srv = newTestServer(t)
		srv.registry.Upsert(cluster.NodeStatus{ID: "worker", Address: upstream.URL})
		response := serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/worker/engines/managed/install-worker", `{}`)
		if response.Code != http.StatusServiceUnavailable ||
			!strings.Contains(response.Body.String(), "node changed while refreshing device memory") {
			t.Fatalf("stale install result = %d %q", response.Code, response.Body.String())
		}
	})
}

func TestNodeChatAndDialogueProxiesPreserveRequestsAndRejectUnavailableNodes(t *testing.T) {
	var observedPath, observedQuery, observedMethod, observedBody, observedHeader string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedPath, observedQuery, observedMethod = r.URL.EscapedPath(), r.URL.RawQuery, r.Method
		observedHeader = r.Header.Get("X-Dialogue")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request", http.StatusBadRequest)
			return
		}
		observedBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "dialogue")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{ID: "chat-node", Address: upstream.URL})
	response := serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/chat-node/chat", `{"messages":[]}`)
	if response.Code != http.StatusAccepted || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("chat proxy response = %d, headers %v", response.Code, response.Header())
	}
	if observedPath != "/chat" || observedMethod != http.MethodPost || observedBody != `{"messages":[]}` {
		t.Fatalf("chat request forwarded as path=%q method=%q body=%q", observedPath, observedMethod, observedBody)
	}

	request := httptest.NewRequest(http.MethodPost,
		"/api/nodes/chat-node/dialogues/session-1/messages?limit=5", strings.NewReader(`{"message":"hi"}`))
	request.Header.Set("X-Dialogue", "preserved")
	dialogueResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(dialogueResponse, request)
	if dialogueResponse.Code != http.StatusAccepted || dialogueResponse.Header().Get("X-Upstream") != "dialogue" {
		t.Fatalf("dialogue proxy response = %d, headers %v", dialogueResponse.Code, dialogueResponse.Header())
	}
	if observedPath != "/dialogues/session-1/messages" || observedQuery != "limit=5" ||
		observedMethod != http.MethodPost || observedBody != `{"message":"hi"}` || observedHeader != "preserved" {
		t.Fatalf("dialogue request forwarded as path=%q query=%q method=%q body=%q header=%q",
			observedPath, observedQuery, observedMethod, observedBody, observedHeader)
	}

	srv.registry.MarkUnhealthy("chat-node")
	for _, path := range []string{
		"/api/nodes/chat-node/chat",
		"/api/nodes/chat-node/dialogues/session-1/messages",
	} {
		response = serveNodeHandlerRequest(srv, http.MethodPost, path, "{}")
		if response.Code != http.StatusNotFound {
			t.Errorf("offline proxy %s returned %d, want %d", path, response.Code, http.StatusNotFound)
		}
	}

	srv.registry.Upsert(cluster.NodeStatus{ID: "invalid-target", Address: "http://bad host"})
	response = serveNodeHandlerRequest(srv, http.MethodPost, "/api/nodes/invalid-target/dialogues/session-1", "{}")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("invalid dialogue target returned %d, want %d", response.Code, http.StatusInternalServerError)
	}

	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	srv.registry.Upsert(cluster.NodeStatus{ID: "unreachable", Address: closedURL})
	for _, path := range []string{
		"/api/nodes/unreachable/chat",
		"/api/nodes/unreachable/dialogues/session-1",
	} {
		response = serveNodeHandlerRequest(srv, http.MethodPost, path, "{}")
		if response.Code != http.StatusBadGateway {
			t.Errorf("unreachable proxy %s returned %d, want %d", path, response.Code, http.StatusBadGateway)
		}
	}
}

type nodeResponseWriter struct {
	header   http.Header
	writeErr error
	writes   int
}

func (w *nodeResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *nodeResponseWriter) WriteHeader(int) {}

func (w *nodeResponseWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(data), nil
}

func TestCopyNodeResponseHandlesClientDisconnect(t *testing.T) {
	writer := &nodeResponseWriter{writeErr: errors.New("client disconnected")}
	copyNodeResponse(writer, strings.NewReader("response"))
	if writer.writes == 0 {
		t.Fatal("response writer was not called")
	}
}

func BenchmarkCopyNodeResponse(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 64<<10)
	writer := &nodeResponseWriter{}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		copyNodeResponse(writer, bytes.NewReader(payload))
	}
}
