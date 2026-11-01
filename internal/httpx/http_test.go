package httpx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/logging"
)

func TestDecodeJSONBoundsBodiesAndStrictModeRejectsTrailingValues(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}

	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"large"}`))
	if _, err := DecodeJSON[payload](httptest.NewRecorder(), request, 4); err == nil {
		t.Fatal("DecodeJSON accepted a body larger than its limit")
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"ok"} {}`))
	if _, err := DecodeJSONStrict[payload](httptest.NewRecorder(), request, 128); err == nil ||
		!strings.Contains(err.Error(), "one JSON object") {
		t.Fatalf("DecodeJSONStrict trailing-value error = %v", err)
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"ok"}`))
	got, err := DecodeJSONStrict[payload](httptest.NewRecorder(), request, 128)
	if err != nil || got.Name != "ok" {
		t.Fatalf("DecodeJSONStrict returned %+v, %v", got, err)
	}
}

func TestReadBodyBoundsRawRequestBodies(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("small"))
	body, err := ReadBody(httptest.NewRecorder(), request, 8)
	if err != nil || string(body) != "small" {
		t.Fatalf("ReadBody returned %q, %v", body, err)
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("larger"))
	_, err = ReadBody(httptest.NewRecorder(), request, 4)
	var maxBytesErr *http.MaxBytesError
	if !errors.As(err, &maxBytesErr) {
		t.Fatalf("ReadBody error = %v, want *http.MaxBytesError", err)
	}
	if _, err := ReadBody(httptest.NewRecorder(), request, 0); err == nil {
		t.Fatal("ReadBody accepted a zero body limit")
	}
}

func TestWriteJSONAndWriteError(t *testing.T) {
	response := httptest.NewRecorder()
	if err := WriteJSON(response, http.StatusCreated, map[string]string{"id": "item"}); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusCreated ||
		response.Header().Get("Content-Type") != "application/json" ||
		response.Body.String() != "{\"id\":\"item\"}\n" {
		t.Fatalf("JSON response = status %d, headers %v, body %q",
			response.Code, response.Header(), response.Body.String())
	}

	response = httptest.NewRecorder()
	WriteError(response, apperr.Invalid(errors.New("invalid input")))
	var errorBody apperr.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &errorBody); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Code != http.StatusBadRequest ||
		response.Header().Get("Content-Type") != "application/json" ||
		errorBody.Error != "invalid input" ||
		errorBody.Code != "invalid_input" ||
		errorBody.Retryable {
		t.Fatalf("error response = status %d, body %q", response.Code, response.Body.String())
	}
}

func TestWriteJSONEnrichesExistingErrorEnvelope(t *testing.T) {
	response := httptest.NewRecorder()
	body := map[string]any{
		"type":  "error",
		"error": map[string]string{"type": "api_error", "message": "upstream failed"},
	}
	if err := WriteJSON(response, http.StatusBadGateway, body); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	var nested map[string]string
	if err := json.Unmarshal(decoded["error"], &nested); err != nil {
		t.Fatalf("decode provider error: %v", err)
	}
	var code string
	if err := json.Unmarshal(decoded["code"], &code); err != nil {
		t.Fatalf("decode error code: %v", err)
	}
	var retryable bool
	if err := json.Unmarshal(decoded["retryable"], &retryable); err != nil {
		t.Fatalf("decode retryability: %v", err)
	}
	if nested["message"] != "upstream failed" || code != "upstream_error" || !retryable {
		t.Fatalf("enriched error response = %s", response.Body.String())
	}
}

func TestWriteJSONErrorUsesTypedContextAndRetryability(t *testing.T) {
	response := httptest.NewRecorder()
	response.Header().Set(RequestIDHeader, "request-456")
	err := apperr.WithContext(
		apperr.WithRetryable(apperr.UnavailableError(errors.New("peer is busy")), false),
		apperr.ErrorContext{NodeID: "node-8", Operation: "POST /api/tasks"},
	)
	if writeErr := WriteJSONError(response, err, map[string]string{"error": err.Error()}); writeErr != nil {
		t.Fatal(writeErr)
	}
	var body apperr.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode typed error response: %v", err)
	}
	if response.Code != http.StatusServiceUnavailable ||
		body.Error != "peer is busy" ||
		body.Code != "unavailable" ||
		body.Retryable ||
		body.RequestID != "request-456" ||
		body.NodeID != "node-8" ||
		body.Operation != "POST /api/tasks" {
		t.Fatalf("typed error response = status %d, body %+v", response.Code, body)
	}
}

func TestFormatErrorsAddsRequestAndRouteContext(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/nodes/{id}/task", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "node unavailable", http.StatusServiceUnavailable)
	})
	handler := Chain(
		mux,
		RequestID,
		WithErrorContext(apperr.ErrorContext{NodeID: "fallback-node"}),
		FormatErrors,
	)
	request := httptest.NewRequest(http.MethodPost, "/api/nodes/node-7/task", nil)
	request.Header.Set(RequestIDHeader, "request-123")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	var body apperr.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode formatted response: %v", err)
	}
	if response.Code != http.StatusServiceUnavailable ||
		response.Header().Get("Content-Type") != "application/json" ||
		body.Error != "node unavailable" ||
		body.Code != "unavailable" ||
		!body.Retryable ||
		body.RequestID != "request-123" ||
		body.NodeID != "node-7" ||
		body.Operation != "POST /api/nodes/{id}/task" {
		t.Fatalf("formatted error response = status %d, body %+v", response.Code, body)
	}
}

func TestFormatErrorsAddsDiscoveryNodeContext(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/discovery/{id}/pairing/invite", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "fingerprint required", http.StatusBadRequest)
	})
	response := httptest.NewRecorder()

	FormatErrors(mux).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/api/discovery/node-7/pairing/invite", nil),
	)

	var body apperr.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode formatted response: %v", err)
	}
	if body.NodeID != "node-7" || body.Operation != "POST /api/discovery/{id}/pairing/invite" {
		t.Fatalf("formatted discovery error context = %+v", body)
	}
}

func TestFormatErrorsUsesStaticNodeContextWithoutNodeRoute(t *testing.T) {
	handler := Chain(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "node failure", http.StatusInternalServerError)
		}),
		RequestID,
		WithErrorContext(apperr.ErrorContext{NodeID: "node-local"}),
		FormatErrors,
	)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/status", nil))

	var body apperr.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode formatted response: %v", err)
	}
	if body.NodeID != "node-local" || body.Operation != http.MethodGet {
		t.Fatalf("formatted error context = %+v", body)
	}
}

func TestFormatErrorsPreservesProviderJSONErrorBodies(t *testing.T) {
	const errorBody = `{"type":"error","error":{"message":"upstream failed","type":"api_error"}}`
	handler := FormatErrors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, errorBody)
	}))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/provider", nil))

	if response.Code != http.StatusBadGateway ||
		response.Header().Get("Content-Type") != "application/json" ||
		response.Body.String() != errorBody {
		t.Fatalf("provider JSON error response = status %d, headers %v, body %q",
			response.Code, response.Header(), response.Body.String())
	}
}

func TestFormatErrorsPreservesStreamingErrorBodies(t *testing.T) {
	const streamBody = "event: error\ndata: {\"error\":\"upstream failed\"}\n\n"
	handler := FormatErrors(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, streamBody)
	}))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/stream", nil))

	if response.Code != http.StatusBadGateway ||
		response.Header().Get("Content-Type") != "text/event-stream" ||
		response.Body.String() != streamBody {
		t.Fatalf("streaming error response = status %d, headers %v, body %q",
			response.Code, response.Header(), response.Body.String())
	}
}

func TestMiddlewareChainAddsRequestIDLimitsBodyAndLogsNoRequestContent(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	bodyLimit, err := LimitBodyBytes(64)
	if err != nil {
		t.Fatal(err)
	}
	var requestID string
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID = RequestIDFromContext(r.Context())
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if string(body) != "private prompt" {
			t.Errorf("request body = %q", body)
		}
		if err := WriteJSON(w, http.StatusCreated, map[string]string{"ok": "true"}); err != nil {
			t.Errorf("write response: %v", err)
		}
	}), RequestLogger(logger), RequestID, bodyLimit)

	request := httptest.NewRequest(http.MethodPost, "/secret-path?access_token=not-for-logs", strings.NewReader("private prompt"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	id := response.Header().Get("X-Request-ID")
	if len(id) != 32 || requestID != id {
		t.Fatalf("request IDs: response=%q context=%q", id, requestID)
	}
	if response.Code != http.StatusCreated {
		t.Fatalf("response status = %d", response.Code)
	}
	for _, secret := range []string{"private prompt", "secret-path", "access_token", "not-for-logs"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("request log leaked %q: %s", secret, logs.String())
		}
	}
	for _, field := range []string{"request_id=" + id, "method=POST", "status=201"} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("request log omitted %q: %s", field, logs.String())
		}
	}
	if !strings.Contains(logs.String(), "component=http") {
		t.Fatalf("request log omitted component field: %s", logs.String())
	}
}

func TestRequestLoggerKeepsAccessRecordsOutOfConsole(t *testing.T) {
	var console, file bytes.Buffer
	logger := logging.NewSlogLogger(&console, &file, slog.LevelInfo)
	handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	if strings.Contains(console.String(), "http request") {
		t.Fatalf("console contains HTTP access log: %s", console.String())
	}
	if !strings.Contains(file.String(), `"msg":"http request"`) ||
		!strings.Contains(file.String(), `"file_only":true`) {
		t.Fatalf("file omitted marked HTTP access log: %s", file.String())
	}
}

func TestRequestIDUsesValidClientAndLegacyHeaders(t *testing.T) {
	for _, test := range []struct {
		name   string
		header string
		value  string
	}{
		{name: "standard", header: RequestIDHeader, value: "client.trace-123"},
		{name: "legacy", header: legacyRequestIDHeader, value: "legacy:trace-123"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var contextID, requestHeaderID string
			handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				contextID = RequestIDFromContext(r.Context())
				requestHeaderID = r.Header.Get(RequestIDHeader)
				w.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set(test.header, test.value)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if contextID != test.value || requestHeaderID != test.value ||
				response.Header().Get(RequestIDHeader) != test.value {
				t.Fatalf("request IDs: context=%q request=%q response=%q",
					contextID, requestHeaderID, response.Header().Get(RequestIDHeader))
			}
		})
	}
}

func TestRequestIDReplacesInvalidClientHeader(t *testing.T) {
	var contextID string
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contextID = RequestIDFromContext(r.Context())
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(RequestIDHeader, "unsafe\r\nInjected: value")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if len(contextID) != 32 || strings.ContainsAny(contextID, "\r\n") ||
		response.Header().Get(RequestIDHeader) != contextID {
		t.Fatalf("invalid request ID was not replaced safely: context=%q response=%q",
			contextID, response.Header().Get(RequestIDHeader))
	}
}

func TestLimitBodyBytesRejectsInvalidLimit(t *testing.T) {
	if _, err := LimitBodyBytes(0); err == nil {
		t.Fatal("LimitBodyBytes accepted a zero limit")
	}
}

func TestLimitBodyBytesBoundsMiddlewareRequests(t *testing.T) {
	bodyLimit, err := LimitBodyBytes(4)
	if err != nil {
		t.Fatal(err)
	}
	var readErr error
	handler := Chain(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
	}), bodyLimit)

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
		http.MethodPost, "/", strings.NewReader("larger"),
	))
	var maxBytesErr *http.MaxBytesError
	if !errors.As(readErr, &maxBytesErr) {
		t.Fatalf("request body error = %v, want *http.MaxBytesError", readErr)
	}
}
