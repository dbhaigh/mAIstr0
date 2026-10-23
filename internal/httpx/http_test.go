package httpx

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/apperr"
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
	if response.Code != http.StatusBadRequest || response.Body.String() != "invalid input\n" {
		t.Fatalf("error response = status %d, body %q", response.Code, response.Body.String())
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
