package handler_utils

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/apperr"
)

type decodePayload struct {
	Value string `json:"value"`
}

func TestResponseBuildersPreserveStatusAndBody(t *testing.T) {
	response := httptest.NewRecorder()
	if err := WriteJSON(response, http.StatusCreated, decodePayload{Value: "created"}); err != nil {
		t.Fatalf("write JSON: %v", err)
	}
	if response.Code != http.StatusCreated {
		t.Fatalf("JSON status = %d, want %d", response.Code, http.StatusCreated)
	}
	var payload decodePayload
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode JSON response: %v", err)
	}
	if payload.Value != "created" {
		t.Fatalf("JSON payload = %+v, want created value", payload)
	}

	response = httptest.NewRecorder()
	err := apperr.NotFoundError(errors.New("missing"))
	if writeErr := WriteJSONError(response, err, map[string]string{"error": err.Error()}); writeErr != nil {
		t.Fatalf("write JSON error: %v", writeErr)
	}
	if response.Code != http.StatusNotFound {
		t.Fatalf("JSON error status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if !strings.Contains(response.Body.String(), `"error":"missing"`) ||
		!strings.Contains(response.Body.String(), `"code":"not_found"`) ||
		!strings.Contains(response.Body.String(), `"retryable":false`) {
		t.Fatalf("JSON error body = %q, want missing error", response.Body.String())
	}
}

func TestDecodeJSONUsesDefaultAndRouteSpecificResponders(t *testing.T) {
	t.Run("default invalid input", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{"))
		if _, ok := DecodeJSON[decodePayload](response, request, 1024); ok {
			t.Fatal("invalid JSON was accepted")
		}
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid JSON status = %d, want %d", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("custom JSON error policy", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{"))
		responderCalled := false
		_, ok := DecodeJSONWithResponder[decodePayload](
			response,
			request,
			1024,
			func(w http.ResponseWriter, _ error) {
				responderCalled = true
				customErr := apperr.ConflictError(errors.New("route-specific rejection"))
				if err := WriteJSONError(w, customErr, map[string]string{"error": customErr.Error()}); err != nil {
					t.Errorf("write custom decode error: %v", err)
				}
			},
		)
		if ok {
			t.Fatal("invalid JSON was accepted")
		}
		if !responderCalled {
			t.Fatal("route-specific error responder was not called")
		}
		if response.Code != http.StatusConflict ||
			!strings.Contains(response.Body.String(), `"error":"route-specific rejection"`) {
			t.Fatalf("custom decode response = %d %q", response.Code, response.Body.String())
		}
	})

	t.Run("valid input", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"value":"ok"}`))
		value, ok := DecodeJSON[decodePayload](response, request, 1024)
		if !ok || value.Value != "ok" {
			t.Fatalf("decoded value = %+v, ok = %t", value, ok)
		}
	})
}

func TestRequirePathValuePreservesRouteValidation(t *testing.T) {
	t.Run("nonempty value is not normalized", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.SetPathValue("id", "  raw  ")
		value, ok := RequirePathValue(
			httptest.NewRecorder(), request, "id", "id is required", RespondInvalid,
		)
		if !ok || value != "  raw  " {
			t.Fatalf("path value = %q, ok = %t; want untrimmed value", value, ok)
		}
	})

	t.Run("missing value uses caller error policy", func(t *testing.T) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		value, ok := RequirePathValue(response, request, "id", "node id is required", RespondInvalid)
		if ok || value != "" {
			t.Fatalf("missing path value = %q, ok = %t; want empty and invalid", value, ok)
		}
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "node id is required") {
			t.Fatalf("missing path response = %d %q", response.Code, response.Body.String())
		}
	})
}
