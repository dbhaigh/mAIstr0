package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestSystemStatusContract(t *testing.T) {
	srv := newTestServer(t)
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/status returned %d: %s", response.Code, response.Body.String())
	}

	var status struct {
		Role        string `json:"role"`
		Persistence struct {
			Available bool   `json:"available"`
			Warning   string `json:"warning"`
		} `json:"persistence"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Role != "orchestrator" || !status.Persistence.Available || status.Persistence.Warning != "" {
		t.Fatalf("unexpected system status contract: %+v", status)
	}
}

func TestSystemStatusReportsUnavailablePersistence(t *testing.T) {
	directory := t.TempDir()
	srv, err := NewWithMemoryAndHarness(directory, "deepseek")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/status returned %d: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"available":false`) ||
		!strings.Contains(response.Body.String(), `"warning":"Persistent memory is unavailable;`) {
		t.Fatalf("system status did not report memory degradation: %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), filepath.Base(directory)) {
		t.Fatalf("system status leaked the failing storage path: %s", response.Body.String())
	}
}

func TestAgentSessionAPIContract(t *testing.T) {
	srv := newTestServer(t)
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/agent/sessions",
		strings.NewReader(`{"title":"contract test","max_steps":4}`)))
	if response.Code != http.StatusCreated {
		t.Fatalf("POST /api/agent/sessions returned %d: %s", response.Code, response.Body.String())
	}

	var session struct {
		ID        string `json:"id"`
		Title     string `json:"title"`
		Harness   string `json:"harness"`
		Messages  []any  `json:"messages"`
		CreatedAt string `json:"created_at"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	if session.ID == "" || session.Title != "contract test" || session.Harness != "deepseek" ||
		session.Messages == nil || session.CreatedAt == "" {
		t.Fatalf("unexpected agent session response contract: %+v", session)
	}

	methodMismatch := httptest.NewRecorder()
	srv.Mux().ServeHTTP(methodMismatch, httptest.NewRequest(http.MethodPut, "/api/agent/sessions", nil))
	if methodMismatch.Code < http.StatusBadRequest {
		t.Fatalf("PUT /api/agent/sessions unexpectedly succeeded with %d", methodMismatch.Code)
	}
}
