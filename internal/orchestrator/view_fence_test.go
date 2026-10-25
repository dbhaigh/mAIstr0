package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStateChangingRequestsRejectStaleOrIncompleteViews(t *testing.T) {
	srv := newTestServer(t)
	handler := srv.fenceStateChangingRequests(srv.Mux())
	tests := []struct {
		name  string
		id    string
		epoch string
	}{
		{name: "different orchestrator", id: "orchestrator-old", epoch: srv.orchestratorEpoch},
		{name: "different epoch", id: srv.SelfID(), epoch: "stale-epoch"},
		{name: "incomplete view", id: srv.SelfID()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/agent/sessions",
				strings.NewReader(`{"title":"must not be created","max_steps":1}`))
			request.Header.Set(HeaderOrchestratorID, test.id)
			if test.epoch != "" {
				request.Header.Set(HeaderOrchestratorEpoch, test.epoch)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusConflict {
				t.Fatalf("POST with stale view returned %d, want 409: %s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), "orchestrator view is stale or incomplete") {
				t.Fatalf("conflict response did not identify the stale view: %s", response.Body.String())
			}
			if response.Header().Get(HeaderOrchestratorID) != srv.SelfID() ||
				response.Header().Get(HeaderOrchestratorEpoch) != srv.orchestratorEpoch {
				t.Fatalf("conflict response omitted current view headers: %#v", response.Header())
			}
		})
	}
}

func TestStateChangingRequestsWithoutViewRemainCompatible(t *testing.T) {
	srv := newTestServer(t)
	handler := srv.fenceStateChangingRequests(srv.Mux())
	request := httptest.NewRequest(http.MethodPost, "/api/agent/sessions",
		strings.NewReader(`{"title":"legacy client","max_steps":1}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("legacy POST returned %d, want 201: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/agent/sessions",
		strings.NewReader(`{"title":"current client","max_steps":1}`))
	request.Header.Set(HeaderOrchestratorID, srv.SelfID())
	request.Header.Set(HeaderOrchestratorEpoch, srv.orchestratorEpoch)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("POST with current view returned %d, want 201: %s", response.Code, response.Body.String())
	}
}

func TestSystemStatusExposesOrchestratorView(t *testing.T) {
	srv := newTestServer(t)
	handler := srv.fenceStateChangingRequests(srv.Mux())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/status", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/status returned %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get(HeaderOrchestratorID) != srv.SelfID() ||
		response.Header().Get(HeaderOrchestratorEpoch) != srv.orchestratorEpoch {
		t.Fatalf("status response omitted current view headers: %#v", response.Header())
	}
	if strings.Contains(response.Body.String(), `"orchestrator_id"`) ||
		strings.Contains(response.Body.String(), `"orchestrator_epoch"`) {
		t.Fatalf("status body changed from its existing wire shape: %s", response.Body.String())
	}
}
