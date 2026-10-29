package nodeagent

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
)

func TestDrainingRejectsNewWorkRoutes(t *testing.T) {
	agent := &Agent{agentState: &agentState{cfg: config.NodeConfig{Draining: true}}}
	handler := agent.rejectDrainingWork(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, path := range []string{"/submit", "/api/tasks", "/api/tasks/task-1/resume", "/api/tasks/task-1/approve", "/peer/message"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("draining POST %s status = %d, want %d", path, response.Code, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestDrainingAllowsReadsAndExistingTaskControls(t *testing.T) {
	agent := &Agent{agentState: &agentState{cfg: config.NodeConfig{Draining: true}}}
	handler := agent.rejectDrainingWork(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/tasks", nil),
		httptest.NewRequest(http.MethodPost, "/api/tasks/task-1/cancel", nil),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("draining %s %s status = %d, want %d", request.Method, request.URL.Path, response.Code, http.StatusNoContent)
		}
	}
}
