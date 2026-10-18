package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestElectedOrchestratorWriteGateFencesYieldedServers(t *testing.T) {
	for _, test := range []struct {
		name         string
		elected      bool
		wantStatus   int
		wantMutation bool
	}{
		{name: "visible higher-ranked leader", elected: false, wantStatus: http.StatusServiceUnavailable},
		{name: "isolated partition winner", elected: true, wantStatus: http.StatusNoContent, wantMutation: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				mutated = true
				w.WriteHeader(http.StatusNoContent)
			})
			handler := requireElectedOrchestrator(func() bool { return test.elected }, next, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/tasks", nil))

			if response.Code != test.wantStatus {
				t.Fatalf("POST returned %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if mutated != test.wantMutation {
				t.Fatalf("mutation reached handler = %t, want %t", mutated, test.wantMutation)
			}
		})
	}
}
