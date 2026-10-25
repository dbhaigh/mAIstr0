package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
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

func TestRetireAndReinstatePromotedServer(t *testing.T) {
	registry := cluster.NewRegistry()
	server := &Server{election: newElectionComponent(registry, nil, "promoted", 148, false)}
	for _, step := range []struct {
		name        string
		action      func()
		wantElected bool
		wantRole    string
	}{
		{name: "promoted", action: func() {}, wantElected: true, wantRole: "orchestrator"},
		{name: "retired", action: server.Retire, wantElected: false, wantRole: "candidate"},
		{name: "reinstated", action: server.Reinstate, wantElected: true, wantRole: "orchestrator"},
	} {
		t.Run(step.name, func(t *testing.T) {
			step.action()
			if got := server.Elected(); got != step.wantElected {
				t.Fatalf("Elected() = %t, want %t", got, step.wantElected)
			}
			ann := server.LeadershipAnnouncement()
			if ann.Role != step.wantRole || ann.ID != "promoted" || ann.Score != 148 {
				t.Fatalf("LeadershipAnnouncement() = %+v, want role %q", ann, step.wantRole)
			}
		})
	}
}
