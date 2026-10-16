package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestTaskAssignmentExplanationUsesStoredAssignment(t *testing.T) {
	srv := newTestServer(t)
	wantFactors := scheduler.ScoreFactors{
		ModelMatch: 2.5, Hardware: 4, LearnedBias: 1.25, TelemetryFresh: true,
		MatchedCapabilities: []string{"fast"},
	}
	task, err := srv.tasks.CreateChecked("explain assignment", []scheduler.Assignment{{
		Subtask: scheduler.Subtask{ID: "work", Description: "Do the work"},
		NodeID:  "worker-1", Model: "model-1", Engine: "engine-1",
		Score: 42.5, Factors: wantFactors,
	}})
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/tasks/"+task.ID+"/assignment-explanation", nil)
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET assignment explanation returned %d: %s", response.Code, response.Body.String())
	}

	var got assignmentExplanationResponse
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.TaskID != task.ID || len(got.Assignments) != 1 {
		t.Fatalf("response identity/assignment count = (%q, %d), want (%q, 1)", got.TaskID, len(got.Assignments), task.ID)
	}
	detail := got.Assignments[0]
	if detail.SubtaskID != "work" || detail.NodeID != "worker-1" || detail.Model != "model-1" ||
		detail.Engine != "engine-1" || detail.Score != 42.5 || detail.LearnedBias != wantFactors.LearnedBias {
		t.Fatalf("assignment detail = %+v, want the persisted assignment", detail)
	}
	if detail.ScoreFactors.ModelMatch != wantFactors.ModelMatch ||
		detail.ScoreFactors.Hardware != wantFactors.Hardware ||
		detail.ScoreFactors.LearnedBias != wantFactors.LearnedBias ||
		detail.ScoreFactors.TelemetryFresh != wantFactors.TelemetryFresh ||
		len(detail.ScoreFactors.MatchedCapabilities) != 1 ||
		detail.ScoreFactors.MatchedCapabilities[0] != "fast" {
		t.Fatalf("score factors = %+v, want stored factors %+v", detail.ScoreFactors, wantFactors)
	}
	if !strings.Contains(detail.Summary, "worker-1") || !strings.Contains(detail.Summary, "learned bias +1.2") ||
		!strings.Contains(detail.Summary, "telemetry was fresh") {
		t.Fatalf("summary = %q, does not explain the stored assignment", detail.Summary)
	}

	missing := httptest.NewRecorder()
	srv.Mux().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/api/tasks/missing/assignment-explanation", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing task returned %d, want %d: %s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
}
