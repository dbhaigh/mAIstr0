package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
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

func TestTaskDependenciesReturnsNodesAndEdges(t *testing.T) {
	srv := newTestServer(t)
	subtasks := []scheduler.Subtask{
		{ID: "prepare", Description: "Prepare inputs"},
		{ID: "execute", Description: "Execute work", DependsOn: []string{"prepare"}},
		{ID: "review", Description: "Review output", DependsOn: []string{"execute"}},
	}
	graph, err := scheduler.TaskGraphFromSubtasks(subtasks)
	if err != nil {
		t.Fatal(err)
	}
	assignments := make([]scheduler.Assignment, 0, len(subtasks))
	for _, subtask := range subtasks {
		assignments = append(assignments, scheduler.Assignment{Subtask: subtask})
	}
	task, err := srv.tasks.CreateWithGraphAndOptionsChecked("workflow", graph, assignments, false)
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/tasks/"+task.ID+"/dependencies", nil)
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET task dependencies returned %d: %s", response.Code, response.Body.String())
	}

	var got taskDependenciesResponse
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 3 || len(got.Edges) != 2 {
		t.Fatalf("dependency graph has %d nodes and %d edges, want 3 and 2", len(got.Nodes), len(got.Edges))
	}
	wantNodes := []taskDependencyNode{
		{ID: "execute", Status: taskmgr.StatusPending, Description: "Execute work"},
		{ID: "prepare", Status: taskmgr.StatusPending, Description: "Prepare inputs"},
		{ID: "review", Status: taskmgr.StatusPending, Description: "Review output"},
	}
	for i, want := range wantNodes {
		if got.Nodes[i] != want {
			t.Errorf("node %d = %+v, want %+v", i, got.Nodes[i], want)
		}
	}
	wantEdges := []taskDependencyEdge{
		{SourceID: "execute", DestID: "review"},
		{SourceID: "prepare", DestID: "execute"},
	}
	for i, want := range wantEdges {
		if got.Edges[i] != want {
			t.Errorf("edge %d = %+v, want %+v", i, got.Edges[i], want)
		}
	}

	missing := httptest.NewRecorder()
	srv.Mux().ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/api/tasks/missing/dependencies", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing task returned %d, want %d: %s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
}
