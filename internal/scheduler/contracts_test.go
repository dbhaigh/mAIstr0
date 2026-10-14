package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

var updateSchedulerContracts = flag.Bool("update", false, "update scheduler contract golden files")

func TestAssignmentContractForFixedNodeAndModel(t *testing.T) {
	subtasks := []Subtask{
		{ID: "write", Description: "Write the summary", Role: "writer", TaskType: "general", DependsOn: []string{"research"}},
		{ID: "research", Description: "Research the report", Role: "researcher", TaskType: "general"},
	}
	nodes := []cluster.NodeStatus{{
		ID: "worker-a", Address: "https://worker-a", Healthy: true, DefaultModel: "general-model",
		Hardware: hardware.Info{Score: 50},
		Models:   []engine.Model{{Name: "general-model", Engine: "simulated", Tags: []string{"general"}}},
	}}
	assignments, err := AssignWithExperience(subtasks, nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := AssignWithExperience(subtasks, nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(assignments, again) {
		t.Fatalf("repeated assignment changed: first=%+v second=%+v", assignments, again)
	}
	if assignments[0].Subtask.ID != "research" || assignments[1].Subtask.ID != "write" {
		t.Fatalf("assignment order did not follow dependencies: %+v", assignments)
	}
	assertSchedulerGolden(t, "assignment.json", assignments)
}

func TestAssignmentErrorsAndTelemetryContract(t *testing.T) {
	_, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nil)
	if !errors.Is(err, ErrNoHealthyNodes) {
		t.Fatalf("no healthy nodes error = %v, want ErrNoHealthyNodes", err)
	}

	nodes := []cluster.NodeStatus{{
		ID: "code-only", Address: "https://code-only", Healthy: true,
		Models: []engine.Model{{Name: "code-model", Tags: []string{"code"}}},
	}}
	_, err = Assign([]Subtask{
		{ID: "one", TaskType: "vision"},
		{ID: "two", TaskType: "math"},
	}, nodes)
	var unassignable *UnassignableSubtasksError
	if !errors.As(err, &unassignable) {
		t.Fatalf("unassignable error = %v, want *UnassignableSubtasksError", err)
	}
	assertSchedulerGolden(t, "unassignable-subtasks.json", unassignable.SubtaskIDs)

	nodes = []cluster.NodeStatus{{
		ID: "stale", Address: "https://stale", Healthy: true,
		TelemetryAt: time.Now().Add(-31 * time.Second),
		Hardware:    hardware.Info{Score: 50, GPUStatsAvailable: true, GPUUtilization: 100},
		Models:      []engine.Model{{Name: "general-model", Tags: []string{"general"}}},
	}}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if got := assignments[0].Factors.GPUUtilizationPenalty; got != 0 || assignments[0].Factors.TelemetryFresh {
		t.Fatalf("stale telemetry affected assignment: %+v", assignments[0].Factors)
	}
}

func TestGraphTopologicalOrderContract(t *testing.T) {
	graph := NewTaskGraph()
	for _, subtask := range []Subtask{
		{ID: "review", Description: "Review", Role: "reviewer", TaskType: "general"},
		{ID: "write", Description: "Write", Role: "writer", TaskType: "general", DependsOn: []string{"research"}},
		{ID: "research", Description: "Research", Role: "researcher", TaskType: "general"},
	} {
		if err := graph.AddNode(subtask); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.AddEdge("research", "write"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddEdge("write", "review"); err != nil {
		t.Fatal(err)
	}
	if err := graph.Validate(); err != nil {
		t.Fatal(err)
	}
	sorted, err := graph.TopologicalSort()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(sorted))
	for i, subtask := range sorted {
		ids[i] = subtask.ID
	}
	assertSchedulerGolden(t, "graph-topological-order.json", ids)
}

func assertSchedulerGolden(t *testing.T, name string, value any) {
	t.Helper()
	actual, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	actual = append(actual, '\n')
	path := filepath.Join("testdata", "contracts", name)
	if *updateSchedulerContracts {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read scheduler contract %s (use -update to create it): %v", path, err)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("scheduler contract %s changed\n--- got ---\n%s\n--- want ---\n%s", name, actual, expected)
	}
}
