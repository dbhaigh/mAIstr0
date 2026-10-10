package scheduler

import (
	"errors"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestAssignRejectsUnassignableSubtasks(t *testing.T) {
	subtasks := []Subtask{
		{ID: "sub-code", TaskType: "code"},
		{ID: "sub-vision", TaskType: "vision"},
	}
	nodes := []cluster.NodeStatus{{
		ID: "code-node", Address: "http://node", Healthy: true,
		Models: []engine.Model{{Name: "coder", Tags: []string{"code"}}},
	}}

	assignments, err := Assign(subtasks, nodes)
	var unassignable *UnassignableSubtasksError
	if !errors.As(err, &unassignable) {
		t.Fatalf("expected UnassignableSubtasksError, got assignments=%v err=%v", assignments, err)
	}
	if len(assignments) != 0 {
		t.Fatalf("partial assignments should not be returned: %+v", assignments)
	}
	if len(unassignable.SubtaskIDs) != 1 || unassignable.SubtaskIDs[0] != "sub-vision" {
		t.Fatalf("unexpected unassignable subtasks: %v", unassignable.SubtaskIDs)
	}
}

func TestAssignAllowsGeneralCapableModelForSpecificTask(t *testing.T) {
	assignments, err := Assign([]Subtask{{ID: "sub-code", TaskType: "code"}}, []cluster.NodeStatus{{
		ID: "general-node", Address: "http://node", Healthy: true,
		Models: []engine.Model{{Name: "general-model", Tags: []string{"general"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || assignments[0].Model != "general-model" {
		t.Fatalf("unexpected assignments: %+v", assignments)
	}
}

func TestAssignPrefersLowerLiveGPUUtilization(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID: "busy", Address: "http://busy", Healthy: true,
			Hardware: hardware.Info{Score: 50, GPUStatsAvailable: true, GPUUtilization: 90},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "free", Address: "http://free", Healthy: true,
			Hardware: hardware.Info{Score: 50, GPUStatsAvailable: true, GPUUtilization: 10},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if assignments[0].NodeID != "free" {
		t.Fatalf("assigned to %q, want the less-loaded GPU node", assignments[0].NodeID)
	}
}
