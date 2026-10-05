package scheduler

import (
	"errors"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
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
