package scheduler

import (
	"encoding/json"
	"testing"
)

func TestTaskGraph_CreationAndCycleDetection(t *testing.T) {
	g := NewTaskGraph()
	if err := g.AddNode(Subtask{ID: "task1", Description: "First task", TaskType: "general"}); err != nil {
		t.Fatalf("AddNode task1 failed: %v", err)
	}
	if err := g.AddNode(Subtask{ID: "task2", Description: "Second task", TaskType: "general"}); err != nil {
		t.Fatalf("AddNode task2 failed: %v", err)
	}
	if err := g.AddNode(Subtask{ID: "task3", Description: "Third task", TaskType: "general"}); err != nil {
		t.Fatalf("AddNode task3 failed: %v", err)
	}

	if err := g.AddEdge("task1", "task2"); err != nil {
		t.Fatalf("AddEdge task1 -> task2 failed: %v", err)
	}
	if err := g.AddEdge("task2", "task3"); err != nil {
		t.Fatalf("AddEdge task2 -> task3 failed: %v", err)
	}

	// Validation should pass
	if err := g.Validate(); err != nil {
		t.Fatalf("Validate failed on valid DAG: %v", err)
	}

	sorted, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort failed: %v", err)
	}
	if len(sorted) != 3 || sorted[0].ID != "task1" || sorted[1].ID != "task2" || sorted[2].ID != "task3" {
		t.Fatalf("Unexpected sort order: %+v", sorted)
	}

	// Adding cycle task3 -> task1 should fail validation
	if err := g.AddEdge("task3", "task1"); err != nil {
		t.Fatalf("AddEdge failed: %v", err)
	}
	if err := g.Validate(); err == nil {
		t.Fatal("Expected error on graph with cycle, got nil")
	}
}

func TestTaskGraph_SelfDependencyAndMissingPrerequisite(t *testing.T) {
	g := NewTaskGraph()
	_ = g.AddNode(Subtask{ID: "a", Description: "A"})
	if err := g.AddEdge("a", "a"); err == nil {
		t.Fatal("Expected error on self dependency, got nil")
	}

	g2 := NewTaskGraph()
	_ = g2.AddNode(Subtask{ID: "a", Description: "A"})
	// Directly adding edge to unknown node
	g2.Edges = append(g2.Edges, Edge{From: "unknown", To: "a"})
	if err := g2.Validate(); err == nil {
		t.Fatal("Expected error on unknown edge prerequisite, got nil")
	}
}

func TestTaskGraph_ChainAndParallelGroup(t *testing.T) {
	g := NewTaskGraph()
	c1, err := g.AddChain(
		Subtask{ID: "step1", Description: "Step 1", TaskType: "general"},
		Subtask{ID: "step2", Description: "Step 2", TaskType: "general"},
	)
	if err != nil {
		t.Fatalf("AddChain failed: %v", err)
	}
	if len(c1) != 2 {
		t.Fatalf("Expected 2 subtasks in chain, got %d", len(c1))
	}

	p1, err := g.AddParallelGroup(
		Subtask{ID: "pA", Description: "Parallel A", TaskType: "code"},
		Subtask{ID: "pB", Description: "Parallel B", TaskType: "math"},
	)
	if err != nil {
		t.Fatalf("AddParallelGroup failed: %v", err)
	}
	if len(p1) != 2 {
		t.Fatalf("Expected 2 subtasks in parallel group, got %d", len(p1))
	}

	if err := g.Validate(); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// In parallel group, InDegree should be 0
	if g.InDegree("pA") != 0 || g.InDegree("pB") != 0 {
		t.Errorf("Parallel nodes should have in-degree 0")
	}
	// step2 should depend on step1
	if g.InDegree("step2") != 1 || g.Dependencies("step2")[0] != "step1" {
		t.Errorf("step2 should have dependency on step1")
	}
}

func TestTaskGraph_BackwardCompatibleJSON(t *testing.T) {
	// 1. Unmarshal from flat subtasks JSON
	flatJSON := `{"subtasks": [
		{"id": "fetch", "description": "Fetch data", "task_type": "general"},
		{"id": "process", "description": "Process data", "task_type": "code", "depends_on": ["fetch"]}
	]}`
	var g TaskGraph
	if err := json.Unmarshal([]byte(flatJSON), &g); err != nil {
		t.Fatalf("Unmarshal flatJSON failed: %v", err)
	}
	if len(g.Nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(g.Nodes))
	}
	if g.InDegree("process") != 1 {
		t.Fatalf("Expected process in-degree 1, got %d", g.InDegree("process"))
	}

	subtasks, err := g.ToSubtasks()
	if err != nil {
		t.Fatalf("ToSubtasks failed: %v", err)
	}
	if len(subtasks) != 2 || subtasks[0].ID != "fetch" || subtasks[1].ID != "process" {
		t.Fatalf("Unexpected subtasks output: %+v", subtasks)
	}

	// 2. Unmarshal from graph nodes/edges JSON
	graphJSON := `{"nodes": [
		{"id": "n1", "description": "Node 1", "task_type": "general"},
		{"id": "n2", "description": "Node 2", "task_type": "general"}
	], "edges": [{"from": "n1", "to": "n2"}]}`
	var g2 TaskGraph
	if err := json.Unmarshal([]byte(graphJSON), &g2); err != nil {
		t.Fatalf("Unmarshal graphJSON failed: %v", err)
	}
	if len(g2.Nodes) != 2 || g2.InDegree("n2") != 1 {
		t.Fatalf("Failed to parse nodes/edges format")
	}
}

func TestTaskGraph_ReadyNodesFrontier(t *testing.T) {
	g := NewTaskGraph()
	_ = g.AddNode(Subtask{ID: "n1", Description: "1"})
	_ = g.AddNode(Subtask{ID: "n2", Description: "2"})
	_ = g.AddNode(Subtask{ID: "n3", Description: "3"})
	_ = g.AddEdge("n1", "n2")
	_ = g.AddEdge("n2", "n3")

	// Initially, only n1 is ready
	completed := map[string]bool{}
	ready := g.ReadyNodes(completed)
	if len(ready) != 1 || ready[0].ID != "n1" {
		t.Fatalf("Expected n1 ready, got %+v", ready)
	}

	// When n1 is done, n2 is ready
	completed["n1"] = true
	ready = g.ReadyNodes(completed)
	if len(ready) != 1 || ready[0].ID != "n2" {
		t.Fatalf("Expected n2 ready, got %+v", ready)
	}

	// When n2 is done, n3 is ready
	completed["n2"] = true
	ready = g.ReadyNodes(completed)
	if len(ready) != 1 || ready[0].ID != "n3" {
		t.Fatalf("Expected n3 ready, got %+v", ready)
	}

	// When n3 is done, frontier is empty
	completed["n3"] = true
	ready = g.ReadyNodes(completed)
	if len(ready) != 0 {
		t.Fatalf("Expected 0 ready, got %+v", ready)
	}
}
