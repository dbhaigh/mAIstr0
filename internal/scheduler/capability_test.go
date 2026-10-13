package scheduler

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestScheduler_CapabilityMatchingRequiredAndPreferred(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID:      "coder-node",
			Address: "http://coder:8080",
			Healthy: true,
			Capabilities: []string{"coder", "fast"},
			Models: []engine.Model{
				{Name: "qwen2.5-coder:7b", Tags: []string{"code", "general"}, SizeGB: 4.5},
			},
		},
		{
			ID:      "research-node",
			Address: "http://research:8080",
			Healthy: true,
			Capabilities: []string{"researcher", "high-quality"},
			Models: []engine.Model{
				{Name: "deepseek-r1:7b", Tags: []string{"reasoning", "general"}, SizeGB: 7.0},
			},
		},
	}

	// Subtask requiring "coder" capability
	codeTask := Subtask{
		ID:                   "sub-1",
		Description:          "Write code",
		TaskType:             "code",
		RequiredCapabilities: []string{"coder"},
	}

	assignments, err := Assign([]Subtask{codeTask}, nodes)
	if err != nil {
		t.Fatalf("Assign failed: %v", err)
	}
	if len(assignments) != 1 || assignments[0].NodeID != "coder-node" {
		t.Fatalf("Expected coder-node to be assigned, got %+v", assignments)
	}
	if assignments[0].Factors.CapabilityScore <= 0 {
		t.Fatalf("Expected positive capability score, got %.2f", assignments[0].Factors.CapabilityScore)
	}

	// Subtask requiring "researcher" capability
	researchTask := Subtask{
		ID:                   "sub-2",
		Description:          "Deep research",
		TaskType:             "general",
		RequiredCapabilities: []string{"researcher"},
		PreferredCapabilities: []string{"high-quality"},
	}
	assignments, err = Assign([]Subtask{researchTask}, nodes)
	if err != nil {
		t.Fatalf("Assign failed: %v", err)
	}
	if len(assignments) != 1 || assignments[0].NodeID != "research-node" {
		t.Fatalf("Expected research-node to be assigned, got %+v", assignments)
	}
	if len(assignments[0].Factors.MatchedCapabilities) == 0 {
		t.Fatalf("Expected matched capabilities, got none")
	}

	// Subtask requiring non-existent capability should fail
	impossibleTask := Subtask{
		ID:                   "sub-3",
		Description:          "Impossible",
		TaskType:             "general",
		RequiredCapabilities: []string{"quantum-computing"},
	}
	_, err = Assign([]Subtask{impossibleTask}, nodes)
	if err == nil {
		t.Fatal("Expected error for impossible required capability, got nil")
	}
}

func TestScheduler_ModelQualityAndSizePreference(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID:      "small-node",
			Address: "http://small:8080",
			Healthy: true,
			Models: []engine.Model{
				{Name: "llama3.2:1b", Tags: []string{"general", "fast"}, SizeGB: 1.3},
			},
		},
		{
			ID:      "large-node",
			Address: "http://large:8080",
			Healthy: true,
			Models: []engine.Model{
				{Name: "llama3.3:70b", Tags: []string{"general", "high-quality"}, SizeGB: 39.0},
			},
		},
	}

	// Subtask requiring min 10GB model
	heavyTask := Subtask{
		ID:             "heavy",
		Description:    "Complex reasoning",
		TaskType:       "general",
		MinModelSizeGB: 10.0,
	}

	assignments, err := Assign([]Subtask{heavyTask}, nodes)
	if err != nil {
		t.Fatalf("Assign failed: %v", err)
	}
	if assignments[0].NodeID != "large-node" {
		t.Fatalf("Expected large-node for min_model_size_gb, got %s", assignments[0].NodeID)
	}
	if assignments[0].Factors.ModelQualityScore <= 0 {
		t.Fatalf("Expected model quality score > 0, got %.2f", assignments[0].Factors.ModelQualityScore)
	}
}

func TestScheduler_TelemetryFreshnessWindow(t *testing.T) {
	now := time.Now()
	// Node with fresh high CPU utilization
	freshBusy := cluster.NodeStatus{
		ID:          "busy-fresh",
		Address:     "http://busy:8080",
		Healthy:     true,
		TelemetryAt: now.Add(-5 * time.Second), // 5 seconds old = fresh
		Hardware: hardware.Info{
			Score:             50.0,
			CPUStatsAvailable: true,
			CPUUtilization:    95, // 95% CPU utilization
		},
		Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
	}

	// Node with stale telemetry (35 seconds old > 30s window)
	staleNode := cluster.NodeStatus{
		ID:          "stale-node",
		Address:     "http://stale:8080",
		Healthy:     true,
		TelemetryAt: now.Add(-35 * time.Second), // 35 seconds old = stale
		Hardware: hardware.Info{
			Score:             50.0,
			CPUStatsAvailable: true,
			CPUUtilization:    95, // should be ignored!
		},
		Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
	}

	st := Subtask{ID: "task", Description: "Task", TaskType: "general"}
	assignments, err := Assign([]Subtask{st}, []cluster.NodeStatus{freshBusy, staleNode})
	if err != nil {
		t.Fatalf("Assign failed: %v", err)
	}

	// The fresh busy node has a CPU penalty; the stale node's utilization is ignored,
	// so the stale node will score higher than the fresh busy node.
	if assignments[0].NodeID != "stale-node" {
		t.Fatalf("Expected stale-node to win because fresh node has 95%% CPU penalty, got %s", assignments[0].NodeID)
	}
}

func TestScheduler_TopologyAwareLoadPenalty(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID:      "node-1",
			Address: "http://n1:8080",
			Healthy: true,
			Hardware: hardware.Info{Score: 80.0},
			Models:  []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID:      "node-2",
			Address: "http://n2:8080",
			Healthy: true,
			Hardware: hardware.Info{Score: 80.0},
			Models:  []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}

	// Create a graph where A has many dependents (critical path) and B is independent
	g := NewTaskGraph()
	_ = g.AddNode(Subtask{ID: "A", Description: "Critical path node", TaskType: "general"})
	_ = g.AddNode(Subtask{ID: "B", Description: "Side node", TaskType: "general"})
	_ = g.AddNode(Subtask{ID: "C", Description: "Dependent of A", TaskType: "general"})
	_ = g.AddNode(Subtask{ID: "D", Description: "Dependent of A", TaskType: "general"})
	_ = g.AddEdge("A", "C")
	_ = g.AddEdge("A", "D")

	cfg := DefaultScoringConfig()
	assignments, err := AssignGraph(g, nodes, &cfg, nil)
	if err != nil {
		t.Fatalf("AssignGraph failed: %v", err)
	}
	if len(assignments) != 4 {
		t.Fatalf("Expected 4 assignments, got %d", len(assignments))
	}

	// Node A should have topology bonus because OutDegree == 2
	var assignA *Assignment
	for i := range assignments {
		if assignments[i].Subtask.ID == "A" {
			assignA = &assignments[i]
		}
	}
	if assignA == nil || assignA.Factors.TopologyScore <= 0 {
		t.Fatalf("Expected positive topology score for node A with out-degree 2, got %+v", assignA)
	}
}
