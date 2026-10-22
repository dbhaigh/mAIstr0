package scheduler

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestFilterSubtaskCandidatesDropsIneligibleNodesAndKeepsFullNodes(t *testing.T) {
	subtask := Subtask{TaskType: "code", RequiredCapabilities: []string{"coder"}, MinModelSizeGB: 7}
	nodes := []cluster.NodeStatus{
		{
			ID: "unhealthy", Healthy: false,
			Models: []engine.Model{{Name: "code", Engine: "ready", Tags: []string{"code"}, SizeGB: 8}},
		},
		{
			ID: "engine-not-ready", Healthy: true,
			Engines: []engine.Status{{Name: "vllm", Ready: false}},
			Models:  []engine.Model{{Name: "code", Engine: "vllm", Tags: []string{"code"}, SizeGB: 8}},
		},
		{
			ID: "wrong-tags", Healthy: true,
			Models: []engine.Model{{Name: "general", Tags: []string{"general"}, SizeGB: 8}},
		},
		{
			ID: "too-small", Healthy: true,
			Models: []engine.Model{{Name: "code", Tags: []string{"code"}, SizeGB: 4}},
		},
		{
			ID: "full", Healthy: true, MaxConcurrentTasks: 1, ActiveTasks: 1, AvailableCapacity: 0,
			Models: []engine.Model{{Name: "code", Tags: []string{"code"}, SizeGB: 8}},
		},
		{
			ID: "available", Healthy: true, MaxConcurrentTasks: 2, AvailableCapacity: 2,
			Models: []engine.Model{{Name: "code", Tags: []string{"code"}, SizeGB: 8}},
		},
	}

	candidates := filterSubtaskCandidates(subtask, 0, nodes)
	got := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		got = append(got, candidate.node.ID)
	}
	if want := []string{"full", "available"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered candidates = %v, want %v", got, want)
	}
}

func TestAssignKeepsFullNodeWhenItsExistingScoreWins(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID: "full", Address: "http://full", Healthy: true, ActiveTasks: 1,
			MaxConcurrentTasks: 1, AvailableCapacity: 0,
			Hardware: hardware.Info{Score: 100},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "available", Address: "http://available", Healthy: true,
			MaxConcurrentTasks: 1, AvailableCapacity: 1,
			Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if assignments[0].NodeID != "full" || assignments[0].Factors.CapacityPenalty != 8 {
		t.Fatalf("full-node scoring behavior changed: %+v", assignments[0])
	}
}

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

func TestAssignUsesLatencyPreferenceAndMeasuredThroughput(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID: "fast", Address: "http://fast", Healthy: true,
			Models: []engine.Model{{Name: "small-model", Tags: []string{"general"}, SizeGB: 7}},
		},
		{
			ID: "quality", Address: "http://quality", Healthy: true,
			Models: []engine.Model{{
				Name: "large-model", Tags: []string{"general", "high-quality", "reasoning"}, SizeGB: 70,
			}},
		},
	}
	experience := routingHistoryFixture{"fast": 100, "quality": 10}
	subtask := Subtask{
		ID: "work", TaskType: "general", Description: strings.Repeat("d", 1000),
		ExpectedOutput: strings.Repeat("o", 1000),
	}
	qualityAssignments, err := AssignWithExperience(
		[]Subtask{subtask}, nodes, experience,
	)
	if err != nil {
		t.Fatal(err)
	}
	if qualityAssignments[0].NodeID != "quality" {
		t.Fatalf("quality preference selected %s, want quality candidate", qualityAssignments[0].NodeID)
	}

	subtask.RoutingPreference = RoutingPreferenceLatency
	latencyAssignments, err := AssignWithExperience(
		[]Subtask{subtask}, nodes, experience,
	)
	if err != nil {
		t.Fatal(err)
	}
	if latencyAssignments[0].NodeID != "fast" {
		t.Fatalf("latency preference selected %s, want faster candidate", latencyAssignments[0].NodeID)
	}
	factors := latencyAssignments[0].Factors
	if factors.EstimatedTokensPerSecond != 100 || factors.ModelSizeGB != 7 ||
		factors.EstimatedCompletionSeconds >= qualityAssignments[0].Factors.EstimatedCompletionSeconds {
		t.Fatalf("selected latency score factors = %+v; quality candidate factors = %+v",
			factors, qualityAssignments[0].Factors)
	}
}

func TestAssignKeepsCompatibleCandidateUnderHeavyLoadPenalty(t *testing.T) {
	assignments, err := Assign([]Subtask{{ID: "review", TaskType: "general"}}, []cluster.NodeStatus{{
		ID: "busy-specialist", Address: "http://node", Healthy: true, ActiveTasks: 10,
		Models: []engine.Model{{Name: "code-model", Tags: []string{"code"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || assignments[0].NodeID != "busy-specialist" {
		t.Fatalf("compatible model disappeared under load penalty: %+v", assignments)
	}
	if assignments[0].Factors.ActiveTasks != 10 || assignments[0].Factors.LoadPenalty != 80 {
		t.Fatalf("load contribution missing from score explanation: %+v", assignments[0].Factors)
	}
}

func TestAssignPrefersNodeWithAdvertisedCapacity(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID: "full", Address: "http://full", Healthy: true, ActiveTasks: 2,
			MaxConcurrentTasks: 2, AvailableCapacity: 0,
			Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "available", Address: "http://available", Healthy: true,
			MaxConcurrentTasks: 2, AvailableCapacity: 2,
			Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if assignments[0].NodeID != "available" {
		t.Fatalf("assigned to %q despite the advertised capacity on the other node", assignments[0].NodeID)
	}
}

func TestAssignPrefersLowerLiveGPUUtilization(t *testing.T) {
	telemetryAt := time.Now()
	nodes := []cluster.NodeStatus{
		{
			ID: "busy", Address: "http://busy", Healthy: true,
			Hardware:    hardware.Info{Score: 50, GPUStatsAvailable: true, GPUUtilization: 90},
			TelemetryAt: telemetryAt,
			Models:      []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "free", Address: "http://free", Healthy: true,
			Hardware:    hardware.Info{Score: 50, GPUStatsAvailable: true, GPUUtilization: 10},
			TelemetryAt: telemetryAt,
			Models:      []engine.Model{{Name: "model", Tags: []string{"general"}}},
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

func TestAssignPrefersModelsAdvertisedByGPUNode(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID: "cpu", Address: "http://cpu", Healthy: true,
			Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "gpu", Address: "http://gpu", Healthy: true,
			Hardware: hardware.Info{HasGPU: true},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}

	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || assignments[0].NodeID != "gpu" {
		t.Fatalf("assigned to %+v, want the GPU node", assignments)
	}
	if got := assignments[0].Factors.GPUCompatibilityScore; got != DefaultScoringConfig().GPUCompatibilityBonus {
		t.Fatalf("GPU compatibility score = %v, want %v", got, DefaultScoringConfig().GPUCompatibilityBonus)
	}
}

func TestAssignPrefersLMStudioAndSmallerModelOnGPU(t *testing.T) {
	nodes := []cluster.NodeStatus{{
		ID: "gpu", Address: "http://gpu", Healthy: true,
		Hardware: hardware.Info{HasGPU: true},
		Models: []engine.Model{
			{Name: "ollama-general", Engine: "ollama", Tags: []string{"general"}},
			{Name: "lmstudio-large", Engine: "lmstudio", Tags: []string{"general"}},
			{Name: "lmstudio-small", Engine: "lmstudio", Tags: []string{"general", "small"}},
		},
	}}

	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || assignments[0].Model != "lmstudio-small" {
		t.Fatalf("assigned to %+v, want the small LM Studio model on the GPU node", assignments)
	}
}

func TestAssignPrefersLowerLiveNPUUtilization(t *testing.T) {
	telemetryAt := time.Now()
	nodes := []cluster.NodeStatus{
		{
			ID: "busy-npu", Address: "http://busy", Healthy: true,
			Hardware:    hardware.Info{Score: 50, NPUStatsAvailable: true, NPUUtilization: 90},
			TelemetryAt: telemetryAt,
			Models:      []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "free-npu", Address: "http://free", Healthy: true,
			Hardware:    hardware.Info{Score: 50, NPUStatsAvailable: true, NPUUtilization: 10},
			TelemetryAt: telemetryAt,
			Models:      []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if assignments[0].NodeID != "free-npu" {
		t.Fatalf("assigned to %q, want the less-loaded NPU node", assignments[0].NodeID)
	}
}

func TestAssignPenalizesQueuedWorkAndExposesScoreFactors(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID: "queued", Address: "http://queued", Healthy: true, QueuedTasks: 3,
			Hardware: hardware.Info{Score: 50},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "idle", Address: "http://idle", Healthy: true,
			Hardware: hardware.Info{Score: 50},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if got := assignments[0].NodeID; got != "idle" {
		t.Fatalf("assigned to %q, want idle node", got)
	}
	factors := assignments[0].Factors
	if factors.Hardware != 15 || factors.LoadPenalty != 0 || factors.ActiveTasks != 0 {
		t.Fatalf("unexpected selected-node score factors: %+v", factors)
	}
}

func TestAssignIncludesQueuedWorkInLoadFactor(t *testing.T) {
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, []cluster.NodeStatus{{
		ID: "queued", Address: "http://queued", Healthy: true, QueuedTasks: 2,
		Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if factors := assignments[0].Factors; factors.QueuedTasks != 2 || factors.LoadPenalty != 16 {
		t.Fatalf("queued work missing from score explanation: %+v", factors)
	}
}

func TestAssignIgnoresStaleAcceleratorTelemetry(t *testing.T) {
	now := time.Now()
	nodes := []cluster.NodeStatus{
		{
			ID: "stale", Address: "http://stale", Healthy: true, TelemetryAt: now.Add(-time.Minute),
			Hardware: hardware.Info{Score: 50, GPUStatsAvailable: true, GPUUtilization: 100},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "fresh", Address: "http://fresh", Healthy: true, TelemetryAt: now,
			Hardware: hardware.Info{Score: 50, GPUStatsAvailable: true, GPUUtilization: 50},
			Models:   []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if got := assignments[0].NodeID; got != "stale" {
		t.Fatalf("stale load reading affected routing; assigned to %q", got)
	}
	if assignments[0].Factors.TelemetryFresh || assignments[0].Factors.GPUUtilizationPenalty != 0 ||
		assignments[0].Factors.TelemetryAgeMs < (30*time.Second).Milliseconds() {
		t.Fatalf("stale telemetry was not marked and ignored: %+v", assignments[0].Factors)
	}
}

func TestAssignExcludesFailedNodes(t *testing.T) {
	nodes := []cluster.NodeStatus{
		{
			ID: "failed", Address: "http://failed", Healthy: false,
			Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
		{
			ID: "healthy", Address: "http://healthy", Healthy: true,
			Models: []engine.Model{{Name: "model", Tags: []string{"general"}}},
		},
	}
	assignments, err := Assign([]Subtask{{ID: "work", TaskType: "general"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 || assignments[0].NodeID != "healthy" {
		t.Fatalf("failed node was assigned work: %+v", assignments)
	}
}

type routingHistoryFixture map[string]float64

func (routingHistoryFixture) LearnedBias(string, string, string) float64 {
	return 0
}

func (history routingHistoryFixture) TokensPerSecond(nodeID, _, _ string) float64 {
	return history[nodeID]
}
