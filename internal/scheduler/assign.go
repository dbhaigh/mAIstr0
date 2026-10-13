package scheduler

import (
	"errors"
	"log"
	"math"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

// Assignment pairs a subtask with the node and model chosen to run it.
type Assignment struct {
	Subtask Subtask      `json:"subtask"`
	NodeID  string       `json:"node_id"`
	Address string       `json:"address"`
	Model   string       `json:"model"`
	Score   float64      `json:"score"`
	Factors ScoreFactors `json:"score_factors"`
}

// ScoreFactors explains the additive scheduler score for an assignment.
type ScoreFactors struct {
	ModelMatch            float64  `json:"model_match"`
	Hardware              float64  `json:"hardware"`
	ActiveTasks           int      `json:"active_tasks"`
	QueuedTasks           int      `json:"queued_tasks"`
	PendingDependents     int      `json:"pending_dependents,omitempty"`
	LoadPenalty           float64  `json:"load_penalty"`
	GPUCompatibilityScore float64  `json:"gpu_compatibility_score,omitempty"`
	GPUUtilizationPenalty float64  `json:"gpu_utilization_penalty"`
	NPUUtilizationPenalty float64  `json:"npu_utilization_penalty"`
	CPUUtilizationPenalty float64  `json:"cpu_utilization_penalty,omitempty"`
	MemoryUsagePenalty    float64  `json:"memory_usage_penalty,omitempty"`
	CapabilityScore       float64  `json:"capability_score,omitempty"`
	MatchedCapabilities   []string `json:"matched_capabilities,omitempty"`
	ModelQualityScore     float64  `json:"model_quality_score,omitempty"`
	TopologyScore         float64  `json:"topology_score,omitempty"`
	LearnedBias           float64  `json:"learned_bias"`
	TelemetryFresh        bool     `json:"telemetry_fresh"`
	TelemetryAgeMs        int64    `json:"telemetry_age_ms,omitempty"`
}

// ScoringConfig defines tunable weights and bonuses for node scoring.
type ScoringConfig struct {
	ModelMatchWeight       float64 `json:"model_match_weight"`
	HardwareWeight         float64 `json:"hardware_weight"`
	LoadPenaltyWeight      float64 `json:"load_penalty_weight"`
	GPUCompatibilityBonus  float64 `json:"gpu_compatibility_bonus"`
	DependentPenaltyWeight float64 `json:"dependent_penalty_weight"`
	GPUUtilizationWeight   float64 `json:"gpu_utilization_weight"`
	NPUUtilizationWeight   float64 `json:"npu_utilization_weight"`
	CPUUtilizationWeight   float64 `json:"cpu_utilization_weight"`
	MemoryUsageWeight      float64 `json:"memory_usage_weight"`
	RequiredCapMatchBonus  float64 `json:"required_cap_match_bonus"`
	PreferredCapMatchBonus float64 `json:"preferred_cap_match_bonus"`
	ModelQualityBonus      float64 `json:"model_quality_bonus"`
	CriticalPathBonus      float64 `json:"critical_path_bonus"`
	LearnedBiasWeight      float64 `json:"learned_bias_weight"`
}

// DefaultScoringConfig returns balanced default weights for scheduler scoring.
func DefaultScoringConfig() ScoringConfig {
	return ScoringConfig{
		ModelMatchWeight:       10.0,
		HardwareWeight:         0.3,
		LoadPenaltyWeight:      8.0,
		GPUCompatibilityBonus:  10.0,
		DependentPenaltyWeight: 2.0,
		GPUUtilizationWeight:   0.1,
		NPUUtilizationWeight:   0.1,
		CPUUtilizationWeight:   0.05,
		MemoryUsageWeight:      0.05,
		RequiredCapMatchBonus:  15.0,
		PreferredCapMatchBonus: 5.0,
		ModelQualityBonus:      3.0,
		CriticalPathBonus:      2.0,
		LearnedBiasWeight:      1.0,
	}
}

// ErrNoHealthyNodes is returned when the cluster has no node available to
// take on any work.
var ErrNoHealthyNodes = errors.New("scheduler: no healthy nodes available")

const telemetryFreshnessWindow = 30 * time.Second
const gpuLMStudioModelBonus = 3.0
const gpuSmallModelBonus = 0.5

// UnassignableSubtasksError reports work for which no healthy node advertises
// a compatible model. Assign is all-or-nothing so callers cannot silently
// report a partially executed task as complete.
type UnassignableSubtasksError struct {
	SubtaskIDs []string
}

func (e *UnassignableSubtasksError) Error() string {
	return "scheduler: no compatible model for subtasks: " + strings.Join(e.SubtaskIDs, ", ")
}

// Experience supplies a learned routing adjustment for a node+model+task
// pairing, derived from the cluster's recorded history.
type Experience interface {
	LearnedBias(nodeID, model, taskType string) float64
}

// Assign picks the best node+model for each subtask.
func Assign(subtasks []Subtask, nodes []cluster.NodeStatus) ([]Assignment, error) {
	return AssignWithExperience(subtasks, nodes, nil)
}

// AssignWithExperience is Assign biased by what the cluster has learned.
func AssignWithExperience(subtasks []Subtask, nodes []cluster.NodeStatus, exp Experience) ([]Assignment, error) {
	cfg := DefaultScoringConfig()
	return AssignWithConfig(subtasks, nodes, &cfg, exp)
}

// AssignWithConfig runs assignment using a custom scoring configuration.
func AssignWithConfig(subtasks []Subtask, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	g, err := TaskGraphFromSubtasks(subtasks)
	if err != nil {
		// Fall back to direct subtask list if normalization fails
		return assignSubtaskList(subtasks, nil, nodes, cfg, exp)
	}
	return AssignGraph(g, nodes, cfg, exp)
}

// AssignGraph assigns subtasks using full graph topology to inform scheduling decisions.
func AssignGraph(g *TaskGraph, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	if cfg == nil {
		c := DefaultScoringConfig()
		cfg = &c
	}
	sortedSubtasks, err := g.ToSubtasks()
	if err != nil {
		return nil, err
	}
	return assignSubtaskList(sortedSubtasks, g, nodes, cfg, exp)
}

func assignSubtaskList(subtasks []Subtask, graph *TaskGraph, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	if cfg == nil {
		c := DefaultScoringConfig()
		cfg = &c
	}
	healthy := make([]cluster.NodeStatus, 0, len(nodes))
	for _, n := range nodes {
		if n.Healthy {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) == 0 {
		return nil, ErrNoHealthyNodes
	}

	simulatedLoad := make(map[string]int, len(healthy))
	pendingDependents := make(map[string]int, len(healthy))
	for _, n := range healthy {
		simulatedLoad[n.ID] = n.ActiveTasks + n.QueuedTasks
	}

	assignments := make([]Assignment, 0, len(subtasks))
	var unassignable []string
	now := time.Now()

	for _, st := range subtasks {
		outDegree := 0
		if graph != nil {
			outDegree = graph.OutDegree(st.ID)
		}

		bestNode, bestModel, bestScore, factors := pickBest(st, outDegree, healthy, simulatedLoad, pendingDependents, exp, cfg, now)
		if bestNode == nil {
			unassignable = append(unassignable, st.ID)
			continue
		}

		simulatedLoad[bestNode.ID]++
		if graph != nil {
			pendingDependents[bestNode.ID] += outDegree
		}

		log.Printf("scheduler: subtask %s (%s) assigned to node %s (model %s, score %.2f) [match=%.1f cap=%.1f qual=%.1f hw=%.1f load=-%.1f tele_fresh=%v]",
			st.ID, st.Role, bestNode.ID, bestModel, bestScore,
			factors.ModelMatch, factors.CapabilityScore, factors.ModelQualityScore,
			factors.Hardware, factors.LoadPenalty, factors.TelemetryFresh)

		assignments = append(assignments, Assignment{
			Subtask: st,
			NodeID:  bestNode.ID,
			Address: bestNode.Address,
			Model:   bestModel,
			Score:   bestScore,
			Factors: factors,
		})
	}

	if len(unassignable) > 0 {
		return nil, &UnassignableSubtasksError{SubtaskIDs: unassignable}
	}
	return assignments, nil
}

func pickBest(st Subtask, outDegree int, nodes []cluster.NodeStatus, load map[string]int, pendingDependents map[string]int, exp Experience, cfg *ScoringConfig, now time.Time) (*cluster.NodeStatus, string, float64, ScoreFactors) {
	var best *cluster.NodeStatus
	bestModel := ""
	bestScore := math.Inf(-1)
	var bestFactors ScoreFactors

	for i := range nodes {
		n := &nodes[i]
		model, matchScore, modelQuality, matchedCaps := bestModelForNode(st, n, n.Models, n.DefaultModel, cfg)
		if model == "" {
			continue
		}

		hw := n.Hardware
		telemetryAge := time.Duration(0)
		if !n.TelemetryAt.IsZero() && !n.TelemetryAt.After(now) {
			telemetryAge = now.Sub(n.TelemetryAt)
		}
		telemetryFresh := !n.TelemetryAt.IsZero() && !n.TelemetryAt.After(now) && telemetryAge <= telemetryFreshnessWindow

		var gpuPenalty, npuPenalty, cpuPenalty, memoryPenalty float64
		if telemetryFresh {
			if hw.GPUStatsAvailable {
				gpuPenalty = float64(hw.GPUUtilization) * cfg.GPUUtilizationWeight
			}
			if hw.NPUStatsAvailable {
				npuPenalty = float64(hw.NPUUtilization) * cfg.NPUUtilizationWeight
			}
			if hw.CPUStatsAvailable {
				cpuPenalty = float64(hw.CPUUtilization) * cfg.CPUUtilizationWeight
			}
			if hw.MemoryStatsAvailable {
				memoryPenalty = float64(hw.MemoryUsage) * cfg.MemoryUsageWeight
			}
		}

		hardwareScore := hw.Score * cfg.HardwareWeight
		loadPenalty := float64(load[n.ID])*cfg.LoadPenaltyWeight + float64(pendingDependents[n.ID])*cfg.DependentPenaltyWeight
		gpuCompatibilityScore := 0.0
		if hw.HasGPU {
			gpuCompatibilityScore = cfg.GPUCompatibilityBonus
		}

		// Topology score: prioritize capable nodes for high-impact critical path nodes
		topologyScore := 0.0
		if outDegree > 0 {
			topologyScore = float64(outDegree) * cfg.CriticalPathBonus * (hw.Score / 100.0)
		}

		learned := 0.0
		if exp != nil {
			learned = exp.LearnedBias(n.ID, model, st.TaskType) * cfg.LearnedBiasWeight
		}

		capScore := float64(len(matchedCaps)) * cfg.PreferredCapMatchBonus
		if len(st.RequiredCapabilities) > 0 {
			capScore += float64(len(st.RequiredCapabilities)) * cfg.RequiredCapMatchBonus
		}

		factors := ScoreFactors{
			ModelMatch:            matchScore * cfg.ModelMatchWeight,
			Hardware:              hardwareScore,
			ActiveTasks:           n.ActiveTasks,
			QueuedTasks:           load[n.ID] - n.ActiveTasks,
			PendingDependents:     pendingDependents[n.ID],
			LoadPenalty:           loadPenalty,
			GPUCompatibilityScore: gpuCompatibilityScore,
			GPUUtilizationPenalty: gpuPenalty,
			NPUUtilizationPenalty: npuPenalty,
			CPUUtilizationPenalty: cpuPenalty,
			MemoryUsagePenalty:    memoryPenalty,
			CapabilityScore:       capScore,
			MatchedCapabilities:   matchedCaps,
			ModelQualityScore:     modelQuality,
			TopologyScore:         topologyScore,
			LearnedBias:           learned,
			TelemetryFresh:        telemetryFresh,
			TelemetryAgeMs:        telemetryAge.Milliseconds(),
		}

		score := factors.ModelMatch + factors.Hardware + factors.GPUCompatibilityScore + factors.CapabilityScore +
			factors.ModelQualityScore + factors.TopologyScore - factors.LoadPenalty -
			factors.GPUUtilizationPenalty - factors.NPUUtilizationPenalty -
			factors.CPUUtilizationPenalty - factors.MemoryUsagePenalty + factors.LearnedBias

		if score > bestScore {
			bestScore = score
			best = n
			bestModel = model
			bestFactors = factors
		}
	}
	return best, bestModel, bestScore, bestFactors
}

// bestModelForNode finds the best model on a node, validating required capabilities and scoring preferences.
func bestModelForNode(st Subtask, n *cluster.NodeStatus, models []engine.Model, defaultModel string, cfg *ScoringConfig) (string, float64, float64, []string) {
	nodeCaps := n.EffectiveCapabilities()

	// Check required capabilities on the node/model level
	if len(st.RequiredCapabilities) > 0 {
		for _, reqCap := range st.RequiredCapabilities {
			reqCapLower := strings.ToLower(strings.TrimSpace(reqCap))
			if !hasCapability(nodeCaps, reqCapLower) {
				return "", 0, 0, nil
			}
		}
	}

	bestName := ""
	bestScore := -1.0
	bestQuality := 0.0
	var bestMatchedCaps []string

	for _, m := range models {
		if st.TaskType != "general" && hasSpecialtyTag(m.Tags) &&
			!containsTag(m.Tags, st.TaskType) && !containsTag(m.Tags, "general") {
			continue
		}

		// Check min model size if specified
		if st.MinModelSizeGB > 0 && m.SizeGB > 0 && m.SizeGB < st.MinModelSizeGB {
			continue
		}

		score := 0.0
		quality := 0.0
		if m.Name == defaultModel {
			score += 1.0
		}
		if st.PreferredModel != "" && strings.EqualFold(m.Name, st.PreferredModel) {
			score += 2.0
		}

		for _, tag := range m.Tags {
			if tag == st.TaskType {
				score += 2
			} else if tag == "general" {
				score += 0.5
			} else if tag == "fast" {
				score += 0.25
			}
			if tag == "high-quality" || tag == "reasoning" {
				quality += cfg.ModelQualityBonus
			}
		}

		if n.Hardware.HasGPU && strings.EqualFold(m.Engine, "lmstudio") {
			score += gpuLMStudioModelBonus
			if containsTag(m.Tags, "small") || containsTag(m.Tags, "fast") {
				score += gpuSmallModelBonus
			}
		}

		if m.SizeGB >= 7.0 {
			quality += cfg.ModelQualityBonus * 0.5
		}

		// Calculate matched preferred capabilities
		matchedCaps := make([]string, 0)
		for _, prefCap := range st.PreferredCapabilities {
			prefLower := strings.ToLower(strings.TrimSpace(prefCap))
			if hasCapability(nodeCaps, prefLower) || containsTag(m.Tags, prefLower) {
				matchedCaps = append(matchedCaps, prefCap)
			}
		}

		if score > bestScore {
			bestScore = score
			bestName = m.Name
			bestQuality = quality
			bestMatchedCaps = matchedCaps
		}
	}

	if bestName == "" {
		return "", 0, 0, nil
	}
	return bestName, bestScore, bestQuality, bestMatchedCaps
}

func hasCapability(caps []string, target string) bool {
	for _, c := range caps {
		if strings.EqualFold(c, target) {
			return true
		}
	}
	return false
}

// bestModelFor maintains backward compatibility for helper callers.
func bestModelFor(st Subtask, models []engine.Model, defaultModel string) (string, float64) {
	cfg := DefaultScoringConfig()
	n := cluster.NodeStatus{Models: models, DefaultModel: defaultModel}
	name, score, _, _ := bestModelForNode(st, &n, models, defaultModel, &cfg)
	return name, score
}

func hasSpecialtyTag(tags []string) bool {
	for _, tag := range tags {
		switch tag {
		case "code", "summarize", "translate", "math", "vision", "creative":
			return true
		}
	}
	return false
}

func containsTag(tags []string, target string) bool {
	for _, tag := range tags {
		if tag == target {
			return true
		}
	}
	return false
}
