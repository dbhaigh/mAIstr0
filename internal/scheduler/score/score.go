// Package score provides deterministic, lock-free scheduler scoring.
//
// Score reads only its arguments and does not acquire locks or mutate shared
// state. Callers should pass a stable node snapshot, resolved experience bias,
// scoring configuration, and timestamp.
package score

import (
	"math"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

// Subtask contains the fields that affect model eligibility and score.
type Subtask struct {
	TaskType              string
	PreferredModel        string
	RequiredCapabilities  []string
	PreferredCapabilities []string
	MinModelSizeGB        float64
	OutDegree             int
}

// Experience is the already-resolved, unweighted learned bias for the chosen
// node/model/task pairing. Resolving it before scoring keeps Score lock-free.
type Experience struct {
	LearnedBias float64
}

// ScoringConfig configures each additive score contribution. Suggested tuning
// ranges are 0–15 for score bonuses and penalty weights, 0–1 for hardware and
// utilization multipliers, and 0–2 for learned-bias weighting. These ranges
// are guidance only and are not enforced, preserving custom configurations.
type ScoringConfig struct {
	ModelMatchWeight       float64 `json:"model_match_weight"`        // Model/task-tag multiplier (default 10; suggested 0–15).
	HardwareWeight         float64 `json:"hardware_weight"`           // Hardware-score multiplier (default 0.3; suggested 0–1).
	LoadPenaltyWeight      float64 `json:"load_penalty_weight"`       // Per active/queued task and capacity-overage penalty (default 8; suggested 0–15).
	GPUCompatibilityBonus  float64 `json:"gpu_compatibility_bonus"`   // Bonus for GPU-equipped nodes (default 10; suggested 0–15).
	DependentPenaltyWeight float64 `json:"dependent_penalty_weight"`  // Per pending dependent task penalty (default 2; suggested 0–15).
	GPUUtilizationWeight   float64 `json:"gpu_utilization_weight"`    // Fresh GPU-utilization multiplier (default 0.1; suggested 0–1).
	NPUUtilizationWeight   float64 `json:"npu_utilization_weight"`    // Fresh NPU-utilization multiplier (default 0.1; suggested 0–1).
	CPUUtilizationWeight   float64 `json:"cpu_utilization_weight"`    // Fresh CPU-utilization multiplier (default 0.05; suggested 0–1).
	MemoryUsageWeight      float64 `json:"memory_usage_weight"`       // Fresh memory-use multiplier (default 0.05; suggested 0–1).
	RequiredCapMatchBonus  float64 `json:"required_cap_match_bonus"`  // Per required-capability bonus (default 15; suggested 0–15).
	PreferredCapMatchBonus float64 `json:"preferred_cap_match_bonus"` // Per preferred-capability bonus (default 5; suggested 0–15).
	ModelQualityBonus      float64 `json:"model_quality_bonus"`       // Quality-tag / large-model bonus (default 3; suggested 0–15).
	CriticalPathBonus      float64 `json:"critical_path_bonus"`       // Per dependent-edge bonus, scaled by hardware (default 2; suggested 0–15).
	LearnedBiasWeight      float64 `json:"learned_bias_weight"`       // Resolved learned-bias multiplier (default 1; suggested 0–2).
}

// ScoreFactors contains the additive score terms and explanatory measurements.
// Positive score factors are bonuses; penalties are subtracted from the total.
type ScoreFactors struct {
	ModelMatch            float64  `json:"model_match"`                       // Model default, preference, and task-tag contribution.
	Hardware              float64  `json:"hardware"`                          // Weighted hardware score.
	ActiveTasks           int      `json:"active_tasks"`                      // Reported active task count.
	QueuedTasks           int      `json:"queued_tasks"`                      // Effective load minus reported active tasks.
	PendingDependents     int      `json:"pending_dependents,omitempty"`      // Topology-dependent work already placed on this node.
	CapacityPenalty       float64  `json:"capacity_penalty,omitempty"`        // Penalty for simulated load at or above the node limit.
	LoadPenalty           float64  `json:"load_penalty"`                      // Weighted active, queued, and dependent load.
	GPUCompatibilityScore float64  `json:"gpu_compatibility_score,omitempty"` // GPU availability bonus.
	GPUUtilizationPenalty float64  `json:"gpu_utilization_penalty"`           // Fresh GPU utilization penalty.
	NPUUtilizationPenalty float64  `json:"npu_utilization_penalty"`           // Fresh NPU utilization penalty.
	CPUUtilizationPenalty float64  `json:"cpu_utilization_penalty,omitempty"` // Fresh CPU utilization penalty.
	MemoryUsagePenalty    float64  `json:"memory_usage_penalty,omitempty"`    // Fresh memory utilization penalty.
	CapabilityScore       float64  `json:"capability_score,omitempty"`        // Required and preferred capability bonuses.
	MatchedCapabilities   []string `json:"matched_capabilities,omitempty"`    // Preferred capabilities found on the node or model.
	ModelQualityScore     float64  `json:"model_quality_score,omitempty"`     // Quality-tag and model-size bonus.
	TopologyScore         float64  `json:"topology_score,omitempty"`          // Critical-path bonus.
	LearnedBias           float64  `json:"learned_bias"`                      // Weighted learned experience contribution.
	TelemetryFresh        bool     `json:"telemetry_fresh"`                   // Whether utilization telemetry was included.
	TelemetryAgeMs        int64    `json:"telemetry_age_ms,omitempty"`        // Age of telemetry, even when stale.
}

// Candidate is a prepared, eligible node/model choice. Prepare creates it;
// callers should pass it back with the same node and subtask to ScorePrepared.
type Candidate struct {
	model                  string
	engine                 string
	modelMatch             float64
	modelQualityTags       int
	modelAtLeastSevenGB    bool
	matchedCapabilityCount int
	matchedCapabilities    []string
}

// Model returns the selected model name.
func (c Candidate) Model() string { return c.model }

// Engine returns the selected engine name.
func (c Candidate) Engine() string { return c.engine }

const (
	telemetryFreshnessWindow = 30 * time.Second
	gpuLMStudioModelBonus    = 3.0
	gpuSmallModelBonus       = 0.5
)

// DefaultScoringConfig returns the established scheduler defaults.
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

// Prepare finds the node's best ready, compatible model and caches its
// model-specific score inputs. It does not read learned experience or mutate
// the node. A false result means no model satisfies the subtask requirements.
func Prepare(node cluster.NodeStatus, subtask Subtask) (Candidate, bool) {
	if !node.Healthy {
		return Candidate{}, false
	}
	return PrepareWithCapabilities(node, subtask, node.EffectiveCapabilities())
}

// PrepareWithCapabilities is Prepare with the node's already-derived
// capabilities. The supplied slice is read-only, allowing assignment loops
// to reuse a stable capability snapshot across multiple subtasks.
func PrepareWithCapabilities(node cluster.NodeStatus, subtask Subtask, nodeCapabilities []string) (Candidate, bool) {
	return prepareWithCapabilities(node, subtask, nodeCapabilities, true)
}

// PrepareWithCapabilitiesForScoring prepares numeric score inputs without
// allocating matched-capability details. Use the result with ScorePreparedValue.
func PrepareWithCapabilitiesForScoring(node cluster.NodeStatus, subtask Subtask, nodeCapabilities []string) (Candidate, bool) {
	return prepareWithCapabilities(node, subtask, nodeCapabilities, false)
}

func prepareWithCapabilities(node cluster.NodeStatus, subtask Subtask, nodeCapabilities []string, includeDetails bool) (Candidate, bool) {
	if !node.Healthy {
		return Candidate{}, false
	}
	for _, required := range subtask.RequiredCapabilities {
		if !hasCapability(nodeCapabilities, strings.ToLower(strings.TrimSpace(required))) {
			return Candidate{}, false
		}
	}

	best := Candidate{}
	bestModelScore := -1.0
	var bestTags []string
	var normalizedPreferred []string
	if len(subtask.PreferredCapabilities) > 0 {
		normalizedPreferred = make([]string, len(subtask.PreferredCapabilities))
		for i, preferred := range subtask.PreferredCapabilities {
			normalizedPreferred[i] = strings.ToLower(strings.TrimSpace(preferred))
		}
	}
	for _, model := range node.Models {
		if !node.EngineReady(model.Engine) || !modelMatchesSubtask(subtask, model) {
			continue
		}

		modelScore := 0.0
		if model.Name == node.DefaultModel {
			modelScore += 1.0
		}
		if subtask.PreferredModel != "" && strings.EqualFold(model.Name, subtask.PreferredModel) {
			modelScore += 2.0
		}

		qualityTags := 0
		for _, tag := range model.Tags {
			if tag == subtask.TaskType {
				modelScore += 2
			} else if tag == "general" {
				modelScore += 0.5
			} else if tag == "fast" {
				modelScore += 0.25
			}
			if tag == "high-quality" || tag == "reasoning" {
				qualityTags++
			}
		}

		if node.Hardware.HasGPU && strings.EqualFold(model.Engine, "lmstudio") {
			modelScore += gpuLMStudioModelBonus
			if containsTag(model.Tags, "small") || containsTag(model.Tags, "fast") {
				modelScore += gpuSmallModelBonus
			}
		}

		if modelScore > bestModelScore {
			bestModelScore = modelScore
			bestTags = model.Tags
			best = Candidate{
				model:               model.Name,
				engine:              model.Engine,
				modelMatch:          modelScore,
				modelQualityTags:    qualityTags,
				modelAtLeastSevenGB: model.SizeGB >= 7.0,
			}
		}
	}
	if best.model == "" {
		return Candidate{}, false
	}
	// Build explanation details only once, for the selected model, not for every eligible model.
	best.matchedCapabilities, best.matchedCapabilityCount = matchedPreferredCapabilities(
		subtask.PreferredCapabilities, normalizedPreferred, nodeCapabilities, bestTags, includeDetails,
	)
	return best, true
}

func matchedPreferredCapabilities(preferred, normalized, capabilities, tags []string, includeDetails bool) ([]string, int) {
	var matched []string
	count := 0
	for i, value := range preferred {
		if hasCapability(capabilities, normalized[i]) || containsTag(tags, normalized[i]) {
			count++
			if includeDetails {
				matched = append(matched, value)
			}
		}
	}
	return matched, count
}

// Score returns the established additive scheduler score and its explanation.
// The experience bias must be resolved by the caller; this function and its
// helpers acquire no locks or access mutable global state. Ineligible nodes
// return negative infinity and zero-valued factors.
func Score(
	node cluster.NodeStatus,
	subtask Subtask,
	load int,
	pendingDependents int,
	exp Experience,
	cfg ScoringConfig,
	now time.Time,
) (float64, ScoreFactors) {
	candidate, ok := Prepare(node, subtask)
	if !ok {
		return math.Inf(-1), ScoreFactors{}
	}
	return ScorePrepared(node, subtask, candidate, load, pendingDependents, exp, cfg, now)
}

// ScorePrepared scores an already prepared candidate. The candidate must have
// been produced from the same node and subtask; the function remains pure and
// lock-free.
func ScorePrepared(
	node cluster.NodeStatus,
	subtask Subtask,
	candidate Candidate,
	load int,
	pendingDependents int,
	exp Experience,
	cfg ScoringConfig,
	now time.Time,
) (float64, ScoreFactors) {
	return scorePrepared(node, subtask, candidate, load, pendingDependents, exp, cfg, now, true)
}

// ScorePreparedValue calculates only the numeric score and avoids allocating
// explanation slices for candidates that may not win assignment.
func ScorePreparedValue(
	node cluster.NodeStatus,
	subtask Subtask,
	candidate Candidate,
	load int,
	pendingDependents int,
	exp Experience,
	cfg ScoringConfig,
	now time.Time,
) float64 {
	scoreValue, _ := scorePrepared(node, subtask, candidate, load, pendingDependents, exp, cfg, now, false)
	return scoreValue
}

func scorePrepared(
	node cluster.NodeStatus,
	subtask Subtask,
	candidate Candidate,
	load int,
	pendingDependents int,
	exp Experience,
	cfg ScoringConfig,
	now time.Time,
	includeDetails bool,
) (float64, ScoreFactors) {
	if candidate.model == "" {
		return math.Inf(-1), ScoreFactors{}
	}

	hardware := node.Hardware
	telemetryAge := time.Duration(0)
	if !node.TelemetryAt.IsZero() && !node.TelemetryAt.After(now) {
		telemetryAge = now.Sub(node.TelemetryAt)
	}
	telemetryFresh := !node.TelemetryAt.IsZero() && !node.TelemetryAt.After(now) &&
		telemetryAge <= telemetryFreshnessWindow

	var gpuPenalty, npuPenalty, cpuPenalty, memoryPenalty float64
	if telemetryFresh {
		if hardware.GPUStatsAvailable {
			gpuPenalty = float64(hardware.GPUUtilization) * cfg.GPUUtilizationWeight
		}
		if hardware.NPUStatsAvailable {
			npuPenalty = float64(hardware.NPUUtilization) * cfg.NPUUtilizationWeight
		}
		if hardware.CPUStatsAvailable {
			cpuPenalty = float64(hardware.CPUUtilization) * cfg.CPUUtilizationWeight
		}
		if hardware.MemoryStatsAvailable {
			memoryPenalty = float64(hardware.MemoryUsage) * cfg.MemoryUsageWeight
		}
	}

	hardwareScore := hardware.Score * cfg.HardwareWeight
	loadPenalty := float64(load)*cfg.LoadPenaltyWeight +
		float64(pendingDependents)*cfg.DependentPenaltyWeight
	capacityPenalty := 0.0
	if node.MaxConcurrentTasks > 0 && load >= node.MaxConcurrentTasks {
		capacityPenalty = float64(load-node.MaxConcurrentTasks+1) * cfg.LoadPenaltyWeight
	}
	gpuCompatibilityScore := 0.0
	if hardware.HasGPU {
		gpuCompatibilityScore = cfg.GPUCompatibilityBonus
	}

	topologyScore := 0.0
	if subtask.OutDegree > 0 {
		topologyScore = float64(subtask.OutDegree) * cfg.CriticalPathBonus * (hardware.Score / 100.0)
	}

	capabilityScore := float64(candidate.matchedCapabilityCount) * cfg.PreferredCapMatchBonus
	if len(subtask.RequiredCapabilities) > 0 {
		capabilityScore += float64(len(subtask.RequiredCapabilities)) * cfg.RequiredCapMatchBonus
	}
	modelQuality := 0.0
	for range candidate.modelQualityTags {
		modelQuality += cfg.ModelQualityBonus
	}
	if candidate.modelAtLeastSevenGB {
		modelQuality += cfg.ModelQualityBonus * 0.5
	}

	var matchedCapabilities []string
	if includeDetails {
		matchedCapabilities = append([]string(nil), candidate.matchedCapabilities...)
	}
	factors := ScoreFactors{
		ModelMatch:            candidate.modelMatch * cfg.ModelMatchWeight,
		Hardware:              hardwareScore,
		ActiveTasks:           node.ActiveTasks,
		QueuedTasks:           load - node.ActiveTasks,
		PendingDependents:     pendingDependents,
		CapacityPenalty:       capacityPenalty,
		LoadPenalty:           loadPenalty,
		GPUCompatibilityScore: gpuCompatibilityScore,
		GPUUtilizationPenalty: gpuPenalty,
		NPUUtilizationPenalty: npuPenalty,
		CPUUtilizationPenalty: cpuPenalty,
		MemoryUsagePenalty:    memoryPenalty,
		CapabilityScore:       capabilityScore,
		MatchedCapabilities:   matchedCapabilities,
		ModelQualityScore:     modelQuality,
		TopologyScore:         topologyScore,
		LearnedBias:           exp.LearnedBias * cfg.LearnedBiasWeight,
		TelemetryFresh:        telemetryFresh,
		TelemetryAgeMs:        telemetryAge.Milliseconds(),
	}
	score := factors.ModelMatch + factors.Hardware + factors.GPUCompatibilityScore + factors.CapabilityScore +
		factors.ModelQualityScore + factors.TopologyScore - factors.LoadPenalty - factors.CapacityPenalty -
		factors.GPUUtilizationPenalty - factors.NPUUtilizationPenalty -
		factors.CPUUtilizationPenalty - factors.MemoryUsagePenalty + factors.LearnedBias
	return score, factors
}

func modelMatchesSubtask(subtask Subtask, model engine.Model) bool {
	if subtask.TaskType != "general" && hasSpecialtyTag(model.Tags) &&
		!containsTag(model.Tags, subtask.TaskType) && !containsTag(model.Tags, "general") {
		return false
	}
	return subtask.MinModelSizeGB <= 0 || model.SizeGB <= 0 || model.SizeGB >= subtask.MinModelSizeGB
}

func hasCapability(capabilities []string, target string) bool {
	for _, capability := range capabilities {
		if strings.EqualFold(capability, target) {
			return true
		}
	}
	return false
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
