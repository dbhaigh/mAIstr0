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
	"unicode/utf8"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

// Subtask contains the fields that affect model eligibility and score.
type Subtask struct {
	TaskType              string
	Description           string
	ExpectedOutput        string
	RoutingPreference     string
	PreferredModel        string
	RequiredCapabilities  []string
	PreferredCapabilities []string
	MinModelSizeGB        float64
	OutDegree             int
}

// Experience is the already-resolved, unweighted learned bias for the chosen
// node/model/task pairing. Resolving it before scoring keeps Score lock-free.
type Experience struct {
	LearnedBias     float64
	TokensPerSecond float64
}

// ScoringConfig configures each additive score contribution. Suggested tuning
// ranges are 0–15 for score bonuses and penalty weights, 0–1 for hardware and
// utilization multipliers, and 0–2 for learned-bias weighting. These ranges
// are guidance only and are not enforced, preserving custom configurations.
type ScoringConfig struct {
	ModelMatchWeight        float64 `json:"model_match_weight"`        // Model/task-tag multiplier (default 10; suggested 0–15).
	HardwareWeight          float64 `json:"hardware_weight"`           // Hardware-score multiplier (default 0.3; suggested 0–1).
	LoadPenaltyWeight       float64 `json:"load_penalty_weight"`       // Per active/queued task and capacity-overage penalty (default 8; suggested 0–15).
	GPUCompatibilityBonus   float64 `json:"gpu_compatibility_bonus"`   // Bonus for GPU-equipped nodes (default 10; suggested 0–15).
	DependentPenaltyWeight  float64 `json:"dependent_penalty_weight"`  // Per pending dependent task penalty (default 2; suggested 0–15).
	GPUUtilizationWeight    float64 `json:"gpu_utilization_weight"`    // Fresh GPU-utilization multiplier (default 0.1; suggested 0–1).
	NPUUtilizationWeight    float64 `json:"npu_utilization_weight"`    // Fresh NPU-utilization multiplier (default 0.1; suggested 0–1).
	CPUUtilizationWeight    float64 `json:"cpu_utilization_weight"`    // Fresh CPU-utilization multiplier (default 0.05; suggested 0–1).
	MemoryUsageWeight       float64 `json:"memory_usage_weight"`       // Fresh memory-use multiplier (default 0.05; suggested 0–1).
	RequiredCapMatchBonus   float64 `json:"required_cap_match_bonus"`  // Per required-capability bonus (default 15; suggested 0–15).
	PreferredCapMatchBonus  float64 `json:"preferred_cap_match_bonus"` // Per preferred-capability bonus (default 5; suggested 0–15).
	ModelQualityBonus       float64 `json:"model_quality_bonus"`       // Quality-tag / large-model bonus (default 3; suggested 0–15).
	CriticalPathBonus       float64 `json:"critical_path_bonus"`       // Per dependent-edge bonus, scaled by hardware (default 2; suggested 0–15).
	LearnedBiasWeight       float64 `json:"learned_bias_weight"`       // Resolved learned-bias multiplier (default 1; suggested 0–2).
	LatencyPreferenceWeight float64 `json:"latency_preference_weight"` // Maximum latency preference bonus (default 30).
}

// ScoreFactors contains the additive score terms and explanatory measurements.
// Positive score factors are bonuses; penalties are subtracted from the total.
type ScoreFactors struct {
	ModelMatch                 float64  `json:"model_match"`                            // Model default, preference, and task-tag contribution.
	Hardware                   float64  `json:"hardware"`                               // Weighted hardware score.
	ActiveTasks                int      `json:"active_tasks"`                           // Reported active task count.
	QueuedTasks                int      `json:"queued_tasks"`                           // Effective load minus reported active tasks.
	PendingDependents          int      `json:"pending_dependents,omitempty"`           // Topology-dependent work already placed on this node.
	CapacityPenalty            float64  `json:"capacity_penalty,omitempty"`             // Penalty for simulated load at or above the node limit.
	LoadPenalty                float64  `json:"load_penalty"`                           // Weighted active, queued, and dependent load.
	GPUCompatibilityScore      float64  `json:"gpu_compatibility_score,omitempty"`      // GPU availability bonus.
	GPUUtilizationPenalty      float64  `json:"gpu_utilization_penalty"`                // Fresh GPU utilization penalty.
	NPUUtilizationPenalty      float64  `json:"npu_utilization_penalty"`                // Fresh NPU utilization penalty.
	CPUUtilizationPenalty      float64  `json:"cpu_utilization_penalty,omitempty"`      // Fresh CPU utilization penalty.
	MemoryUsagePenalty         float64  `json:"memory_usage_penalty,omitempty"`         // Fresh memory utilization penalty.
	CapabilityScore            float64  `json:"capability_score,omitempty"`             // Required and preferred capability bonuses.
	MatchedCapabilities        []string `json:"matched_capabilities,omitempty"`         // Preferred capabilities found on the node or model.
	ModelQualityScore          float64  `json:"model_quality_score,omitempty"`          // Quality-tag and model-size bonus.
	TopologyScore              float64  `json:"topology_score,omitempty"`               // Critical-path bonus.
	EstimatedCompletionSeconds float64  `json:"estimated_completion_seconds,omitempty"` // Heuristic completion estimate including queued work.
	EstimatedTokensPerSecond   float64  `json:"estimated_tokens_per_second,omitempty"`  // Historical throughput or model-size fallback.
	ModelSizeGB                float64  `json:"model_size_gb,omitempty"`                // Selected model size used by the estimate.
	LatencyScore               float64  `json:"latency_score,omitempty"`                // Preference-adjusted latency bonus.
	LearnedBias                float64  `json:"learned_bias"`                           // Weighted learned experience contribution.
	TelemetryFresh             bool     `json:"telemetry_fresh"`                        // Whether utilization telemetry was included.
	TelemetryAgeMs             int64    `json:"telemetry_age_ms,omitempty"`             // Age of telemetry, even when stale.
}

// Candidate is a prepared, eligible node/model choice. Prepare creates it;
// callers should pass it back with the same node and subtask to ScorePrepared.
type Candidate struct {
	model                  string
	engine                 string
	modelSizeGB            float64
	modelMatch             float64
	modelQualityTags       int
	modelAtLeastSevenGB    bool
	matchedCapabilityCount int
	matchedCapabilities    []string
}

// ModelEligibilityRejection explains why an NPU-backed model was excluded.
type ModelEligibilityRejection struct {
	NodeID  string   `json:"node_id"`
	Model   string   `json:"model"`
	Engine  string   `json:"engine,omitempty"`
	Reasons []string `json:"reasons"`
}

// Model returns the selected model name.
func (c Candidate) Model() string { return c.model }

// Engine returns the selected engine name.
func (c Candidate) Engine() string { return c.engine }

const (
	telemetryFreshnessWindow   = 30 * time.Second
	acceleratorModelBonus      = 3.0
	acceleratorSmallModelBonus = 0.5
)

// DefaultScoringConfig returns the established scheduler defaults.
func DefaultScoringConfig() ScoringConfig {
	return ScoringConfig{
		ModelMatchWeight:        10.0,
		HardwareWeight:          0.3,
		LoadPenaltyWeight:       8.0,
		GPUCompatibilityBonus:   10.0,
		DependentPenaltyWeight:  2.0,
		GPUUtilizationWeight:    0.1,
		NPUUtilizationWeight:    0.1,
		CPUUtilizationWeight:    0.05,
		MemoryUsageWeight:       0.05,
		RequiredCapMatchBonus:   15.0,
		PreferredCapMatchBonus:  5.0,
		ModelQualityBonus:       3.0,
		CriticalPathBonus:       2.0,
		LearnedBiasWeight:       1.0,
		LatencyPreferenceWeight: 30.0,
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
		if !node.EngineReady(model.Engine) || !modelMatchesSubtask(subtask, model) ||
			npuModelEligibilityIssues(node, model) != 0 {
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
			switch tag {
			case subtask.TaskType:
				modelScore += 2
			case "general":
				modelScore += 0.5
			case "fast":
				modelScore += 0.25
			}
			if tag == "high-quality" || tag == "reasoning" {
				qualityTags++
			}
		}

		if bonus := acceleratorCompatibilityBonus(node, model); bonus > 0 {
			modelScore += bonus
		}

		if modelScore > bestModelScore {
			bestModelScore = modelScore
			bestTags = model.Tags
			best = Candidate{
				model:               model.Name,
				engine:              model.Engine,
				modelSizeGB:         model.SizeGB,
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

func acceleratorCompatibilityBonus(node cluster.NodeStatus, model engine.Model) float64 {
	if model.Engine == "" {
		return 0
	}
	bonus := acceleratorModelBonus
	if containsTag(model.Tags, "small") || containsTag(model.Tags, "fast") {
		bonus += acceleratorSmallModelBonus
	}
	for _, status := range node.Engines {
		if !strings.EqualFold(status.Name, model.Engine) {
			continue
		}
		if !status.Ready || status.ModelCount <= 0 {
			continue
		}
		capabilities := status.Capabilities
		if len(capabilities.OS) > 0 && !hasCapability(capabilities.OS, node.Hardware.OS) {
			continue
		}
		if strings.EqualFold(model.Device, "npu") {
			if supportsNodeNPU(capabilities, node.Hardware) {
				return bonus
			}
			continue
		}
		if supportsNodeGPU(capabilities, node.Hardware) {
			return bonus
		}
	}
	return 0
}

func supportsNodeGPU(capabilities engine.Capabilities, hw hardware.Info) bool {
	if len(hw.Devices) == 0 {
		return hw.HasGPU && engine.SupportsDeviceKind(capabilities.Devices, "gpu")
	}
	for _, device := range hw.Devices {
		kind := strings.ToLower(strings.TrimSpace(device.Kind))
		if kind == "gpu" &&
			engine.SupportsDeviceKind(capabilities.Devices, kind) &&
			engine.BackendsOverlap(capabilities.Backends, device.Backends) {
			return true
		}
	}
	return false
}

func supportsNodeNPU(capabilities engine.Capabilities, hw hardware.Info) bool {
	if len(hw.Devices) == 0 {
		return hw.HasNPU && npuDeviceMatchesCapabilities(
			hardware.Device{Kind: "npu", Vendor: hw.NPUVendor}, hw, capabilities,
		)
	}
	for _, device := range hw.Devices {
		if !engine.SupportsDeviceKind(capabilities.Devices, "npu") {
			continue
		}
		if !npuDeviceMatchesCapabilities(device, hw, capabilities) {
			continue
		}
		return true
	}
	return false
}

func npuDeviceMatchesCapabilities(device hardware.Device, hw hardware.Info, capabilities engine.Capabilities) bool {
	if !strings.EqualFold(strings.TrimSpace(device.Kind), "npu") ||
		!engine.SupportsDeviceKind(capabilities.Devices, "npu") {
		return false
	}
	vendor := device.Vendor
	if vendor == "" {
		vendor = hw.NPUVendor
	}
	return engine.SupportsNPUVendor(capabilities.NPUVendors, vendor) &&
		engine.BackendsOverlap(capabilities.Backends, device.Backends)
}

type npuEligibilityIssue uint16

const (
	npuIssueNoDevice npuEligibilityIssue = 1 << iota
	npuIssueWrongVendorOrBackend
	npuIssueMissingSize
	npuIssueMemoryUnavailable
	npuIssueDoesNotFit
	npuIssueMissingQuantization
	npuIssueUnsupportedQuantization
	npuIssueMissingContextCap
	npuIssueContextCapTooLarge
)

func npuModelEligibilityIssues(node cluster.NodeStatus, model engine.Model) npuEligibilityIssue {
	if !strings.EqualFold(strings.TrimSpace(model.Device), "npu") {
		return 0
	}
	hasNPU := node.Hardware.HasNPU
	for _, device := range node.Hardware.Devices {
		if strings.EqualFold(strings.TrimSpace(device.Kind), "npu") {
			hasNPU = true
			break
		}
	}
	if !hasNPU {
		return npuIssueNoDevice
	}
	capabilities := engine.Capabilities{}
	for _, status := range node.Engines {
		if strings.EqualFold(status.Name, model.Engine) {
			capabilities = status.Capabilities
			break
		}
	}
	if len(capabilities.Devices) > 0 && !engine.SupportsDeviceKind(capabilities.Devices, "npu") {
		return npuIssueWrongVendorOrBackend
	}

	var issues npuEligibilityIssue
	matchingDevices, hasMemory, fits := false, false, false
	if len(node.Hardware.Devices) == 0 {
		if node.Hardware.HasNPU {
			matchingDevices, hasMemory, fits = npuDeviceModelFit(
				hardware.Device{Kind: "npu", Vendor: node.Hardware.NPUVendor},
				node.Hardware, capabilities, model.SizeGB,
			)
		}
	} else {
		for _, device := range node.Hardware.Devices {
			matches, hasFreeMemory, modelFits := npuDeviceModelFit(
				device, node.Hardware, capabilities, model.SizeGB,
			)
			matchingDevices = matchingDevices || matches
			hasMemory = hasMemory || hasFreeMemory
			fits = fits || modelFits
		}
	}
	if !matchingDevices {
		issues |= npuIssueWrongVendorOrBackend
	}

	if model.SizeGB <= 0 {
		issues |= npuIssueMissingSize
	} else if matchingDevices {
		switch {
		case fits:
		case !hasMemory:
			issues |= npuIssueMemoryUnavailable
		default:
			issues |= npuIssueDoesNotFit
		}
	}

	requiresQuantization := len(capabilities.Quantizations) > 0 || capabilities.PrimaryQuantization != ""
	if requiresQuantization {
		quantization := strings.TrimSpace(model.Quantization)
		if quantization == "" {
			issues |= npuIssueMissingQuantization
		} else if !hasCapability(capabilities.Quantizations, quantization) &&
			!strings.EqualFold(capabilities.PrimaryQuantization, quantization) {
			issues |= npuIssueUnsupportedQuantization
		}
	}

	if capabilities.RequiresContextCap && model.ContextLength <= 0 {
		issues |= npuIssueMissingContextCap
	}
	if model.ContextLength > 0 && capabilities.MaxContextLength > 0 &&
		model.ContextLength > capabilities.MaxContextLength {
		issues |= npuIssueContextCapTooLarge
	}
	return issues
}

func npuDeviceModelFit(
	device hardware.Device,
	hw hardware.Info,
	capabilities engine.Capabilities,
	modelSizeGB float64,
) (matches, hasMemory, fits bool) {
	if !npuDeviceMatchesCapabilities(device, hw, capabilities) {
		return false, false, false
	}
	memoryMB := device.FreeMemoryMB
	if memoryMB == 0 {
		memoryMB = device.MemoryMB
	}
	if memoryMB == 0 {
		return true, false, false
	}
	return true, true, modelSizeGB > 0 && modelSizeGB <= float64(memoryMB)/1024
}

func explainNPUEligibilityIssues(issues npuEligibilityIssue, capabilities engine.Capabilities) []string {
	var reasons []string
	if issues&npuIssueNoDevice != 0 {
		reasons = append(reasons, "node has no NPU device")
	}
	if issues&npuIssueWrongVendorOrBackend != 0 {
		reasons = append(reasons, "node NPU vendor or backend is not supported by the engine")
	}
	if issues&npuIssueMissingSize != 0 {
		reasons = append(reasons, "model size is unavailable")
	}
	if issues&npuIssueMemoryUnavailable != 0 {
		reasons = append(reasons, "NPU memory capacity is unavailable")
	}
	if issues&npuIssueDoesNotFit != 0 {
		reasons = append(reasons, "model does not fit in reported NPU memory")
	}
	if issues&npuIssueMissingQuantization != 0 {
		reasons = append(reasons, "model quantization is unavailable")
	}
	if issues&npuIssueUnsupportedQuantization != 0 {
		required := capabilities.PrimaryQuantization
		if required == "" {
			required = strings.Join(capabilities.Quantizations, ", ")
		}
		reasons = append(reasons, "model quantization is not supported by the engine (expected "+required+")")
	}
	if issues&npuIssueMissingContextCap != 0 {
		reasons = append(reasons, "engine requires a reported context cap")
	}
	if issues&npuIssueContextCapTooLarge != 0 {
		reasons = append(reasons, "model context cap exceeds the engine maximum")
	}
	return reasons
}

// ExplainNPUModelRejections reports NPU-specific exclusion reasons for models
// considered by the scheduler. It is intended for failure explanations, not
// the allocation-free scoring path.
func ExplainNPUModelRejections(nodes []cluster.NodeStatus, subtask Subtask) []ModelEligibilityRejection {
	var rejections []ModelEligibilityRejection
	for _, node := range nodes {
		for _, model := range node.Models {
			if !node.EngineReady(model.Engine) || !modelMatchesSubtask(subtask, model) {
				continue
			}
			issues := npuModelEligibilityIssues(node, model)
			if issues == 0 {
				continue
			}
			capabilities := engine.Capabilities{}
			for _, status := range node.Engines {
				if strings.EqualFold(status.Name, model.Engine) {
					capabilities = status.Capabilities
					break
				}
			}
			rejections = append(rejections, ModelEligibilityRejection{
				NodeID: node.ID, Model: model.Name, Engine: model.Engine,
				Reasons: explainNPUEligibilityIssues(issues, capabilities),
			})
		}
	}
	return rejections
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
	telemetryAge, telemetryFresh := TelemetryFreshness(node.TelemetryAt, now)

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
	if subtask.RoutingPreference == "latency" {
		modelQuality *= 0.5
	}

	tokensPerSecond := exp.TokensPerSecond
	if tokensPerSecond <= 0 {
		tokensPerSecond = fallbackTokensPerSecond(candidate.modelSizeGB)
	}
	queueDepth := max(0, load-node.ActiveTasks)
	workTokens := max(32.0, float64(utf8.RuneCountInString(subtask.Description)+
		utf8.RuneCountInString(subtask.ExpectedOutput))/4.0)
	modelLoadSeconds := max(0, candidate.modelSizeGB) * 0.05
	estimatedCompletionSeconds := (workTokens/tokensPerSecond)*float64(queueDepth+1) + modelLoadSeconds
	latencyScore := 0.0
	if subtask.RoutingPreference == "latency" {
		latencyScore = cfg.LatencyPreferenceWeight / (1 + estimatedCompletionSeconds)
	}

	var matchedCapabilities []string
	if includeDetails {
		matchedCapabilities = append([]string(nil), candidate.matchedCapabilities...)
	}
	factors := ScoreFactors{
		ModelMatch:                 candidate.modelMatch * cfg.ModelMatchWeight,
		Hardware:                   hardwareScore,
		ActiveTasks:                node.ActiveTasks,
		QueuedTasks:                load - node.ActiveTasks,
		PendingDependents:          pendingDependents,
		CapacityPenalty:            capacityPenalty,
		LoadPenalty:                loadPenalty,
		GPUCompatibilityScore:      gpuCompatibilityScore,
		GPUUtilizationPenalty:      gpuPenalty,
		NPUUtilizationPenalty:      npuPenalty,
		CPUUtilizationPenalty:      cpuPenalty,
		MemoryUsagePenalty:         memoryPenalty,
		CapabilityScore:            capabilityScore,
		MatchedCapabilities:        matchedCapabilities,
		ModelQualityScore:          modelQuality,
		TopologyScore:              topologyScore,
		EstimatedCompletionSeconds: estimatedCompletionSeconds,
		EstimatedTokensPerSecond:   tokensPerSecond,
		ModelSizeGB:                candidate.modelSizeGB,
		LatencyScore:               latencyScore,
		LearnedBias:                exp.LearnedBias * cfg.LearnedBiasWeight,
		TelemetryFresh:             telemetryFresh,
		TelemetryAgeMs:             telemetryAge.Milliseconds(),
	}
	score := factors.ModelMatch + factors.Hardware + factors.GPUCompatibilityScore + factors.CapabilityScore +
		factors.ModelQualityScore + factors.TopologyScore + factors.LatencyScore -
		factors.LoadPenalty - factors.CapacityPenalty -
		factors.GPUUtilizationPenalty - factors.NPUUtilizationPenalty -
		factors.CPUUtilizationPenalty - factors.MemoryUsagePenalty + factors.LearnedBias
	return score, factors
}

// TelemetryFreshness exposes the scorer's established utilization policy.
// Missing and future timestamps are not fresh and have an unknown (zero) age.
func TelemetryFreshness(at, now time.Time) (time.Duration, bool) {
	if at.IsZero() || at.After(now) {
		return 0, false
	}
	age := now.Sub(at)
	return age, age <= telemetryFreshnessWindow
}

func fallbackTokensPerSecond(modelSizeGB float64) float64 {
	if modelSizeGB <= 0 {
		modelSizeGB = 7
	}
	return max(1.0, 24/math.Sqrt(modelSizeGB))
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
