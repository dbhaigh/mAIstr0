package scheduler

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

var decisionSequence atomic.Uint64

// LogDecision emits only fixed reason codes, numeric measurements, and hashed
// references. Callers must never pass request text as stage or reason.
// Debug gating precedes hashing and attribute construction.
func LogDecision(stage, reason string, node cluster.NodeStatus, model string, value float64, factors ScoreFactors) {
	logger := slog.Default()
	if !logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	logDecision(logger, slog.LevelDebug, stage, reason, node, model, value, factors, score.Eligibility{})
}

func logDecision(logger *slog.Logger, level slog.Level, stage, reason string, node cluster.NodeStatus, model string, value float64, f ScoreFactors, eligibility score.Eligibility) {
	if !logger.Enabled(context.Background(), level) {
		return
	}
	if math.IsInf(value, 0) || math.IsNaN(value) {
		value = 0
	}
	if stage != "planning" || (reason != "eligible" && reason != "selected") {
		age, fresh := score.TelemetryFreshness(node.TelemetryAt, time.Now())
		f.TelemetryAgeMs, f.TelemetryFresh = age.Milliseconds(), fresh
	}
	ranking := "not_scored"
	switch stage {
	case "planning":
		ranking = "additive"
	case "pipeline":
		ranking = "max_layers_then_fast_score"
	}
	// Do not log ScoreFactors wholesale: MatchedCapabilities can contain
	// operator-provided strings. Identifiers may also contain private names.
	logger.LogAttrs(context.Background(), level, "scheduler decision",
		slog.String("component", "scheduler"),
		slog.String("stage", stage), slog.String("reason", reason),
		slog.String("ranking", ranking),
		slog.String("node_ref", decisionReference(node.ID)),
		slog.String("model_ref", decisionReference(model)),
		slog.Bool("healthy", node.Healthy),
		slog.Int("active", node.ActiveTasks), slog.Int("queued", node.QueuedTasks),
		slog.Int("effective_queued", f.QueuedTasks),
		slog.Int("capacity_limit", node.MaxConcurrentTasks),
		slog.Int("advertised_available", node.AvailableCapacity),
		slog.Int("max_layers", node.MaxLayersItCanHost),
		slog.Bool("reservation_required", true),
		slog.Bool("eligibility_evaluated", eligibility.Reason != ""),
		slog.Int("ready_models", eligibility.ReadyModels),
		slog.Int("compatible_models", eligibility.CompatibleModels),
		slog.Int("resource_eligible_models", eligibility.ResourceModels),
		slog.Int("accelerator_issue_mask", int(eligibility.ResourceIssues)),
		slog.Float64("score", value),
		slog.Float64("model_match", f.ModelMatch), slog.Float64("hardware", f.Hardware),
		slog.Float64("capability", f.CapabilityScore), slog.Float64("quality", f.ModelQualityScore),
		slog.Float64("gpu_compatibility", f.GPUCompatibilityScore),
		slog.Float64("topology", f.TopologyScore), slog.Float64("latency", f.LatencyScore),
		slog.Float64("learned_bias", f.LearnedBias),
		slog.Float64("load_penalty", f.LoadPenalty), slog.Float64("capacity_penalty", f.CapacityPenalty),
		slog.Float64("gpu_penalty", f.GPUUtilizationPenalty), slog.Float64("npu_penalty", f.NPUUtilizationPenalty),
		slog.Float64("cpu_penalty", f.CPUUtilizationPenalty), slog.Float64("memory_penalty", f.MemoryUsagePenalty),
		slog.Bool("telemetry_fresh", f.TelemetryFresh), slog.Int64("telemetry_age_ms", f.TelemetryAgeMs),
		slog.Bool("telemetry_present", !node.TelemetryAt.IsZero()),
		slog.Float64("estimated_completion_seconds", f.EstimatedCompletionSeconds),
	)
}

func decisionReference(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:12])
}
