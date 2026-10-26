package score

import (
	"strings"

	"github.com/maistr0/maistr0/internal/cluster"
)

// Eligibility explains hard filters, not reservation availability. Counts use
// the same predicates as Prepare; no request text or model identifiers escape.
type Eligibility struct {
	Reason           string
	ReadyModels      int
	CompatibleModels int
	ResourceModels   int
	ResourceIssues   uint16
}

// ExplainEligibility is a cold-path diagnostic. Scoring callers should still
// use Prepare, avoiding an extra model scan when diagnostics are disabled.
func ExplainEligibility(node cluster.NodeStatus, subtask Subtask) Eligibility {
	if !node.Healthy {
		return Eligibility{Reason: "unhealthy"}
	}
	capabilities := node.EffectiveCapabilities()
	for _, required := range subtask.RequiredCapabilities {
		if !hasCapability(capabilities, strings.ToLower(strings.TrimSpace(required))) {
			return Eligibility{Reason: "required_capability"}
		}
	}
	result := Eligibility{Reason: "eligible"}
	for _, model := range node.Models {
		if !node.EngineReady(model.Engine) {
			continue
		}
		result.ReadyModels++
		if !modelMatchesSubtask(subtask, model) {
			continue
		}
		result.CompatibleModels++
		issues := npuModelEligibilityIssues(node, model)
		result.ResourceIssues |= uint16(issues)
		if issues == 0 && model.Name != "" {
			result.ResourceModels++
		}
	}
	switch {
	case len(node.Models) == 0:
		result.Reason = "no_models"
	case result.ReadyModels == 0:
		result.Reason = "engine_unready"
	case result.CompatibleModels == 0:
		result.Reason = "model_requirements"
	case result.ResourceModels == 0 && result.ResourceIssues == 0:
		result.Reason = "no_usable_model"
	case result.ResourceModels == 0:
		result.Reason = "accelerator_constraints"
		if npuEligibilityIssue(result.ResourceIssues)&npuIssueDoesNotFit != 0 {
			result.Reason = "accelerator_memory_insufficient"
		}
	}
	return result
}
