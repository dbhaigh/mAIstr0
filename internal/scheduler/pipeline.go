package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

var ErrNoPipelinePlacement = errors.New("scheduler: no complete pipeline placement is available")

type pipelineCandidate struct {
	node        cluster.NodeStatus
	maxLayers   int
	stageEngine string
}

// PlacePipeline constructs a complete, contiguous layer plan from current
// readiness snapshots. max_layers_it_can_host is treated as a per-stage
// upper bound; each selected node receives a non-empty range within it.
func PlacePipeline(
	model config.ShardedModelConfig,
	nodes []cluster.NodeStatus,
	coordinatorEpoch string,
) (config.PipelinePlan, error) {
	model.Normalize()
	if err := model.Validate(); err != nil {
		return config.PipelinePlan{}, fmt.Errorf("invalid sharded model: %w", err)
	}
	if strings.TrimSpace(coordinatorEpoch) == "" {
		return config.PipelinePlan{}, errors.New("scheduler: pipeline coordinator epoch must not be empty")
	}

	candidates := make([]pipelineCandidate, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if !node.Healthy || node.ID == "" || node.Address == "" ||
			node.MaxLayersItCanHost <= 0 || !supportsShardModel(node, model) {
			continue
		}
		if _, exists := seen[node.ID]; exists {
			continue
		}
		stageEngine, maxLayers := readyStageEngine(node, model.SourceModelID)
		if stageEngine == "" || maxLayers <= 0 {
			continue
		}
		maxLayers = min(maxLayers, node.MaxLayersItCanHost)
		seen[node.ID] = struct{}{}
		candidates = append(candidates, pipelineCandidate{
			node: node, maxLayers: maxLayers, stageEngine: stageEngine,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].maxLayers != candidates[j].maxLayers {
			return candidates[i].maxLayers > candidates[j].maxLayers
		}
		if candidates[i].node.FastScore != candidates[j].node.FastScore {
			return candidates[i].node.FastScore > candidates[j].node.FastScore
		}
		return candidates[i].node.ID < candidates[j].node.ID
	})

	selected := make([]pipelineCandidate, 0, len(candidates))
	totalCapacity := 0
	for _, candidate := range candidates {
		selected = append(selected, candidate)
		if candidate.maxLayers >= model.TotalLayers-totalCapacity {
			totalCapacity = model.TotalLayers
		} else {
			totalCapacity += candidate.maxLayers
		}
		if len(selected) >= 2 && totalCapacity >= model.TotalLayers {
			break
		}
	}
	if len(selected) < 2 || totalCapacity < model.TotalLayers {
		return config.PipelinePlan{}, ErrNoPipelinePlacement
	}

	plan := config.PipelinePlan{
		Model: model, PlanVersion: 1, CoordinatorEpoch: strings.TrimSpace(coordinatorEpoch),
		Stages: make([]config.StageSpec, 0, len(selected)),
	}
	layer := 0
	for i, candidate := range selected {
		remaining := model.TotalLayers - layer
		remainingCapacity := 0
		for _, next := range selected[i+1:] {
			if next.maxLayers >= model.TotalLayers-remainingCapacity {
				remainingCapacity = model.TotalLayers
			} else {
				remainingCapacity += next.maxLayers
			}
		}
		minLayers := max(1, remaining-remainingCapacity)
		maxLayers := min(candidate.maxLayers, remaining-(len(selected)-i-1))
		targetLayers := (remaining + len(selected) - i - 1) / (len(selected) - i)
		layerCount := min(max(targetLayers, minLayers), maxLayers)
		if layerCount < minLayers || layerCount > maxLayers {
			return config.PipelinePlan{}, ErrNoPipelinePlacement
		}
		plan.Stages = append(plan.Stages, config.StageSpec{
			NodeID: candidate.node.ID, StartLayer: layer, EndLayer: layer + layerCount,
			Engine: candidate.stageEngine,
		})
		layer += layerCount
	}
	plan.Normalize()
	if err := plan.Validate(); err != nil {
		return config.PipelinePlan{}, fmt.Errorf("scheduler built invalid pipeline plan: %w", err)
	}
	return plan, nil
}

func supportsShardModel(node cluster.NodeStatus, model config.ShardedModelConfig) bool {
	for _, advertised := range node.SupportedShardModels {
		if strings.EqualFold(advertised, model.SourceModelID) ||
			strings.EqualFold(advertised, model.LogicalID) {
			return true
		}
	}
	return false
}

func readyStageEngine(node cluster.NodeStatus, modelID string) (string, int) {
	engines := append([]engine.Status(nil), node.Engines...)
	sort.Slice(engines, func(i, j int) bool {
		return strings.ToLower(engines[i].Name) < strings.ToLower(engines[j].Name)
	})
	for _, candidate := range engines {
		if !candidate.Ready || strings.TrimSpace(candidate.Name) == "" ||
			candidate.PipelineStage == nil ||
			candidate.PipelineStage.ProtocolVersion != engine.PipelineStageProtocolVersion ||
			!candidate.PipelineStage.SupportsModel(modelID, 1) {
			continue
		}
		for _, supported := range candidate.PipelineStage.Models {
			if strings.EqualFold(supported.ModelID, modelID) {
				return strings.ToLower(strings.TrimSpace(candidate.Name)), supported.MaxLayers
			}
		}
	}
	return "", 0
}
