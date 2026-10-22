package config

import (
	"errors"
	"fmt"
	"strings"
)

// ShardedModelConfig identifies one immutable model revision and its layer
// count. Tokenizer and model-config identities must match across all stages.
type ShardedModelConfig struct {
	LogicalID     string `json:"logical_id"`
	SourceModelID string `json:"source_model_id"`
	Revision      string `json:"revision"`
	TokenizerID   string `json:"tokenizer_id"`
	ModelConfigID string `json:"model_config_id"`
	TotalLayers   int    `json:"total_layers"`
}

func (cfg *ShardedModelConfig) Normalize() {
	cfg.LogicalID = strings.TrimSpace(cfg.LogicalID)
	cfg.SourceModelID = strings.TrimSpace(cfg.SourceModelID)
	cfg.Revision = strings.TrimSpace(cfg.Revision)
	cfg.TokenizerID = strings.TrimSpace(cfg.TokenizerID)
	cfg.ModelConfigID = strings.TrimSpace(cfg.ModelConfigID)
}

func (cfg ShardedModelConfig) Validate() error {
	var violations []error
	if cfg.LogicalID == "" {
		violations = append(violations, errors.New("logical_id must not be empty"))
	}
	if cfg.SourceModelID == "" {
		violations = append(violations, errors.New("source_model_id must not be empty"))
	}
	if cfg.Revision == "" {
		violations = append(violations, errors.New("revision must not be empty"))
	}
	if cfg.TokenizerID == "" {
		violations = append(violations, errors.New("tokenizer_id must not be empty"))
	}
	if cfg.ModelConfigID == "" {
		violations = append(violations, errors.New("model_config_id must not be empty"))
	}
	if cfg.TotalLayers <= 0 {
		violations = append(violations, errors.New("total_layers must be greater than zero"))
	}
	return errors.Join(violations...)
}

// StageSpec assigns a contiguous, half-open layer range to a node and engine.
type StageSpec struct {
	NodeID     string `json:"node_id"`
	StartLayer int    `json:"start_layer"`
	EndLayer   int    `json:"end_layer"`
	Engine     string `json:"engine"`
}

func (stage *StageSpec) Normalize() {
	stage.NodeID = strings.TrimSpace(stage.NodeID)
	stage.Engine = strings.ToLower(strings.TrimSpace(stage.Engine))
}

func (stage StageSpec) Validate() error {
	var violations []error
	if stage.NodeID == "" {
		violations = append(violations, errors.New("node_id must not be empty"))
	}
	if stage.Engine == "" {
		violations = append(violations, errors.New("engine must not be empty"))
	}
	if stage.StartLayer < 0 {
		violations = append(violations, errors.New("start_layer must not be negative"))
	}
	if stage.EndLayer <= stage.StartLayer {
		violations = append(violations, errors.New("end_layer must be greater than start_layer"))
	}
	return errors.Join(violations...)
}

// PipelinePlan orders stages for one model revision under a coordinator epoch.
type PipelinePlan struct {
	Model            ShardedModelConfig `json:"model"`
	Stages           []StageSpec        `json:"stages"`
	PlanVersion      uint64             `json:"plan_version"`
	CoordinatorEpoch string             `json:"coordinator_epoch"`
}

func (plan *PipelinePlan) Normalize() {
	plan.Model.Normalize()
	plan.CoordinatorEpoch = strings.TrimSpace(plan.CoordinatorEpoch)
	if plan.PlanVersion == 0 {
		plan.PlanVersion = 1
	}
	for i := range plan.Stages {
		plan.Stages[i].Normalize()
	}
}

func (plan PipelinePlan) Validate() error {
	var violations []error
	if err := plan.Model.Validate(); err != nil {
		violations = append(violations, fmt.Errorf("model: %w", err))
	}
	if plan.PlanVersion == 0 {
		violations = append(violations, errors.New("plan_version must be greater than zero"))
	}
	if plan.CoordinatorEpoch == "" {
		violations = append(violations, errors.New("coordinator_epoch must not be empty"))
	}
	if len(plan.Stages) < 2 {
		violations = append(violations, errors.New("stages must contain at least two entries"))
	}

	nodes := make(map[string]struct{}, len(plan.Stages))
	for i, stage := range plan.Stages {
		if err := stage.Validate(); err != nil {
			violations = append(violations, fmt.Errorf("stages[%d]: %w", i, err))
		}
		if stage.EndLayer > plan.Model.TotalLayers {
			violations = append(violations, fmt.Errorf(
				"stages[%d].end_layer must not exceed total_layers", i,
			))
		}
		nodes[stage.NodeID] = struct{}{}
		if i == 0 {
			if stage.StartLayer != 0 {
				violations = append(violations, errors.New("stages must start at layer zero"))
			}
			continue
		}
		previous := plan.Stages[i-1]
		switch {
		case stage.StartLayer < previous.EndLayer:
			violations = append(violations, fmt.Errorf(
				"stages[%d] overlaps stages[%d]", i, i-1,
			))
		case stage.StartLayer > previous.EndLayer:
			violations = append(violations, fmt.Errorf(
				"stages[%d] leaves a gap after stages[%d]", i, i-1,
			))
		}
	}
	if len(nodes) < 2 {
		violations = append(violations, errors.New("stages must be assigned to at least two distinct nodes"))
	}
	if len(plan.Stages) > 0 && plan.Stages[len(plan.Stages)-1].EndLayer != plan.Model.TotalLayers {
		violations = append(violations, errors.New("stages must cover all model layers"))
	}
	return errors.Join(violations...)
}
