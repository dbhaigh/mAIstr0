package scheduler

import (
	"errors"
	"reflect"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestPlacePipelineBuildsContiguousRangesFromReadyNodes(t *testing.T) {
	model := config.ShardedModelConfig{
		LogicalID: "large-model", SourceModelID: "source-model", Revision: "rev-1",
		TokenizerID: "tokenizer-1", ModelConfigID: "config-1", TotalLayers: 10,
	}
	nodes := []cluster.NodeStatus{
		{
			ID: "offline", Address: "https://offline", Healthy: false,
			SupportedShardModels: []string{"source-model"}, MaxLayersItCanHost: 10,
			Engines: []engine.Status{pipelineStageStatus("vllm", "source-model", 10)},
		},
		{
			ID: "worker-b", Address: "https://worker-b", Healthy: true, FastScore: 4,
			SupportedShardModels: []string{"source-model"}, MaxLayersItCanHost: 6,
			Engines: []engine.Status{pipelineStageStatus("vllm", "source-model", 6)},
		},
		{
			ID: "worker-a", Address: "https://worker-a", Healthy: true, FastScore: 5,
			SupportedShardModels: []string{"large-model"}, MaxLayersItCanHost: 6,
			Engines: []engine.Status{
				pipelineStageStatus("llamacpp", "source-model", 6),
				pipelineStageStatus("vllm", "source-model", 6),
			},
		},
		{
			ID: "unready", Address: "https://unready", Healthy: true,
			SupportedShardModels: []string{"source-model"}, MaxLayersItCanHost: 10,
			Engines: []engine.Status{{Name: "vllm", Ready: false, PipelineStage: pipelineStageStatus("vllm", "source-model", 10).PipelineStage}},
		},
	}

	plan, err := PlacePipeline(model, nodes, "epoch-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("placed invalid plan: %v", err)
	}
	if len(plan.Stages) != 2 || plan.Stages[0].NodeID != "worker-a" ||
		plan.Stages[1].NodeID != "worker-b" || plan.Stages[0].Engine != "llamacpp" {
		t.Fatalf("unexpected stage placement: %+v", plan.Stages)
	}
	if plan.Stages[0].StartLayer != 0 || plan.Stages[0].EndLayer != 5 ||
		plan.Stages[1].StartLayer != 5 || plan.Stages[1].EndLayer != 10 {
		t.Fatalf("layer ranges = %+v, want [0,5), [5,10)", plan.Stages)
	}
	again, err := PlacePipeline(model, nodes, "epoch-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan, again) {
		t.Fatalf("placement was not deterministic: first=%+v second=%+v", plan, again)
	}
}

func TestPlacePipelineRejectsIncompleteReadiness(t *testing.T) {
	model := config.ShardedModelConfig{
		LogicalID: "large-model", SourceModelID: "source-model", Revision: "rev-1",
		TokenizerID: "tokenizer-1", ModelConfigID: "config-1", TotalLayers: 10,
	}
	nodes := []cluster.NodeStatus{{
		ID: "worker", Address: "https://worker", Healthy: true,
		SupportedShardModels: []string{"source-model"}, MaxLayersItCanHost: 10,
		Engines: []engine.Status{pipelineStageStatus("vllm", "source-model", 10)},
	}}
	if _, err := PlacePipeline(model, nodes, "epoch-1"); !errors.Is(err, ErrNoPipelinePlacement) {
		t.Fatalf("single-node plan error = %v, want ErrNoPipelinePlacement", err)
	}
	nodes = append(nodes, cluster.NodeStatus{
		ID: "unsupported", Address: "https://unsupported", Healthy: true,
		SupportedShardModels: []string{"other"}, MaxLayersItCanHost: 10,
		Engines: []engine.Status{pipelineStageStatus("vllm", "other", 10)},
	})
	if _, err := PlacePipeline(model, nodes, "epoch-1"); !errors.Is(err, ErrNoPipelinePlacement) {
		t.Fatalf("unsupported node plan error = %v, want ErrNoPipelinePlacement", err)
	}
	nodes = append(nodes, cluster.NodeStatus{
		ID: "declared-only", Address: "https://declared-only", Healthy: true,
		SupportedShardModels: []string{"source-model"}, MaxLayersItCanHost: 10,
		Engines: []engine.Status{{Name: "vllm", Ready: true}},
	})
	if _, err := PlacePipeline(model, nodes, "epoch-1"); !errors.Is(err, ErrNoPipelinePlacement) {
		t.Fatalf("declaration-only node plan error = %v, want ErrNoPipelinePlacement", err)
	}
}

func pipelineStageStatus(runtime, modelID string, maxLayers int) engine.Status {
	return engine.Status{
		Name: runtime, Ready: true,
		PipelineStage: &engine.PipelineStageCapabilities{
			ProtocolVersion: engine.PipelineStageProtocolVersion,
			Runtime:         runtime,
			Models:          []engine.PipelineStageModelCapability{{ModelID: modelID, MaxLayers: maxLayers}},
			TensorDTypes:    []engine.TensorDType{engine.TensorFloat16, engine.TensorFloat32},
		},
	}
}
