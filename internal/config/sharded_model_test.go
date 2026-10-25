package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestShardedModelTypesNormalizeAndRoundTrip(t *testing.T) {
	plan := PipelinePlan{
		Model: ShardedModelConfig{
			LogicalID: " logical-model ", SourceModelID: " source-model ",
			Revision: " rev-1 ", TokenizerID: " tokenizer-1 ",
			ModelConfigID: " config-1 ", TotalLayers: 8,
		},
		Stages: []StageSpec{
			{NodeID: " node-a ", StartLayer: 0, EndLayer: 4, Engine: " VLLM "},
			{NodeID: " node-b ", StartLayer: 4, EndLayer: 8, Engine: " Ollama "},
		},
		CoordinatorEpoch: " epoch-1 ",
	}
	plan.Normalize()
	if err := plan.Validate(); err != nil {
		t.Fatalf("normalized plan is invalid: %v", err)
	}
	if plan.PlanVersion != 1 {
		t.Fatalf("normalized plan version = %d, want 1", plan.PlanVersion)
	}
	if plan.Model.LogicalID != "logical-model" || plan.Stages[0].Engine != "vllm" ||
		plan.Stages[1].Engine != "ollama" || plan.CoordinatorEpoch != "epoch-1" {
		t.Fatalf("plan was not normalized: %+v", plan)
	}

	values := []any{
		plan.Model,
		plan.Stages[0],
		plan,
	}
	for _, value := range values {
		t.Run(reflect.TypeOf(value).Name(), func(t *testing.T) {
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			decoded := reflect.New(reflect.TypeOf(value))
			if err := json.Unmarshal(encoded, decoded.Interface()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(value, decoded.Elem().Interface()) {
				t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", decoded.Elem().Interface(), value)
			}
		})
	}
}

func TestStageSpecValidateRejectsInvalidLayerRanges(t *testing.T) {
	tests := []struct {
		name  string
		stage StageSpec
		want  string
	}{
		{
			name:  "negative start",
			stage: StageSpec{NodeID: "node-a", Engine: "vllm", StartLayer: -1, EndLayer: 2},
			want:  "start_layer",
		},
		{
			name:  "empty range",
			stage: StageSpec{NodeID: "node-a", Engine: "vllm", StartLayer: 2, EndLayer: 2},
			want:  "end_layer",
		},
		{
			name:  "reversed range",
			stage: StageSpec{NodeID: "node-a", Engine: "vllm", StartLayer: 4, EndLayer: 2},
			want:  "end_layer",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.stage.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want it to contain %q", err, test.want)
			}
		})
	}
}

func TestPipelinePlanValidateRejectsInvalidLayerCoverage(t *testing.T) {
	tests := []struct {
		name   string
		stages []StageSpec
		want   string
	}{
		{
			name: "overlapping stages",
			stages: []StageSpec{
				{NodeID: "node-a", Engine: "vllm", StartLayer: 0, EndLayer: 5},
				{NodeID: "node-b", Engine: "vllm", StartLayer: 4, EndLayer: 8},
			},
			want: "overlaps",
		},
		{
			name: "gap between stages",
			stages: []StageSpec{
				{NodeID: "node-a", Engine: "vllm", StartLayer: 0, EndLayer: 3},
				{NodeID: "node-b", Engine: "vllm", StartLayer: 4, EndLayer: 8},
			},
			want: "gap",
		},
		{
			name: "stage exceeds model layer count",
			stages: []StageSpec{
				{NodeID: "node-a", Engine: "vllm", StartLayer: 0, EndLayer: 4},
				{NodeID: "node-b", Engine: "vllm", StartLayer: 4, EndLayer: 9},
			},
			want: "total_layers",
		},
		{
			name: "stages do not start at zero",
			stages: []StageSpec{
				{NodeID: "node-a", Engine: "vllm", StartLayer: 1, EndLayer: 4},
				{NodeID: "node-b", Engine: "vllm", StartLayer: 4, EndLayer: 8},
			},
			want: "start at layer zero",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := validPipelinePlan()
			plan.Stages = test.stages
			err := plan.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want it to contain %q", err, test.want)
			}
		})
	}
}

func TestPipelinePlanValidateRequiresDistinctNodes(t *testing.T) {
	plan := validPipelinePlan()
	plan.Stages[1].NodeID = plan.Stages[0].NodeID
	if err := plan.Validate(); err == nil || !strings.Contains(err.Error(), "distinct nodes") {
		t.Fatalf("validation error = %v, want distinct-node error", err)
	}
}

func validPipelinePlan() PipelinePlan {
	return PipelinePlan{
		Model: ShardedModelConfig{
			LogicalID: "logical-model", SourceModelID: "source-model",
			Revision: "rev-1", TokenizerID: "tokenizer-1",
			ModelConfigID: "config-1", TotalLayers: 8,
		},
		Stages: []StageSpec{
			{NodeID: "node-a", Engine: "vllm", StartLayer: 0, EndLayer: 4},
			{NodeID: "node-b", Engine: "vllm", StartLayer: 4, EndLayer: 8},
		},
		PlanVersion: 1, CoordinatorEpoch: "epoch-1",
	}
}
