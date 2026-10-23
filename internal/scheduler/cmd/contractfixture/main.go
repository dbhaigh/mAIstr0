package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/scheduler"
)

type fixtureRequest struct {
	Nodes    []cluster.NodeStatus `json:"nodes"`
	Models   []engine.Model       `json:"models"`
	Subtasks []scheduler.Subtask  `json:"subtasks"`
}

type fixture struct {
	Name               string         `json:"name"`
	Request            fixtureRequest `json:"request"`
	ExpectedResponse   map[string]any `json:"expected_response"`
	NormalizationRules []string       `json:"normalization_rules"`
}

func main() {
	generate("assignment", []cluster.NodeStatus{{
		ID: "worker-a", Address: "https://worker-a", Healthy: true, DefaultModel: "general-model",
		Hardware: hardware.Info{Score: 50},
		Models: []engine.Model{{
			Name: "general-model", Engine: "simulated", Tags: []string{"general"},
		}},
	}}, []engine.Model{{
		Name: "general-model", Engine: "simulated", Tags: []string{"general"},
	}}, []scheduler.Subtask{
		{ID: "write", Description: "Write the summary", Role: "writer", TaskType: "general", DependsOn: []string{"research"}},
		{ID: "research", Description: "Research the report", Role: "researcher", TaskType: "general"},
	})
	generate("capability-routing", []cluster.NodeStatus{
		{
			ID: "general-worker", Address: "https://general-worker", Healthy: true,
			Hardware: hardware.Info{Score: 30},
			Models:   []engine.Model{{Name: "general-model", Engine: "simulated", Tags: []string{"general"}}},
		},
		{
			ID: "vision-worker", Address: "https://vision-worker", Healthy: true,
			Hardware: hardware.Info{Score: 20},
			Models:   []engine.Model{{Name: "vision-model", Engine: "simulated", Tags: []string{"vision"}}},
		},
	}, []engine.Model{
		{Name: "general-model", Engine: "simulated", Tags: []string{"general"}},
		{Name: "vision-model", Engine: "simulated", Tags: []string{"vision"}},
	}, []scheduler.Subtask{{
		ID: "inspect", Description: "Inspect the image", Role: "vision", TaskType: "vision",
		Tags: []string{"vision"}, RequiredCapabilities: []string{"vision"},
	}})
	generate("engine-device-capability", []cluster.NodeStatus{{
		ID: "npu-worker", Address: "https://npu-worker", Healthy: true,
		Hardware: hardware.Info{
			Score: 20, HasNPU: true,
			Devices: []hardware.Device{{Kind: "npu", Vendor: "huawei", Name: "Ascend 910", Backends: []string{"cann"}}},
		},
		Engines: []engine.Status{{
			Name: "runtime", Ready: true, ModelCount: 1,
			Capabilities: engine.Capabilities{Devices: []string{"npu"}, Backends: []string{"cann"}},
		}},
		Models: []engine.Model{{
			Name: "npu-model", Engine: "runtime", Tags: []string{"general"}, Device: "npu",
		}},
	}}, []engine.Model{{
		Name: "npu-model", Engine: "runtime", Tags: []string{"general"}, Device: "npu",
	}}, []scheduler.Subtask{{
		ID: "infer", Description: "Run inference", Role: "worker", TaskType: "general",
	}})
}

func generate(name string, nodes []cluster.NodeStatus, models []engine.Model, subtasks []scheduler.Subtask) {
	assignments, err := scheduler.AssignWithExperience(subtasks, nodes, nil)
	if err != nil {
		fail(err)
	}
	document := fixture{
		Name: name,
		Request: fixtureRequest{
			Nodes: nodes, Models: models, Subtasks: subtasks,
		},
		ExpectedResponse: map[string]any{"assignments": assignments},
		NormalizationRules: []string{
			"No normalization; fixed inputs produce deterministic scores and assignments.",
		},
	}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		fail(err)
	}
	data = append(data, '\n')
	output := filepath.Join("..", "..", "contracts", "fixtures", "scheduler", name+".json")
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		fail(err)
	}
	if err := os.WriteFile(output, data, 0o644); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
