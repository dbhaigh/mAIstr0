// Command exportshards keeps sharding fixtures tied to the Go wire types.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/maistr0/maistr0/internal/orchestrator"
	"github.com/maistr0/maistr0/internal/shard"
)

type message struct {
	Method string `json:"method,omitempty"`
	Path   string `json:"path,omitempty"`
	Status int    `json:"status,omitempty"`
	Schema string `json:"schema,omitempty"`
	Body   any    `json:"body,omitempty"`
}

type fixture struct {
	Name               string   `json:"name"`
	Request            message  `json:"request"`
	ExpectedResponse   message  `json:"expected_response"`
	NormalizationRules []string `json:"normalization_rules"`
}

func main() {
	if err := export(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func export() error {
	profile := shard.ModelProfile{
		Name: "large-model", Architecture: "llama", Parameters: 7_000_000_000,
		LayerCount: 32, HiddenSize: 4096, KVHeads: 8, HeadDim: 128,
		VocabSize: 32000, Quantization: "q4_0", FileSizeBytes: 4_000_000_000, RequestedContext: 4096,
	}
	now := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	group := orchestrator.ShardGroup{
		ID: "shard-demo", Model: profile, Mode: shard.ModeLlamaCPPRPC, State: orchestrator.ShardGroupReady,
		PinnedNodes: []string{"worker-1", "worker-2"}, CreatedAt: now, UpdatedAt: now,
		Rationale: []string{"Memory fit on both nodes."},
		Plan: shard.ShardPlan{
			Mode: shard.ModeLlamaCPPRPC, Model: profile,
			RPC: "192.0.2.1:7452,192.0.2.2:7452", TensorSplit: "16,16",
			Rationale: []string{"Memory fit on both nodes."},
			Nodes:     []shard.NodePlan{},
		},
		Members: []orchestrator.ShardGroupMember{},
	}
	for i, id := range group.PinnedNodes {
		relay := fmt.Sprintf("192.0.2.%d:7452", i+1)
		group.Plan.Nodes = append(group.Plan.Nodes, shard.NodePlan{
			NodeID: id, RPCAddress: relay, StartLayer: i * 16, EndLayer: (i + 1) * 16, TensorSplitLayers: 16,
			Devices: []shard.DeviceAllocation{{
				Kind: "gpu", Vendor: "nvidia", Name: "GPU 0", Index: 0,
				StartLayer: i * 16, EndLayer: (i + 1) * 16,
				WeightsBytes: 2_000_000_000, KVCacheBytes: 268_435_456, SafetyMarginBytes: 536_870_912,
				RequiredBytes: 2_805_306_368, FreeMemoryBytes: 8_589_934_592,
			}},
		})
		group.Members = append(group.Members, orchestrator.ShardGroupMember{
			NodeID: id, Address: "https://" + id + ":7451", Role: "worker",
			EngineID: "rpc", EngineKind: "ggml-rpc-server", Started: true,
			RPCRelayEndpoint: relay, RPCBackendAddress: "127.0.0.1:50052",
		})
	}
	group.Members = append(group.Members, orchestrator.ShardGroupMember{
		NodeID: "worker-1", Address: "https://worker-1:7451", Role: "head",
		EngineID: "head", EngineKind: "llama-server", Started: true,
	})
	request := orchestrator.CreateShardGroupRequest{Model: profile, Mode: group.Mode, PinnedNodes: group.PinnedNodes}
	fixtures := map[string]fixture{
		"create": {
			Name:             "create-shard-group",
			Request:          message{Method: "POST", Path: "/api/shards", Schema: "create-shard-group.schema.json", Body: request},
			ExpectedResponse: message{Status: 201, Schema: "shard-group.schema.json", Body: group},
		},
		"get": {
			Name: "get-shard-group", Request: message{Method: "GET", Path: "/api/shards/{id}"},
			ExpectedResponse: message{Status: 200, Schema: "shard-group.schema.json", Body: group},
		},
		"list": {
			Name: "list-shard-groups", Request: message{Method: "GET", Path: "/api/shards"},
			ExpectedResponse: message{Status: 200, Schema: "shard-group-list.schema.json", Body: struct {
				Groups []orchestrator.ShardGroup `json:"groups"`
			}{Groups: []orchestrator.ShardGroup{}}},
		},
		"delete": {
			Name: "delete-shard-group", Request: message{Method: "DELETE", Path: "/api/shards/{id}"},
			ExpectedResponse: message{Status: 204},
		},
	}
	for _, state := range []orchestrator.ShardGroupState{orchestrator.ShardGroupDegraded, orchestrator.ShardGroupFailed} {
		snapshot := group
		snapshot.State = state
		snapshot.Error = "one or more managed engines are not ready"
		if state == orchestrator.ShardGroupFailed {
			snapshot.Members = append([]orchestrator.ShardGroupMember(nil), group.Members...)
			for i := range snapshot.Members {
				snapshot.Members[i].Started = false
			}
		}
		fixtures[string(state)] = fixture{
			Name: "shard-group-" + string(state), Request: message{Method: "GET", Path: "/api/shards/{id}"},
			ExpectedResponse: message{Status: 200, Schema: "shard-group.schema.json", Body: snapshot},
		}
	}
	vllm := group
	vllm.Mode, vllm.Plan.Mode = shard.ModeVLLMMP, shard.ModeVLLMMP
	vllm.Plan.RPC, vllm.Plan.TensorSplit = "", ""
	vllm.Plan.TensorParallelSize, vllm.Plan.PipelineParallelSize = 1, 2
	vllm.Plan.VLLMParallelism = shard.VLLMParallelismPipeline
	vllm.Plan.Nodes = append([]shard.NodePlan(nil), group.Plan.Nodes...)
	for i := range vllm.Plan.Nodes {
		vllm.Plan.Nodes[i].RPCAddress = ""
		vllm.Plan.Nodes[i].TensorSplitLayers = 0
	}
	vllm.Members = []orchestrator.ShardGroupMember{
		{NodeID: "worker-2", Address: "https://worker-2:7451", Role: "worker", EngineID: "vllm", EngineKind: "vllm", Rank: 1, MasterAddr: "192.0.2.1", Started: true},
		{NodeID: "worker-1", Address: "https://worker-1:7451", Role: "head", EngineID: "vllm", EngineKind: "vllm", MasterAddr: "192.0.2.1", Started: true},
	}
	request.Mode = shard.ModeVLLMMP
	fixtures["create-vllm"] = fixture{
		Name:             "create-vllm-shard-group",
		Request:          message{Method: "POST", Path: "/api/shards", Schema: "create-shard-group.schema.json", Body: request},
		ExpectedResponse: message{Status: 201, Schema: "shard-group.schema.json", Body: vllm},
	}
	for name, item := range fixtures {
		item.NormalizationRules = []string{"Fixed IDs, addresses, and RFC3339 timestamps; generated from Go wire types."}
		data, err := json.MarshalIndent(item, "", "  ")
		if err != nil {
			return fmt.Errorf("encode %s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join("fixtures", "shards", name+".json"), append(data, '\n'), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}
