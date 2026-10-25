// Command exportshards keeps sharding fixtures tied to the Go wire types.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
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
		"members": {
			Name:             "reconfigure-shard-members",
			Request:          message{Method: "POST", Path: "/api/shards/{id}/members", Schema: "shard-membership.schema.json", Body: orchestrator.ShardMembershipRequest{NodeIDs: group.PinnedNodes}},
			ExpectedResponse: message{Status: 200, Schema: "shard-group.schema.json", Body: group},
		},
		"peer-update": {
			Name:             "offer-paired-peer-executable",
			Request:          message{Method: "GET", Path: "/api/updates/manifest"},
			ExpectedResponse: message{Status: 200, Schema: "peer-update.schema.json", Body: map[string]any{"version": "0.0.24.1", "os": "linux", "arch": "amd64", "sha256": strings.Repeat("a", 64), "size": 14000000}},
		},
		"download-head-model": {
			Name:             "download-recommended-hugging-face-gguf",
			Request:          message{Method: "POST", Path: "/api/nodes/{id}/engines/managed/download-model", Schema: "download-head-model.schema.json", Body: map[string]any{"model": "qwen2.5-3b-instruct-q4", "requested_context": 4096}},
			ExpectedResponse: message{Status: 200, Body: supervisor.LocalHeadModel{ID: strings.Repeat("a", 64), Name: profile.Name, Source: "huggingface", Profile: profile}},
		},
		"local-head-models": {
			Name:    "discover-local-model-heads",
			Request: message{Method: "GET", Path: "/api/nodes/{id}/engines/managed/head-models"},
			ExpectedResponse: message{Status: 200, Schema: "local-head-models.schema.json", Body: supervisor.LocalHeadModels{
				Models:   []supervisor.LocalHeadModel{{ID: strings.Repeat("a", 64), Name: profile.Name, Source: "lmstudio", Profile: profile}},
				Warnings: []string{},
			}},
		},
		"configure-head": {
			Name:             "configure-local-model-head",
			Request:          message{Method: "POST", Path: "/api/nodes/{id}/engines/managed/configure-head", Schema: "configure-local-head.schema.json", Body: map[string]any{"model_id": strings.Repeat("a", 64), "requested_context": 4096}},
			ExpectedResponse: message{Status: 200, Schema: "managed-engine-status.schema.json", Body: supervisor.Status{ID: "maistr0-auto-head", Kind: "llama-server", State: "stopped"}},
		},
		"create-selected-head": {
			Name: "create-shard-group-selected-head",
			Request: message{Method: "POST", Path: "/api/shards", Schema: "create-shard-group.schema.json", Body: orchestrator.CreateShardGroupRequest{
				Model: profile, Mode: group.Mode, PinnedNodes: group.PinnedNodes, HeadNodeID: "worker-1", HeadEngineID: "head",
			}},
			ExpectedResponse: message{Status: 201, Schema: "shard-group.schema.json", Body: group},
		},
		"install-worker": {
			Name:    "install-managed-worker",
			Request: message{Method: "POST", Path: "/api/nodes/{id}/engines/managed/install-worker", Schema: "worker-install-request.schema.json", Body: map[string]string{"source": "official"}},
			ExpectedResponse: message{Status: 200, Schema: "worker-install-result.schema.json", Body: supervisor.WorkerInstallResult{
				Source: "official", Version: "test", Updated: true,
				Engines: []supervisor.Status{{ID: "maistr0-auto-rpc", Kind: "ggml-rpc-server", State: "stopped"}},
			}},
		},
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
		stopped := group
		stopped.State = orchestrator.ShardGroupStopped
		stopped.Members = append([]orchestrator.ShardGroupMember(nil), group.Members...)
		for i := range stopped.Members {
			stopped.Members[i].Started = false
		}
		for _, action := range []string{"start", "stop", "restart"} {
			result := group
			if action == "stop" {
				result = stopped
			}
			fixtures[action] = fixture{
				Name:             "shard-group-" + action,
				Request:          message{Method: "POST", Path: "/api/shards/{id}/" + action},
				ExpectedResponse: message{Status: 200, Schema: "shard-group.schema.json", Body: result},
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
