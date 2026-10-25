package contracts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestRegisteredHTTPRoutesAreDocumented(t *testing.T) {
	specPath := filepath.Join("openapi.yaml")
	spec, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest, operations := parseOpenAPI(t, string(spec))
	for _, server := range []struct {
		name string
		file string
	}{
		{"orchestrator", filepath.Join("..", "internal", "orchestrator", "server.go")},
		{"node", filepath.Join("..", "internal", "nodeagent", "agent_server.go")},
	} {
		registered := parseRegisteredRoutes(t, server.file)
		for route := range registered {
			if _, ok := manifest[server.name+"|"+route]; !ok {
				t.Errorf("%s route %q is missing from OpenAPI x-maistr0-registered-routes", server.name, route)
			}
			method, path, hasMethod := strings.Cut(route, " ")
			if !hasMethod {
				path = route
			}
			if _, ok := operations[path]; !ok {
				t.Errorf("%s route %q has no OpenAPI path", server.name, route)
			} else if hasMethod && !operations[path][strings.ToLower(method)] {
				t.Errorf("%s route %q has no OpenAPI operation", server.name, route)
			}
		}
		for route := range manifest {
			serverName, pattern, _ := strings.Cut(route, "|")
			if serverName == server.name {
				if _, ok := registered[pattern]; !ok {
					t.Errorf("OpenAPI lists unregistered %s route %q", server.name, pattern)
				}
			}
		}
	}
}

func parseRegisteredRoutes(t *testing.T, filename string) map[string]struct{} {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatalf("parse route registrations in %s: %v", filename, err)
	}
	routes := make(map[string]struct{})
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "Handle" && selector.Sel.Name != "HandleFunc") {
			return true
		}
		receiver, ok := selector.X.(*ast.Ident)
		if !ok || receiver.Name != "mux" || len(call.Args) == 0 {
			return true
		}
		pattern, ok := call.Args[0].(*ast.BasicLit)
		if !ok || pattern.Kind != token.STRING {
			t.Errorf("%s has a nonliteral mux route; update this test to enumerate it", filename)
			return true
		}
		value, err := strconv.Unquote(pattern.Value)
		if err != nil {
			t.Errorf("unquote mux route in %s: %v", filename, err)
			return true
		}
		routes[value] = struct{}{}
		return true
	})
	return routes
}

func parseOpenAPI(t *testing.T, content string) (map[string]struct{}, map[string]map[string]bool) {
	t.Helper()
	manifest := make(map[string]struct{})
	operations := make(map[string]map[string]bool)
	inPaths, inRouteManifest := false, false
	currentPath := ""
	for _, line := range strings.Split(content, "\n") {
		if strings.TrimSpace(line) == "paths:" {
			inPaths, inRouteManifest = true, false
			continue
		}
		if strings.TrimSpace(line) == "x-maistr0-registered-routes:" {
			inPaths, inRouteManifest = false, true
			continue
		}
		if len(line) > 0 && line[0] != ' ' {
			inPaths, inRouteManifest = false, false
		}
		if inPaths {
			indent := len(line) - len(strings.TrimLeft(line, " "))
			trimmed := strings.TrimSpace(line)
			if indent == 2 && strings.HasPrefix(trimmed, "/") && strings.HasSuffix(trimmed, ":") {
				currentPath = strings.TrimSuffix(trimmed, ":")
				if _, exists := operations[currentPath]; exists {
					t.Errorf("duplicate OpenAPI path %q", currentPath)
				}
				operations[currentPath] = make(map[string]bool)
				continue
			}
			if indent == 4 && currentPath != "" && strings.HasSuffix(trimmed, ":") {
				method := strings.TrimSuffix(trimmed, ":")
				switch method {
				case "get", "post", "put", "patch", "delete", "options", "head", "trace":
					operations[currentPath][method] = true
				}
			}
		}
		if inRouteManifest {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "- ") {
				entry, err := strconv.Unquote(strings.TrimPrefix(trimmed, "- "))
				if err != nil {
					t.Fatalf("parse OpenAPI route manifest entry %q: %v", trimmed, err)
				}
				if _, exists := manifest[entry]; exists {
					t.Errorf("duplicate OpenAPI route manifest entry %q", entry)
				}
				manifest[entry] = struct{}{}
			}
		}
	}
	return manifest, operations
}

func TestRepresentativeValuesValidateAgainstJSONSchemas(t *testing.T) {
	now := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	subtask := scheduler.Subtask{
		ID: "work", Description: "Do the work", TaskType: "general", Tags: []string{},
	}
	assignment := scheduler.Assignment{
		Subtask: subtask, NodeID: "worker-1", Address: "https://worker-1", Model: "general",
		Factors: scheduler.ScoreFactors{},
	}
	graph := scheduler.NewTaskGraph()
	if err := graph.AddNode(subtask); err != nil {
		t.Fatal(err)
	}
	result := taskmgr.SubtaskResult{Assignment: assignment, Status: taskmgr.StatusPending}
	task := taskmgr.Task{
		ID: "task-1", Description: "Do the work", Status: taskmgr.StatusPending,
		Subtasks: []taskmgr.SubtaskResult{result}, Graph: graph, CreatedAt: now, UpdatedAt: now,
	}
	nodeConfig := config.DefaultNodeConfig()
	nodeConfig.NodeID = "worker-1"
	nodeConfig.DisabledModels = []string{}
	nodeConfig.ManagedEngineBinaryAllowlist = []string{"/opt/maistr0/bin"}
	nodeConfig.ManagedEngineModelAllowlist = []string{"/models"}
	nodeConfig.ManagedEngines = []config.ManagedEngineConfig{{
		ID: "llama-main", Kind: config.ManagedEngineLlamaServer,
		BinaryPath: "/opt/maistr0/bin/llama-server", ModelPath: "/models/model.gguf",
		Options: config.ManagedEngineOptions{LlamaServer: &config.LlamaServerOptions{
			Host: "127.0.0.1", Port: 8080, ContextSize: 4096,
		}},
	}}
	orchestratorConfig := config.DefaultOrchestratorConfig()
	nodeStatus := cluster.NodeStatus{
		ID: "worker-1", Version: "1.0.0", Address: "https://worker-1",
		Hardware:             hardware.Info{OS: "linux", Arch: "amd64", CPUCores: 8, TotalRAMMB: 16384, Score: 10},
		Models:               []engine.Model{{Name: "general", Engine: "simulated", Tags: []string{}}},
		SupportedShardModels: []string{"large-model"}, MaxLayersItCanHost: 16,
		CurrentStageAssignments: 1, FastScore: 1, LastSeen: now, Healthy: true,
		GPUHistory:    []hardware.GPUUsageSample{{Timestamp: now, Available: true, Utilization: 40}},
		NPUHistory:    []hardware.NPUUsageSample{{Timestamp: now, Available: false}},
		SystemHistory: []hardware.SystemUsageSample{{Timestamp: now, CPUAvailable: true, MemoryAvailable: true}},
	}
	snapshot := memory.Snapshot{
		Experiences: []memory.Experience{{
			ID: 1, Kind: "task", Description: "Completed work", Success: true, CreatedAt: now,
		}},
		Facts: []memory.Fact{{Key: "key", Value: "value", CreatedAt: now, UpdatedAt: now}},
		Performances: []memory.Performance{{
			NodeID: "worker-1", Model: "general", TaskType: "general", LastUsed: now,
		}},
		Dialogues: []memory.DialogueRecord{{
			ID: "dialogue-1", Participants: []string{"worker-1"}, Turns: []memory.Turn{},
			CreatedAt: now, UpdatedAt: now,
		}},
	}

	shardProfile := map[string]any{
		"name": "large-model", "architecture": "llama", "parameters": 7000000000,
		"layer_count": 32, "hidden_size": 4096, "kv_heads": 32, "head_dim": 128,
		"vocab_size": 32000, "quantization": "q4_0", "file_size_bytes": 4000000000,
		"requested_context": 4096,
	}
	shardPlan := map[string]any{
		"mode": "llamacpp-rpc", "model": shardProfile,
		"nodes": []any{
			map[string]any{
				"node_id": "worker-1", "rpc_address": "worker-1:50052", "start_layer": 0, "end_layer": 16,
				"devices": []any{map[string]any{
					"kind": "gpu", "vendor": "nvidia", "name": "GPU 0", "index": 0,
					"start_layer": 0, "end_layer": 16, "weights_bytes": 1000, "kv_cache_bytes": 1000,
					"safety_margin_bytes": 1000, "required_bytes": 3000, "free_memory_bytes": 4000,
				}},
			},
			map[string]any{
				"node_id": "worker-2", "rpc_address": "worker-2:50052", "start_layer": 16, "end_layer": 32,
				"devices": []any{map[string]any{
					"kind": "gpu", "vendor": "nvidia", "name": "GPU 0", "index": 0,
					"start_layer": 16, "end_layer": 32, "weights_bytes": 1000, "kv_cache_bytes": 1000,
					"safety_margin_bytes": 1000, "required_bytes": 3000, "free_memory_bytes": 4000,
				}},
			},
		},
		"rpc": "worker-1:50052,worker-2:50052", "tensor_split": "1,1",
		"rationale": []string{"Memory fit on both nodes."},
	}
	shardGroup := map[string]any{
		"id": "shard-demo", "model": shardProfile, "mode": "llamacpp-rpc", "state": "ready",
		"plan": shardPlan,
		"members": []any{
			map[string]any{
				"node_id": "worker-1", "address": "https://worker-1:7451", "role": "worker",
				"engine_id": "rpc-1", "engine_kind": "ggml-rpc-server", "started": true,
			},
			map[string]any{
				"node_id": "worker-2", "address": "https://worker-2:7451", "role": "head",
				"engine_id": "llama-head", "engine_kind": "llama-server", "started": true,
			},
		},
		"pinned_nodes": []string{"worker-1", "worker-2"},
		"rationale":    []string{"Memory fit on both nodes."},
		"created_at":   now, "updated_at": now,
	}

	values := map[string]any{
		"shard-membership.schema.json": map[string]any{"node_ids": []string{"worker-1", "worker-2"}},
		"peer-update.schema.json":      map[string]any{"version": "0.0.24.1", "os": "linux", "arch": "amd64", "sha256": strings.Repeat("a", 64), "size": 14000000},
		"dashboard-pipeline.schema.json": map[string]any{
			"model_inventory": []any{
				map[string]any{"model_id": "whole-model", "kind": "whole", "nodes": []string{"worker-1"}},
				map[string]any{"model_id": "large-model", "kind": "sharded"},
			},
			"router_events": []any{
				map[string]any{
					"request_id": "request-1", "phase": "attempt_failed",
					"timestamp": "2025-01-02T03:04:05Z", "failure_class": "upstream_5xx",
				},
			},
			"router_circuit_breakers": []any{
				map[string]any{
					"node_id": "worker-1", "model": "demo", "state": "closed", "recent_failures": 0,
				},
			},
			"pipeline_plans": []any{map[string]any{
				"model_id": "large-model", "status": "ready",
				"plan": config.PipelinePlan{
					Model: config.ShardedModelConfig{
						LogicalID: "large-model", SourceModelID: "source-model", Revision: "revision-1",
						TokenizerID: "tokenizer-1", ModelConfigID: "config-1", TotalLayers: 32,
					},
					Stages: []config.StageSpec{
						{NodeID: "worker-1", StartLayer: 0, EndLayer: 16, Engine: "vllm"},
						{NodeID: "worker-2", StartLayer: 16, EndLayer: 32, Engine: "vllm"},
					},
					PlanVersion: 1, CoordinatorEpoch: "epoch-1",
				},
				"stages": []any{
					map[string]any{
						"stage": 0, "node_id": "worker-1", "start_layer": 0, "end_layer": 16,
						"engine": "vllm", "status": "ready", "active": 0, "capacity": 2,
						"utilization_percent": 0,
					},
					map[string]any{
						"stage": 1, "node_id": "worker-2", "start_layer": 16, "end_layer": 32,
						"engine": "vllm", "status": "ready", "active": 1, "capacity": 2,
						"utilization_percent": 50, "activation_transfer_ms": 1.5, "activation_bytes": 4096,
					},
				},
			}},
		},
		"cluster-health.schema.json": map[string]any{
			"nodes": []any{map[string]any{
				"node_id": "worker-1", "healthy": true, "active": 2, "max": 4,
				"remaining": 2, "oversubscribed": false, "telemetry_state": "fresh", "telemetry_age_ms": 500,
			}},
			"global_dispatch": map[string]any{
				"global_limit": 8, "active_slots": 2, "queue_depth": 1, "active_workers": 2,
			},
			"engine_readiness": map[string]any{
				"ready_engines": 1, "total_engines": 2, "nodes_with_engines": 1, "nodes_with_ready_engine": 1,
			},
			"telemetry_freshness": map[string]any{
				"fresh_nodes": 1, "stale_nodes": 0, "unavailable_nodes": 0, "freshness_window_ms": 30000,
			},
			"warnings": []string{},
		},
		"node-config.schema.json": nodeConfig,
		"managed-engine-status.schema.json": map[string]any{
			"id": "llama-main", "kind": "llama-server", "state": "ready",
			"pid": 1234, "restart_count": 0, "started_at": now,
			"warnings": []string{},
		},
		"managed-engine-start.schema.json": map[string]any{
			"shard_plan": map[string]any{
				"nnodes": 2, "node_rank": 1, "master_addr": "192.0.2.10",
			},
		},
		"worker-install-request.schema.json": map[string]any{"source": "official"},
		"local-head-models.schema.json":      map[string]any{"models": []any{}, "warnings": []string{}},
		"configure-local-head.schema.json":   map[string]any{"model_id": strings.Repeat("a", 64), "requested_context": 4096},
		"download-head-model.schema.json":    map[string]any{"model": "qwen2.5-3b-instruct-q4", "requested_context": 4096},
		"worker-install-result.schema.json": map[string]any{
			"source": "official", "version": "test", "updated": true,
			"engines": []any{map[string]any{
				"id": "maistr0-auto-rpc", "kind": "ggml-rpc-server", "state": "stopped", "restart_count": 0,
			}},
		},
		"create-shard-group.schema.json": map[string]any{
			"model": shardProfile, "mode": "llamacpp-rpc", "pinned_nodes": []string{"worker-1", "worker-2"},
		},
		"shard-model-profile.schema.json": shardProfile,
		"shard-plan.schema.json":          shardPlan,
		"shard-group.schema.json":         shardGroup,
		"shard-group-list.schema.json":    map[string]any{"groups": []any{shardGroup}},
		"orchestrator-config.schema.json": orchestratorConfig,
		"task.schema.json":                task,
		"subtask-result.schema.json":      result,
		"subtask.schema.json":             subtask,
		"task-graph.schema.json":          graph,
		"assignment.schema.json":          assignment,
		"score-factors.schema.json":       scheduler.ScoreFactors{},
		"node-status.schema.json":         nodeStatus,
		"stage-assignment.schema.json": engine.StageAssignment{
			ModelID: "large-model", StartLayer: 0, EndLayer: 16, Engine: "vllm", IsFinal: true,
		},
		"stage-forward-frame.schema.json": engine.StageForwardFrame{
			Type:   engine.StageFrameStart,
			Prompt: "hello", Temperature: 0.2, MaxTokens: 32,
			Assignment: &engine.StageAssignment{
				ModelID: "large-model", StartLayer: 0, EndLayer: 16,
				NextStageAddr: "https://worker-2:7451", Engine: "vllm",
			},
			KVCache: []byte("cache"),
		},
		"engine-status.schema.json": engine.Status{
			Name: "llamacpp", Ready: true, InventoryState: engine.ModelInventoryAvailable, ModelCount: 1,
			PipelineStage: &engine.PipelineStageCapabilities{
				ProtocolVersion: engine.PipelineStageProtocolVersion, Runtime: "llamacpp",
				Models:         []engine.PipelineStageModelCapability{{ModelID: "large-model", MaxLayers: 32}},
				TensorDTypes:   []engine.TensorDType{engine.TensorFloat16},
				DeviceBackends: []string{"cpu"},
			},
		},
		"activation-tensor.schema.json": map[string]any{
			"dtype": engine.TensorFloat32, "shape": []int64{1, 4, 8},
		},
		"pipeline-stage-capabilities.schema.json": engine.PipelineStageCapabilities{
			ProtocolVersion: engine.PipelineStageProtocolVersion, Runtime: "llama.cpp",
			Models:         []engine.PipelineStageModelCapability{{ModelID: "large-model", MaxLayers: 32}},
			TensorDTypes:   []engine.TensorDType{engine.TensorFloat16, engine.TensorFloat32},
			DeviceBackends: []string{"cpu"},
		},
		"pipeline-stage-control.schema.json": engine.PipelineStageControlRequest{
			ProtocolVersion: engine.PipelineStageProtocolVersion, RequestID: "request-1",
			Assignment: engine.StageAssignment{ModelID: "large-model", StartLayer: 0, EndLayer: 11},
		},
		"pipeline-stage-request.schema.json": engine.PipelineStageRequest{
			ProtocolVersion: engine.PipelineStageProtocolVersion, RequestID: "request-1",
			Assignment: engine.StageAssignment{ModelID: "large-model", StartLayer: 0, EndLayer: 11},
			Prompt:     "hello", MaxTokens: 16,
		},
		"pipeline-stage-output.schema.json": engine.PipelineStageOutput{
			Token: &engine.PipelineToken{ID: 1, Text: "hello"},
		},
		"pipeline-stage-message.schema.json": engine.StageMessageHeader{
			Request: engine.PipelineStageRequest{
				ProtocolVersion: engine.PipelineStageProtocolVersion, RequestID: "request-1",
				Assignment: engine.StageAssignment{ModelID: "large-model", StartLayer: 0, EndLayer: 11},
				Prompt:     "hello",
			},
			Output: &engine.PipelineStageOutput{
				Token: &engine.PipelineToken{ID: 1, Text: "hello"},
			},
		},
		"sharded-model-config.schema.json": config.ShardedModelConfig{
			LogicalID: "large-model", SourceModelID: "source-model", Revision: "revision-1",
			TokenizerID: "tokenizer-1", ModelConfigID: "config-1", TotalLayers: 32,
		},
		"stage-spec.schema.json": config.StageSpec{
			NodeID: "worker-1", StartLayer: 0, EndLayer: 16, Engine: "vllm",
		},
		"pipeline-plan.schema.json": config.PipelinePlan{
			Model: config.ShardedModelConfig{
				LogicalID: "large-model", SourceModelID: "source-model", Revision: "revision-1",
				TokenizerID: "tokenizer-1", ModelConfigID: "config-1", TotalLayers: 32,
			},
			Stages: []config.StageSpec{
				{NodeID: "worker-1", StartLayer: 0, EndLayer: 16, Engine: "vllm"},
				{NodeID: "worker-2", StartLayer: 16, EndLayer: 32, Engine: "vllm"},
			},
			PlanVersion: 1, CoordinatorEpoch: "epoch-1",
		},
		"announcement.schema.json":    discovery.Announcement{Role: "node", ID: "worker-1", HTTPAddr: "http://worker-1", PeerAddr: "https://worker-1"},
		"experience.schema.json":      snapshot.Experiences[0],
		"fact.schema.json":            snapshot.Facts[0],
		"performance.schema.json":     snapshot.Performances[0],
		"dialogue-record.schema.json": snapshot.Dialogues[0],
		"snapshot.schema.json":        snapshot,
		"assignment-explanation.schema.json": map[string]any{
			"task_id": "task-1",
			"assignments": []any{map[string]any{
				"subtask_id": "work", "description": "Do the work", "node_id": "worker-1",
				"model": "general", "score": 1.5, "score_factors": scheduler.ScoreFactors{},
				"learned_bias": 0.25, "summary": "Assigned using the stored scheduler score.",
			}},
		},
	}

	schemaDir := filepath.Join("schemas")
	files, err := filepath.Glob(filepath.Join(schemaDir, "*.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	schemaIDs := make(map[string]string, len(files))
	for _, filename := range files {
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		id, ok := document["$id"].(string)
		if !ok {
			t.Fatalf("%s has no string $id", filename)
		}
		if err := compiler.AddResource(id, document); err != nil {
			t.Fatalf("register %s: %v", filename, err)
		}
		schemaIDs[filepath.Base(filename)] = id
	}
	compiler.AssertFormat()
	for filename, value := range values {
		t.Run(strings.TrimSuffix(filename, ".schema.json"), func(t *testing.T) {
			id, ok := schemaIDs[filename]
			if !ok {
				t.Fatalf("schema %s is missing", filename)
			}
			schema, err := compiler.Compile(id)
			if err != nil {
				t.Fatalf("compile %s: %v", filename, err)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var document any
			if err := json.Unmarshal(encoded, &document); err != nil {
				t.Fatal(err)
			}
			if err := schema.Validate(document); err != nil {
				t.Errorf("%s rejected marshaled value %s: %v", filename, encoded, err)
			}
		})
	}
	fixtureFiles, err := filepath.Glob(filepath.Join("fixtures", "shards", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fixtureFiles) == 0 {
		t.Fatal("no shard-group contract fixtures found")
	}
	validateFixtureBody := func(filename string, body json.RawMessage) error {
		var fixtureValue any
		if err := json.Unmarshal(body, &fixtureValue); err != nil {
			return err
		}
		schema, err := compiler.Compile(schemaIDs[filename])
		if err != nil {
			return err
		}
		return schema.Validate(fixtureValue)
	}
	for _, filename := range fixtureFiles {
		t.Run(filepath.Join("shard-fixture", strings.TrimSuffix(filepath.Base(filename), ".json")), func(t *testing.T) {
			var fixture struct {
				Name    string `json:"name"`
				Request struct {
					Schema string          `json:"schema"`
					Body   json.RawMessage `json:"body"`
				} `json:"request"`
				ExpectedResponse struct {
					Schema string          `json:"schema"`
					Body   json.RawMessage `json:"body"`
				} `json:"expected_response"`
				NormalizationRules []string `json:"normalization_rules"`
			}
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &fixture); err != nil {
				t.Fatal(err)
			}
			if fixture.Name == "" || len(fixture.NormalizationRules) == 0 {
				t.Fatal("fixture must include a name and normalization rules")
			}
			if fixture.Request.Schema != "" {
				if err := validateFixtureBody(fixture.Request.Schema, fixture.Request.Body); err != nil {
					t.Errorf("request does not validate against %s: %v", fixture.Request.Schema, err)
				}
			}
			if fixture.ExpectedResponse.Schema != "" {
				if err := validateFixtureBody(fixture.ExpectedResponse.Schema, fixture.ExpectedResponse.Body); err != nil {
					t.Errorf("response does not validate against %s: %v", fixture.ExpectedResponse.Schema, err)
				}
			}
		})
	}
}

func TestDashboardRouterPropertiesAreTopLevel(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("schemas", "dashboard-pipeline.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"router_events", "router_circuit_breakers"} {
		if _, ok := schema.Properties[name]; !ok {
			t.Errorf("dashboard snapshot schema is missing top-level property %q", name)
		}
	}
	var pipelinePlans struct {
		Items struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"items"`
	}
	if err := json.Unmarshal(schema.Properties["pipeline_plans"], &pipelinePlans); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"router_events", "router_circuit_breakers"} {
		if _, ok := pipelinePlans.Items.Properties[name]; ok {
			t.Errorf("dashboard property %q is incorrectly nested under pipeline plans", name)
		}
	}
}

func TestSchemaSetIsComplete(t *testing.T) {
	expected := []string{
		"announcement.schema.json", "assignment-explanation.schema.json", "assignment.schema.json", "cluster-health.schema.json", "dashboard-pipeline.schema.json", "dialogue-record.schema.json",
		"activation-tensor.schema.json", "dispatch-attempt.schema.json", "engine-status.schema.json", "experience.schema.json",
		"fact.schema.json", "node-config.schema.json", "create-shard-group.schema.json",
		"managed-engine-start.schema.json", "managed-engine-status.schema.json",
		"worker-install-request.schema.json", "worker-install-result.schema.json",
		"local-head-models.schema.json", "configure-local-head.schema.json",
		"download-head-model.schema.json",
		"node-status.schema.json", "orchestrator-config.schema.json", "performance.schema.json", "pipeline-plan.schema.json",
		"pipeline-stage-capabilities.schema.json", "pipeline-stage-control.schema.json", "pipeline-stage-message.schema.json",
		"pipeline-stage-output.schema.json", "pipeline-stage-request.schema.json",
		"router-circuit-breaker.schema.json", "router-request-event.schema.json", "router-requests.schema.json",
		"score-factors.schema.json", "snapshot.schema.json",
		"shard-group-list.schema.json", "shard-group.schema.json", "shard-model-profile.schema.json", "shard-plan.schema.json",
		"peer-update.schema.json", "shard-membership.schema.json",
		"stage-assignment.schema.json", "stage-forward-frame.schema.json",
		"sharded-model-config.schema.json", "stage-spec.schema.json", "subtask-result.schema.json", "subtask.schema.json", "task-dependencies.schema.json",
		"task-graph.schema.json", "task.schema.json",
	}
	actual, err := filepath.Glob(filepath.Join("schemas", "*.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range actual {
		actual[i] = filepath.Base(actual[i])
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if fmt.Sprint(actual) != fmt.Sprint(expected) {
		t.Fatalf("schema set = %v, want %v", actual, expected)
	}
}

func TestGeneratedSchedulerFixturesMatchRealScheduler(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("fixtures", "scheduler", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 2 {
		t.Fatalf("expected multiple generated scheduler cases, got %d", len(files))
	}
	for _, filename := range files {
		t.Run(strings.TrimSuffix(filepath.Base(filename), ".json"), func(t *testing.T) {
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			var document struct {
				Request struct {
					Nodes    []cluster.NodeStatus `json:"nodes"`
					Models   []engine.Model       `json:"models"`
					Subtasks []scheduler.Subtask  `json:"subtasks"`
				} `json:"request"`
				ExpectedResponse struct {
					Assignments []scheduler.Assignment `json:"assignments"`
				} `json:"expected_response"`
				NormalizationRules []string `json:"normalization_rules"`
			}
			if err := json.Unmarshal(data, &document); err != nil {
				t.Fatal(err)
			}
			if len(document.Request.Nodes) == 0 || len(document.Request.Models) == 0 || len(document.Request.Subtasks) == 0 {
				t.Fatal("scheduler fixture must include node, model, and subtask inputs")
			}
			actual, err := scheduler.AssignWithExperience(document.Request.Subtasks, document.Request.Nodes, nil)
			if err != nil {
				t.Fatal(err)
			}
			actualJSON, err := json.Marshal(actual)
			if err != nil {
				t.Fatal(err)
			}
			expectedJSON, err := json.Marshal(document.ExpectedResponse.Assignments)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actualJSON, expectedJSON) {
				t.Fatalf("scheduler fixture assignments drifted:\n got: %s\nwant: %s", actualJSON, expectedJSON)
			}
			if len(document.NormalizationRules) == 0 {
				t.Fatal("scheduler fixture must declare its normalization rules")
			}
		})
	}
}

func TestExportedGoldenFixturesRetainRequestsAndResponses(t *testing.T) {
	sourceFiles, err := filepath.Glob(filepath.Join("..", "internal", "orchestrator", "testdata", "contracts", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceFiles) == 0 {
		t.Fatal("no source golden fixtures found")
	}
	for _, sourcePath := range sourceFiles {
		name := filepath.Base(sourcePath)
		t.Run(strings.TrimSuffix(name, ".json"), func(t *testing.T) {
			var exported struct {
				Request struct {
					Method string `json:"method"`
					Path   string `json:"path"`
				} `json:"request"`
				ExpectedResponse   json.RawMessage `json:"expected_response"`
				NormalizationRules []string        `json:"normalization_rules"`
			}
			exportedPath := filepath.Join("fixtures", "phase1", name)
			data, err := os.ReadFile(exportedPath)
			if err != nil {
				t.Fatalf("read exported fixture: %v", err)
			}
			if err := json.Unmarshal(data, &exported); err != nil {
				t.Fatalf("decode exported fixture: %v", err)
			}
			if exported.Request.Method == "" || exported.Request.Path == "" {
				t.Fatal("fixture request must include a method and path")
			}
			if len(exported.NormalizationRules) == 0 {
				t.Fatal("fixture must declare normalization rules")
			}
			source, err := os.ReadFile(sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			var sourceResponse any
			var exportedResponse any
			if err := json.Unmarshal(source, &sourceResponse); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(exported.ExpectedResponse, &exportedResponse); err != nil {
				t.Fatal(err)
			}
			sourceJSON, err := json.Marshal(sourceResponse)
			if err != nil {
				t.Fatal(err)
			}
			exportedJSON, err := json.Marshal(exportedResponse)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(sourceJSON, exportedJSON) {
				t.Errorf("exported response differs from source golden")
			}
		})
	}
}
