package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/shard"
)

func (p *autoModelProvisioner) provisioningNodes() []cluster.NodeStatus {
	nodes := p.registry.Active()
	if p.shards != nil {
		nodes = shardSchedulerNodes(nodes, p.shards.List())
	}
	return nodes
}

func (p *autoModelProvisioner) provisionGGUF(ctx context.Context, subtask scheduler.Subtask) error {
	select {
	case p.ggufGate <- struct{}{}:
		defer func() { <-p.ggufGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, err := scheduler.Assign([]scheduler.Subtask{subtask}, p.provisioningNodes()); err == nil {
		return nil
	}
	client, ok := p.client.(interface {
		managedRequest(context.Context, string, string, io.Reader) (*http.Response, error)
	})
	if !ok {
		return errors.New("managed GGUF model acquisition is unavailable")
	}
	call := func(node cluster.NodeStatus, method, path string, body any, result any) error {
		var input io.Reader
		if body != nil {
			payload, err := json.Marshal(body)
			if err != nil {
				return err
			}
			input = bytes.NewReader(payload)
		}
		response, err := client.managedRequest(ctx, method, strings.TrimRight(node.Address, "/")+path, input)
		if err != nil {
			return err
		}
		defer closeutil.Discard(response.Body)
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return managedEngineResponseError(response)
		}
		return json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(result)
	}
	const contextSize = 4096
	var failures []error
	type availableNode struct {
		node   cluster.NodeStatus
		models []supervisor.LocalHeadModel
	}
	var available []availableNode
	runtimeInstalled := make(map[string]bool)
	for _, node := range p.registry.Active() {
		if !node.Healthy || node.ActiveTasks > 0 {
			continue
		}
		busy := false
		for _, group := range p.shards.List() {
			for _, member := range group.Members {
				if member.NodeID == node.ID && group.State != ShardGroupStopped && group.State != ShardGroupFailed {
					busy = true
				}
			}
		}
		if busy {
			continue
		}
		var statuses []supervisor.Status
		if err := call(node, http.MethodGet, "/api/engines/managed", nil, &statuses); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", node.ID, err))
			continue
		}
		for _, status := range statuses {
			if status.State != "stopped" && status.State != "failed" {
				busy = true
			}
			if status.Kind == "llama-server" && status.ID != "maistr0-auto-head" {
				busy = true
			}
			if status.Kind == "ggml-rpc-server" && status.ID == "maistr0-auto-rpc" {
				runtimeInstalled[node.ID] = true
			}
		}
		if busy {
			continue
		}
		var inventory supervisor.LocalHeadModels
		if err := call(node, http.MethodGet, "/api/engines/managed/head-models?requested_context=4096", nil, &inventory); err != nil {
			failures = append(failures, fmt.Errorf("%s local models: %w", node.ID, err))
			continue
		}
		available = append(available, availableNode{node, inventory.Models})
	}
	ensureRuntime := func(node cluster.NodeStatus) error {
		if runtimeInstalled[node.ID] {
			return nil
		}
		var installed supervisor.WorkerInstallResult
		if err := call(node, http.MethodPost, "/api/engines/managed/install-worker", map[string]string{"source": "official"}, &installed); err != nil {
			return fmt.Errorf("install compatible runtime: %w", err)
		}
		if len(installed.Devices) > 0 && !p.registry.UpdateDevices(node.ID, node.Address, installed.Devices) {
			return errors.New("node changed while installing the runtime")
		}
		runtimeInstalled[node.ID] = true
		return nil
	}
	start := func(node cluster.NodeStatus, model supervisor.LocalHeadModel) error {
		current, ok := p.registry.Get(node.ID)
		if !ok || !current.Healthy || current.Address != node.Address {
			return errors.New("model target node changed or became unavailable")
		}
		workerNodes := make([]cluster.NodeStatus, 0, len(available))
		prepareWorker := func(candidate cluster.NodeStatus) error {
			if containsNode(workerNodes, candidate.ID) {
				return nil
			}
			if err := ensureRuntime(candidate); err != nil {
				return fmt.Errorf("%s install worker runtime: %w", candidate.ID, err)
			}
			current, ok := p.registry.Get(candidate.ID)
			if !ok || !current.Healthy || current.Address != candidate.Address {
				return fmt.Errorf("%s: node changed while preparing shard capacity", candidate.ID)
			}
			workerNodes = append(workerNodes, current)
			return nil
		}
		if err := prepareWorker(node); err != nil {
			return err
		}
		plan, planErr := p.planGGUF(ctx, model.Profile, workerNodes)
		var preparationErrors []error
		for _, candidate := range available {
			if planErr == nil {
				break
			}
			if candidate.node.ID == node.ID {
				continue
			}
			if err := prepareWorker(candidate.node); err != nil {
				preparationErrors = append(preparationErrors, err)
				continue
			}
			plan, planErr = p.planGGUF(ctx, model.Profile, workerNodes)
		}
		if planErr != nil {
			return errors.Join(
				fmt.Errorf("plan model across idle cluster nodes: %w", planErr),
				errors.Join(preparationErrors...),
			)
		}
		if !compatibleGGUF(subtask, model, plan) {
			return errors.New("selected GGUF does not satisfy the task requirements")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		current, ok = p.registry.Get(node.ID)
		if !ok || !current.Healthy || current.Address != node.Address {
			return errors.New("model target node changed or became unavailable")
		}
		var status supervisor.Status
		if err := call(node, http.MethodPost, "/api/engines/managed/configure-head", map[string]any{"model_id": model.ID, "requested_context": contextSize}, &status); err != nil {
			return fmt.Errorf("configure local model: %w", err)
		}
		pinnedNodes := make([]string, 0, len(plan.Nodes)+1)
		for _, plannedNode := range plan.Nodes {
			pinnedNodes = append(pinnedNodes, plannedNode.NodeID)
		}
		if !slices.Contains(pinnedNodes, node.ID) {
			pinnedNodes = append(pinnedNodes, node.ID)
		}
		group, err := p.shards.Create(ctx, CreateShardGroupRequest{
			Model: model.Profile, Mode: shard.ModeLlamaCPPRPC, PinnedNodes: pinnedNodes,
			HeadNodeID: node.ID, HeadEngineID: status.ID,
		})
		if err != nil {
			return fmt.Errorf("start model group %s: %w", group.ID, err)
		}
		if _, err := scheduler.Assign([]scheduler.Subtask{subtask}, p.provisioningNodes()); err != nil {
			return fmt.Errorf("loaded model is not eligible for requested task: %w", err)
		}
		return nil
	}
	for _, candidate := range available {
		for _, model := range candidate.models {
			if !compatibleGGUFTask(subtask) {
				continue
			}
			logging.Infof("orchestrator", "loading compatible local GGUF %s on node %s", model.Name, candidate.node.ID)
			if err := start(candidate.node, model); err == nil {
				return nil
			} else {
				failures = append(failures, fmt.Errorf("%s local GGUF: %w", candidate.node.ID, err))
			}
		}
	}
	for _, recommendation := range supervisor.RecommendedGGUF() {
		if !compatibleGGUFTask(subtask) {
			continue
		}
		for _, candidate := range available {
			logging.Infof("orchestrator", "downloading recommended Hugging Face GGUF %s on node %s", recommendation.ID, candidate.node.ID)
			var model supervisor.LocalHeadModel
			if err := call(candidate.node, http.MethodPost, "/api/engines/managed/download-model", map[string]any{"model": recommendation.ID, "requested_context": contextSize}, &model); err != nil {
				failures = append(failures, fmt.Errorf("%s download: %w", candidate.node.ID, err))
				continue
			}
			if err := start(candidate.node, model); err == nil {
				return nil
			} else {
				failures = append(failures, fmt.Errorf("%s downloaded GGUF: %w", candidate.node.ID, err))
			}
		}
	}
	if len(failures) == 0 {
		return errors.New("no idle managed node has the required capabilities and memory for a local or recommended GGUF; active groups are never replaced")
	}
	return errors.Join(failures...)
}

func (p *autoModelProvisioner) planGGUF(
	ctx context.Context,
	model shard.ModelProfile,
	nodes []cluster.NodeStatus,
) (shard.ShardPlan, error) {
	pinnedNodes := make([]string, 0, len(nodes))
	for _, node := range nodes {
		pinnedNodes = append(pinnedNodes, node.ID)
	}
	request := CreateShardGroupRequest{
		Mode: shard.ModeLlamaCPPRPC, PinnedNodes: pinnedNodes, allowHeadlessPlanning: true,
	}
	candidates, _, err := p.shards.candidates(ctx, request)
	if err != nil {
		return shard.ShardPlan{}, fmt.Errorf("collect compatible managed workers: %w", err)
	}
	plan, err := p.shards.planner(model, candidates, shard.Policy{
		Mode: shard.ModeLlamaCPPRPC, SafetyMarginMB: managedShardSafetyMarginMB,
	})
	if err != nil {
		return shard.ShardPlan{}, err
	}
	warnings, err := shardPlanNetworkWarnings(plan, candidates)
	if err != nil {
		return shard.ShardPlan{}, err
	}
	plan.Rationale = append(plan.Rationale, warnings...)
	return plan, nil
}

func compatibleGGUFTask(subtask scheduler.Subtask) bool {
	if subtask.TaskType == "vision" {
		return false
	}
	for _, capability := range subtask.RequiredCapabilities {
		if strings.EqualFold(strings.TrimSpace(capability), "vision") {
			return false
		}
	}
	return true
}

func compatibleGGUF(subtask scheduler.Subtask, model supervisor.LocalHeadModel, plan shard.ShardPlan) bool {
	weights, err := model.Profile.EffectiveWeightsSizeBytes()
	if err != nil || weights == 0 {
		return false
	}
	node := cluster.NodeStatus{
		ID: "gguf-candidate", Address: "http://gguf-candidate", Healthy: true,
		Models: []engine.Model{{
			Name: model.Name, Engine: "gguf-candidate", Tags: shardTextTags(model.Name),
			SizeGB: float64(weights) / (1 << 30),
		}},
		Engines: []engine.Status{{Name: "gguf-candidate", Ready: true}},
	}
	node.Capabilities = shardGroupHardwareCapabilities(ShardGroup{Plan: plan})
	for _, capability := range node.EffectiveCapabilities() {
		if !containsCapability(node.Capabilities, capability) {
			node.Capabilities = append(append([]string(nil), node.Capabilities...), capability)
		}
	}
	_, err = scheduler.Assign([]scheduler.Subtask{subtask}, []cluster.NodeStatus{node})
	return err == nil
}

func containsNode(nodes []cluster.NodeStatus, id string) bool {
	for _, node := range nodes {
		if node.ID == id {
			return true
		}
	}
	return false
}
