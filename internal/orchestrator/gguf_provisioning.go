package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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
		defer response.Body.Close()
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
		if err := ensureRuntime(node); err != nil {
			return err
		}
		current, ok := p.registry.Get(node.ID)
		if !ok || !current.Healthy || current.Address != node.Address {
			return errors.New("model target node changed or became unavailable")
		}
		weights, err := model.Profile.EffectiveWeightsSizeBytes()
		if err != nil {
			return fmt.Errorf("read selected model size: %w", err)
		}
		if !compatibleGGUF(subtask, current, model.Name, weights) {
			return errors.New("selected GGUF does not satisfy the task requirements or available memory")
		}
		var status supervisor.Status
		if err := call(node, http.MethodPost, "/api/engines/managed/configure-head", map[string]any{"model_id": model.ID, "requested_context": contextSize}, &status); err != nil {
			return fmt.Errorf("configure local model: %w", err)
		}
		group, err := p.shards.Create(ctx, CreateShardGroupRequest{
			Model: model.Profile, Mode: shard.ModeLlamaCPPRPC, PinnedNodes: []string{node.ID}, HeadNodeID: node.ID, HeadEngineID: status.ID,
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
			weights, err := model.Profile.EffectiveWeightsSizeBytes()
			if err != nil {
				failures = append(failures, fmt.Errorf("%s local model weight size: %w", candidate.node.ID, err))
				continue
			}
			if !compatibleGGUF(subtask, candidate.node, model.Name, weights) {
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
		for _, candidate := range available {
			if !compatibleGGUF(subtask, candidate.node, strings.TrimSuffix(recommendation.Filename, ".gguf"), uint64(recommendation.SizeBytes)) {
				continue
			}
			if err := ensureRuntime(candidate.node); err != nil {
				failures = append(failures, err)
				continue
			}
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

func compatibleGGUF(subtask scheduler.Subtask, node cluster.NodeStatus, name string, bytes uint64) bool {
	if node.TelemetryAt.IsZero() || node.TelemetryAt.After(time.Now()) || time.Since(node.TelemetryAt) > clusterTelemetryFreshnessWindow {
		return false
	}
	if bytes == 0 || !nodeCanFitModel(node, float64(bytes)/(1<<30)) {
		return false
	}
	var deviceBudget uint64
	for _, device := range node.Hardware.Devices {
		if device.Kind == "gpu" && device.FreeMemoryMB > 2048 {
			deviceBudget += (device.FreeMemoryMB - 2048) * (1 << 20)
		}
	}
	if float64(deviceBudget) < float64(bytes)*modelMemoryReserve {
		return false
	}
	node.Capabilities = append([]string(nil), node.EffectiveCapabilities()...)
	node.Models = []engine.Model{{Name: name, Engine: "gguf-candidate", Tags: shardTextTags(name), SizeGB: float64(bytes) / (1 << 30)}}
	node.Engines = []engine.Status{{Name: "gguf-candidate", Ready: true}}
	for _, capability := range (cluster.NodeStatus{Models: node.Models}).EffectiveCapabilities() {
		if !containsCapability(node.Capabilities, capability) {
			node.Capabilities = append(append([]string(nil), node.Capabilities...), capability)
		}
	}
	_, err := scheduler.Assign([]scheduler.Subtask{subtask}, []cluster.NodeStatus{node})
	return err == nil
}
