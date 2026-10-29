package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
	"github.com/maistr0/maistr0/internal/workpool"
)

func (m *ShardManager) candidates(
	ctx context.Context,
	request CreateShardGroupRequest,
) ([]shard.CandidateNode, map[string]map[string]supervisor.Status, error) {
	pinned := make(map[string]struct{}, len(request.PinnedNodes))
	for _, id := range request.PinnedNodes {
		pinned[id] = struct{}{}
	}
	nodes := m.registry.All()
	healthyCount := 0
	for _, node := range nodes {
		if node.Healthy {
			healthyCount++
		}
	}
	seenPinned := make(map[string]bool, len(pinned))
	var candidates []shard.CandidateNode
	engineByNode := make(map[string]map[string]supervisor.Status)
	for _, node := range nodes {
		if snapshot, ok := request.nodeSnapshots[node.ID]; ok {
			node = snapshot
		}
		if !node.Healthy {
			continue
		}
		if len(pinned) > 0 {
			if _, ok := pinned[node.ID]; !ok {
				continue
			}
			seenPinned[node.ID] = true
		}
		statuses, err := m.engines.List(ctx, node.Address)
		if err != nil {
			return nil, nil, fmt.Errorf("list managed engines on node %s: %w", node.ID, err)
		}
		byKind := make(map[string]supervisor.Status)
		for _, status := range statuses {
			kind := strings.ToLower(status.Kind)
			if kind == "llama-server" && request.HeadNodeID != "" &&
				(node.ID != request.HeadNodeID || status.ID != request.HeadEngineID) {
				continue
			}
			if _, exists := byKind[kind]; !exists {
				byKind[kind] = status
			}
		}
		engineByNode[node.ID] = byKind
		var modeEngine supervisor.Status
		switch request.Mode {
		case shard.ModeLlamaCPPRPC:
			modeEngine = byKind["ggml-rpc-server"]
			if modeEngine.ID == "" {
				continue
			}
		case shard.ModeVLLMMP:
			modeEngine = byKind["vllm"]
			if modeEngine.ID == "" {
				continue
			}
		default:
			continue
		}
		capabilities := managedShardCapabilities(node, request.Mode)
		rpcAddress := ""
		switch request.Mode {
		case shard.ModeLlamaCPPRPC:
			rpcAddress = modeEngine.RelayEndpoint
		case shard.ModeVLLMMP:
			rpcAddress = advertisedEndpointAddress(modeEngine.Endpoint, node.Address)
		}
		rpcProtected := request.Mode != shard.ModeLlamaCPPRPC ||
			validRemoteRPCRelayAddress(modeEngine.RelayEndpoint)
		candidates = append(candidates, shard.CandidateNode{
			ID: node.ID, OS: strings.ToLower(node.Hardware.OS),
			RPCAddress: rpcAddress, RPCProtected: rpcProtected,
			AllowInsecureShardNetwork: node.AllowInsecureShardNetwork,
			Devices:                   append([]hardware.Device(nil), node.Hardware.Devices...),
			NetworkLinks:              append([]hardware.NetworkLink(nil), node.Hardware.NetworkLinks...),
			Capabilities:              capabilities,
		})
	}
	for id := range pinned {
		if !seenPinned[id] {
			return nil, nil, fmt.Errorf("%w: pinned node %q is not healthy or not registered", errShardGroupUnavailable, id)
		}
	}
	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("%w: no healthy nodes have the managed engines required by the requested shard mode", errShardGroupUnavailable)
	}
	if request.Mode == shard.ModeLlamaCPPRPC && !request.allowHeadlessPlanning {
		hasHead := false
		for _, candidate := range candidates {
			if engineByNode[candidate.ID]["llama-server"].ID != "" {
				hasHead = true
				break
			}
		}
		if !hasHead {
			return nil, nil, fmt.Errorf("%w: no candidate node has a configured llama-server head engine", errShardGroupUnavailable)
		}
	}
	return candidates, engineByNode, nil
}

func managedShardCapabilities(node cluster.NodeStatus, mode shard.Mode) engine.Capabilities {
	var capabilities engine.Capabilities
	expectedName := "llamacpp"
	devices := []string{"gpu"}
	if mode == shard.ModeVLLMMP {
		expectedName = "vllm"
	}
	for _, status := range node.Engines {
		if strings.EqualFold(status.Name, expectedName) {
			capabilities = engine.CloneCapabilities(status.Capabilities)
			break
		}
	}
	if len(capabilities.Devices) == 0 {
		capabilities.Devices = devices
	}
	capabilities.SupportsSharding = appendUniqueString(capabilities.SupportsSharding, string(mode))
	if mode == shard.ModeVLLMMP {
		capabilities.HomogeneousOnly = true
		capabilities.OS = appendUniqueString(capabilities.OS, "linux")
	}
	for _, status := range node.Engines {
		capabilities.ValidatedRPC = capabilities.ValidatedRPC || status.Capabilities.ValidatedRPC
	}
	return capabilities
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			return values
		}
	}
	return append(values, value)
}

func buildShardMembers(
	plan shard.ShardPlan,
	engineByNode map[string]map[string]supervisor.Status,
	registry ShardNodeRegistry,
	selectedHead string,
) ([]ShardGroupMember, error) {
	if len(plan.Nodes) == 0 {
		return nil, errors.New("shard planner returned an empty node list")
	}
	var members []ShardGroupMember
	switch plan.Mode {
	case shard.ModeLlamaCPPRPC:
		for _, node := range plan.Nodes {
			status := engineByNode[node.NodeID]["ggml-rpc-server"]
			if status.ID == "" {
				return nil, fmt.Errorf("node %s has no managed ggml-rpc-server", node.NodeID)
			}
			members = append(members, ShardGroupMember{
				NodeID: node.NodeID, Role: "worker", EngineID: status.ID,
				EngineKind: status.Kind, RPCRelayEndpoint: status.RelayEndpoint,
				RPCBackendAddress: status.Endpoint,
			})
		}
		var head ShardGroupMember
		if selectedHead != "" {
			status := engineByNode[selectedHead]["llama-server"]
			head = ShardGroupMember{NodeID: selectedHead, Role: "head", EngineID: status.ID, EngineKind: status.Kind}
		}
		for _, node := range plan.Nodes {
			if selectedHead != "" {
				break
			}
			if status := engineByNode[node.NodeID]["llama-server"]; status.ID != "" {
				head = ShardGroupMember{
					NodeID: node.NodeID, Role: "head", EngineID: status.ID,
					EngineKind: status.Kind,
				}
				break
			}
		}
		if head.EngineID == "" {
			return nil, errors.New("planned nodes do not include a managed llama-server head")
		}
		members = append(members, head)
	case shard.ModeVLLMMP:
		headNode := plan.Nodes[0]
		headStatus := engineByNode[headNode.NodeID]["vllm"]
		if headStatus.ID == "" {
			return nil, fmt.Errorf("node %s has no managed vllm engine", headNode.NodeID)
		}
		for rank, node := range plan.Nodes[1:] {
			status := engineByNode[node.NodeID]["vllm"]
			if status.ID == "" {
				return nil, fmt.Errorf("node %s has no managed vllm engine", node.NodeID)
			}
			members = append(members, ShardGroupMember{
				NodeID: node.NodeID, Role: "worker", EngineID: status.ID,
				EngineKind: status.Kind, Rank: rank + 1,
			})
		}
		members = append(members, ShardGroupMember{
			NodeID: headNode.NodeID, Role: "head", EngineID: headStatus.ID,
			EngineKind: headStatus.Kind, Rank: 0,
		})
	default:
		return nil, fmt.Errorf("unsupported shard mode %q", plan.Mode)
	}
	for i := range members {
		node, ok := registry.Get(members[i].NodeID)
		if !ok || !node.Healthy || strings.TrimSpace(node.Address) == "" {
			return nil, fmt.Errorf("node %s is no longer healthy or has no address", members[i].NodeID)
		}
		members[i].Address = node.Address
	}
	if plan.Mode == shard.ModeVLLMMP {
		headNode := plan.Nodes[0]
		headStatus := engineByNode[headNode.NodeID]["vllm"]
		head, ok := registry.Get(headNode.NodeID)
		if !ok || !head.Healthy || strings.TrimSpace(head.Address) == "" {
			return nil, fmt.Errorf("node %s is no longer healthy or has no address", headNode.NodeID)
		}
		masterAddr := advertisedNodeHost(head.Address)
		if masterAddr == "" || headStatus.Endpoint == "" {
			return nil, fmt.Errorf("vllm head node %s does not have a routable endpoint", headNode.NodeID)
		}
		for i := range members {
			members[i].MasterAddr = masterAddr
		}
	}
	for _, member := range members {
		if member.Address == "" {
			return nil, fmt.Errorf("no node address is available for member %s", member.NodeID)
		}
	}
	return members, nil
}

func memberStartPlan(plan shard.ShardPlan, member ShardGroupMember, members []ShardGroupMember) *supervisor.ShardPlan {
	switch plan.Mode {
	case shard.ModeLlamaCPPRPC:
		if member.Role != "head" {
			return nil
		}
		targets := make([]supervisor.RPCRelayTarget, 0, len(plan.Nodes))
		for _, node := range plan.Nodes {
			var worker *ShardGroupMember
			for index := range members {
				if members[index].NodeID == node.NodeID && members[index].Role == "worker" {
					worker = &members[index]
					break
				}
			}
			if worker == nil {
				return nil
			}
			target := supervisor.RPCRelayTarget{PeerID: worker.NodeID}
			if worker.NodeID == member.NodeID {
				target.Address = worker.RPCBackendAddress
			} else {
				target.Address = worker.RPCRelayEndpoint
			}
			targets = append(targets, target)
		}
		return &supervisor.ShardPlan{TensorSplit: plan.TensorSplit, RPCRelays: targets}
	case shard.ModeVLLMMP:
		if member.MasterAddr == "" || len(plan.Nodes) == 0 {
			return nil
		}
		return &supervisor.ShardPlan{
			NNodes: len(plan.Nodes), NodeRank: member.Rank,
			MasterAddr:           member.MasterAddr,
			TensorParallelSize:   plan.TensorParallelSize,
			PipelineParallelSize: plan.PipelineParallelSize,
		}
	default:
		return nil
	}
}

func capacityTargets(members []ShardGroupMember) []workpool.CapacityTarget {
	seen := make(map[string]bool, len(members))
	targets := make([]workpool.CapacityTarget, 0, len(members))
	for _, member := range members {
		if seen[member.NodeID] {
			continue
		}
		seen[member.NodeID] = true
		targets = append(targets, workpool.CapacityTarget{
			NodeID: member.NodeID, Address: member.Address,
		})
	}
	return targets
}
