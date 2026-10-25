package shard

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

type eligibleNode struct {
	node    CandidateNode
	devices []hardware.Device
}

func planVLLMMP(
	model ModelProfile,
	weightsBytes, kvBytes, marginBytes uint64,
	nodes []CandidateNode,
	policy Policy,
) (ShardPlan, error) {
	rationale := make([]string, 0, len(nodes)*2)
	var eligible []eligibleNode
	var hasLinuxCandidate bool
	untrustedNetworkRejected := false
	otherCandidateAcceptedNetwork := false
	for _, node := range nodes {
		if !strings.EqualFold(strings.TrimSpace(node.OS), "linux") {
			rationale = append(rationale, fmt.Sprintf(
				"node %q rejected: vllm-mp requires Linux, got %q", node.ID, node.OS,
			))
			continue
		}
		hasLinuxCandidate = true
		if reason, ok := nodeModeRejection(node, string(ModeVLLMMP)); !ok {
			rationale = append(rationale, fmt.Sprintf("node %q rejected: %s", node.ID, reason))
			continue
		}
		if !hasTrustedNetworkLink(node) && !node.AllowInsecureShardNetwork {
			untrustedNetworkRejected = true
			rationale = append(rationale, fmt.Sprintf(
				"node %q rejected: vllm-mp requires a trusted network interface; configure trusted_network_interfaces or explicitly allow insecure shard networking",
				node.ID,
			))
			continue
		}
		otherCandidateAcceptedNetwork = true
		if !hasTrustedNetworkLink(node) {
			rationale = append(rationale, fmt.Sprintf(
				"WARNING: node %q permits untrusted vllm-mp traffic because allow_insecure_shard_network is enabled",
				node.ID,
			))
		}
		var gpus []hardware.Device
		for _, device := range sortedDevices(node.Devices) {
			if normalizeDeviceKind(device.Kind) != "gpu" {
				rationale = append(rationale, fmt.Sprintf(
					"node %q device %s:%d excluded: vllm-mp requires GPU devices",
					node.ID, device.Kind, device.Index,
				))
				continue
			}
			if !deviceSupportedByEngine(node.Capabilities, device) ||
				!engine.BackendsOverlap(node.Capabilities.Backends, device.Backends) {
				rationale = append(rationale, fmt.Sprintf(
					"node %q GPU device %d rejected: device or backend is not supported by the engine capability",
					node.ID, device.Index,
				))
				continue
			}
			if device.FreeMemoryMB == 0 {
				rationale = append(rationale, fmt.Sprintf(
					"node %q GPU device %d rejected: free device memory is unavailable",
					node.ID, device.Index,
				))
				continue
			}
			gpus = append(gpus, device)
		}
		if len(gpus) == 0 {
			rationale = append(rationale, fmt.Sprintf("node %q rejected: no compatible GPU devices", node.ID))
			continue
		}
		eligible = append(eligible, eligibleNode{node: node, devices: gpus})
		rationale = append(rationale, fmt.Sprintf(
			"node %q accepted with %d compatible GPU device(s)", node.ID, len(gpus),
		))
	}
	if len(eligible) == 0 {
		code := RefusalNoCompatibleNodes
		message := "no Linux nodes have compatible GPU devices for vllm-mp"
		if !hasLinuxCandidate {
			code = RefusalUnsupportedOS
			message = "vllm-mp is supported only on Linux nodes"
		} else if untrustedNetworkRejected && !otherCandidateAcceptedNetwork {
			code = RefusalUntrustedNetwork
			message = "vllm-mp requires an operator-trusted network interface on each node"
		}
		return ShardPlan{}, &Refusal{Code: code, Message: message, Rationale: rationale}
	}
	if len(eligible) > model.LayerCount {
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidPolicy,
			Message: fmt.Sprintf(
				"vllm-mp cannot pipeline %d nodes across only %d model layers",
				len(eligible), model.LayerCount,
			),
			Rationale: append(rationale, "pipeline parallelism requires at least one model layer per node"),
		}
	}
	if reason := homogeneityViolation(eligible); reason != "" {
		return ShardPlan{}, &Refusal{
			Code:      RefusalHeterogeneity,
			Message:   "vllm-mp requires homogeneous GPU nodes: " + reason,
			Rationale: append(rationale, reason),
		}
	}

	topology, linkRationale, refusal := chooseVLLMParallelism(eligible, policy)
	rationale = append(rationale, linkRationale...)
	if refusal != nil {
		refusal.Rationale = rationale
		return ShardPlan{}, refusal
	}

	nodeCount := len(eligible)
	devicesPerNode := len(eligible[0].devices)
	tensorParallelSize, pipelineParallelSize := devicesPerNode, nodeCount
	if topology == VLLMParallelismTensorAcrossNodes {
		tensorParallelSize, pipelineParallelSize = nodeCount*devicesPerNode, 1
	}
	nodePlans := make([]NodePlan, 0, nodeCount)
	for nodeIndex, candidate := range eligible {
		startLayer, endLayer := 0, model.LayerCount
		if topology != VLLMParallelismTensorAcrossNodes {
			startLayer, endLayer = layerRange(model.LayerCount, nodeCount, nodeIndex)
		}
		layerCount := endLayer - startLayer
		weightPart, err := multiplyDivideCeil(weightsBytes, uint64(layerCount), uint64(model.LayerCount))
		if err != nil {
			return ShardPlan{}, &Refusal{
				Code: RefusalInvalidModel, Message: "weight allocation overflows memory calculations",
				Rationale: rationale,
			}
		}
		kvPart, err := multiplyDivideCeil(kvBytes, uint64(layerCount), uint64(model.LayerCount))
		if err != nil {
			return ShardPlan{}, &Refusal{
				Code: RefusalInvalidModel, Message: "KV allocation overflows memory calculations",
				Rationale: rationale,
			}
		}
		nodePlan := NodePlan{
			NodeID: candidate.node.ID, StartLayer: startLayer, EndLayer: endLayer,
		}
		for _, device := range candidate.devices {
			freeBytes, ok := mbToBytes(device.FreeMemoryMB)
			if !ok {
				return ShardPlan{}, &Refusal{
					Code:      RefusalInsufficientMemory,
					Message:   fmt.Sprintf("node %q GPU %d has an invalid free-memory value", candidate.node.ID, device.Index),
					Rationale: rationale,
				}
			}
			devicesInGroup := uint64(devicesPerNode)
			if topology == VLLMParallelismTensorAcrossNodes {
				devicesInGroup = uint64(nodeCount * devicesPerNode)
			}
			deviceWeights, err := multiplyDivideCeil(weightPart, 1, devicesInGroup)
			if err != nil {
				return ShardPlan{}, &Refusal{
					Code: RefusalInvalidModel, Message: "tensor weight allocation overflows",
					Rationale: rationale,
				}
			}
			deviceKV, err := multiplyDivideCeil(kvPart, 1, devicesInGroup)
			if err != nil {
				return ShardPlan{}, &Refusal{
					Code: RefusalInvalidModel, Message: "tensor KV allocation overflows",
					Rationale: rationale,
				}
			}
			allocation := makeAllocation(device, startLayer, endLayer, deviceWeights, deviceKV, marginBytes, freeBytes)
			if allocation.RequiredBytes > freeBytes {
				return ShardPlan{}, &Refusal{
					Code: RefusalInsufficientMemory,
					Message: fmt.Sprintf(
						"node %q GPU %d requires %s including safety margin but has %s free",
						candidate.node.ID, device.Index, humanBytes(allocation.RequiredBytes), humanBytes(freeBytes),
					),
					Rationale: append(rationale, fmt.Sprintf(
						"node %q GPU %d rejected: memory requirement exceeds free device memory",
						candidate.node.ID, device.Index,
					)),
				}
			}
			nodePlan.Devices = append(nodePlan.Devices, allocation)
		}
		nodePlans = append(nodePlans, nodePlan)
		rationale = append(rationale, fmt.Sprintf(
			"node %q assigned pipeline layers [%d,%d) across %d local GPU(s)",
			nodePlan.NodeID, nodePlan.StartLayer, nodePlan.EndLayer, devicesPerNode,
		))
	}
	if topology == VLLMParallelismTensorAcrossNodes {
		rationale = append(rationale, "all GPUs participate in tensor parallelism because every node link meets the configured minimum")
	}
	return ShardPlan{
		Mode: ModeVLLMMP, Model: model, Nodes: nodePlans,
		TensorParallelSize: tensorParallelSize, PipelineParallelSize: pipelineParallelSize,
		VLLMParallelism: topology, Rationale: rationale,
	}, nil
}

func chooseVLLMParallelism(nodes []eligibleNode, policy Policy) (VLLMParallelism, []string, *Refusal) {
	requested := policy.VLLMParallelism
	if requested == "" {
		requested = VLLMParallelismPipeline
	}
	if requested == VLLMParallelismPipeline {
		return VLLMParallelismPipeline, []string{
			"selected pipeline parallelism: tensor parallelism stays within each node",
		}, nil
	}
	linkSpeed := minimumNodeLinkSpeed(nodes)
	linkOK := linkSpeed > 0 && linkSpeed >= policy.MinimumLinkSpeedMbps
	switch requested {
	case VLLMParallelismAuto:
		if policy.MinimumLinkSpeedMbps > 0 && linkOK {
			return VLLMParallelismTensorAcrossNodes, []string{
				fmt.Sprintf(
					"selected cross-node tensor parallelism: slowest node link is %d Mbps, meeting the %d Mbps minimum",
					linkSpeed, policy.MinimumLinkSpeedMbps,
				),
			}, nil
		}
		if policy.MinimumLinkSpeedMbps > 0 {
			return VLLMParallelismPipeline, []string{
				fmt.Sprintf(
					"node links do not meet the %d Mbps minimum (slowest advertised node link: %d Mbps); preferring pipeline parallelism",
					policy.MinimumLinkSpeedMbps, linkSpeed,
				),
			}, nil
		}
		return VLLMParallelismPipeline, []string{
			"no cross-node tensor-parallel link minimum was configured; preferring pipeline parallelism",
		}, nil
	case VLLMParallelismTensorAcrossNodes:
		if !linkOK {
			return "", []string{fmt.Sprintf(
				"cross-node tensor parallelism requires at least %d Mbps on every node; slowest advertised node link is %d Mbps",
				policy.MinimumLinkSpeedMbps, linkSpeed,
			)}, &Refusal{
				Code: RefusalLinkTooSlow,
				Message: fmt.Sprintf(
					"cannot use cross-node tensor parallelism: node links do not meet the %d Mbps minimum",
					policy.MinimumLinkSpeedMbps,
				),
			}
		}
		return VLLMParallelismTensorAcrossNodes, []string{
			fmt.Sprintf(
				"cross-node tensor parallelism requested and link minimum met: slowest node link is %d Mbps",
				linkSpeed,
			),
		}, nil
	default:
		return "", nil, &Refusal{
			Code: RefusalInvalidPolicy, Message: fmt.Sprintf("unsupported vLLM parallelism mode %q", requested),
		}
	}
}

func minimumNodeLinkSpeed(nodes []eligibleNode) uint64 {
	minimum := uint64(math.MaxUint64)
	for _, candidate := range nodes {
		fastest := uint64(0)
		for _, link := range candidate.node.NetworkLinks {
			if hasTrustedNetworkLink(candidate.node) && !link.Trusted {
				continue
			}
			if link.SpeedMbps > fastest {
				fastest = link.SpeedMbps
			}
		}
		if fastest == 0 {
			return 0
		}
		if fastest < minimum {
			minimum = fastest
		}
	}
	if minimum == math.MaxUint64 {
		return 0
	}
	return minimum
}

func hasTrustedNetworkLink(node CandidateNode) bool {
	for _, link := range node.NetworkLinks {
		if link.Trusted {
			return true
		}
	}
	return false
}

func homogeneityViolation(nodes []eligibleNode) string {
	if len(nodes) < 2 {
		return ""
	}
	referenceCount := len(nodes[0].devices)
	referenceSignature := deviceSignature(nodes[0].devices[0])
	for _, candidate := range nodes {
		if len(candidate.devices) != referenceCount {
			return fmt.Sprintf(
				"node %q has %d GPU(s), but node %q has %d",
				candidate.node.ID, len(candidate.devices), nodes[0].node.ID, referenceCount,
			)
		}
		for _, device := range candidate.devices {
			if deviceSignature(device) != referenceSignature {
				return fmt.Sprintf(
					"node %q GPU %d differs from the homogeneous GPU signature %q",
					candidate.node.ID, device.Index, referenceSignature,
				)
			}
		}
	}
	return ""
}

func deviceSignature(device hardware.Device) string {
	backends := append([]string(nil), device.Backends...)
	sort.Strings(backends)
	for i := range backends {
		backends[i] = strings.ToLower(strings.TrimSpace(backends[i]))
	}
	return fmt.Sprintf("%s|%s|%s|%d|%s",
		normalizeDeviceKind(device.Kind),
		strings.ToLower(strings.TrimSpace(device.Vendor)),
		strings.ToLower(strings.TrimSpace(device.Name)),
		device.MemoryMB,
		strings.Join(backends, ","),
	)
}
