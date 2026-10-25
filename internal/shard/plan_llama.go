package shard

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
)

func planLlamaCPPRPC(
	model ModelProfile,
	weightsBytes, kvBytes, marginBytes uint64,
	nodes []CandidateNode,
	policy Policy,
) (ShardPlan, error) {
	var resources []memoryDevice
	var wholeModelNPUs []memoryDevice
	unprotectedCandidate := false
	otherCandidateRejection := false
	rationale := make([]string, 0, len(nodes)*2)
	for _, node := range nodes {
		reason, supported := nodeModeRejection(node, string(ModeLlamaCPPRPC))
		if supported && !node.RPCProtected {
			reason, supported = "authenticated mTLS RPC relay is unavailable", false
			unprotectedCandidate = true
		}
		if supported && strings.TrimSpace(node.RPCAddress) == "" {
			reason, supported = "RPC address is missing", false
		}
		if supported && strings.ContainsAny(node.RPCAddress, ", \t\r\n") {
			reason, supported = "RPC address contains a comma or whitespace separator", false
		}
		if !supported {
			if reason != "authenticated mTLS RPC relay is unavailable" {
				otherCandidateRejection = true
			}
			rationale = append(rationale, fmt.Sprintf("node %q rejected: %s", node.ID, reason))
			continue
		}
		var added int
		for _, device := range sortedDevices(node.Devices) {
			kind := normalizeDeviceKind(device.Kind)
			if !deviceSupportedByEngine(node.Capabilities, device) {
				rationale = append(rationale, fmt.Sprintf(
					"node %q device %s:%d rejected: engine capability does not include device kind %s",
					node.ID, kind, device.Index, kind,
				))
				continue
			}
			if !engine.BackendsOverlap(node.Capabilities.Backends, device.Backends) {
				rationale = append(rationale, fmt.Sprintf(
					"node %q device %s:%d rejected: engine and device backends do not overlap",
					node.ID, kind, device.Index,
				))
				continue
			}
			switch kind {
			case "gpu":
			case "cpu":
				if !policy.AllowCPUOffload {
					rationale = append(rationale, fmt.Sprintf(
						"node %q CPU device %d excluded: CPU offload is disabled; host RAM is not counted",
						node.ID, device.Index,
					))
					continue
				}
			case "npu":
				if !node.Capabilities.ValidatedRPC {
					entry, ok := makeMemoryDevice(node, device, marginBytes)
					if ok {
						wholeModelNPUs = append(wholeModelNPUs, entry)
					}
					rationale = append(rationale, fmt.Sprintf(
						"node %q NPU device %d excluded from layer sharding: ValidatedRPC is false",
						node.ID, device.Index,
					))
					continue
				}
			default:
				rationale = append(rationale, fmt.Sprintf(
					"node %q device %d rejected: unknown device kind %q",
					node.ID, device.Index, device.Kind,
				))
				continue
			}
			entry, ok := makeMemoryDevice(node, device, marginBytes)
			if !ok {
				rationale = append(rationale, fmt.Sprintf(
					"node %q device %s:%d rejected: free device memory is unavailable or invalid",
					node.ID, kind, device.Index,
				))
				continue
			}
			entry.maxLayers = maximumLayersThatFit(entry.usableBytes, weightsBytes, kvBytes, model.LayerCount)
			resources = append(resources, entry)
			added++
		}
		if added > 0 {
			rationale = append(rationale, fmt.Sprintf(
				"node %q accepted with %d device(s) eligible for proportional layer placement",
				node.ID, added,
			))
		} else if len(node.Devices) == 0 {
			rationale = append(rationale, fmt.Sprintf("node %q rejected: no device inventory was supplied", node.ID))
		}
	}
	sortMemoryDevices(resources)

	assignments, fits := proportionalLayerAssignments(resources, model.LayerCount)
	if fits {
		plan, err := buildLlamaPlan(model, weightsBytes, kvBytes, marginBytes, resources, assignments, rationale)
		if err != nil {
			return ShardPlan{}, &Refusal{
				Code: RefusalInsufficientMemory, Message: err.Error(), Rationale: rationale,
			}
		}
		return plan, nil
	}

	if len(wholeModelNPUs) > 0 {
		if plan, ok := planWholeModelNPU(model, weightsBytes, kvBytes, marginBytes, wholeModelNPUs, rationale); ok {
			return plan, nil
		}
	}

	if len(resources) == 0 && len(wholeModelNPUs) == 0 {
		if unprotectedCandidate && !otherCandidateRejection {
			return ShardPlan{}, &Refusal{
				Code:      RefusalUntrustedNetwork,
				Message:   "llamacpp-rpc requires a pinned mTLS relay on every selected worker",
				Rationale: append(rationale, "no candidate has an authenticated RPC relay endpoint"),
			}
		}
		return ShardPlan{}, &Refusal{
			Code:      RefusalNoCompatibleNodes,
			Message:   "no nodes have a compatible device with known free memory for llamacpp-rpc",
			Rationale: rationale,
		}
	}
	return ShardPlan{}, &Refusal{
		Code:      RefusalInsufficientMemory,
		Message:   fmt.Sprintf("no compatible device set can safely place all %d model layers", model.LayerCount),
		Rationale: append(rationale, layerCapacityRationale(resources, model.LayerCount)...),
	}
}

func planWholeModelNPU(
	model ModelProfile,
	weightsBytes, kvBytes, marginBytes uint64,
	devices []memoryDevice,
	rationale []string,
) (ShardPlan, bool) {
	sort.Slice(devices, func(i, j int) bool {
		if devices[i].freeBytes != devices[j].freeBytes {
			return devices[i].freeBytes > devices[j].freeBytes
		}
		if devices[i].node.ID != devices[j].node.ID {
			return devices[i].node.ID < devices[j].node.ID
		}
		return devices[i].device.Index < devices[j].device.Index
	})
	for _, device := range devices {
		required, ok := addThree(weightsBytes, kvBytes, marginBytes)
		if !ok || required > device.freeBytes {
			rationale = append(rationale, fmt.Sprintf(
				"node %q NPU device %d rejected for whole-model placement: requires %s, has %s",
				device.node.ID, device.device.Index, humanBytes(required), humanBytes(device.freeBytes),
			))
			continue
		}
		allocation := makeAllocation(device.device, 0, model.LayerCount, weightsBytes, kvBytes, marginBytes, device.freeBytes)
		nodePlan := NodePlan{
			NodeID: device.node.ID, RPCAddress: strings.TrimSpace(device.node.RPCAddress),
			StartLayer: 0, EndLayer: model.LayerCount,
			TensorSplitLayers: model.LayerCount,
			Devices:           []DeviceAllocation{allocation},
		}
		return ShardPlan{
			Mode: ModeLlamaCPPRPC, Model: model, Nodes: []NodePlan{nodePlan},
			RPC: nodePlan.RPCAddress, TensorSplit: strconv.Itoa(nodePlan.TensorSplitLayers),
			WholeModel: true,
			Rationale: append(rationale, fmt.Sprintf(
				"node %q selected as a whole-model NPU member; the model is not split across NPU devices",
				device.node.ID,
			)),
		}, true
	}
	return ShardPlan{}, false
}

func buildLlamaPlan(
	model ModelProfile,
	weightsBytes, kvBytes, marginBytes uint64,
	resources []memoryDevice,
	assignments []int,
	rationale []string,
) (ShardPlan, error) {
	nodesByID := make(map[string]*NodePlan)
	nodeOrder := make([]string, 0, len(resources))
	layer := 0
	for i, count := range assignments {
		if count == 0 {
			continue
		}
		resource := resources[i]
		start, end := layer, layer+count
		layer = end
		weightPart, err := multiplyDivideCeil(weightsBytes, uint64(count), uint64(model.LayerCount))
		if err != nil {
			return ShardPlan{}, err
		}
		kvPart, err := multiplyDivideCeil(kvBytes, uint64(count), uint64(model.LayerCount))
		if err != nil {
			return ShardPlan{}, err
		}
		allocation := makeAllocation(resource.device, start, end, weightPart, kvPart, marginBytes, resource.freeBytes)
		if allocation.RequiredBytes > allocation.FreeMemoryBytes {
			return ShardPlan{}, fmt.Errorf(
				"node %q device %s:%d requires %s including safety margin but has %s free",
				resource.node.ID, resource.device.Kind, resource.device.Index,
				humanBytes(allocation.RequiredBytes), humanBytes(allocation.FreeMemoryBytes),
			)
		}
		nodePlan, ok := nodesByID[resource.node.ID]
		if !ok {
			nodePlan = &NodePlan{
				NodeID: resource.node.ID, RPCAddress: strings.TrimSpace(resource.node.RPCAddress),
				StartLayer: start, EndLayer: end,
			}
			nodesByID[resource.node.ID] = nodePlan
			nodeOrder = append(nodeOrder, resource.node.ID)
		} else {
			if start < nodePlan.StartLayer {
				nodePlan.StartLayer = start
			}
			if end > nodePlan.EndLayer {
				nodePlan.EndLayer = end
			}
		}
		nodePlan.TensorSplitLayers += count
		nodePlan.Devices = append(nodePlan.Devices, allocation)
	}
	if layer != model.LayerCount {
		return ShardPlan{}, fmt.Errorf("internal planner assigned %d of %d model layers", layer, model.LayerCount)
	}

	nodePlans := make([]NodePlan, 0, len(nodeOrder))
	addresses := make([]string, 0, len(nodeOrder))
	splits := make([]string, 0, len(nodeOrder))
	for _, id := range nodeOrder {
		node := *nodesByID[id]
		nodePlans = append(nodePlans, node)
		addresses = append(addresses, node.RPCAddress)
		splits = append(splits, strconv.Itoa(node.TensorSplitLayers))
		rationale = append(rationale, fmt.Sprintf(
			"node %q assigned layers [%d,%d) with tensor-split layer weight %d",
			node.NodeID, node.StartLayer, node.EndLayer, node.TensorSplitLayers,
		))
	}
	for i, count := range assignments {
		if count == 0 {
			rationale = append(rationale, fmt.Sprintf(
				"node %q device %s:%d not selected: its proportional safe layer share rounded to zero",
				resources[i].node.ID, resources[i].device.Kind, resources[i].device.Index,
			))
		}
	}
	return ShardPlan{
		Mode: ModeLlamaCPPRPC, Model: model, Nodes: nodePlans,
		RPC: strings.Join(addresses, ","), TensorSplit: strings.Join(splits, ","),
		WholeModel: len(nodePlans) == 1 && len(nodePlans[0].Devices) == 1 &&
			nodePlans[0].EndLayer-nodePlans[0].StartLayer == model.LayerCount,
		Rationale: rationale,
	}, nil
}
