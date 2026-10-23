package shard

import (
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

type Mode string

const (
	ModeLlamaCPPRPC Mode = "llamacpp-rpc"
	ModeVLLMMP      Mode = "vllm-mp"
)

type VLLMParallelism string

const (
	VLLMParallelismAuto              VLLMParallelism = "auto"
	VLLMParallelismPipeline          VLLMParallelism = "pipeline"
	VLLMParallelismTensorAcrossNodes VLLMParallelism = "tensor-across-nodes"
)

// CandidateNode is a caller-provided snapshot. Plan never probes or refreshes it.
type CandidateNode struct {
	ID           string                 `json:"id"`
	OS           string                 `json:"os"`
	RPCAddress   string                 `json:"rpc_address,omitempty"`
	Devices      []hardware.Device      `json:"devices"`
	NetworkLinks []hardware.NetworkLink `json:"network_links,omitempty"`
	Capabilities engine.Capabilities    `json:"capabilities,omitempty"`
}

// Policy configures a deterministic plan. A zero KV cache element size uses
// two bytes (FP16/BF16); a zero vLLM parallelism mode selects pipeline parallel.
type Policy struct {
	Mode                   Mode            `json:"mode"`
	SafetyMarginMB         uint64          `json:"safety_margin_mb,omitempty"`
	KVCacheBytesPerElement uint64          `json:"kv_cache_bytes_per_element,omitempty"`
	AllowCPUOffload        bool            `json:"allow_cpu_offload,omitempty"`
	MinimumLinkSpeedMbps   uint64          `json:"minimum_link_speed_mbps,omitempty"`
	VLLMParallelism        VLLMParallelism `json:"vllm_parallelism,omitempty"`
}

type RefusalCode string

const (
	RefusalInvalidModel       RefusalCode = "invalid_model"
	RefusalInvalidPolicy      RefusalCode = "invalid_policy"
	RefusalInvalidCandidates  RefusalCode = "invalid_candidates"
	RefusalNoCompatibleNodes  RefusalCode = "no_compatible_nodes"
	RefusalInsufficientMemory RefusalCode = "insufficient_memory"
	RefusalHeterogeneity      RefusalCode = "homogeneity_violated"
	RefusalUnsupportedOS      RefusalCode = "unsupported_os"
	RefusalLinkTooSlow        RefusalCode = "link_too_slow"
)

// Refusal is a typed, human-readable explanation that no valid plan was found.
type Refusal struct {
	Code      RefusalCode `json:"code"`
	Message   string      `json:"message"`
	Rationale []string    `json:"rationale"`
}

func (refusal *Refusal) Error() string {
	if refusal == nil {
		return ""
	}
	return refusal.Message
}

// ShardPlan is a complete memory-checked placement. RPC and TensorSplit contain
// the values for llama.cpp's --rpc and --tensor-split options respectively.
type ShardPlan struct {
	Mode                 Mode            `json:"mode"`
	Model                ModelProfile    `json:"model"`
	Nodes                []NodePlan      `json:"nodes"`
	RPC                  string          `json:"rpc,omitempty"`
	TensorSplit          string          `json:"tensor_split,omitempty"`
	TensorParallelSize   int             `json:"tensor_parallel_size,omitempty"`
	PipelineParallelSize int             `json:"pipeline_parallel_size,omitempty"`
	VLLMParallelism      VLLMParallelism `json:"vllm_parallelism,omitempty"`
	WholeModel           bool            `json:"whole_model,omitempty"`
	Rationale            []string        `json:"rationale"`
}

type NodePlan struct {
	NodeID            string             `json:"node_id"`
	RPCAddress        string             `json:"rpc_address,omitempty"`
	StartLayer        int                `json:"start_layer"`
	EndLayer          int                `json:"end_layer"`
	TensorSplitLayers int                `json:"tensor_split_layers,omitempty"`
	Devices           []DeviceAllocation `json:"devices"`
}

type DeviceAllocation struct {
	Kind              string `json:"kind"`
	Vendor            string `json:"vendor,omitempty"`
	Name              string `json:"name,omitempty"`
	Index             int    `json:"index"`
	StartLayer        int    `json:"start_layer"`
	EndLayer          int    `json:"end_layer"`
	WeightsBytes      uint64 `json:"weights_bytes"`
	KVCacheBytes      uint64 `json:"kv_cache_bytes"`
	SafetyMarginBytes uint64 `json:"safety_margin_bytes"`
	RequiredBytes     uint64 `json:"required_bytes"`
	FreeMemoryBytes   uint64 `json:"free_memory_bytes"`
}

type memoryDevice struct {
	node        CandidateNode
	device      hardware.Device
	freeBytes   uint64
	usableBytes uint64
	maxLayers   int
}

type eligibleNode struct {
	node    CandidateNode
	devices []hardware.Device
}

const mebibyte = uint64(1024 * 1024)

// Plan builds a deterministic plan using only model metadata and the supplied
// node snapshots. It performs no network, filesystem, or process I/O.
func Plan(model ModelProfile, candidateNodes []CandidateNode, policy Policy) (ShardPlan, error) {
	if err := validateModelProfile(model); err != nil {
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidModel, Message: "model profile is incomplete: " + err.Error(),
			Rationale: []string{err.Error()},
		}
	}
	if policy.Mode != ModeLlamaCPPRPC && policy.Mode != ModeVLLMMP {
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidPolicy, Message: fmt.Sprintf("unsupported sharding mode %q", policy.Mode),
			Rationale: []string{fmt.Sprintf("mode %q is not supported", policy.Mode)},
		}
	}
	if policy.SafetyMarginMB > math.MaxUint64/mebibyte {
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidPolicy, Message: "safety margin overflows memory size calculations",
			Rationale: []string{"configured safety margin cannot be represented in bytes"},
		}
	}
	if policy.Mode == ModeVLLMMP {
		switch policy.VLLMParallelism {
		case "", VLLMParallelismAuto, VLLMParallelismPipeline, VLLMParallelismTensorAcrossNodes:
		default:
			return ShardPlan{}, &Refusal{
				Code:    RefusalInvalidPolicy,
				Message: fmt.Sprintf("unsupported vLLM parallelism mode %q", policy.VLLMParallelism),
				Rationale: []string{fmt.Sprintf(
					"parallelism mode %q is not supported", policy.VLLMParallelism,
				)},
			}
		}
	}

	weightsBytes, err := model.EffectiveWeightsSizeBytes()
	if err != nil {
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidModel, Message: err.Error(), Rationale: []string{err.Error()},
		}
	}
	kvBytesPerElement := policy.KVCacheBytesPerElement
	if kvBytesPerElement == 0 {
		kvBytesPerElement = 2
	}
	kvBytes, ok := modelKVBytes(model, kvBytesPerElement)
	if !ok {
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidModel, Message: "KV cache size overflows memory size calculations",
			Rationale: []string{"the requested context and model dimensions overflow the KV cache size"},
		}
	}
	marginBytes := policy.SafetyMarginMB * mebibyte
	nodes, err := normalizeCandidates(candidateNodes)
	if err != nil {
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidCandidates, Message: err.Error(), Rationale: []string{err.Error()},
		}
	}
	if len(nodes) == 0 {
		return ShardPlan{}, &Refusal{
			Code: RefusalNoCompatibleNodes, Message: "no candidate nodes were provided",
			Rationale: []string{"candidate node list is empty"},
		}
	}

	switch policy.Mode {
	case ModeLlamaCPPRPC:
		return planLlamaCPPRPC(model, weightsBytes, kvBytes, marginBytes, nodes, policy)
	case ModeVLLMMP:
		return planVLLMMP(model, weightsBytes, kvBytes, marginBytes, nodes, policy)
	default:
		return ShardPlan{}, &Refusal{
			Code: RefusalInvalidPolicy, Message: fmt.Sprintf("unsupported sharding mode %q", policy.Mode),
			Rationale: []string{fmt.Sprintf("mode %q is not supported", policy.Mode)},
		}
	}
}

func planLlamaCPPRPC(
	model ModelProfile,
	weightsBytes, kvBytes, marginBytes uint64,
	nodes []CandidateNode,
	policy Policy,
) (ShardPlan, error) {
	var resources []memoryDevice
	var wholeModelNPUs []memoryDevice
	rationale := make([]string, 0, len(nodes)*2)
	for _, node := range nodes {
		reason, supported := nodeModeRejection(node, string(ModeLlamaCPPRPC))
		if supported && strings.TrimSpace(node.RPCAddress) == "" {
			reason, supported = "RPC address is missing", false
		}
		if supported && strings.ContainsAny(node.RPCAddress, ", \t\r\n") {
			reason, supported = "RPC address contains a comma or whitespace separator", false
		}
		if !supported {
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

func planVLLMMP(
	model ModelProfile,
	weightsBytes, kvBytes, marginBytes uint64,
	nodes []CandidateNode,
	policy Policy,
) (ShardPlan, error) {
	rationale := make([]string, 0, len(nodes)*2)
	var eligible []eligibleNode
	var hasLinuxCandidate bool
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

func layerRange(totalLayers, partitions, index int) (int, int) {
	base, remainder := totalLayers/partitions, totalLayers%partitions
	start := index * base
	if index < remainder {
		start += index
	} else {
		start += remainder
	}
	count := base
	if index < remainder {
		count++
	}
	return start, start + count
}

func proportionalLayerAssignments(resources []memoryDevice, layerCount int) ([]int, bool) {
	if len(resources) == 0 {
		return nil, false
	}
	totalCapacity := uint64(0)
	for i := range resources {
		if resources[i].freeBytes > math.MaxUint64-totalCapacity {
			return nil, false
		}
		totalCapacity += resources[i].freeBytes
	}
	if totalCapacity == 0 {
		return nil, false
	}
	maxLayerCapacity := 0
	for _, resource := range resources {
		if resource.maxLayers >= layerCount-maxLayerCapacity {
			maxLayerCapacity = layerCount
			break
		}
		maxLayerCapacity += resource.maxLayers
	}
	if maxLayerCapacity < layerCount {
		return nil, false
	}

	assignments := make([]int, len(resources))
	initialRemainders := make([]uint64, len(resources))
	assigned := 0
	for i, resource := range resources {
		if resource.freeBytes == 0 {
			continue
		}
		share, remainder, ok := multiplyDivideFloorRem(uint64(layerCount), resource.freeBytes, totalCapacity)
		if !ok {
			return nil, false
		}
		if share > uint64(resource.maxLayers) {
			share = uint64(resource.maxLayers)
		}
		assignments[i] = int(share)
		initialRemainders[i] = remainder
		assigned += int(share)
	}
	remaining := layerCount - assigned
	if remaining > 0 {
		order := make([]int, 0, len(resources))
		for i, resource := range resources {
			if assignments[i] < resource.maxLayers {
				order = append(order, i)
			}
		}
		sort.SliceStable(order, func(i, j int) bool {
			left, right := order[i], order[j]
			if initialRemainders[left] != initialRemainders[right] {
				return initialRemainders[left] > initialRemainders[right]
			}
			return left < right
		})
		for _, i := range order {
			if remaining == 0 {
				break
			}
			assignments[i]++
			remaining--
		}
	}
	for remaining > 0 {
		activeWeight := uint64(0)
		for i, resource := range resources {
			if assignments[i] < resource.maxLayers {
				if resource.freeBytes > math.MaxUint64-activeWeight {
					return nil, false
				}
				activeWeight += resource.freeBytes
			}
		}
		if activeWeight == 0 {
			return nil, false
		}
		before := remaining
		roundLayers := remaining
		roundRemainders := make([]uint64, len(resources))
		for i, resource := range resources {
			available := resource.maxLayers - assignments[i]
			if available <= 0 {
				continue
			}
			share, remainder, ok := multiplyDivideFloorRem(
				uint64(roundLayers), resource.freeBytes, activeWeight,
			)
			if !ok {
				return nil, false
			}
			if share > uint64(available) {
				share = uint64(available)
			}
			assignments[i] += int(share)
			remaining -= int(share)
			roundRemainders[i] = remainder
		}
		if remaining == 0 {
			break
		}
		order := make([]int, 0, len(resources))
		for i, resource := range resources {
			if assignments[i] < resource.maxLayers {
				order = append(order, i)
			}
		}
		sort.SliceStable(order, func(i, j int) bool {
			left, right := order[i], order[j]
			if roundRemainders[left] != roundRemainders[right] {
				return roundRemainders[left] > roundRemainders[right]
			}
			return left < right
		})
		for _, i := range order {
			if remaining == 0 {
				break
			}
			assignments[i]++
			remaining--
		}
		if remaining == before {
			return nil, false
		}
	}
	return assignments, true
}

func maximumLayersThatFit(usableBytes, weightsBytes, kvBytes uint64, layerCount int) int {
	low, high := 0, layerCount
	for low < high {
		middle := low + (high-low+1)/2
		weights, weightsErr := multiplyDivideCeil(weightsBytes, uint64(middle), uint64(layerCount))
		kv, kvErr := multiplyDivideCeil(kvBytes, uint64(middle), uint64(layerCount))
		total, totalOK := addTwo(weights, kv)
		if weightsErr == nil && kvErr == nil && totalOK && total <= usableBytes {
			low = middle
		} else {
			high = middle - 1
		}
	}
	return low
}

func layerCapacityRationale(resources []memoryDevice, requiredLayers int) []string {
	result := make([]string, 0, len(resources)+1)
	maximum := 0
	for _, resource := range resources {
		maximum += resource.maxLayers
		result = append(result, fmt.Sprintf(
			"node %q device %s:%d can safely hold at most %d of %d layers after its safety margin",
			resource.node.ID, resource.device.Kind, resource.device.Index,
			resource.maxLayers, requiredLayers,
		))
	}
	result = append(result, fmt.Sprintf(
		"compatible devices can hold at most %d of %d layers in total", maximum, requiredLayers,
	))
	return result
}

func normalizeCandidates(candidates []CandidateNode) ([]CandidateNode, error) {
	nodes := make([]CandidateNode, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for i, candidate := range candidates {
		candidate.ID = strings.TrimSpace(candidate.ID)
		if candidate.ID == "" {
			return nil, fmt.Errorf("candidate node at index %d has an empty ID", i)
		}
		if _, exists := seen[candidate.ID]; exists {
			return nil, fmt.Errorf("candidate node ID %q is duplicated", candidate.ID)
		}
		seen[candidate.ID] = struct{}{}
		deviceIDs := make(map[string]struct{}, len(candidate.Devices))
		for _, device := range candidate.Devices {
			kind := normalizeDeviceKind(device.Kind)
			key := fmt.Sprintf("%s:%d", kind, device.Index)
			if _, exists := deviceIDs[key]; exists {
				return nil, fmt.Errorf("candidate node %q repeats device %s", candidate.ID, key)
			}
			deviceIDs[key] = struct{}{}
			if device.Index < 0 {
				return nil, fmt.Errorf("candidate node %q has a negative device index", candidate.ID)
			}
			if device.MemoryMB > 0 && device.FreeMemoryMB > device.MemoryMB {
				return nil, fmt.Errorf(
					"candidate node %q device %s has more free memory than total memory",
					candidate.ID, key,
				)
			}
		}
		candidate.OS = strings.ToLower(strings.TrimSpace(candidate.OS))
		candidate.RPCAddress = strings.TrimSpace(candidate.RPCAddress)
		candidate.Devices = append([]hardware.Device(nil), candidate.Devices...)
		candidate.NetworkLinks = append([]hardware.NetworkLink(nil), candidate.NetworkLinks...)
		candidate.Capabilities = engine.CloneCapabilities(candidate.Capabilities)
		nodes[i] = candidate
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

func nodeModeRejection(node CandidateNode, mode string) (string, bool) {
	if len(node.Capabilities.SupportsSharding) > 0 {
		found := false
		for _, supported := range node.Capabilities.SupportsSharding {
			if strings.EqualFold(strings.TrimSpace(supported), mode) {
				found = true
				break
			}
		}
		if !found {
			return "engine capability does not advertise " + mode, false
		}
	}
	if len(node.Capabilities.OS) > 0 {
		found := false
		for _, supported := range node.Capabilities.OS {
			if strings.EqualFold(strings.TrimSpace(supported), node.OS) {
				found = true
				break
			}
		}
		if !found {
			return "engine capability does not support operating system " + node.OS, false
		}
	}
	return "", true
}

func deviceSupportedByEngine(capabilities engine.Capabilities, device hardware.Device) bool {
	if len(capabilities.Devices) > 0 &&
		!engine.SupportsDeviceKind(capabilities.Devices, normalizeDeviceKind(device.Kind)) {
		return false
	}
	return true
}

func sortedDevices(devices []hardware.Device) []hardware.Device {
	result := append([]hardware.Device(nil), devices...)
	sort.Slice(result, func(i, j int) bool {
		left, right := normalizeDeviceKind(result[i].Kind), normalizeDeviceKind(result[j].Kind)
		if left != right {
			return deviceKindOrder(left) < deviceKindOrder(right)
		}
		if result[i].Index != result[j].Index {
			return result[i].Index < result[j].Index
		}
		if result[i].Vendor != result[j].Vendor {
			return result[i].Vendor < result[j].Vendor
		}
		return result[i].Name < result[j].Name
	})
	return result
}

func sortMemoryDevices(devices []memoryDevice) {
	sort.Slice(devices, func(i, j int) bool {
		left, right := devices[i], devices[j]
		if left.node.ID != right.node.ID {
			return left.node.ID < right.node.ID
		}
		leftKind, rightKind := normalizeDeviceKind(left.device.Kind), normalizeDeviceKind(right.device.Kind)
		if leftKind != rightKind {
			return deviceKindOrder(leftKind) < deviceKindOrder(rightKind)
		}
		if left.device.Index != right.device.Index {
			return left.device.Index < right.device.Index
		}
		if left.device.Vendor != right.device.Vendor {
			return left.device.Vendor < right.device.Vendor
		}
		return left.device.Name < right.device.Name
	})
}

func makeMemoryDevice(node CandidateNode, device hardware.Device, marginBytes uint64) (memoryDevice, bool) {
	freeBytes, ok := mbToBytes(device.FreeMemoryMB)
	if !ok || freeBytes <= marginBytes {
		return memoryDevice{}, false
	}
	return memoryDevice{
		node: node, device: device, freeBytes: freeBytes,
		usableBytes: freeBytes - marginBytes,
	}, true
}

func makeAllocation(
	device hardware.Device,
	startLayer, endLayer int,
	weightsBytes, kvBytes, marginBytes, freeBytes uint64,
) DeviceAllocation {
	required, ok := addThree(weightsBytes, kvBytes, marginBytes)
	if !ok {
		required = math.MaxUint64
	}
	return DeviceAllocation{
		Kind: normalizeDeviceKind(device.Kind), Vendor: device.Vendor, Name: device.Name,
		Index: device.Index, StartLayer: startLayer, EndLayer: endLayer,
		WeightsBytes: weightsBytes, KVCacheBytes: kvBytes,
		SafetyMarginBytes: marginBytes, RequiredBytes: required, FreeMemoryBytes: freeBytes,
	}
}

func modelKVBytes(model ModelProfile, bytesPerElement uint64) (uint64, bool) {
	value := uint64(2)
	factors := []uint64{
		uint64(model.LayerCount), uint64(model.KVHeads), uint64(model.HeadDim),
		uint64(model.RequestedContext), bytesPerElement,
	}
	for _, factor := range factors {
		if value != 0 && factor > math.MaxUint64/value {
			return 0, false
		}
		value *= factor
	}
	return value, true
}

func mbToBytes(memoryMB uint64) (uint64, bool) {
	if memoryMB > math.MaxUint64/mebibyte {
		return 0, false
	}
	return memoryMB * mebibyte, true
}

func multiplyDivideFloorRem(a, b, divisor uint64) (uint64, uint64, bool) {
	if divisor == 0 {
		return 0, 0, false
	}
	hi, lo := bits.Mul64(a, b)
	if hi >= divisor {
		return 0, 0, false
	}
	quotient, remainder := bits.Div64(hi, lo, divisor)
	return quotient, remainder, true
}

func addTwo(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}

func addThree(a, b, c uint64) (uint64, bool) {
	sum, ok := addTwo(a, b)
	if !ok {
		return 0, false
	}
	return addTwo(sum, c)
}

func normalizeDeviceKind(kind string) string {
	return strings.ToLower(strings.TrimSpace(kind))
}

func deviceKindOrder(kind string) int {
	switch kind {
	case "gpu":
		return 0
	case "npu":
		return 1
	case "cpu":
		return 2
	default:
		return 3
	}
}

func humanBytes(value uint64) string {
	const gib = uint64(1024 * 1024 * 1024)
	if value%gib == 0 {
		return fmt.Sprintf("%d GiB", value/gib)
	}
	return fmt.Sprintf("%.2f GiB", float64(value)/float64(gib))
}
