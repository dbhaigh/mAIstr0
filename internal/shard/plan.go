package shard

import (
	"fmt"
	"math"

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
	ID                        string                 `json:"id"`
	OS                        string                 `json:"os"`
	RPCAddress                string                 `json:"rpc_address,omitempty"`
	RPCProtected              bool                   `json:"rpc_protected,omitempty"`
	AllowInsecureShardNetwork bool                   `json:"allow_insecure_shard_network,omitempty"`
	Devices                   []hardware.Device      `json:"devices"`
	NetworkLinks              []hardware.NetworkLink `json:"network_links,omitempty"`
	Capabilities              engine.Capabilities    `json:"capabilities,omitempty"`
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
	RefusalUntrustedNetwork   RefusalCode = "untrusted_network"
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
