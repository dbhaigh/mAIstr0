package shard

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/testutil"
)

const (
	testMiB = uint64(1024 * 1024)
	testGiB = uint64(1024 * 1024 * 1024)
)

var updateShardGoldens = flag.Bool("update-shard-goldens", false, "update shard planner golden files")

func TestPlanGoldens(t *testing.T) {
	tests := []struct {
		name   string
		model  ModelProfile
		nodes  []CandidateNode
		policy Policy
	}{
		{
			name: "7b",
			model: ModelProfile{
				Architecture: "llama", Parameters: 7_000_000_000, LayerCount: 32,
				HiddenSize: 4096, KVHeads: 8, HeadDim: 128, VocabSize: 32000,
				Quantization: "Q4_K_M", WeightsSizeBytes: 4_000_000_000, RequestedContext: 4096,
			},
			nodes: []CandidateNode{
				llamaNode("gpu-a", "10.0.0.1:50052", gpu("nvidia", "A10", 0, 24576, 8192)),
				llamaNode("gpu-b", "10.0.0.2:50052", gpu("nvidia", "A10", 0, 24576, 16384)),
			},
			policy: Policy{Mode: ModeLlamaCPPRPC, SafetyMarginMB: 512},
		},
		{
			name: "70b",
			model: ModelProfile{
				Architecture: "llama", Parameters: 70_000_000_000, LayerCount: 80,
				HiddenSize: 8192, KVHeads: 8, HeadDim: 128, VocabSize: 32000,
				Quantization: "Q4_K_M", WeightsSizeBytes: 36 * testGiB, RequestedContext: 4096,
			},
			nodes: []CandidateNode{
				llamaNode("gpu-a", "10.0.1.1:50052", gpu("nvidia", "A10", 0, 24576, 24576)),
				llamaNode("gpu-b", "10.0.1.2:50052", gpu("nvidia", "A10", 0, 49152, 49152)),
			},
			policy: Policy{Mode: ModeLlamaCPPRPC, SafetyMarginMB: 1024},
		},
		{
			name: "mixed-cpu-gpu-npu",
			model: ModelProfile{
				Architecture: "llama", Parameters: 7_000_000_000, LayerCount: 32,
				HiddenSize: 4096, KVHeads: 8, HeadDim: 128, VocabSize: 32000,
				Quantization: "Q4_K_M", WeightsSizeBytes: 10 * testGiB, RequestedContext: 4096,
			},
			nodes: []CandidateNode{
				llamaNode("cpu-node", "10.0.2.1:50052", hardware.Device{
					Kind: "cpu", Vendor: "intel", Name: "Xeon", Index: 0, FreeMemoryMB: 131072,
				}),
				llamaNode("gpu-a", "10.0.2.2:50052", gpu("nvidia", "A10", 0, 8192, 8192)),
				llamaNode("gpu-b", "10.0.2.3:50052", gpu("nvidia", "A10", 0, 8192, 8192)),
				llamaNode("npu-node", "10.0.2.4:50052", hardware.Device{
					Kind: "npu", Vendor: "intel", Name: "NPU", Index: 0, FreeMemoryMB: 8192,
				}),
			},
			policy: Policy{Mode: ModeLlamaCPPRPC, SafetyMarginMB: 512},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := Plan(test.model, test.nodes, test.policy)
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			testutil.AssertJSONGolden(
				t, filepath.Join("testdata", "golden", "plan-"+test.name+".json"),
				plan, *updateShardGoldens,
			)
		})
	}
}

func TestPlanLlamaRPCProducesMemoryProportionalSplit(t *testing.T) {
	plan, err := Plan(profile7B(), []CandidateNode{
		llamaNode("a-small", "small:50052", gpu("nvidia", "A10", 0, 24576, 8192)),
		llamaNode("b-large", "large:50052", gpu("nvidia", "A10", 0, 24576, 16384)),
	}, Policy{Mode: ModeLlamaCPPRPC})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan.RPC != "small:50052,large:50052" || plan.TensorSplit != "11,21" {
		t.Fatalf("RPC/tensor split = %q/%q", plan.RPC, plan.TensorSplit)
	}
	if plan.Nodes[0].EndLayer-plan.Nodes[0].StartLayer != 11 ||
		plan.Nodes[1].EndLayer-plan.Nodes[1].StartLayer != 21 {
		t.Fatalf("layer split = [%d,%d), [%d,%d)",
			plan.Nodes[0].StartLayer, plan.Nodes[0].EndLayer,
			plan.Nodes[1].StartLayer, plan.Nodes[1].EndLayer,
		)
	}
}

func TestPlanLlamaRPCRedistributesLayersAroundDeviceCapacity(t *testing.T) {
	model := profileWithWeights(32, 30*testGiB)
	plan, err := Plan(model, []CandidateNode{
		llamaNode("a-small", "small:50052", gpu("nvidia", "A10", 0, 4096, 4096)),
		llamaNode("b-large", "large:50052", gpu("nvidia", "A10", 0, 40960, 40960)),
	}, Policy{Mode: ModeLlamaCPPRPC, SafetyMarginMB: 2048})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan.Nodes[0].EndLayer-plan.Nodes[0].StartLayer != 2 ||
		plan.Nodes[1].EndLayer-plan.Nodes[1].StartLayer != 30 {
		t.Fatalf("capacity-adjusted split = [%d,%d), [%d,%d)",
			plan.Nodes[0].StartLayer, plan.Nodes[0].EndLayer,
			plan.Nodes[1].StartLayer, plan.Nodes[1].EndLayer,
		)
	}
	assertPlanWithinDeviceMemory(t, plan)
}

func TestPlanUsesSpecifiedKVFormulaAndPerDeviceMargin(t *testing.T) {
	model := ModelProfile{
		LayerCount: 2, HiddenSize: 8, KVHeads: 2, HeadDim: 4,
		WeightsSizeBytes: 1024, RequestedContext: 8,
	}
	plan, err := Plan(model, []CandidateNode{
		llamaNode("a", "a:50052", gpu("nvidia", "A10", 0, 16, 16)),
		llamaNode("b", "b:50052", gpu("nvidia", "A10", 0, 16, 16)),
	}, Policy{Mode: ModeLlamaCPPRPC, SafetyMarginMB: 1})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	for _, node := range plan.Nodes {
		device := node.Devices[0]
		if device.WeightsBytes != 512 || device.KVCacheBytes != 256 ||
			device.SafetyMarginBytes != testMiB ||
			device.RequiredBytes != 512+256+testMiB {
			t.Fatalf("device memory accounting = %+v", device)
		}
	}
}

func TestPlanVLLMParallelismAndLinkPolicy(t *testing.T) {
	nodes := vllmNodes(1_000)
	plan, err := Plan(profile7B(), nodes, Policy{
		Mode: ModeVLLMMP, VLLMParallelism: VLLMParallelismAuto,
		MinimumLinkSpeedMbps: 10_000,
	})
	if err != nil {
		t.Fatalf("Plan() on slow links error = %v", err)
	}
	if plan.TensorParallelSize != 1 || plan.PipelineParallelSize != 2 ||
		plan.VLLMParallelism != VLLMParallelismPipeline {
		t.Fatalf("slow-link parallelism = tp:%d pp:%d topology:%q",
			plan.TensorParallelSize, plan.PipelineParallelSize, plan.VLLMParallelism)
	}
	if !containsRationale(plan.Rationale, "preferring pipeline parallelism") {
		t.Fatalf("slow-link rationale = %v", plan.Rationale)
	}

	nodes = vllmNodes(100_000)
	plan, err = Plan(profile7B(), nodes, Policy{
		Mode: ModeVLLMMP, VLLMParallelism: VLLMParallelismAuto,
		MinimumLinkSpeedMbps: 10_000,
	})
	if err != nil {
		t.Fatalf("Plan() on fast links error = %v", err)
	}
	if plan.TensorParallelSize != 2 || plan.PipelineParallelSize != 1 ||
		plan.VLLMParallelism != VLLMParallelismTensorAcrossNodes {
		t.Fatalf("fast-link parallelism = tp:%d pp:%d topology:%q",
			plan.TensorParallelSize, plan.PipelineParallelSize, plan.VLLMParallelism)
	}
}

func TestPlanTypedRefusals(t *testing.T) {
	tests := []struct {
		name    string
		model   ModelProfile
		nodes   []CandidateNode
		policy  Policy
		want    RefusalCode
		message string
	}{
		{
			name: "nothing fits", model: profileWithWeights(32, 20*testGiB),
			nodes:  []CandidateNode{llamaNode("tiny", "tiny:50052", gpu("nvidia", "small", 0, 1024, 1024))},
			policy: Policy{Mode: ModeLlamaCPPRPC}, want: RefusalInsufficientMemory,
		},
		{
			name: "vllm heterogeneity", model: profile7B(),
			nodes: []CandidateNode{
				vllmNode("one", "A10", 65536, 24000),
				vllmNode("two", "L40", 65536, 24000),
			},
			policy: Policy{Mode: ModeVLLMMP}, want: RefusalHeterogeneity,
		},
		{
			name: "link too slow", model: profile7B(), nodes: vllmNodes(1000),
			policy: Policy{
				Mode: ModeVLLMMP, VLLMParallelism: VLLMParallelismTensorAcrossNodes,
				MinimumLinkSpeedMbps: 10_000,
			},
			want: RefusalLinkTooSlow,
		},
		{
			name: "vllm windows", model: profile7B(),
			nodes: []CandidateNode{{
				ID: "windows", OS: "windows", Devices: []hardware.Device{gpu("nvidia", "A10", 0, 24576, 24576)},
				Capabilities: plannerCapabilities(),
			}},
			policy: Policy{Mode: ModeVLLMMP}, want: RefusalUnsupportedOS,
		},
		{
			name: "host RAM is not device memory", model: profileWithWeights(32, 2*testGiB),
			nodes: []CandidateNode{llamaNode("cpu", "cpu:50052", hardware.Device{
				Kind: "cpu", Name: "CPU", Index: 0, FreeMemoryMB: 256 * 1024,
			})},
			policy: Policy{Mode: ModeLlamaCPPRPC}, want: RefusalNoCompatibleNodes,
		},
		{
			name: "duplicate devices", model: profile7B(),
			nodes: []CandidateNode{llamaNode("duplicate", "duplicate:50052",
				gpu("nvidia", "A10", 0, 24576, 24576),
				gpu("nvidia", "A10", 0, 24576, 24576),
			)},
			policy: Policy{Mode: ModeLlamaCPPRPC}, want: RefusalInvalidCandidates,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Plan(test.model, test.nodes, test.policy)
			var refusal *Refusal
			if !errors.As(err, &refusal) {
				t.Fatalf("Plan() error = %v, want *Refusal", err)
			}
			if refusal.Code != test.want {
				t.Fatalf("refusal code = %q, want %q (%s)", refusal.Code, test.want, refusal.Message)
			}
			if strings.TrimSpace(refusal.Message) == "" || len(refusal.Rationale) == 0 {
				t.Fatalf("refusal must include a message and rationale: %+v", refusal)
			}
		})
	}
}

func TestPlanAllowsCPUOffloadOnlyWhenExplicit(t *testing.T) {
	model := profileWithWeights(32, 2*testGiB)
	node := llamaNode("cpu", "cpu:50052", hardware.Device{
		Kind: "cpu", Vendor: "intel", Name: "CPU", Index: 0, FreeMemoryMB: 8192,
	})
	plan, err := Plan(model, []CandidateNode{node}, Policy{
		Mode: ModeLlamaCPPRPC, AllowCPUOffload: true,
	})
	if err != nil {
		t.Fatalf("CPU-offload Plan() error = %v", err)
	}
	if len(plan.Nodes) != 1 || plan.Nodes[0].Devices[0].Kind != "cpu" {
		t.Fatalf("CPU offload plan = %+v", plan.Nodes)
	}
}

func TestPlanNPURequiresValidatedRPCForLayerShards(t *testing.T) {
	model := profileWithWeights(32, 16*testGiB)
	nodes := []CandidateNode{
		llamaNode("gpu", "gpu:50052", gpu("nvidia", "A10", 0, 16384, 16384)),
		{
			ID: "npu", OS: "linux", RPCAddress: "npu:50052",
			Devices:      []hardware.Device{{Kind: "npu", Vendor: "intel", Name: "NPU", Index: 0, FreeMemoryMB: 16384}},
			Capabilities: plannerCapabilities(),
		},
	}
	if _, err := Plan(model, nodes, Policy{Mode: ModeLlamaCPPRPC}); err == nil {
		t.Fatal("unvalidated NPU should not be used as a layer shard")
	} else {
		var refusal *Refusal
		if !errors.As(err, &refusal) || refusal.Code != RefusalInsufficientMemory {
			t.Fatalf("Plan() error = %v, want insufficient-memory refusal", err)
		}
		if !containsRationale(refusal.Rationale, "ValidatedRPC is false") {
			t.Fatalf("refusal rationale = %v", refusal.Rationale)
		}
	}

	nodes[1].Capabilities.ValidatedRPC = true
	plan, err := Plan(model, nodes, Policy{Mode: ModeLlamaCPPRPC})
	if err != nil {
		t.Fatalf("validated NPU Plan() error = %v", err)
	}
	foundNPU := false
	for _, node := range plan.Nodes {
		for _, device := range node.Devices {
			if device.Kind == "npu" {
				foundNPU = true
			}
		}
	}
	if !foundNPU {
		t.Fatalf("validated NPU was not used: %+v", plan.Nodes)
	}
}

func TestPlanAllowsUnvalidatedNPUOnlyAsWholeModel(t *testing.T) {
	model := profileWithWeights(32, 4*testGiB)
	node := CandidateNode{
		ID: "npu", OS: "linux", RPCAddress: "npu:50052",
		Devices:      []hardware.Device{{Kind: "npu", Vendor: "intel", Name: "NPU", Index: 0, FreeMemoryMB: 8192}},
		Capabilities: plannerCapabilities(),
	}
	plan, err := Plan(model, []CandidateNode{node}, Policy{Mode: ModeLlamaCPPRPC})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if !plan.WholeModel || len(plan.Nodes) != 1 || len(plan.Nodes[0].Devices) != 1 ||
		plan.Nodes[0].Devices[0].StartLayer != 0 ||
		plan.Nodes[0].Devices[0].EndLayer != model.LayerCount {
		t.Fatalf("unvalidated NPU plan is not whole-model: %+v", plan)
	}
}

func TestPlanNeverExceedsDeviceMemory(t *testing.T) {
	random := rand.New(rand.NewSource(73421))
	for iteration := 0; iteration < 300; iteration++ {
		layerCount := 2 + random.Intn(95)
		model := ModelProfile{
			LayerCount: layerCount, HiddenSize: 4096, KVHeads: 1 + random.Intn(32),
			HeadDim: 32 + random.Intn(225), RequestedContext: 128 + random.Intn(8064),
			WeightsSizeBytes: uint64(1+random.Intn(64)) * testGiB,
		}
		nodeCount := 1 + random.Intn(8)
		nodes := make([]CandidateNode, 0, nodeCount)
		for index := 0; index < nodeCount; index++ {
			free := uint64(512 + random.Intn(32768))
			nodes = append(nodes, llamaNode(
				fmt.Sprintf("node-%02d", index), fmt.Sprintf("node-%02d:50052", index),
				gpu("nvidia", "test-gpu", 0, free, free),
			))
		}
		plan, err := Plan(model, nodes, Policy{
			Mode: ModeLlamaCPPRPC, SafetyMarginMB: uint64(random.Intn(1025)),
		})
		if err == nil {
			assertPlanWithinDeviceMemory(t, plan)
			assertContiguousLayerCoverage(t, plan, layerCount)
		}
	}

	for iteration := 0; iteration < 100; iteration++ {
		layerCount := 4 + random.Intn(60)
		nodeCount := 1 + random.Intn(4)
		if nodeCount > layerCount {
			continue
		}
		model := ModelProfile{
			LayerCount: layerCount, HiddenSize: 4096, KVHeads: 8, HeadDim: 128,
			RequestedContext: 256 + random.Intn(4096),
			WeightsSizeBytes: uint64(1+random.Intn(30)) * testGiB,
		}
		nodes := make([]CandidateNode, 0, nodeCount)
		for index := 0; index < nodeCount; index++ {
			devices := make([]hardware.Device, 1+random.Intn(4))
			for deviceIndex := range devices {
				free := uint64(4096 + random.Intn(65536))
				devices[deviceIndex] = gpu("nvidia", "homogeneous", deviceIndex, 65536, free)
			}
			node := vllmNodeWithDevices(fmt.Sprintf("node-%02d", index), devices, 100_000)
			nodes = append(nodes, node)
		}
		plan, err := Plan(model, nodes, Policy{
			Mode: ModeVLLMMP, SafetyMarginMB: uint64(random.Intn(2049)),
		})
		if err == nil {
			assertPlanWithinDeviceMemory(t, plan)
		}
	}
}

func TestPlanIsIndependentOfCandidateInputOrder(t *testing.T) {
	nodes := []CandidateNode{
		llamaNode("b", "b:50052", gpu("nvidia", "A10", 0, 24576, 12288)),
		llamaNode("a", "a:50052", gpu("nvidia", "A10", 0, 24576, 8192)),
	}
	first, err := Plan(profile7B(), nodes, Policy{Mode: ModeLlamaCPPRPC})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	nodes[0], nodes[1] = nodes[1], nodes[0]
	second, err := Plan(profile7B(), nodes, Policy{Mode: ModeLlamaCPPRPC})
	if err != nil {
		t.Fatalf("Plan() after input reorder error = %v", err)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if !reflect.DeepEqual(firstJSON, secondJSON) {
		t.Fatalf("candidate order changed plan:\n%s\n%s", firstJSON, secondJSON)
	}
}

func assertPlanWithinDeviceMemory(t *testing.T, plan ShardPlan) {
	t.Helper()
	for _, node := range plan.Nodes {
		for _, device := range node.Devices {
			if device.RequiredBytes > device.FreeMemoryBytes {
				t.Fatalf(
					"plan exceeds %s:%d memory on node %q: requires %d, has %d",
					device.Kind, device.Index, node.NodeID, device.RequiredBytes, device.FreeMemoryBytes,
				)
			}
			if device.RequiredBytes != device.WeightsBytes+device.KVCacheBytes+device.SafetyMarginBytes {
				t.Fatalf("reported device requirement is inconsistent: %+v", device)
			}
		}
	}
}

func assertContiguousLayerCoverage(t *testing.T, plan ShardPlan, layerCount int) {
	t.Helper()
	var allocations []DeviceAllocation
	for _, node := range plan.Nodes {
		allocations = append(allocations, node.Devices...)
	}
	sort.Slice(allocations, func(i, j int) bool {
		return allocations[i].StartLayer < allocations[j].StartLayer
	})
	next := 0
	for _, allocation := range allocations {
		if allocation.StartLayer != next || allocation.EndLayer < allocation.StartLayer {
			t.Fatalf("layer assignment has gap or overlap at %d: %+v", next, allocation)
		}
		next = allocation.EndLayer
	}
	if next != layerCount {
		t.Fatalf("layer assignments cover %d layers, want %d", next, layerCount)
	}
}

func profile7B() ModelProfile {
	return ModelProfile{
		Architecture: "llama", Parameters: 7_000_000_000, LayerCount: 32,
		HiddenSize: 4096, KVHeads: 8, HeadDim: 128, VocabSize: 32000,
		Quantization: "Q4_K_M", WeightsSizeBytes: 4_000_000_000, RequestedContext: 4096,
	}
}

func profileWithWeights(layers int, weights uint64) ModelProfile {
	model := profile7B()
	model.LayerCount = layers
	model.WeightsSizeBytes = weights
	return model
}

func plannerCapabilities() engine.Capabilities {
	return engine.Capabilities{
		Devices:          []string{"cpu", "gpu", "npu"},
		SupportsSharding: []string{string(ModeLlamaCPPRPC), string(ModeVLLMMP)},
	}
}

func llamaNode(id, address string, devices ...hardware.Device) CandidateNode {
	return CandidateNode{
		ID: id, OS: "linux", RPCAddress: address, Devices: devices,
		Capabilities: plannerCapabilities(),
	}
}

func vllmNodes(speed uint64) []CandidateNode {
	return []CandidateNode{
		vllmNode("node-a", "A10", 24576, speed),
		vllmNode("node-b", "A10", 24576, speed),
	}
}

func vllmNode(id, name string, memory, speed uint64) CandidateNode {
	return vllmNodeWithDevices(id, []hardware.Device{gpu("nvidia", name, 0, memory, memory)}, speed)
}

func vllmNodeWithDevices(id string, devices []hardware.Device, speed uint64) CandidateNode {
	return CandidateNode{
		ID: id, OS: "linux", Devices: devices,
		NetworkLinks: []hardware.NetworkLink{{Name: "eth0", SpeedMbps: speed}},
		Capabilities: plannerCapabilities(),
	}
}

func gpu(vendor, name string, index int, memoryMB, freeMemoryMB uint64) hardware.Device {
	return hardware.Device{
		Kind: "gpu", Vendor: vendor, Name: name, Index: index,
		MemoryMB: memoryMB, FreeMemoryMB: freeMemoryMB, Backends: []string{"cuda"},
	}
}

func containsRationale(rationale []string, part string) bool {
	for _, item := range rationale {
		if strings.Contains(item, part) {
			return true
		}
	}
	return false
}
