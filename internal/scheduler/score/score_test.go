package score

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestScoreFactorsTable(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name    string
		node    cluster.NodeStatus
		subtask Subtask
		load    int
		pending int
		exp     Experience
		cfg     ScoringConfig
		want    float64
		factors ScoreFactors
	}{
		{
			name:    "model match",
			node:    scoreNode(engine.Model{Name: "m", Tags: []string{"general"}}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{ModelMatchWeight: 3},
			want:    6, factors: ScoreFactors{ModelMatch: 6},
		},
		{
			name: "hardware",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.Hardware.Score = 40
				return node
			}(),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{HardwareWeight: 0.5},
			want:    20, factors: ScoreFactors{Hardware: 20},
		},
		{
			name: "reported and queued task counts",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.ActiveTasks = 2
				return node
			}(),
			subtask: Subtask{TaskType: "general"},
			load:    5,
			factors: ScoreFactors{ActiveTasks: 2, QueuedTasks: 3},
		},
		{
			name: "load penalty",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.ActiveTasks = 1
				return node
			}(),
			subtask: Subtask{TaskType: "general"},
			load:    3,
			cfg:     ScoringConfig{LoadPenaltyWeight: 2},
			want:    -6, factors: ScoreFactors{ActiveTasks: 1, QueuedTasks: 2, LoadPenalty: 6},
		},
		{
			name:    "pending dependent penalty",
			node:    scoreNode(engine.Model{Name: "m"}),
			subtask: Subtask{TaskType: "general"},
			pending: 3,
			cfg:     ScoringConfig{DependentPenaltyWeight: 2},
			want:    -6, factors: ScoreFactors{PendingDependents: 3, LoadPenalty: 6},
		},
		{
			name: "load and capacity penalties",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.MaxConcurrentTasks = 1
				return node
			}(),
			subtask: Subtask{TaskType: "general"},
			load:    2,
			cfg:     ScoringConfig{LoadPenaltyWeight: 3},
			want:    -12, factors: ScoreFactors{QueuedTasks: 2, LoadPenalty: 6, CapacityPenalty: 6},
		},
		{
			name: "GPU compatibility",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.Hardware.HasGPU = true
				return node
			}(),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{GPUCompatibilityBonus: 7},
			want:    7, factors: ScoreFactors{GPUCompatibilityScore: 7},
		},
		{
			name: "GPU utilization",
			node: withFreshTelemetry(engine.Model{Name: "m"}, now, hardware.Info{
				GPUStatsAvailable: true, GPUUtilization: 40,
			}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{GPUUtilizationWeight: 0.5},
			want:    -20, factors: ScoreFactors{GPUUtilizationPenalty: 20, TelemetryFresh: true},
		},
		{
			name: "NPU utilization",
			node: withFreshTelemetry(engine.Model{Name: "m"}, now, hardware.Info{
				NPUStatsAvailable: true, NPUUtilization: 20,
			}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{NPUUtilizationWeight: 0.25},
			want:    -5, factors: ScoreFactors{NPUUtilizationPenalty: 5, TelemetryFresh: true},
		},
		{
			name: "CPU utilization",
			node: withFreshTelemetry(engine.Model{Name: "m"}, now, hardware.Info{
				CPUStatsAvailable: true, CPUUtilization: 30,
			}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{CPUUtilizationWeight: 0.1},
			want:    -3, factors: ScoreFactors{CPUUtilizationPenalty: 3, TelemetryFresh: true},
		},
		{
			name: "memory usage",
			node: withFreshTelemetry(engine.Model{Name: "m"}, now, hardware.Info{
				MemoryStatsAvailable: true, MemoryUsage: 50,
			}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{MemoryUsageWeight: 0.2},
			want:    -10, factors: ScoreFactors{MemoryUsagePenalty: 10, TelemetryFresh: true},
		},
		{
			name: "required and preferred capabilities",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.Capabilities = []string{"coder", "fast"}
				return node
			}(),
			subtask: Subtask{
				TaskType: "general", RequiredCapabilities: []string{"coder"},
				PreferredCapabilities: []string{"fast"},
			},
			cfg:     ScoringConfig{RequiredCapMatchBonus: 2, PreferredCapMatchBonus: 3},
			want:    5,
			factors: ScoreFactors{CapabilityScore: 5, MatchedCapabilities: []string{"fast"}},
		},
		{
			name: "model quality tags and size",
			node: scoreNode(engine.Model{
				Name: "m", Tags: []string{"high-quality", "reasoning"}, SizeGB: 7,
			}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{ModelQualityBonus: 2},
			want:    5, factors: ScoreFactors{ModelQualityScore: 5},
		},
		{
			name: "topology",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.Hardware.Score = 50
				return node
			}(),
			subtask: Subtask{TaskType: "general", OutDegree: 2},
			cfg:     ScoringConfig{CriticalPathBonus: 4},
			want:    4, factors: ScoreFactors{TopologyScore: 4},
		},
		{
			name:    "learned bias",
			node:    scoreNode(engine.Model{Name: "m"}),
			subtask: Subtask{TaskType: "general"},
			exp:     Experience{LearnedBias: 3},
			cfg:     ScoringConfig{LearnedBiasWeight: 2},
			want:    6, factors: ScoreFactors{LearnedBias: 6},
		},
		{
			name: "stale telemetry is ignored but measured",
			node: withFreshTelemetry(engine.Model{Name: "m"}, now.Add(-time.Minute), hardware.Info{
				GPUStatsAvailable: true, GPUUtilization: 80,
			}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{GPUUtilizationWeight: 0.5},
			factors: ScoreFactors{TelemetryAgeMs: time.Minute.Milliseconds()},
		},
		{
			name: "combined factors",
			node: func() cluster.NodeStatus {
				node := withFreshTelemetry(engine.Model{
					Name: "m", Tags: []string{"general", "fast", "high-quality"}, SizeGB: 8,
				}, now.Add(-5*time.Second), hardware.Info{
					Score: 80, HasGPU: true, GPUStatsAvailable: true, GPUUtilization: 40,
					NPUStatsAvailable: true, NPUUtilization: 20,
					CPUStatsAvailable: true, CPUUtilization: 60,
					MemoryStatsAvailable: true, MemoryUsage: 50,
				})
				node.ID = "combined"
				node.ActiveTasks = 1
				node.MaxConcurrentTasks = 2
				node.Capabilities = []string{"coder"}
				return node
			}(),
			subtask: Subtask{
				TaskType: "general", RequiredCapabilities: []string{"coder"},
				PreferredCapabilities: []string{"fast"}, OutDegree: 2,
			},
			load: 2, pending: 1, exp: Experience{LearnedBias: 0.5},
			cfg:  DefaultScoringConfig(),
			want: 47.2,
			factors: ScoreFactors{
				ModelMatch: 22.5, Hardware: 24, ActiveTasks: 1, QueuedTasks: 1,
				PendingDependents: 1, CapacityPenalty: 8, LoadPenalty: 18,
				GPUCompatibilityScore: 10, GPUUtilizationPenalty: 4,
				NPUUtilizationPenalty: 2, CPUUtilizationPenalty: 3, MemoryUsagePenalty: 2.5,
				CapabilityScore: 20, MatchedCapabilities: []string{"fast"},
				ModelQualityScore: 4.5, TopologyScore: 3.2, LearnedBias: 0.5,
				TelemetryFresh: true, TelemetryAgeMs: 5000,
			},
		},
		{
			name: "combined stale telemetry",
			node: withFreshTelemetry(engine.Model{Name: "m", Tags: []string{"general"}}, now.Add(-time.Minute), hardware.Info{
				Score: 50, CPUStatsAvailable: true, CPUUtilization: 100,
			}),
			subtask: Subtask{TaskType: "general"},
			cfg:     ScoringConfig{ModelMatchWeight: 1, HardwareWeight: 1, CPUUtilizationWeight: 1},
			want:    52,
			factors: ScoreFactors{
				ModelMatch: 2, Hardware: 50, TelemetryFresh: false,
				TelemetryAgeMs: time.Minute.Milliseconds(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, factors := Score(test.node, test.subtask, test.load, test.pending, test.exp, test.cfg, now)
			legacyFactors := factors
			legacyFactors.EstimatedCompletionSeconds = 0
			legacyFactors.EstimatedTokensPerSecond = 0
			legacyFactors.ModelSizeGB = 0
			legacyFactors.LatencyScore = 0
			if got != test.want || !reflect.DeepEqual(legacyFactors, test.factors) {
				t.Fatalf("Score() = (%v, %+v), want (%v, %+v)", got, factors, test.want, test.factors)
			}
			candidate, ok := Prepare(test.node, test.subtask)
			if !ok {
				t.Fatal("eligible test candidate was rejected by Prepare")
			}
			preparedScore, preparedFactors := ScorePrepared(
				test.node, test.subtask, candidate, test.load, test.pending, test.exp, test.cfg, now,
			)
			if preparedScore != got || !reflect.DeepEqual(preparedFactors, factors) {
				t.Fatalf("ScorePrepared() = (%v, %+v), want Score() result (%v, %+v)",
					preparedScore, preparedFactors, got, factors)
			}
			fastCandidate, ok := PrepareWithCapabilitiesForScoring(test.node, test.subtask, test.node.EffectiveCapabilities())
			if !ok {
				t.Fatal("eligible test candidate was rejected by PrepareWithCapabilitiesForScoring")
			}
			if fastScore := ScorePreparedValue(
				test.node, test.subtask, fastCandidate, test.load, test.pending, test.exp, test.cfg, now,
			); fastScore != got {
				t.Fatalf("ScorePreparedValue() = %v, want Score() score %v", fastScore, got)
			}
		})
	}
}

func TestScoreReportsCompletionEstimateAndRoutingPreference(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	node := scoreNode(engine.Model{Name: "m", Tags: []string{"general", "high-quality"}, SizeGB: 8})
	node.ActiveTasks = 1
	subtask := Subtask{
		TaskType: "general", Description: strings.Repeat("d", 160),
		ExpectedOutput: strings.Repeat("o", 80), RoutingPreference: "latency",
	}
	scoreValue, factors := Score(
		node, subtask, 3, 0, Experience{TokensPerSecond: 4}, DefaultScoringConfig(), now,
	)
	if math.Abs(factors.EstimatedCompletionSeconds-45.4) > 1e-9 {
		t.Fatalf("estimated completion = %v, want 45.4 seconds", factors.EstimatedCompletionSeconds)
	}
	if factors.EstimatedTokensPerSecond != 4 || factors.ModelSizeGB != 8 {
		t.Fatalf("cost factors = %+v, want measured throughput 4 tokens/s and model size 8 GB", factors)
	}
	if math.Abs(factors.LatencyScore-30.0/46.4) > 1e-9 {
		t.Fatalf("latency score = %v, want %v", factors.LatencyScore, 30.0/46.4)
	}

	subtask.RoutingPreference = "quality"
	qualityScore, qualityFactors := Score(
		node, subtask, 3, 0, Experience{TokensPerSecond: 4}, DefaultScoringConfig(), now,
	)
	if qualityFactors.LatencyScore != 0 || qualityFactors.ModelQualityScore != 2*factors.ModelQualityScore {
		t.Fatalf("quality preference factors = %+v, latency factors = %+v", qualityFactors, factors)
	}
	if scoreValue == qualityScore {
		t.Fatal("routing preference did not affect the candidate score")
	}
}

func TestScoreRejectsIneligibleModels(t *testing.T) {
	tests := []struct {
		name    string
		node    cluster.NodeStatus
		subtask Subtask
	}{
		{
			name: "unhealthy node",
			node: func() cluster.NodeStatus {
				node := scoreNode(engine.Model{Name: "m"})
				node.Healthy = false
				return node
			}(),
			subtask: Subtask{TaskType: "general"},
		},
		{
			name: "unready engine",
			node: cluster.NodeStatus{
				Healthy: true, Engines: []engine.Status{{Name: "vllm", Ready: false}},
				Models: []engine.Model{{Name: "m", Engine: "vllm", Tags: []string{"code"}}},
			},
			subtask: Subtask{TaskType: "code"},
		},
		{
			name:    "missing required capability",
			node:    scoreNode(engine.Model{Name: "m", Tags: []string{"general"}}),
			subtask: Subtask{TaskType: "general", RequiredCapabilities: []string{"gpu"}},
		},
		{
			name:    "incompatible specialty tag",
			node:    scoreNode(engine.Model{Name: "m", Tags: []string{"math"}}),
			subtask: Subtask{TaskType: "code"},
		},
		{
			name:    "model below minimum size",
			node:    scoreNode(engine.Model{Name: "m", SizeGB: 4}),
			subtask: Subtask{TaskType: "general", MinModelSizeGB: 7},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, factors := Score(test.node, test.subtask, 0, 0, Experience{}, ScoringConfig{}, time.Now())
			if !math.IsInf(got, -1) || !reflect.DeepEqual(factors, ScoreFactors{}) {
				t.Fatalf("Score() = (%v, %+v), want (-Inf, zero factors)", got, factors)
			}
		})
	}
}

func TestAcceleratorCompatibilityBonusUsesEngineCapabilities(t *testing.T) {
	tests := []struct {
		name      string
		hardware  hardware.Info
		status    engine.Status
		model     engine.Model
		wantBonus float64
	}{
		{
			name: "GPU backend match",
			hardware: hardware.Info{Devices: []hardware.Device{
				{Kind: "gpu", Backends: []string{"cuda"}},
			}},
			status: engine.Status{
				Name: "engine", Ready: true, ModelCount: 1,
				Capabilities: engine.Capabilities{Devices: []string{"gpu"}, Backends: []string{"cuda"}},
			},
			model:     engine.Model{Engine: "engine", Tags: []string{"fast"}},
			wantBonus: acceleratorModelBonus + acceleratorSmallModelBonus,
		},
		{
			name: "NPU backend match",
			hardware: hardware.Info{Devices: []hardware.Device{
				{Kind: "npu", Vendor: "huawei-npu", FreeMemoryMB: 8192, Backends: []string{"cann"}},
			}},
			status: engine.Status{
				Name: "engine", Ready: true, ModelCount: 1,
				Capabilities: engine.Capabilities{Devices: []string{"npu"}, Backends: []string{"cann"}},
			},
			model:     engine.Model{Engine: "engine", Device: "npu", SizeGB: 2},
			wantBonus: acceleratorModelBonus,
		},
		{
			name: "NPU bonus requires reported NPU execution",
			hardware: hardware.Info{Devices: []hardware.Device{
				{Kind: "npu", Vendor: "huawei-npu", FreeMemoryMB: 8192, Backends: []string{"cann"}},
			}},
			status: engine.Status{
				Name: "engine", Ready: true, ModelCount: 1,
				Capabilities: engine.Capabilities{Devices: []string{"npu"}, Backends: []string{"cann"}},
			},
			model: engine.Model{Engine: "engine", SizeGB: 2},
		},
		{
			name: "NPU vendor mismatch",
			hardware: hardware.Info{Devices: []hardware.Device{
				{Kind: "npu", Vendor: "qualcomm-npu", FreeMemoryMB: 8192, Backends: []string{"hexagon"}},
			}},
			status: engine.Status{
				Name: "engine", Ready: true, ModelCount: 1,
				Capabilities: engine.Capabilities{
					Devices: []string{"npu"}, Backends: []string{"openvino"}, NPUVendors: []string{"intel-npu"},
				},
			},
			model: engine.Model{Engine: "engine", Device: "npu", SizeGB: 2},
		},
		{
			name: "device kind mismatch",
			hardware: hardware.Info{Devices: []hardware.Device{
				{Kind: "gpu", Backends: []string{"cuda"}},
			}},
			status: engine.Status{
				Name: "engine", Ready: true, ModelCount: 1,
				Capabilities: engine.Capabilities{Devices: []string{"npu"}},
			},
			model: engine.Model{Engine: "engine"},
		},
		{
			name: "backend mismatch",
			hardware: hardware.Info{Devices: []hardware.Device{
				{Kind: "gpu", Backends: []string{"rocm"}},
			}},
			status: engine.Status{
				Name: "engine", Ready: true, ModelCount: 1,
				Capabilities: engine.Capabilities{Devices: []string{"gpu"}, Backends: []string{"cuda"}},
			},
			model: engine.Model{Engine: "engine"},
		},
		{
			name: "engine not ready",
			hardware: hardware.Info{Devices: []hardware.Device{
				{Kind: "gpu"},
			}},
			status: engine.Status{
				Name: "engine", Ready: false, ModelCount: 1,
				Capabilities: engine.Capabilities{Devices: []string{"gpu"}},
			},
			model: engine.Model{Engine: "engine"},
		},
		{
			name:     "legacy hardware flags still match",
			hardware: hardware.Info{HasGPU: true},
			status: engine.Status{
				Name: "engine", Ready: true, ModelCount: 1,
				Capabilities: engine.Capabilities{Devices: []string{"gpu"}},
			},
			model:     engine.Model{Engine: "engine"},
			wantBonus: acceleratorModelBonus,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := cluster.NodeStatus{Hardware: test.hardware, Engines: []engine.Status{test.status}}
			if got := acceleratorCompatibilityBonus(node, test.model); got != test.wantBonus {
				t.Fatalf("acceleratorCompatibilityBonus() = %v, want %v", got, test.wantBonus)
			}
		})
	}
}

func TestNPUModelEligibilityMatchesDocumentedVendorProfiles(t *testing.T) {
	tests := []struct {
		name         string
		vendor       string
		backend      string
		capabilities engine.Capabilities
	}{
		{
			name:   "Intel OpenVINO",
			vendor: "intel-npu", backend: "openvino",
			capabilities: engine.Capabilities{
				Devices: []string{"npu"}, Backends: []string{"openvino"},
				NPUVendors: []string{"intel-npu"}, Quantizations: []string{"q4_0"},
				PrimaryQuantization: "q4_0", MaxParallelSeqs: 1, RequiresContextCap: true,
				MaxContextLength: 8192, Modalities: []string{"text"},
			},
		},
		{
			name:   "Qualcomm Hexagon",
			vendor: "qualcomm-npu", backend: "hexagon",
			capabilities: engine.Capabilities{
				Devices: []string{"npu"}, Backends: []string{"hexagon"},
				NPUVendors: []string{"qualcomm-npu"}, Quantizations: []string{"q4_0"},
			},
		},
		{
			name:   "AMD XDNA 2",
			vendor: "amd-npu", backend: "xdna",
			capabilities: engine.Capabilities{
				Devices: []string{"npu"}, Backends: []string{"xdna"},
				NPUVendors: []string{"amd-npu"}, MaxContextLength: 262144,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := scoreNode(engine.Model{
				Name: "model", Engine: "npu-engine", Device: "npu",
				SizeGB: 2, ContextLength: 4096, Quantization: "Q4_0",
			})
			node.Hardware = hardware.Info{HasNPU: true, NPUVendor: test.vendor, Devices: []hardware.Device{
				{Kind: "npu", Vendor: test.vendor, Backends: []string{test.backend}, FreeMemoryMB: 4096},
			}}
			node.Engines = []engine.Status{{
				Name: "npu-engine", Ready: true, ModelCount: 1, Capabilities: test.capabilities,
			}}
			if _, ok := Prepare(node, Subtask{TaskType: "general"}); !ok {
				t.Fatalf("documented NPU profile was rejected: %+v", ExplainNPUModelRejections(
					[]cluster.NodeStatus{node}, Subtask{TaskType: "general"},
				))
			}
			if got := acceleratorCompatibilityBonus(node, node.Models[0]); got != acceleratorModelBonus {
				t.Fatalf("NPU compatibility bonus = %v, want %v", got, acceleratorModelBonus)
			}
		})
	}
}

func TestNPUModelRejectionExplanationIncludesFitQuantizationAndContext(t *testing.T) {
	node := scoreNode(engine.Model{
		Name: "model", Engine: "openvino", Device: "npu",
		SizeGB: 8, ContextLength: 16384, Quantization: "q5_k",
	})
	node.Hardware = hardware.Info{
		HasNPU: true, NPUVendor: "intel-npu",
		Devices: []hardware.Device{{
			Kind: "npu", Vendor: "intel-npu", Backends: []string{"openvino"}, FreeMemoryMB: 4096,
		}},
	}
	node.Engines = []engine.Status{{
		Name: "openvino", Ready: true, ModelCount: 1,
		Capabilities: engine.Capabilities{
			Devices: []string{"npu"}, Backends: []string{"openvino"}, NPUVendors: []string{"intel-npu"},
			Quantizations: []string{"q4_0"}, PrimaryQuantization: "q4_0",
			RequiresContextCap: true, MaxContextLength: 8192,
		},
	}}
	subtask := Subtask{TaskType: "general"}
	if _, ok := Prepare(node, subtask); ok {
		t.Fatal("model exceeding NPU constraints was eligible")
	}
	rejections := ExplainNPUModelRejections([]cluster.NodeStatus{node}, subtask)
	if len(rejections) != 1 {
		t.Fatalf("rejections = %+v, want one model explanation", rejections)
	}
	explanation := strings.Join(rejections[0].Reasons, "; ")
	for _, reason := range []string{
		"model does not fit in reported NPU memory",
		"model quantization is not supported",
		"model context cap exceeds the engine maximum",
	} {
		if !strings.Contains(explanation, reason) {
			t.Errorf("explanation %q does not include %q", explanation, reason)
		}
	}
}

func scoreNode(model engine.Model) cluster.NodeStatus {
	return cluster.NodeStatus{
		ID: "node", Healthy: true, Models: []engine.Model{model},
	}
}

func withFreshTelemetry(model engine.Model, at time.Time, info hardware.Info) cluster.NodeStatus {
	node := scoreNode(model)
	node.TelemetryAt = at
	node.Hardware = info
	return node
}
