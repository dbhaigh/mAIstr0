package evaluation

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

// Times are integer microseconds on a virtual clock, never wall-clock sleeps.
type Request struct {
	ArrivalUS    int64         `json:"arrival_us"`
	OutputTokens int           `json:"output_tokens"`
	Task         score.Subtask `json:"task"`
}

type Outage struct {
	Node  int   `json:"node"`
	AtUS  int64 `json:"at_us"`
	EndUS int64 `json:"end_us"`
}

type Scenario struct {
	Name            string               `json:"name"`
	Seed            int64                `json:"seed"`
	Nodes           []cluster.NodeStatus `json:"nodes"`
	TokensPerSecond []float64            `json:"synthetic_tokens_per_second"`
	PrefillUS       []int64              `json:"synthetic_prefill_us"`
	Requests        []Request            `json:"requests"`
	Outages         []Outage             `json:"outages,omitempty"`
	TimeoutUS       int64                `json:"timeout_us"`
}

func Epoch() time.Time { return time.Unix(1_700_000_000, 0).UTC() }

// Scenarios produces fixed-order synthetic clusters and seeded workloads.
// No recorded prompts, model content, addresses, or credentials are consumed.
func Scenarios(seed int64, count int) ([]Scenario, error) {
	if count < 1 || count > 100_000 {
		return nil, fmt.Errorf("request count must be between 1 and 100000")
	}
	names := []string{"homogeneous", "heterogeneous", "uneven_models", "stale_telemetry",
		"long_running", "bursts", "concurrent_arrivals", "node_failures", "resource_limits"}
	result := make([]Scenario, 0, len(names))
	for _, name := range names {
		s := Scenario{Name: name, Seed: seed, TimeoutUS: 30_000_000}
		for i := range 8 {
			node := cluster.NodeStatus{
				ID: fmt.Sprintf("synthetic-%02d", i), Address: fmt.Sprintf("http://synthetic-%02d.invalid", i),
				Healthy: true, TelemetryAt: Epoch(), LastSeen: Epoch(),
				MaxConcurrentTasks: 2, AvailableCapacity: 2,
				Hardware: hardware.Info{Score: 50, CPUStatsAvailable: true, CPUUtilization: 25},
				Engines:  []engine.Status{{Name: "vllm", Ready: true, ModelCount: 1}},
				Models:   []engine.Model{{Name: "synthetic-general", Engine: "vllm", SizeGB: 4, Tags: []string{"general"}}},
			}
			tps, prefill := 80.0, int64(25_000)
			switch name {
			case "heterogeneous", "long_running", "bursts", "node_failures":
				tps = []float64{20, 40, 80, 160}[i%4]
				prefill = int64(2_000_000 / tps)
				node.Hardware.Score = tps / 2
				node.MaxConcurrentTasks = 1 + i%3
				node.AvailableCapacity = node.MaxConcurrentTasks
			case "uneven_models":
				if i%3 != 0 {
					node.Models[0].Tags = []string{"math"}
				}
			case "stale_telemetry":
				node.Hardware.CPUUtilization = 10 + i*10
				switch i % 4 {
				case 0:
					node.TelemetryAt = time.Time{}
				case 1:
					node.TelemetryAt = Epoch().Add(-time.Minute)
				case 2:
					node.TelemetryAt = Epoch().Add(time.Hour)
				}
			case "resource_limits":
				if i%2 == 0 {
					node.Hardware.HasNPU = true
					node.Hardware.Devices = []hardware.Device{{
						Kind: "npu", Vendor: "intel-npu", MemoryMB: 8192, FreeMemoryMB: 1024,
						Backends: []string{"openvino"},
					}}
					node.Models[0].Device = "npu"
					node.Engines[0].Capabilities = engine.Capabilities{
						Devices: []string{"npu"}, NPUVendors: []string{"intel-npu"}, Backends: []string{"openvino"},
					}
				}
			}
			s.Nodes = append(s.Nodes, node)
			s.TokensPerSecond = append(s.TokensPerSecond, tps)
			s.PrefillUS = append(s.PrefillUS, prefill)
		}
		random := rand.New(rand.NewSource(seed))
		arrival := int64(0)
		for i := range count {
			task := score.Subtask{TaskType: "general"}
			if name == "uneven_models" {
				task.TaskType = "code"
			}
			if name == "resource_limits" && i%3 == 0 {
				task.RequiredCapabilities = []string{"general"}
			}
			tokens := 24 + random.Intn(80)
			if name == "long_running" && i%10 == 0 {
				tokens *= 20
			}
			switch name {
			case "bursts":
				arrival = int64(i/32) * 2_000_000
			case "concurrent_arrivals":
				arrival = int64(i/8) * 600_000
			default:
				arrival += int64(40_000 + random.Intn(80_000))
			}
			s.Requests = append(s.Requests, Request{ArrivalUS: arrival, OutputTokens: tokens, Task: task})
		}
		if name == "node_failures" {
			s.Outages = []Outage{{Node: 3, AtUS: 2_000_000, EndUS: 7_000_000}, {Node: 7, AtUS: 4_000_000, EndUS: 9_000_000}}
		}
		result = append(result, s)
	}
	return result, nil
}
