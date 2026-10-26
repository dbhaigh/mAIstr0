package score

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestEligibilityDiagnosticsMatchHardFilters(t *testing.T) {
	for _, test := range []struct {
		name, reason string
		edit         func(*cluster.NodeStatus, *Subtask)
	}{
		{"ready", "eligible", func(_ *cluster.NodeStatus, s *Subtask) { s.RequiredCapabilities = []string{" GENERAL "} }},
		{"health", "unhealthy", func(n *cluster.NodeStatus, _ *Subtask) { n.Healthy = false }},
		{"capability", "required_capability", func(_ *cluster.NodeStatus, s *Subtask) { s.RequiredCapabilities = []string{"gpu"} }},
		{"no model", "no_models", func(n *cluster.NodeStatus, _ *Subtask) { n.Models = nil }},
		{"engine", "engine_unready", func(n *cluster.NodeStatus, _ *Subtask) { n.Engines[0].Ready = false }},
		{"minimum size", "model_requirements", func(_ *cluster.NodeStatus, s *Subtask) { s.MinModelSizeGB = 10 }},
		{"NPU memory", "accelerator_memory_insufficient", func(n *cluster.NodeStatus, _ *Subtask) {
			n.Models[0].Device = "npu"
			n.Hardware = hardware.Info{HasNPU: true, Devices: []hardware.Device{{
				Kind: "npu", Vendor: "intel-npu", FreeMemoryMB: 1024, Backends: []string{"openvino"},
			}}}
			n.Engines[0].Capabilities = engine.Capabilities{
				Devices: []string{"npu"}, NPUVendors: []string{"intel-npu"}, Backends: []string{"openvino"},
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := cluster.NodeStatus{
				Healthy: true, Models: []engine.Model{{Name: "synthetic", Engine: "vllm", Tags: []string{"general"}, SizeGB: 4}},
				Engines: []engine.Status{{Name: "vllm", Ready: true}},
			}
			task := Subtask{TaskType: "general"}
			test.edit(&node, &task)
			explanation := ExplainEligibility(node, task)
			_, eligible := Prepare(node, task)
			if explanation.Reason != test.reason || eligible != (test.reason == "eligible") {
				t.Fatalf("diagnostic disagrees with filter: %+v, eligible=%v", explanation, eligible)
			}
			if test.name == "NPU memory" && explanation.ResourceIssues == 0 {
				t.Fatal("missing accelerator issue mask")
			}
		})
	}
}

func TestTelemetryDiagnosticBoundaries(t *testing.T) {
	now := time.Unix(1700000000, 0)
	for _, test := range []struct {
		name  string
		at    time.Time
		fresh bool
		age   time.Duration
	}{
		{"missing", time.Time{}, false, 0},
		{"future", now.Add(time.Nanosecond), false, 0},
		{"current", now, true, 0},
		{"boundary", now.Add(-30 * time.Second), true, 30 * time.Second},
		{"expired", now.Add(-30*time.Second - time.Nanosecond), false, 30*time.Second + time.Nanosecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			age, fresh := TelemetryFreshness(test.at, now)
			if age != test.age || fresh != test.fresh {
				t.Fatalf("age=%v fresh=%v, want %v %v", age, fresh, test.age, test.fresh)
			}
		})
	}
}
