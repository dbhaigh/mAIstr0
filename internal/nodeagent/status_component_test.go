package nodeagent

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestNodeStatusIncludesCopiedGPUHistory(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "gpu-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.GPUUsageSample{
		Timestamp: time.Now().UTC(), Available: true, Utilization: 41,
	}
	agent.hwMu.Lock()
	agent.gpuHistory = []hardware.GPUUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.GPUHistory) != 1 || status.GPUHistory[0] != sample {
		t.Fatalf("node status GPU history = %#v", status.GPUHistory)
	}
	if !status.TelemetryAt.Equal(sample.Timestamp) {
		t.Fatalf("node telemetry timestamp = %s, want %s", status.TelemetryAt, sample.Timestamp)
	}
	status.GPUHistory[0].Utilization = 99
	if agent.gpuHistorySnapshot()[0].Utilization != sample.Utilization {
		t.Fatal("node status exposed mutable telemetry history")
	}
}

func TestNodeStatusIncludesCopiedNPUHistory(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "npu-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.NPUUsageSample{
		Timestamp: time.Now().UTC(), Available: true, Utilization: 41,
	}
	agent.hwMu.Lock()
	agent.npuHistory = []hardware.NPUUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.NPUHistory) != 1 || status.NPUHistory[0] != sample {
		t.Fatalf("node status NPU history = %#v", status.NPUHistory)
	}
	status.NPUHistory[0].Utilization = 99
	if agent.npuHistorySnapshot()[0].Utilization != sample.Utilization {
		t.Fatal("node status exposed mutable telemetry history")
	}
}

func TestNodeStatusIncludesCopiedSystemHistory(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "system-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.SystemUsageSample{
		Timestamp: time.Now().UTC(), CPUAvailable: true, CPUUtilization: 37,
		MemoryAvailable: true, MemoryUtilization: 62,
	}
	agent.hwMu.Lock()
	agent.systemHistory = []hardware.SystemUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.SystemHistory) != 1 || status.SystemHistory[0] != sample {
		t.Fatalf("node status system history = %#v", status.SystemHistory)
	}
	status.SystemHistory[0].CPUUtilization = 99
	if agent.systemHistorySnapshot()[0].CPUUtilization != sample.CPUUtilization {
		t.Fatal("node status exposed mutable system telemetry history")
	}
}

func TestVerifiedStageReadinessRequiresRuntimeCapability(t *testing.T) {
	cfg := config.NodeConfig{
		SupportedShardModels: []string{"large-model"},
		MaxLayersItCanHost:   24,
	}
	status := engine.Status{Name: "llamacpp", Ready: true, ModelCount: 1}
	if models, layers := verifiedStageReadiness(cfg, []engine.Status{status}); len(models) != 0 || layers != 0 {
		t.Fatalf("operator declaration alone advertised stage readiness: models=%v layers=%d", models, layers)
	}

	status.PipelineStage = &engine.PipelineStageCapabilities{
		ProtocolVersion: engine.PipelineStageProtocolVersion, Runtime: "llamacpp",
		Models:       []engine.PipelineStageModelCapability{{ModelID: "large-model", MaxLayers: 32}},
		TensorDTypes: []engine.TensorDType{engine.TensorFloat16},
	}
	if models, layers := verifiedStageReadiness(cfg, []engine.Status{status}); len(models) != 1 ||
		models[0] != "large-model" || layers != cfg.MaxLayersItCanHost {
		t.Fatalf("verified stage readiness = models=%v layers=%d", models, layers)
	}

	status.Ready = false
	if models, layers := verifiedStageReadiness(cfg, []engine.Status{status}); len(models) != 0 || layers != 0 {
		t.Fatalf("unready runtime advertised stage readiness: models=%v layers=%d", models, layers)
	}
}
