package nodeagent

import (
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/version"
)

type statusComponent struct {
	state interface {
		configSnapshot() config.NodeConfig
	}
	telemetry interface {
		hardwareSnapshot() hardware.Info
		telemetryTimestamp() time.Time
		gpuHistorySnapshot() []hardware.GPUUsageSample
		npuHistorySnapshot() []hardware.NPUUsageSample
		systemHistorySnapshot() []hardware.SystemUsageSample
	}
	models interface {
		reportedModelList() []engine.Model
		reportedEngineList() []engine.Status
		defaultModelName() string
	}
	executor interface {
		activeTaskCount() int
		availableCapacity(int) int
	}
}

type statusResponse struct {
	ID                 string                       `json:"id"`
	Version            string                       `json:"version"`
	Address            string                       `json:"address"`
	Hardware           hardware.Info                `json:"hardware"`
	TelemetryAt        time.Time                    `json:"telemetry_at"`
	GPUHistory         []hardware.GPUUsageSample    `json:"gpu_history,omitempty"`
	NPUHistory         []hardware.NPUUsageSample    `json:"npu_history,omitempty"`
	SystemHistory      []hardware.SystemUsageSample `json:"system_history,omitempty"`
	Models             []engine.Model               `json:"models"`
	Engines            []engine.Status              `json:"engines,omitempty"`
	Capabilities       []string                     `json:"capabilities,omitempty"`
	DefaultModel       string                       `json:"default_model,omitempty"`
	FastScore          float64                      `json:"fast_score"`
	ActiveTasks        int                          `json:"active_tasks"`
	MaxConcurrentTasks int                          `json:"max_concurrent_tasks"`
	AvailableCapacity  int                          `json:"available_capacity"`
}

func newStatusComponent(
	state interface {
		configSnapshot() config.NodeConfig
	},
	telemetry interface {
		hardwareSnapshot() hardware.Info
		telemetryTimestamp() time.Time
		gpuHistorySnapshot() []hardware.GPUUsageSample
		npuHistorySnapshot() []hardware.NPUUsageSample
		systemHistorySnapshot() []hardware.SystemUsageSample
	},
	models interface {
		reportedModelList() []engine.Model
		reportedEngineList() []engine.Status
		defaultModelName() string
	},
	executor interface {
		activeTaskCount() int
		availableCapacity(int) int
	},
) *statusComponent {
	return &statusComponent{
		state: state, telemetry: telemetry, models: models, executor: executor,
	}
}

func (c *statusComponent) snapshot() statusResponse {
	cfg := c.state.configSnapshot()
	active := c.executor.activeTaskCount()
	hw := c.telemetry.hardwareSnapshot()
	models := c.models.reportedModelList()
	return statusResponse{
		ID:                 cfg.NodeID,
		Version:            version.String(),
		Address:            cfg.AdvertiseAddr,
		Hardware:           hw,
		TelemetryAt:        c.telemetry.telemetryTimestamp(),
		GPUHistory:         c.telemetry.gpuHistorySnapshot(),
		NPUHistory:         c.telemetry.npuHistorySnapshot(),
		SystemHistory:      c.telemetry.systemHistorySnapshot(),
		Models:             models,
		Engines:            c.models.reportedEngineList(),
		Capabilities:       cluster.NodeStatus{Hardware: hw, Models: models}.EffectiveCapabilities(),
		DefaultModel:       c.models.defaultModelName(),
		FastScore:          fastScore(hw),
		ActiveTasks:        active,
		MaxConcurrentTasks: cfg.MaxConcurrentTasks,
		AvailableCapacity:  c.executor.availableCapacity(cfg.MaxConcurrentTasks),
	}
}

func (a *Agent) status() statusResponse {
	return a.statusReporter.snapshot()
}
