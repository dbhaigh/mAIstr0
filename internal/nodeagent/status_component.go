package nodeagent

import (
	"sort"
	"strings"
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
	stages interface {
		readinessSnapshot() stageReadinessSnapshot
	}
}

type statusResponse struct {
	ID                        string                       `json:"id"`
	Version                   string                       `json:"version"`
	Address                   string                       `json:"address"`
	Hardware                  hardware.Info                `json:"hardware"`
	TelemetryAt               time.Time                    `json:"telemetry_at"`
	GPUHistory                []hardware.GPUUsageSample    `json:"gpu_history,omitempty"`
	NPUHistory                []hardware.NPUUsageSample    `json:"npu_history,omitempty"`
	SystemHistory             []hardware.SystemUsageSample `json:"system_history,omitempty"`
	Models                    []engine.Model               `json:"models"`
	Engines                   []engine.Status              `json:"engines,omitempty"`
	SupportedShardModels      []string                     `json:"supported_shard_models,omitempty"`
	MaxLayersItCanHost        int                          `json:"max_layers_it_can_host,omitempty"`
	AllowInsecureShardNetwork bool                         `json:"allow_insecure_shard_network,omitempty"`
	CurrentStageAssignments   int                          `json:"current_stage_assignments,omitempty"`
	Capabilities              []string                     `json:"capabilities,omitempty"`
	DefaultModel              string                       `json:"default_model,omitempty"`
	FastScore                 float64                      `json:"fast_score"`
	ActiveTasks               int                          `json:"active_tasks"`
	MaxConcurrentTasks        int                          `json:"max_concurrent_tasks"`
	AvailableCapacity         int                          `json:"available_capacity"`
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
	stages interface {
		readinessSnapshot() stageReadinessSnapshot
	},
) *statusComponent {
	return &statusComponent{
		state: state, telemetry: telemetry, models: models, executor: executor, stages: stages,
	}
}

func (c *statusComponent) snapshot() statusResponse {
	cfg := c.state.configSnapshot()
	active := c.executor.activeTaskCount()
	hw := c.telemetry.hardwareSnapshot()
	trustedInterfaces := make(map[string]struct{}, len(cfg.TrustedNetworkInterfaces))
	for _, name := range cfg.TrustedNetworkInterfaces {
		trustedInterfaces[name] = struct{}{}
	}
	for index := range hw.NetworkLinks {
		_, hw.NetworkLinks[index].Trusted = trustedInterfaces[hw.NetworkLinks[index].Name]
	}
	models := c.models.reportedModelList()
	engines := c.models.reportedEngineList()
	stageReadiness := c.stages.readinessSnapshot()
	verifiedModels, verifiedMaxLayers := verifiedStageReadiness(cfg, engines)
	supportedShardModels := mergeShardModels(verifiedModels, stageReadiness.SupportedModels)
	return statusResponse{
		ID:                        cfg.NodeID,
		Version:                   version.String(),
		Address:                   cfg.AdvertiseAddr,
		Hardware:                  hw,
		TelemetryAt:               c.telemetry.telemetryTimestamp(),
		GPUHistory:                c.telemetry.gpuHistorySnapshot(),
		NPUHistory:                c.telemetry.npuHistorySnapshot(),
		SystemHistory:             c.telemetry.systemHistorySnapshot(),
		Models:                    models,
		Engines:                   engines,
		SupportedShardModels:      supportedShardModels,
		MaxLayersItCanHost:        max(verifiedMaxLayers, stageReadiness.MaxLayersItCanHost),
		AllowInsecureShardNetwork: cfg.AllowInsecureShardNetwork,
		CurrentStageAssignments:   stageReadiness.Assignments,
		Capabilities:              cluster.NodeStatus{Hardware: hw, Models: models}.EffectiveCapabilities(),
		DefaultModel:              c.models.defaultModelName(),
		FastScore:                 fastScore(hw),
		ActiveTasks:               active,
		MaxConcurrentTasks:        cfg.MaxConcurrentTasks,
		AvailableCapacity:         c.executor.availableCapacity(cfg.MaxConcurrentTasks),
	}
}

func verifiedStageReadiness(
	cfg config.NodeConfig,
	engines []engine.Status,
) ([]string, int) {
	allowed := make(map[string]struct{}, len(cfg.SupportedShardModels))
	for _, modelID := range cfg.SupportedShardModels {
		allowed[strings.ToLower(strings.TrimSpace(modelID))] = struct{}{}
	}
	models := make([]string, 0)
	maxLayers := 0
	for _, status := range engines {
		if !status.Ready || status.PipelineStage == nil {
			continue
		}
		for _, capability := range status.PipelineStage.Models {
			key := strings.ToLower(strings.TrimSpace(capability.ModelID))
			if key == "" {
				continue
			}
			if len(allowed) > 0 {
				if _, ok := allowed[key]; !ok {
					continue
				}
			}
			models = append(models, capability.ModelID)
			layers := capability.MaxLayers
			if cfg.MaxLayersItCanHost > 0 {
				layers = min(layers, cfg.MaxLayersItCanHost)
			}
			maxLayers = max(maxLayers, layers)
		}
	}
	return mergeShardModels(models, nil), maxLayers
}

func mergeShardModels(configured, loaded []string) []string {
	seen := make(map[string]struct{}, len(configured)+len(loaded))
	models := make([]string, 0, len(configured)+len(loaded))
	for _, modelID := range append(append([]string(nil), configured...), loaded...) {
		modelID = strings.TrimSpace(modelID)
		key := strings.ToLower(modelID)
		if modelID == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		models = append(models, modelID)
	}
	sort.Strings(models)
	return models
}

func (a *Agent) status() statusResponse {
	return a.statusReporter.snapshot()
}
