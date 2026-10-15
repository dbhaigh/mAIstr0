package nodeagent

import (
	"log"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

const engineDetectionInterval = 10 * time.Second

func prioritizeAcceleratedEngines(engines []engine.Engine, hw hardware.Info) []engine.Engine {
	ordered := make([]engine.Engine, 0, len(engines))
	priorities := make([]string, 0, 2)
	if hw.HasGPU {
		priorities = append(priorities, "lmstudio")
	}
	if hw.HasNPU {
		priorities = append(priorities, "fastflowlm")
	}
	preferred := make(map[string]bool, len(priorities))
	for _, name := range priorities {
		preferred[name] = true
		for _, eng := range engines {
			if eng.Name() == name {
				ordered = append(ordered, eng)
			}
		}
	}
	for _, eng := range engines {
		if !preferred[eng.Name()] {
			ordered = append(ordered, eng)
		}
	}
	return ordered
}

// ModelState pairs a discovered model with whether it is currently enabled
// for scheduling, used by the GUI's per-node model configuration panel.
type ModelState struct {
	engine.Model
	Enabled bool `json:"enabled"`
	Default bool `json:"default"`
}

func (c *engineComponent) enabledModels() []engine.Model {
	models, _ := c.inspectInventory()
	return models
}

func (c *engineComponent) inspectInventory() ([]engine.Model, []engine.Status) {
	engines := c.engineSnapshot()
	disabled := c.state.disabledModelSnapshot()
	models := make([]engine.Model, 0)
	statuses := make([]engine.Status, 0, len(engines))
	for _, eng := range engines {
		protocols := engine.ProtocolsForEngine(eng.Name())
		status := engine.Status{
			Name: eng.Name(), Protocols: protocols, LastCheckedAt: time.Now(),
		}
		listed, err := eng.ListModels()
		if err != nil {
			log.Printf("nodeagent: list models for engine %s failed: %v", eng.Name(), err)
			statuses = append(statuses, status)
			continue
		}
		status.Ready = true
		status.ModelCount = len(listed)
		for _, model := range listed {
			model.Engine = eng.Name()
			model.Protocols = append([]string(nil), protocols...)
			if !disabled[model.Name] {
				models = append(models, model)
			}
		}
		statuses = append(statuses, status)
	}
	return models, statuses
}

// refreshDetectedEngines periodically detects engines started after the node
// agent, even when another engine was already available.
func (c *engineComponent) refreshDetectedEngines() {
	c.engineScanMu.Lock()
	defer c.engineScanMu.Unlock()

	c.enginesMu.RLock()
	needsDetection := time.Since(c.lastEngineScan) >= engineDetectionInterval
	c.enginesMu.RUnlock()
	if needsDetection {
		c.scanDetectedEnginesLocked()
	}
}

func (c *engineComponent) rescanDetectedEngines() {
	c.engineScanMu.Lock()
	defer c.engineScanMu.Unlock()
	c.scanDetectedEnginesLocked()
}

func (c *engineComponent) scanDetectedEnginesLocked() {
	detector := c.engineDetector
	if detector == nil {
		detector = engine.DetectAll
	}
	engines := detector()
	engines = prioritizeAcceleratedEngines(engines, c.telemetry.hardwareSnapshot())
	c.enginesMu.Lock()
	c.detectedEngines = engines
	c.lastEngineScan = time.Now()
	c.enginesMu.Unlock()
}

func (c *engineComponent) engineSnapshot() []engine.Engine {
	c.enginesMu.RLock()
	configured := append([]engine.Engine(nil), c.engines...)
	detected := append([]engine.Engine(nil), c.detectedEngines...)
	c.enginesMu.RUnlock()
	engines := make([]engine.Engine, 0, len(configured)+len(detected))
	seen := make(map[string]struct{}, len(configured)+len(detected))
	for _, group := range [][]engine.Engine{configured, detected} {
		for _, eng := range group {
			key := strings.ToLower(eng.Name()) + "\x00" + engineURL(eng)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			engines = append(engines, eng)
		}
	}
	return prioritizeAcceleratedEngines(engines, c.telemetry.hardwareSnapshot())
}

func (c *engineComponent) allModelsWithState() []ModelState {
	c.refreshDetectedEngines()
	disabled := c.state.disabledModelSnapshot()
	var out []ModelState
	for _, eng := range c.engineSnapshot() {
		protocols := engine.ProtocolsForEngine(eng.Name())
		models, err := eng.ListModels()
		if err != nil {
			log.Printf("nodeagent: list models for engine %s failed: %v", eng.Name(), err)
			continue
		}
		for _, model := range models {
			model.Engine = eng.Name()
			model.Protocols = append([]string(nil), protocols...)
			out = append(out, ModelState{
				Model: model, Enabled: !disabled[model.Name],
				Default: model.Name == c.state.defaultModelName(),
			})
		}
	}
	return out
}

func (c *engineComponent) reportedModelList() []engine.Model {
	c.modelMu.Lock()
	models := append([]engine.Model(nil), c.reportedModels...)
	for i := range models {
		models[i].Tags = append([]string(nil), models[i].Tags...)
		models[i].Protocols = append([]string(nil), models[i].Protocols...)
	}
	c.modelMu.Unlock()
	return models
}

func (c *engineComponent) reportedEngineList() []engine.Status {
	c.modelMu.Lock()
	engines := append([]engine.Status(nil), c.reportedEngines...)
	for i := range engines {
		engines[i].Protocols = append([]string(nil), engines[i].Protocols...)
	}
	c.modelMu.Unlock()
	return engines
}

// refreshReportedModels updates the status cache without holding modelMu while
// contacting an LLM server. Status and registration must stay responsive even
// when a local model is loading or the engine is unavailable.
func (c *engineComponent) refreshReportedModels() {
	models, engines := c.inspectInventory()
	c.modelMu.Lock()
	c.reportedModels = models
	c.reportedEngines = engines
	c.reportedAt = time.Now()
	c.modelMu.Unlock()
}

func (c *engineComponent) modelRefreshLoop() {
	c.refreshReportedModels()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.refreshReportedModels()
	}
}

func (c *engineComponent) engineFor(model string) engine.Engine {
	return c.engineForModel(model, "")
}

func (c *engineComponent) engineForModel(model, engineName string) engine.Engine {
	c.refreshDetectedEngines()
	for _, eng := range c.engineSnapshot() {
		if engineName != "" && !strings.EqualFold(eng.Name(), engineName) {
			continue
		}
		models, err := eng.ListModels()
		if err != nil {
			continue
		}
		for _, candidate := range models {
			if candidate.Name == model {
				return eng
			}
		}
	}
	return nil
}

func (c *engineComponent) defaultModelName() string {
	return c.state.defaultModelName()
}

func (c *engineComponent) configureModels(disabled map[string]bool, defaultModel string) {
	c.state.setModelConfig(disabled, defaultModel)
	c.invalidateReportedModels()
}

func (c *engineComponent) replaceConfiguredEngine(eng engine.Engine) {
	c.enginesMu.Lock()
	c.engines = []engine.Engine{eng}
	c.enginesMu.Unlock()
	c.invalidateReportedModels()
}

func (c *engineComponent) invalidateReportedModels() {
	c.modelMu.Lock()
	c.reportedModels = nil
	c.reportedAt = time.Time{}
	c.modelMu.Unlock()
}

func (a *Agent) enabledModels() []engine.Model { return a.engineComponent.enabledModels() }
func (a *Agent) refreshDetectedEngines()       { a.engineComponent.refreshDetectedEngines() }
func (a *Agent) rescanDetectedEngines()        { a.engineComponent.rescanDetectedEngines() }
func (a *Agent) engineSnapshot() []engine.Engine {
	return a.engineComponent.engineSnapshot()
}
func (a *Agent) allModelsWithState() []ModelState {
	return a.engineComponent.allModelsWithState()
}
func (a *Agent) reportedModelList() []engine.Model {
	return a.engineComponent.reportedModelList()
}
func (a *Agent) reportedEngineList() []engine.Status {
	return a.engineComponent.reportedEngineList()
}
func (a *Agent) refreshReportedModels() { a.engineComponent.refreshReportedModels() }
func (a *Agent) modelRefreshLoop()      { a.engineComponent.modelRefreshLoop() }
func (a *Agent) defaultModelName() string {
	return a.engineComponent.defaultModelName()
}
func (a *Agent) engineFor(model string) engine.Engine {
	return a.engineComponent.engineFor(model)
}
func (a *Agent) engineForModel(model, engineName string) engine.Engine {
	return a.engineComponent.engineForModel(model, engineName)
}
