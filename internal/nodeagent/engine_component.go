package nodeagent

import (
	"log"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

const (
	engineDetectionInterval    = 10 * time.Second
	engineDetectionGracePeriod = 3 * engineDetectionInterval
)

func prioritizeAcceleratedEngines(engines []engine.Engine, hw hardware.Info) []engine.Engine {
	ordered := make([]engine.Engine, 0, len(engines))
	priorities := make([]string, 0, 2)
	if hw.HasNPU {
		priorities = append(priorities, "fastflowlm")
	}
	if hw.HasGPU {
		priorities = append(priorities, "lmstudio")
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
	c.refreshReportedModels()
	return c.reportedModelList()
}

func (c *engineComponent) inspectInventory() ([]ModelState, []engine.Status) {
	engines := c.engineSnapshot()
	disabled := c.state.disabledModelSnapshot()
	defaultModel := c.state.defaultModelName()
	models := make([]ModelState, 0)
	statuses := make([]engine.Status, 0, len(engines))
	for _, eng := range engines {
		protocols := engine.ProtocolsForEngine(eng.Name())
		status := engine.Status{
			Name: eng.Name(), Protocols: protocols, InventoryState: engine.ModelInventoryUnknown,
		}
		listed, err := eng.ListModels()
		status.LastCheckedAt = time.Now()
		if err != nil {
			log.Printf("nodeagent: list models for engine %s failed: %v", eng.Name(), err)
			status.InventoryState = engine.ModelInventoryError
			status.LastError = err.Error()
			statuses = append(statuses, status)
			continue
		}
		status.Ready = true
		status.ModelCount = len(listed)
		status.LastSuccessfulAt = status.LastCheckedAt
		if len(listed) == 0 {
			status.InventoryState = engine.ModelInventoryEmpty
		} else {
			status.InventoryState = engine.ModelInventoryAvailable
		}
		for _, model := range listed {
			model.Engine = eng.Name()
			model.Protocols = append([]string(nil), protocols...)
			models = append(models, ModelState{
				Model: model, Enabled: !disabled[model.Name],
				Default: model.Name == defaultModel,
			})
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
		c.scanDetectedEnginesLocked(true)
	}
}

func (c *engineComponent) rescanDetectedEngines() {
	c.engineScanMu.Lock()
	defer c.engineScanMu.Unlock()
	c.scanDetectedEnginesLocked(false)
}

func (c *engineComponent) scanDetectedEnginesLocked(retainMissing bool) {
	detector := c.engineDetector
	if detector == nil {
		detector = engine.DetectAll
	}
	engines := detector()
	engines = prioritizeAcceleratedEngines(engines, c.telemetry.hardwareSnapshot())
	now := time.Now()
	c.enginesMu.Lock()
	detected := make([]engine.Engine, 0, len(engines)+len(c.detectedEngines))
	seen := make(map[string]struct{}, len(engines)+len(c.detectedEngines))
	lastSeen := make(map[string]time.Time, len(c.detectedSeenAt)+len(engines))
	for key, seenAt := range c.detectedSeenAt {
		lastSeen[key] = seenAt
	}
	for _, eng := range engines {
		key := engineIdentity(eng)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		lastSeen[key] = now
		detected = append(detected, eng)
	}
	if retainMissing {
		for _, eng := range c.detectedEngines {
			key := engineIdentity(eng)
			if _, ok := seen[key]; ok {
				continue
			}
			seenAt, ok := lastSeen[key]
			if !ok {
				seenAt = now
				lastSeen[key] = seenAt
			}
			if now.Sub(seenAt) > engineDetectionGracePeriod {
				delete(lastSeen, key)
				continue
			}
			seen[key] = struct{}{}
			detected = append(detected, eng)
		}
	} else {
		for key := range lastSeen {
			if _, ok := seen[key]; !ok {
				delete(lastSeen, key)
			}
		}
	}
	c.detectedEngines = detected
	c.detectedSeenAt = lastSeen
	c.lastEngineScan = now
	c.enginesMu.Unlock()
}

func engineIdentity(eng engine.Engine) string {
	return strings.ToLower(eng.Name()) + "\x00" + engineURL(eng)
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
			key := engineIdentity(eng)
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
	c.refreshReportedModels()
	return c.reportedAllModelList()
}

func (c *engineComponent) reportedModelList() []engine.Model {
	all := c.reportedAllModelList()
	models := make([]engine.Model, 0, len(all))
	for _, model := range all {
		if model.Enabled {
			models = append(models, model.Model)
		}
	}
	return models
}

func (c *engineComponent) reportedAllModelList() []ModelState {
	c.modelMu.Lock()
	states := append([]ModelState(nil), c.reportedModels...)
	for i := range states {
		states[i].Tags = append([]string(nil), states[i].Tags...)
		states[i].Protocols = append([]string(nil), states[i].Protocols...)
	}
	c.modelMu.Unlock()
	return states
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
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	models, engines := c.inspectInventory()
	c.modelMu.Lock()
	disabled := c.state.disabledModelSnapshot()
	defaultModel := c.state.defaultModelName()
	previousModels := append([]ModelState(nil), c.reportedModels...)
	previousEngines := make(map[string]engine.Status, len(c.reportedEngines))
	for _, status := range c.reportedEngines {
		previousEngines[strings.ToLower(status.Name)] = status
	}
	for i := range engines {
		status := &engines[i]
		if status.InventoryState != engine.ModelInventoryError {
			continue
		}
		previous, ok := previousEngines[strings.ToLower(status.Name)]
		if !ok {
			continue
		}
		status.ModelCount = previous.ModelCount
		status.LastSuccessfulAt = previous.LastSuccessfulAt
		for _, model := range previousModels {
			if !strings.EqualFold(model.Engine, status.Name) {
				continue
			}
			model.Enabled = !disabled[model.Name]
			model.Default = model.Name == defaultModel
			models = append(models, model)
		}
	}
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
	defaultModel = c.state.defaultModelName()
	c.modelMu.Lock()
	for i := range c.reportedModels {
		c.reportedModels[i].Enabled = !disabled[c.reportedModels[i].Name]
		c.reportedModels[i].Default = c.reportedModels[i].Name == defaultModel
	}
	c.modelMu.Unlock()
}

func (c *engineComponent) replaceConfiguredEngine(eng engine.Engine) {
	c.enginesMu.Lock()
	c.engines = []engine.Engine{eng}
	c.enginesMu.Unlock()
	c.invalidateReportedModels()
}

func (c *engineComponent) invalidateReportedModels() {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.modelMu.Lock()
	c.reportedModels = nil
	c.reportedEngines = nil
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
func (a *Agent) reportedAllModelList() []ModelState {
	return a.engineComponent.reportedAllModelList()
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
