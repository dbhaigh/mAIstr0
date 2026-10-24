package nodeagent

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/logging"
)

const (
	engineDetectionInterval    = 10 * time.Second
	engineDetectionGracePeriod = 3 * engineDetectionInterval
	modelInventoryCacheTTL     = 5 * time.Second
)

func prioritizeEnginesByCapabilities(engines []engine.Engine, hw hardware.Info, statuses []engine.Status) []engine.Engine {
	type enginePriority struct {
		backend engine.Engine
		ready   bool
		device  int
	}
	priorities := make([]enginePriority, len(engines))
	for i, backend := range engines {
		capabilities := engine.CapabilitiesFor(backend)
		ready := false
		for _, status := range statuses {
			if !strings.EqualFold(status.Name, backend.Name()) {
				continue
			}
			capabilities = engine.MergeCapabilities(capabilities, status.Capabilities)
			ready = ready || status.Ready && status.ModelCount > 0
		}
		priorities[i] = enginePriority{
			backend: backend, ready: ready,
			device: engineDeviceMatchScore(capabilities, hw),
		}
	}
	sort.SliceStable(priorities, func(i, j int) bool {
		if priorities[i].ready != priorities[j].ready {
			return priorities[i].ready
		}
		return priorities[i].device > priorities[j].device
	})
	ordered := make([]engine.Engine, len(priorities))
	for i := range priorities {
		ordered[i] = priorities[i].backend
	}
	return ordered
}

func engineDeviceMatchScore(capabilities engine.Capabilities, hw hardware.Info) int {
	if len(capabilities.OS) > 0 && !containsFold(capabilities.OS, hw.OS) {
		return 0
	}
	best := 0
	for _, device := range availableHardwareDevices(hw) {
		kind := strings.ToLower(strings.TrimSpace(device.Kind))
		if !engine.SupportsDeviceKind(capabilities.Devices, kind) {
			continue
		}
		if kind == "npu" {
			vendor := device.Vendor
			if vendor == "" {
				vendor = hw.NPUVendor
			}
			if !engine.SupportsNPUVendor(capabilities.NPUVendors, vendor) {
				continue
			}
		}
		if !engine.BackendsOverlap(capabilities.Backends, device.Backends) {
			continue
		}
		if priority := deviceKindPriority(kind); priority > best {
			best = priority
		}
	}
	return best
}

func availableHardwareDevices(hw hardware.Info) []hardware.Device {
	devices := append([]hardware.Device(nil), hw.Devices...)
	if hw.HasGPU && !hasHardwareDevice(devices, "gpu") {
		devices = append(devices, hardware.Device{Kind: "gpu"})
	}
	if hw.HasNPU && !hasHardwareDevice(devices, "npu") {
		devices = append(devices, hardware.Device{Kind: "npu", Vendor: hw.NPUVendor})
	}
	return devices
}

func hasHardwareDevice(devices []hardware.Device, kind string) bool {
	for _, device := range devices {
		if strings.EqualFold(device.Kind, kind) {
			return true
		}
	}
	return false
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}

func deviceKindPriority(kind string) int {
	switch kind {
	case "npu":
		return 3
	case "gpu":
		return 2
	case "cpu":
		return 1
	default:
		return 0
	}
}

// ModelState pairs a discovered model with whether it is currently enabled
// for scheduling, used by the GUI's per-node model configuration panel.
type ModelState struct {
	engine.Model
	Enabled bool `json:"enabled"`
	Default bool `json:"default"`
}

func (c *engineComponent) enabledModels() []engine.Model {
	c.refreshReportedModelsIfStale()
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
			Capabilities: engine.CapabilitiesFor(eng),
		}
		listed, err := eng.ListModels()
		status.LastCheckedAt = time.Now()
		if err != nil {
			logging.Errorf("nodeagent", "nodeagent: list models for engine %s failed: %v", eng.Name(), err)
			status.InventoryState = engine.ModelInventoryError
			status.LastError = err.Error()
			statuses = append(statuses, status)
			continue
		}
		status.Ready = true
		status.ModelCount = len(listed)
		status.LastSuccessfulAt = status.LastCheckedAt
		status.Capabilities = engine.CapabilitiesFor(eng)
		for _, model := range listed {
			if model.Device != "" {
				status.Capabilities = engine.MergeCapabilities(
					status.Capabilities, engine.Capabilities{Devices: []string{model.Device}},
				)
			}
		}
		if capabilityProber, ok := eng.(engine.CapabilityProber); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			capabilities, probeErr := capabilityProber.ProbeCapabilities(ctx)
			cancel()
			if probeErr != nil {
				status.CapabilitiesError = probeErr.Error()
			} else {
				status.Capabilities = engine.MergeCapabilities(status.Capabilities, capabilities)
			}
		}
		if override, ok := c.operatorCapabilities[strings.ToLower(strings.TrimSpace(eng.Name()))]; ok {
			status.Capabilities = engine.MergeCapabilities(status.Capabilities, override)
		}
		if len(listed) == 0 {
			status.InventoryState = engine.ModelInventoryEmpty
		} else {
			status.InventoryState = engine.ModelInventoryAvailable
		}
		if capabilityProvider, ok := eng.(engine.PipelineStageCapabilityProvider); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			capabilities, probeErr := capabilityProvider.ProbePipelineStageCapabilities(ctx)
			cancel()
			if probeErr != nil {
				status.PipelineStageError = probeErr.Error()
			} else {
				status.PipelineStage = capabilities
			}
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
	c.enginesMu.RLock()
	needsDetection := time.Since(c.lastEngineScan) >= engineDetectionInterval
	c.enginesMu.RUnlock()
	if needsDetection {
		c.scanDetectedEngines(true)
	}
}

func (c *engineComponent) rescanDetectedEngines() {
	c.scanDetectedEngines(false)
}

func (c *engineComponent) scanDetectedEngines(retainMissing bool) {
	for {
		c.engineScanMu.Lock()
		if done := c.engineScanDone; done != nil {
			c.engineScanMu.Unlock()
			<-done
			if retainMissing {
				return
			}
			continue
		}
		done := make(chan struct{})
		c.engineScanDone = done
		c.engineScanMu.Unlock()
		defer func() {
			c.engineScanMu.Lock()
			c.engineScanDone = nil
			close(done)
			c.engineScanMu.Unlock()
		}()
		break
	}

	detector := c.engineDetector
	if detector == nil {
		detector = engine.DetectAll
	}
	engines := detector()
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
	c.engineGeneration++
	c.engineSnapshotCacheValid = false
	c.enginesMu.Unlock()
	c.markReportedModelsStale()
}

func engineIdentity(eng engine.Engine) string {
	return strings.ToLower(eng.Name()) + "\x00" + engineURL(eng)
}

func engineEndpointIdentity(eng engine.Engine) string {
	return strings.TrimRight(strings.TrimSpace(engineURL(eng)), "/")
}

func (c *engineComponent) engineSnapshot() []engine.Engine {
	hw, hardwareGeneration := c.telemetry.hardwareSnapshotVersioned()
	statuses := c.engineCapabilityStatusSnapshot()
	c.enginesMu.Lock()
	var engines []engine.Engine
	if c.engineSnapshotCacheValid &&
		c.engineSnapshotGeneration == c.engineGeneration &&
		c.engineSnapshotHardwareGen == hardwareGeneration {
		engines = append([]engine.Engine(nil), c.engineSnapshotCache...)
	} else {
		engines = make([]engine.Engine, 0, len(c.engines)+len(c.detectedEngines))
		seen := make(map[string]struct{}, len(c.engines)+len(c.detectedEngines))
		configuredEndpoints := make(map[string]struct{}, len(c.engines))
		for groupIndex, group := range [][]engine.Engine{c.engines, c.detectedEngines} {
			for _, eng := range group {
				endpoint := engineEndpointIdentity(eng)
				if groupIndex == 1 && endpoint != "" {
					if _, configured := configuredEndpoints[endpoint]; configured {
						continue
					}
				}
				key := engineIdentity(eng)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				if groupIndex == 0 && endpoint != "" {
					configuredEndpoints[endpoint] = struct{}{}
				}
				engines = append(engines, eng)
			}
		}
		c.engineSnapshotCache = append([]engine.Engine(nil), engines...)
		c.engineSnapshotGeneration = c.engineGeneration
		c.engineSnapshotHardwareGen = hardwareGeneration
		c.engineSnapshotCacheValid = true
	}
	c.enginesMu.Unlock()
	return prioritizeEnginesByCapabilities(engines, hw, statuses)
}

func (c *engineComponent) engineCapabilityStatusSnapshot() []engine.Status {
	c.modelMu.Lock()
	statuses := make([]engine.Status, len(c.reportedEngines))
	for i, status := range c.reportedEngines {
		statuses[i] = engine.Status{
			Name: status.Name, Ready: status.Ready, ModelCount: status.ModelCount,
			// Capability slices are read-only after publication; refresh replaces the status snapshot.
			Capabilities: status.Capabilities,
		}
	}
	c.modelMu.Unlock()
	return statuses
}

func (c *engineComponent) allModelsWithState() []ModelState {
	c.refreshDetectedEngines()
	c.refreshReportedModelsIfStale()
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
		engines[i].Capabilities = engine.CloneCapabilities(engines[i].Capabilities)
		if engines[i].PipelineStage != nil {
			capabilities := *engines[i].PipelineStage
			capabilities.Models = append([]engine.PipelineStageModelCapability(nil), capabilities.Models...)
			capabilities.TensorDTypes = append([]engine.TensorDType(nil), capabilities.TensorDTypes...)
			capabilities.DeviceBackends = append([]string(nil), capabilities.DeviceBackends...)
			engines[i].PipelineStage = &capabilities
		}
	}
	c.modelMu.Unlock()
	return engines
}

// refreshReportedModels updates the status cache without holding modelMu while
// contacting an LLM server. Status and registration must stay responsive even
// when a local model is loading or the engine is unavailable.
func (c *engineComponent) refreshReportedModels() {
	c.refreshReportedModelsWithForce(true)
}

func (c *engineComponent) refreshReportedModelsIfStale() {
	c.refreshReportedModelsWithForce(false)
}

func (c *engineComponent) refreshReportedModelsWithForce(force bool) {
	for {
		c.refreshMu.Lock()
		if done := c.refreshDone; done != nil {
			c.refreshMu.Unlock()
			<-done
			continue
		}
		c.modelMu.Lock()
		fresh := !c.reportedAt.IsZero() && time.Since(c.reportedAt) < modelInventoryCacheTTL
		c.modelMu.Unlock()
		if !force && fresh {
			c.refreshMu.Unlock()
			return
		}
		done := make(chan struct{})
		c.refreshDone = done
		generation := c.modelInventoryGeneration
		c.refreshMu.Unlock()

		models, engines := c.inspectInventory()
		disabled := c.state.disabledModelSnapshot()
		defaultModel := c.state.defaultModelName()
		c.modelMu.Lock()
		previousModels := append([]ModelState(nil), c.reportedModels...)
		previousEngines := make(map[string]engine.Status, len(c.reportedEngines))
		for _, status := range c.reportedEngines {
			previousEngines[strings.ToLower(status.Name)] = status
		}
		c.modelMu.Unlock()
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

		c.refreshMu.Lock()
		if generation != c.modelInventoryGeneration {
			c.refreshDone = nil
			close(done)
			c.refreshMu.Unlock()
			continue
		}
		c.modelMu.Lock()
		c.reportedModels = models
		c.reportedEngines = engines
		c.reportedAt = time.Now()
		c.modelMu.Unlock()
		c.refreshDone = nil
		close(done)
		c.refreshMu.Unlock()
		return
	}
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
	c.refreshReportedModelsIfStale()
	for _, candidate := range c.reportedAllModelList() {
		if candidate.Name != model || (engineName != "" && !strings.EqualFold(candidate.Engine, engineName)) {
			continue
		}
		ready := false
		for _, status := range c.reportedEngineList() {
			if strings.EqualFold(status.Name, candidate.Engine) && status.Ready {
				ready = true
				break
			}
		}
		if !ready {
			continue
		}
		for _, eng := range c.engineSnapshot() {
			if strings.EqualFold(eng.Name(), candidate.Engine) &&
				(engineName == "" || strings.EqualFold(eng.Name(), engineName)) {
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
	c.engineGeneration++
	c.engineSnapshotCacheValid = false
	c.enginesMu.Unlock()
	c.invalidateReportedModels()
}

func (c *engineComponent) invalidateReportedModels() {
	c.refreshMu.Lock()
	c.modelInventoryGeneration++
	c.modelMu.Lock()
	c.reportedModels = nil
	c.reportedEngines = nil
	c.reportedAt = time.Time{}
	c.modelMu.Unlock()
	c.refreshMu.Unlock()
}

func (c *engineComponent) markReportedModelsStale() {
	c.refreshMu.Lock()
	c.modelInventoryGeneration++
	c.modelMu.Lock()
	c.reportedAt = time.Time{}
	c.modelMu.Unlock()
	c.refreshMu.Unlock()
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
