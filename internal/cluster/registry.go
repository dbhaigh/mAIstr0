// Package cluster holds the orchestrator's in-memory view of registered
// nodes: their hardware, available models, and current load.
package cluster

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/version"
)

// NodeStatus is the snapshot a node agent reports about itself, either on
// registration or on every heartbeat poll from the orchestrator.
type NodeStatus struct {
	ID                      string                       `json:"id"`
	Version                 string                       `json:"version"`
	Address                 string                       `json:"address"` // base URL the orchestrator can reach the node at
	Hardware                hardware.Info                `json:"hardware"`
	Models                  []engine.Model               `json:"models"`
	Engines                 []engine.Status              `json:"engines,omitempty"`
	SupportedShardModels    []string                     `json:"supported_shard_models,omitempty"`
	MaxLayersItCanHost      int                          `json:"max_layers_it_can_host,omitempty"`
	CurrentStageAssignments int                          `json:"current_stage_assignments,omitempty"`
	Capabilities            []string                     `json:"capabilities,omitempty"`
	GPUHistory              []hardware.GPUUsageSample    `json:"gpu_history,omitempty"`
	NPUHistory              []hardware.NPUUsageSample    `json:"npu_history,omitempty"`
	SystemHistory           []hardware.SystemUsageSample `json:"system_history,omitempty"`
	TelemetryAt             time.Time                    `json:"telemetry_at,omitempty"`
	DefaultModel            string                       `json:"default_model,omitempty"`
	FastScore               float64                      `json:"fast_score"`
	ActiveTasks             int                          `json:"active_tasks"`
	MaxConcurrentTasks      int                          `json:"max_concurrent_tasks,omitempty"`
	AvailableCapacity       int                          `json:"available_capacity,omitempty"`
	QueuedTasks             int                          `json:"-"`
	LastSeen                time.Time                    `json:"last_seen"`
	Healthy                 bool                         `json:"healthy"`
	Leader                  bool                         `json:"leader"` // elected cluster leader (most capable healthy node)
	VersionError            string                       `json:"version_error,omitempty"`
}

func (n NodeStatus) EngineReady(name string) bool {
	if len(n.Engines) == 0 {
		return true
	}
	for _, status := range n.Engines {
		if strings.EqualFold(status.Name, name) {
			return status.Ready
		}
	}
	return false
}

// EffectiveCapabilities returns the node's advertised capabilities, deriving them
// from models and hardware if not explicitly declared.
func (n NodeStatus) EffectiveCapabilities() []string {
	return n.AppendEffectiveCapabilities(nil)
}

// AppendEffectiveCapabilities appends the node's effective capabilities to dst
// without changing its existing prefix.
func (n NodeStatus) AppendEffectiveCapabilities(dst []string) []string {
	start := len(dst)
	add := func(c string) {
		c = strings.ToLower(strings.TrimSpace(c))
		if c == "" {
			return
		}
		for _, existing := range dst[start:] {
			if existing == c {
				return
			}
		}
		dst = append(dst, c)
	}
	for _, c := range n.Capabilities {
		add(c)
	}
	for _, m := range n.Models {
		for _, t := range m.Tags {
			add(t)
			switch t {
			case "code":
				add("coder")
			case "math":
				add("math")
				add("analyst")
			case "creative":
				add("creative")
				add("writer")
			case "summarize":
				add("summarizer")
			case "translate":
				add("translator")
			case "reasoning":
				add("high-quality")
			case "vision":
				add("vision")
			case "fast":
				add("fast")
			}
		}
		if m.SizeGB >= 7.0 || strings.Contains(strings.ToLower(m.Name), "r1") {
			add("high-quality")
		}
		lower := strings.ToLower(m.Name)
		if strings.Contains(lower, "coder") || strings.Contains(lower, "code") {
			add("coder")
		}
		if strings.Contains(lower, "llama3") || strings.Contains(lower, "qwen2.5") || strings.Contains(lower, "phi4") || strings.Contains(lower, "mistral") {
			add("long-context")
		}
	}
	if n.Hardware.HasGPU {
		add("gpu")
		add("fast")
	}
	if n.Hardware.HasNPU {
		add("npu")
		add("fast")
	}
	if n.Hardware.TotalRAMMB >= 16*1024 {
		add("long-context")
	}
	if len(dst) == start {
		add("general")
	}
	sort.Strings(dst[start:])
	return dst
}

// Registry is a thread-safe store of known nodes.
type Registry struct {
	// mu protects nodes and the snapshot cache; every mutation invalidates the
	// cache before unlocking. It is a leaf lock: callers use copied snapshots
	// before invoking other components or doing I/O.
	mu            sync.RWMutex
	nodes         map[string]*NodeStatus
	allCache      []NodeStatus
	allCacheValid bool
}

func NewRegistry() *Registry {
	return &Registry{nodes: make(map[string]*NodeStatus)}
}

// Upsert registers a node or updates its latest reported status.
func (r *Registry) Upsert(status NodeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status = cloneNodeStatus(status)
	status.LastSeen = time.Now()
	status.Healthy = true
	status.QueuedTasks = 0
	status.VersionError = ""
	for _, existing := range r.nodes {
		if existing.Version == "" || status.Version == "" || existing.Version == status.Version {
			continue
		}
		if version.Compare(status.Version, existing.Version) < 0 {
			status.VersionError = "node version " + status.Version + " is older than cluster version " + existing.Version
			break
		}
		if version.Compare(existing.Version, status.Version) < 0 {
			existing.VersionError = "node version " + existing.Version + " is older than cluster version " + status.Version
		}
	}
	r.nodes[status.ID] = &status
	r.invalidateSnapshotLocked()
}

// MarkUnhealthy flags a node as unreachable without removing it, so it's
// still visible in the GUI but excluded from scheduling.
func (r *Registry) MarkUnhealthy(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		n.Healthy = false
		r.invalidateSnapshotLocked()
	}
}

// Remove ejects a node from the cluster registry.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; ok {
		delete(r.nodes, id)
		r.invalidateSnapshotLocked()
	}
}

// Get returns a copy of a single node's status.
func (r *Registry) Get(id string) (NodeStatus, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return NodeStatus{}, false
	}
	return cloneNodeStatus(*n), true
}

// All returns a snapshot of every known node. Leadership is assigned only
// after at least two healthy members have joined the same registry.
func (r *Registry) All() []NodeStatus {
	r.mu.RLock()
	if r.allCacheValid {
		out := cloneNodeStatuses(r.allCache)
		r.mu.RUnlock()
		return out
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.allCacheValid {
		r.allCache = r.buildSnapshotLocked()
		r.allCacheValid = true
	}
	return cloneNodeStatuses(r.allCache)
}

func cloneNodeStatuses(statuses []NodeStatus) []NodeStatus {
	out := make([]NodeStatus, len(statuses))
	for i := range statuses {
		out[i] = cloneNodeStatus(statuses[i])
	}
	return out
}

func (r *Registry) buildSnapshotLocked() []NodeStatus {
	out := make([]NodeStatus, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, cloneNodeStatus(*n))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	healthy := 0
	for i := range out {
		out[i].Leader = false
		if out[i].Healthy {
			healthy++
		}
	}
	if healthy < 2 {
		return out
	}

	leader := -1
	for i := range out {
		if !out[i].Healthy {
			continue
		}
		if leader < 0 || CapabilityScore(out[i]) > CapabilityScore(out[leader]) ||
			(CapabilityScore(out[i]) == CapabilityScore(out[leader]) && out[i].ID < out[leader].ID) {
			leader = i
		}
	}
	if leader >= 0 {
		out[leader].Leader = true
	}
	return out
}

func (r *Registry) invalidateSnapshotLocked() {
	r.allCacheValid = false
	r.allCache = nil
}

func cloneNodeStatus(status NodeStatus) NodeStatus {
	status.Hardware.Devices = cloneSlice(status.Hardware.Devices)
	for i := range status.Hardware.Devices {
		status.Hardware.Devices[i].Backends = cloneSlice(status.Hardware.Devices[i].Backends)
	}
	status.Hardware.NUMANodes = cloneSlice(status.Hardware.NUMANodes)
	status.Hardware.NetworkLinks = cloneSlice(status.Hardware.NetworkLinks)
	status.Models = cloneSlice(status.Models)
	for i := range status.Models {
		status.Models[i].Tags = cloneSlice(status.Models[i].Tags)
		status.Models[i].Protocols = cloneSlice(status.Models[i].Protocols)
	}
	status.Engines = cloneSlice(status.Engines)
	for i := range status.Engines {
		status.Engines[i].Protocols = cloneSlice(status.Engines[i].Protocols)
		status.Engines[i].Capabilities = engine.CloneCapabilities(status.Engines[i].Capabilities)
	}
	status.Capabilities = cloneSlice(status.Capabilities)
	status.GPUHistory = cloneSlice(status.GPUHistory)
	status.NPUHistory = cloneSlice(status.NPUHistory)
	status.SystemHistory = cloneSlice(status.SystemHistory)
	return status
}

func cloneSlice[T any](items []T) []T {
	if items == nil {
		return nil
	}
	cloned := make([]T, len(items))
	copy(cloned, items)
	return cloned
}

// Active returns only currently healthy cluster members.
func (r *Registry) Active() []NodeStatus {
	all := r.All()
	active := make([]NodeStatus, 0, len(all))
	for _, n := range all {
		if n.Healthy {
			active = append(active, n)
		}
	}
	return active
}

// CapabilityScore ranks the node's raw machine strength, using hardware and
// model-acceleration capability as the tie-breaker for orchestrator election.
const (
	npuHardwareScoreBonus = 30
	npuFastScoreBonus     = 100
)

func CapabilityScore(n NodeStatus) float64 {
	runningNPU := hasReadyNPUModel(n)
	fastScore := n.FastScore
	if hasHardwareNPU(n.Hardware) {
		legacyFastScore := float64(n.Hardware.CPUCores)*2 + npuFastScoreBonus
		if n.Hardware.HasGPU {
			legacyFastScore += 100
		}
		if fastScore == legacyFastScore {
			fastScore -= npuFastScoreBonus
		}
		if runningNPU {
			fastScore += npuFastScoreBonus
		}
	}
	if n.Hardware.Score == 0 {
		return fastScore + float64(n.Hardware.CPUCores)*1.5
	}
	hardwareScore := n.Hardware.Score
	if hasHardwareNPU(n.Hardware) && !runningNPU {
		hardwareScore -= npuHardwareScoreBonus
	}
	return hardwareScore + fastScore*0.75
}

func hasReadyNPUModel(node NodeStatus) bool {
	if !hasHardwareNPU(node.Hardware) {
		return false
	}
	for _, model := range node.Models {
		if engine.NormalizeDeviceKind(model.Device) != "npu" || model.Engine == "" {
			continue
		}
		for _, status := range node.Engines {
			if strings.EqualFold(status.Name, model.Engine) && status.Ready && status.ModelCount > 0 {
				return true
			}
		}
	}
	return false
}

func hasHardwareNPU(info hardware.Info) bool {
	if info.HasNPU {
		return true
	}
	for _, device := range info.Devices {
		if strings.EqualFold(device.Kind, "npu") {
			return true
		}
	}
	return false
}

// GPUUtilizationPenalty converts live GPU activity into the scheduler's
// existing score scale. Missing telemetry is neutral, not interpreted as idle.
func GPUUtilizationPenalty(hw hardware.Info) float64 {
	if !hw.GPUStatsAvailable {
		return 0
	}
	return float64(hw.GPUUtilization) * 0.1
}

// NPUUtilizationPenalty uses the same scheduler scale as GPU utilization.
// Missing telemetry is neutral, not interpreted as idle.
func NPUUtilizationPenalty(hw hardware.Info) float64 {
	if !hw.NPUStatsAvailable {
		return 0
	}
	return float64(hw.NPUUtilization) * 0.1
}

// FastScore is the coordination-speed metric, shared so every process
// announces election scores computed the same way.
func FastScore(hw hardware.Info) float64 {
	s := float64(hw.CPUCores) * 2
	if hw.HasGPU {
		s += 100
	}
	return s
}

// SelfScore is the capability score a process announces for itself on the LAN.
func SelfScore(hw hardware.Info) float64 {
	return CapabilityScore(NodeStatus{Hardware: hw, FastScore: FastScore(hw), Healthy: true})
}

// MostCapableNode returns the healthiest candidate with the highest capability
// score. The node that wins this election is the default coordinator when a
// cluster has multiple potential orchestrators.
func (r *Registry) MostCapableNode() (NodeStatus, bool) {
	return r.Leader()
}

// Leader returns the currently elected cluster leader, if any healthy node exists.
func (r *Registry) Leader() (NodeStatus, bool) {
	for _, n := range r.All() {
		if n.Leader {
			return n, true
		}
	}
	return NodeStatus{}, false
}

// IncrementLoad adjusts the tracked active-task count for a node, used by
// the scheduler to avoid piling every subtask onto a single fast node.
func (r *Registry) IncrementLoad(id string, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		n.ActiveTasks += delta
		if n.ActiveTasks < 0 {
			n.ActiveTasks = 0
		}
		if delta != 0 {
			r.invalidateSnapshotLocked()
		}
	}
}
