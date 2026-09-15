// Package cluster holds the orchestrator's in-memory view of registered
// nodes: their hardware, available models, and current load.
package cluster

import (
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/version"
)

// NodeStatus is the snapshot a node agent reports about itself, either on
// registration or on every heartbeat poll from the orchestrator.
type NodeStatus struct {
	ID           string         `json:"id"`
	Version      string         `json:"version"`
	Address      string         `json:"address"` // base URL the orchestrator can reach the node at
	Hardware     hardware.Info  `json:"hardware"`
	Models       []engine.Model `json:"models"`
	DefaultModel string         `json:"default_model,omitempty"`
	FastScore    float64        `json:"fast_score"`
	ActiveTasks  int            `json:"active_tasks"`
	LastSeen     time.Time      `json:"last_seen"`
	Healthy      bool           `json:"healthy"`
	Leader       bool           `json:"leader"` // elected cluster leader (most capable healthy node)
	VersionError string         `json:"version_error,omitempty"`
}

// Registry is a thread-safe store of known nodes.
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*NodeStatus
}

func NewRegistry() *Registry {
	return &Registry{nodes: make(map[string]*NodeStatus)}
}

// Upsert registers a node or updates its latest reported status.
func (r *Registry) Upsert(status NodeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	status.LastSeen = time.Now()
	status.Healthy = true
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
}

// MarkUnhealthy flags a node as unreachable without removing it, so it's
// still visible in the GUI but excluded from scheduling.
func (r *Registry) MarkUnhealthy(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		n.Healthy = false
	}
}

// Remove ejects a node from the cluster registry.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, id)
}

// Get returns a copy of a single node's status.
func (r *Registry) Get(id string) (NodeStatus, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return NodeStatus{}, false
	}
	return *n, true
}

// All returns a snapshot of every known node. Leadership is assigned only
// after at least two healthy members have joined the same registry.
func (r *Registry) All() []NodeStatus {
	r.mu.RLock()
	out := make([]NodeStatus, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	r.mu.RUnlock()

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
func CapabilityScore(n NodeStatus) float64 {
	if n.Hardware.Score == 0 {
		return n.FastScore + float64(n.Hardware.CPUCores)*1.5
	}
	return n.Hardware.Score + n.FastScore*0.75
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
	}
}
