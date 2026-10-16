package orchestrator

import (
	"fmt"
	"net/http"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
)

const clusterTelemetryFreshnessWindow = 30 * time.Second

// ClusterHealth is the single source of truth for cluster capacity and
// readiness in both the SSE snapshot and GET /api/cluster/health.
type ClusterHealth struct {
	Nodes              []NodeCapacityHealth      `json:"nodes"`
	GlobalDispatch     GlobalDispatchHealth      `json:"global_dispatch"`
	EngineReadiness    EngineReadinessSummary    `json:"engine_readiness"`
	TelemetryFreshness TelemetryFreshnessSummary `json:"telemetry_freshness"`
	Warnings           []string                  `json:"warnings"`
}

type NodeCapacityHealth struct {
	NodeID         string `json:"node_id"`
	Healthy        bool   `json:"healthy"`
	Active         int    `json:"active"`
	Max            int    `json:"max"`
	Remaining      int    `json:"remaining"`
	Oversubscribed bool   `json:"oversubscribed"`
	TelemetryState string `json:"telemetry_state"`
	TelemetryAgeMs int64  `json:"telemetry_age_ms,omitempty"`
}

type GlobalDispatchHealth struct {
	GlobalLimit   int `json:"global_limit"`
	ActiveSlots   int `json:"active_slots"`
	QueueDepth    int `json:"queue_depth"`
	ActiveWorkers int `json:"active_workers"`
}

type EngineReadinessSummary struct {
	ReadyEngines         int `json:"ready_engines"`
	TotalEngines         int `json:"total_engines"`
	NodesWithEngines     int `json:"nodes_with_engines"`
	NodesWithReadyEngine int `json:"nodes_with_ready_engine"`
}

type TelemetryFreshnessSummary struct {
	FreshNodes        int   `json:"fresh_nodes"`
	StaleNodes        int   `json:"stale_nodes"`
	UnavailableNodes  int   `json:"unavailable_nodes"`
	FreshnessWindowMs int64 `json:"freshness_window_ms"`
}

type nodeCapacitySnapshotter interface {
	snapshotNodeCapacity(cluster.NodeStatus, int) (active, limit int, ok bool)
}

func (s *Server) clusterHealth() ClusterHealth {
	nodes := s.registry.All()
	var dispatch *dispatchSnapshot
	if s.dispatcher != nil {
		dispatch = s.dispatcher.snapshot()
	}
	return s.clusterHealthFor(nodes, dispatch)
}

func (s *Server) clusterHealthFor(nodes []cluster.NodeStatus, dispatch *dispatchSnapshot) ClusterHealth {
	health := ClusterHealth{
		Nodes:    make([]NodeCapacityHealth, 0, len(nodes)),
		Warnings: make([]string, 0),
	}
	if dispatch != nil {
		health.GlobalDispatch = GlobalDispatchHealth{
			GlobalLimit: dispatch.GlobalLimit, ActiveSlots: dispatch.ActiveGlobalSlots,
			QueueDepth: dispatch.QueueDepth, ActiveWorkers: dispatch.ActiveDispatchWorkers,
		}
	}
	health.TelemetryFreshness.FreshnessWindowMs = clusterTelemetryFreshnessWindow.Milliseconds()

	now := time.Now()
	var gate nodeCapacitySnapshotter
	if s.dispatcher != nil {
		gate, _ = s.dispatcher.capacityGate.(nodeCapacitySnapshotter)
	}
	for _, node := range nodes {
		limit := node.MaxConcurrentTasks
		available := node.AvailableCapacity
		if limit <= 0 {
			limit = health.GlobalDispatch.GlobalLimit
			available = limit
		}
		active := max(limit-available, node.ActiveTasks)
		if gate != nil {
			if gateActive, gateLimit, ok := gate.snapshotNodeCapacity(node, health.GlobalDispatch.GlobalLimit); ok {
				active, limit = gateActive, gateLimit
			}
		}
		remaining := max(0, limit-active)
		oversubscribed := active > limit
		telemetryState, telemetryAge := telemetryFreshness(node.TelemetryAt, now)
		health.Nodes = append(health.Nodes, NodeCapacityHealth{
			NodeID: node.ID, Healthy: node.Healthy, Active: active, Max: limit,
			Remaining: remaining, Oversubscribed: oversubscribed,
			TelemetryState: telemetryState, TelemetryAgeMs: telemetryAge,
		})
		if node.Healthy && oversubscribed {
			health.Warnings = append(health.Warnings,
				fmt.Sprintf("node %s is oversubscribed (%d active for a limit of %d)", node.ID, active, limit))
		}

		if !node.Healthy {
			continue
		}
		hasReadyEngine := false
		if len(node.Engines) > 0 {
			health.EngineReadiness.NodesWithEngines++
		}
		for _, engine := range node.Engines {
			health.EngineReadiness.TotalEngines++
			if engine.Ready {
				health.EngineReadiness.ReadyEngines++
				hasReadyEngine = true
			}
		}
		if hasReadyEngine {
			health.EngineReadiness.NodesWithReadyEngine++
		}
	}
	if health.GlobalDispatch.ActiveSlots > health.GlobalDispatch.GlobalLimit {
		health.Warnings = append(health.Warnings, fmt.Sprintf(
			"global dispatch is oversubscribed (%d active slots for a limit of %d)",
			health.GlobalDispatch.ActiveSlots, health.GlobalDispatch.GlobalLimit))
	}
	for _, node := range health.Nodes {
		switch node.TelemetryState {
		case "fresh":
			health.TelemetryFreshness.FreshNodes++
		case "stale":
			health.TelemetryFreshness.StaleNodes++
		default:
			health.TelemetryFreshness.UnavailableNodes++
		}
	}
	return health
}

func telemetryFreshness(at, now time.Time) (string, int64) {
	if at.IsZero() {
		return "unavailable", 0
	}
	age := now.Sub(at)
	if age < 0 {
		return "stale", 0
	}
	if age <= clusterTelemetryFreshnessWindow {
		return "fresh", age.Milliseconds()
	}
	return "stale", age.Milliseconds()
}

func (s *Server) handleClusterHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.clusterHealth())
}
