package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"log/slog"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/workpool"
)

type dispatchCapacityProbe interface {
	discoveredNodeStatusContext(context.Context, string) (cluster.NodeStatus, error)
}

type dispatchCapacityGate struct {
	registry      *cluster.Registry
	capacityProbe dispatchCapacityProbe
	fallbackLimit int
	slots         chan struct{}

	// nodeGateMu protects only the gate map; release it before probing nodes or acquiring a per-node gate.
	nodeGateMu sync.Mutex
	nodeGates  map[string]*nodeDispatchGate
}

func newDispatchCapacityGate(
	registry *cluster.Registry,
	capacityProbe dispatchCapacityProbe,
	globalLimit int,
) *dispatchCapacityGate {
	return &dispatchCapacityGate{
		registry: registry, capacityProbe: capacityProbe,
		fallbackLimit: globalLimit, slots: make(chan struct{}, globalLimit),
		nodeGates: make(map[string]*nodeDispatchGate),
	}
}

func (g *dispatchCapacityGate) Reserve(ctx context.Context, candidates []workpool.CapacityTarget) (workpool.CapacityReservation, error) {
	if len(candidates) == 0 {
		return nil, errors.New("capacity reservation requires at least one node")
	}
	for _, candidate := range candidates {
		if candidate.NodeID == "" || candidate.Address == "" {
			return nil, errors.New("capacity reservation candidate requires a node ID and address")
		}
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var changed <-chan struct{}
		healthyCandidates := 0
		for _, candidate := range candidates {
			status, exists := g.registry.Get(candidate.NodeID)
			if !exists || !status.Healthy || status.Address != candidate.Address {
				scheduler.LogDecision("reservation", "node_unavailable", status, "", 0, scheduler.ScoreFactors{})
				continue
			}
			limit := status.MaxConcurrentTasks
			if limit <= 0 {
				limit = g.fallbackLimit
			}
			nodeGate := g.nodeDispatchGate(candidate.NodeID, limit)
			acquired, healthy, signal := nodeGate.tryAcquire(func() (int, int, int, bool) {
				return g.dispatchCapacity(ctx, nodeGate, candidate.NodeID, candidate.Address)
			})
			if !healthy {
				scheduler.LogDecision("reservation", "refresh_unavailable", status, "", 0, scheduler.ScoreFactors{})
				continue
			}
			healthyCandidates++
			if acquired {
				if len(g.slots) == cap(g.slots) {
					logNodeCapacityDecision("reservation", "global_capacity_wait", status, nodeGate)
				}
				select {
				case g.slots <- struct{}{}:
					if err := ctx.Err(); err != nil {
						nodeGate.release()
						<-g.slots
						return nil, err
					}
					logNodeCapacityDecision("reservation", "reserved", status, nodeGate)
					return &dispatchCapacityReservation{
						target: candidate, nodeGate: nodeGate, slots: g.slots,
					}, nil
				case <-ctx.Done():
					nodeGate.release()
					return nil, ctx.Err()
				}
			}
			logNodeCapacityDecision("reservation", "node_capacity_wait", status, nodeGate)
			if changed == nil {
				changed = signal
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if healthyCandidates == 0 {
			return nil, errors.New("node is no longer available")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}

func (g *dispatchCapacityGate) logCapacityTargets(stage, reason string, targets []workpool.CapacityTarget) {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	for _, target := range targets {
		status, ok := g.registry.Get(target.NodeID)
		if !ok {
			status.ID = target.NodeID
		}
		g.nodeGateMu.Lock()
		nodeGate := g.nodeGates[target.NodeID]
		g.nodeGateMu.Unlock()
		logNodeCapacityDecision(stage, reason, status, nodeGate)
	}
}

func logNodeCapacityDecision(stage, reason string, status cluster.NodeStatus, gate *nodeDispatchGate) {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	if gate != nil {
		status.ActiveTasks, status.MaxConcurrentTasks = gate.occupancy()
		status.AvailableCapacity = max(0, status.MaxConcurrentTasks-status.ActiveTasks)
	}
	scheduler.LogDecision(stage, reason, status, "", 0, scheduler.ScoreFactors{})
}

func (g *dispatchCapacityGate) Active() int {
	return len(g.slots)
}

func (g *dispatchCapacityGate) snapshotNodeCapacity(status cluster.NodeStatus, fallbackLimit int) (active, limit int, ok bool) {
	g.nodeGateMu.Lock()
	nodeGate, ok := g.nodeGates[status.ID]
	g.nodeGateMu.Unlock()
	if !ok {
		return 0, 0, false
	}

	limit = status.MaxConcurrentTasks
	available := status.AvailableCapacity
	if limit <= 0 {
		limit = fallbackLimit
		available = limit
	}
	nodeGate.setCapacity(limit, available, status.ActiveTasks)
	active, limit = nodeGate.occupancy()
	return active, limit, true
}

func (g *dispatchCapacityGate) nodeDispatchGate(nodeID string, limit int) *nodeDispatchGate {
	g.nodeGateMu.Lock()
	defer g.nodeGateMu.Unlock()
	nodeGate, ok := g.nodeGates[nodeID]
	if !ok {
		nodeGate = newNodeDispatchGate(limit)
		g.nodeGates[nodeID] = nodeGate
	}
	return nodeGate
}

func (g *dispatchCapacityGate) dispatchCapacity(
	ctx context.Context,
	nodeGate *nodeDispatchGate,
	nodeID, address string,
) (int, int, int, bool) {
	var status cluster.NodeStatus
	for {
		current, exists := g.registry.Get(nodeID)
		if !exists || !current.Healthy || current.Address != address {
			return 0, 0, 0, false
		}
		status = current
		if time.Since(status.LastSeen) <= nodeCapacityFreshnessWindow {
			break
		}
		scheduler.LogDecision("reservation", "stale_capacity_refresh", status, "", 0, scheduler.ScoreFactors{})

		done, owner := nodeGate.beginRefresh()
		if !owner {
			select {
			case <-ctx.Done():
				return 0, 0, 0, false
			case <-done:
				continue
			}
		}

		refreshed, err := g.capacityProbe.discoveredNodeStatusContext(ctx, status.Address)
		if err != nil {
			if ctx.Err() != nil {
				nodeGate.endRefresh()
				return 0, 0, 0, false
			}
			logging.Errorf("orchestrator", "orchestrator: refreshing capacity for node %s failed: %v", nodeID, err)
			g.registry.MarkUnhealthy(nodeID)
			nodeGate.endRefresh()
			return 0, 0, 0, false
		}
		if refreshed.ID != nodeID || refreshed.Address != address {
			logging.Errorf("orchestrator", "orchestrator: capacity response identity mismatch for node %s", nodeID)
			g.registry.MarkUnhealthy(nodeID)
			nodeGate.endRefresh()
			return 0, 0, 0, false
		}
		g.registry.Upsert(refreshed)
		nodeGate.endRefresh()
		status = refreshed
		break
	}

	limit := status.MaxConcurrentTasks
	available := status.AvailableCapacity
	if limit <= 0 {
		limit = g.fallbackLimit
		available = limit
	}
	return limit, available, status.ActiveTasks, true
}

type dispatchCapacityReservation struct {
	target   workpool.CapacityTarget
	nodeGate *nodeDispatchGate
	slots    chan struct{}
	// once makes release idempotent across request completion and cancellation.
	once sync.Once
}

func (r *dispatchCapacityReservation) Target() workpool.CapacityTarget {
	return r.target
}

func (r *dispatchCapacityReservation) Release() {
	r.once.Do(func() {
		r.nodeGate.release()
		select {
		case <-r.slots:
		default:
			panic(fmt.Sprintf("capacity reservation for node %s has no global slot", r.target.NodeID))
		}
	})
}
