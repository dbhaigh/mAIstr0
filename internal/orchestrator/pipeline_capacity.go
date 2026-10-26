package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/workpool"
)

var errPipelineNodeUnavailable = errors.New("pipeline node is no longer available")

type pipelineCapacityReservation struct {
	targets []workpool.CapacityTarget
	gates   []*nodeDispatchGate
	slots   chan struct{}
	once    sync.Once
}

func (g *dispatchCapacityGate) ReservePipeline(
	ctx context.Context,
	candidates []workpool.CapacityTarget,
) (*pipelineCapacityReservation, error) {
	if len(candidates) < 2 {
		return nil, errors.New("pipeline capacity reservation requires at least two nodes")
	}
	targets := append([]workpool.CapacityTarget(nil), candidates...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].NodeID < targets[j].NodeID })
	for i, target := range targets {
		if target.NodeID == "" || target.Address == "" {
			return nil, errors.New("pipeline capacity candidate requires a node ID and address")
		}
		if i > 0 && targets[i-1].NodeID == target.NodeID {
			return nil, fmt.Errorf("pipeline capacity candidate %q is duplicated", target.NodeID)
		}
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		gates := make([]*nodeDispatchGate, len(targets))
		healthy := true
		for i, target := range targets {
			status, exists := g.registry.Get(target.NodeID)
			if !exists || !status.Healthy || status.Address != target.Address {
				healthy = false
				break
			}
			limit := status.MaxConcurrentTasks
			if limit <= 0 {
				limit = g.fallbackLimit
			}
			gate := g.nodeDispatchGate(target.NodeID, limit)
			nodeLimit, available, reportedActive, ok := g.dispatchCapacity(ctx, gate, target.NodeID, target.Address)
			if !ok {
				healthy = false
				break
			}
			gate.setCapacity(nodeLimit, available, reportedActive)
			gates[i] = gate
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !healthy {
			g.logCapacityTargets("pipeline_reservation", "member_unavailable", targets)
			return nil, errPipelineNodeUnavailable
		}

		select {
		case g.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			continue
		}

		for _, target := range targets {
			status, exists := g.registry.Get(target.NodeID)
			if !exists || !status.Healthy || status.Address != target.Address {
				healthy = false
				break
			}
		}
		if !healthy {
			<-g.slots
			return nil, errPipelineNodeUnavailable
		}

		for _, gate := range gates {
			gate.mu.Lock()
		}
		acquired := true
		var changed <-chan struct{}
		for _, gate := range gates {
			if gate.active+gate.external >= gate.limit {
				acquired = false
				if changed == nil {
					changed = gate.changed
				}
			}
		}
		if acquired {
			for _, gate := range gates {
				gate.active++
			}
		}
		for i := len(gates) - 1; i >= 0; i-- {
			gates[i].mu.Unlock()
		}
		if acquired {
			if err := ctx.Err(); err != nil {
				for _, gate := range gates {
					gate.release()
				}
				<-g.slots
				return nil, err
			}
			g.logCapacityTargets("pipeline_reservation", "reserved", targets)
			return &pipelineCapacityReservation{targets: targets, gates: gates, slots: g.slots}, nil
		}
		<-g.slots
		g.logCapacityTargets("pipeline_reservation", "node_capacity_wait", targets)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}

func (reservation *pipelineCapacityReservation) Release() {
	if reservation == nil {
		return
	}
	reservation.once.Do(func() {
		for _, gate := range reservation.gates {
			gate.release()
		}
		<-reservation.slots
	})
}
