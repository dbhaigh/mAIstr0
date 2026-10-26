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

type persistentNodeCapacityReservation struct {
	targets []workpool.CapacityTarget
	gates   []*nodeDispatchGate
	once    sync.Once
}

func (g *dispatchCapacityGate) ReserveShardNodes(
	ctx context.Context,
	candidates []workpool.CapacityTarget,
) (ShardCapacityReservation, error) {
	if len(candidates) == 0 {
		return nil, errors.New("shard capacity reservation requires at least one node")
	}
	targets := append([]workpool.CapacityTarget(nil), candidates...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].NodeID < targets[j].NodeID })
	for i, target := range targets {
		if target.NodeID == "" || target.Address == "" {
			return nil, errors.New("shard capacity candidate requires a node ID and address")
		}
		if i > 0 && targets[i-1].NodeID == target.NodeID {
			return nil, fmt.Errorf("shard capacity candidate %q is duplicated", target.NodeID)
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
			g.logCapacityTargets("shard_reservation", "member_unavailable", targets)
			return nil, errors.New("a shard member is no longer available")
		}
		for _, target := range targets {
			status, exists := g.registry.Get(target.NodeID)
			if !exists || !status.Healthy || status.Address != target.Address {
				healthy = false
				break
			}
		}
		if !healthy {
			return nil, errors.New("a shard member is no longer available")
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
				return nil, err
			}
			g.logCapacityTargets("shard_reservation", "reserved", targets)
			return &persistentNodeCapacityReservation{targets: targets, gates: gates}, nil
		}
		g.logCapacityTargets("shard_reservation", "node_capacity_wait", targets)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}

func (reservation *persistentNodeCapacityReservation) Release() {
	if reservation == nil {
		return
	}
	reservation.once.Do(func() {
		for _, gate := range reservation.gates {
			gate.release()
		}
	})
}
