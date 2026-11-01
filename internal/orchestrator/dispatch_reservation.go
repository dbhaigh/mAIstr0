package orchestrator

import (
	"context"
	"sync"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/workpool"
)

type dispatchReservation struct {
	capacity workpool.CapacityReservation
	registry *cluster.Registry
	nodeID   string
	loaded   bool
	once     sync.Once
}

func reserveDispatchCapacity(
	ctx context.Context,
	gate workpool.CapacityGate,
	registry *cluster.Registry,
	assignment scheduler.Assignment,
) (*dispatchReservation, error) {
	capacity, err := gate.Reserve(ctx, []workpool.CapacityTarget{{
		NodeID: assignment.NodeID, Address: assignment.Address,
	}})
	if err != nil {
		return nil, err
	}
	return &dispatchReservation{
		capacity: capacity, registry: registry, nodeID: assignment.NodeID,
	}, nil
}

// Start records the reserved node as active in the registry once dispatch
// begins. A nil or already-started reservation is unchanged.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [capacity_gate_test.go](capacity_gate_test.go).
func (r *dispatchReservation) Start() {
	if r == nil || r.loaded {
		return
	}
	r.registry.IncrementLoad(r.nodeID, 1)
	r.loaded = true
}

// Release removes the active registry load, when started, and returns reserved
// capacity. Repeated calls are safe.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [capacity_gate_test.go](capacity_gate_test.go).
func (r *dispatchReservation) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.loaded {
			r.registry.IncrementLoad(r.nodeID, -1)
		}
		r.capacity.Release()
	})
}
