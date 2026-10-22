package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/workpool"
)

func TestPipelineCapacityReservationIsAtomicAcrossNodes(t *testing.T) {
	registry := cluster.NewRegistry()
	for _, nodeID := range []string{"node-a", "node-b"} {
		registry.Upsert(cluster.NodeStatus{
			ID: nodeID, Address: "https://" + nodeID,
			MaxConcurrentTasks: 1, AvailableCapacity: 1,
		})
	}
	gate := newDispatchCapacityGate(registry, nil, 2)
	targets := []workpool.CapacityTarget{
		{NodeID: "node-b", Address: "https://node-b"},
		{NodeID: "node-a", Address: "https://node-a"},
	}

	reservation, err := gate.ReservePipeline(context.Background(), targets)
	if err != nil {
		t.Fatal(err)
	}
	if gate.Active() != 1 {
		t.Fatalf("global active reservations = %d, want one pipeline slot", gate.Active())
	}
	for _, nodeID := range []string{"node-a", "node-b"} {
		nodeGate := gate.nodeDispatchGate(nodeID, 1)
		if active, _ := nodeGate.occupancy(); active != 1 {
			t.Fatalf("node %s active reservations = %d, want one", nodeID, active)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := gate.ReservePipeline(ctx, targets); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("over-capacity reservation error = %v, want deadline exceeded", err)
	}
	if gate.Active() != 1 {
		t.Fatalf("failed pipeline reservation leaked global capacity: %d", gate.Active())
	}
	reservation.Release()
	if gate.Active() != 0 {
		t.Fatalf("released pipeline still occupies global capacity: %d", gate.Active())
	}
	for _, nodeID := range []string{"node-a", "node-b"} {
		nodeGate := gate.nodeDispatchGate(nodeID, 1)
		if active, _ := nodeGate.occupancy(); active != 0 {
			t.Fatalf("released pipeline still occupies node %s capacity: %d", nodeID, active)
		}
	}
}

func TestPipelineCapacityDoesNotKeepPartialNodeReservation(t *testing.T) {
	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{
		ID: "available", Address: "https://available",
		MaxConcurrentTasks: 1, AvailableCapacity: 1,
	})
	registry.Upsert(cluster.NodeStatus{
		ID: "busy", Address: "https://busy",
		MaxConcurrentTasks: 1, ActiveTasks: 1, AvailableCapacity: 0,
	})
	gate := newDispatchCapacityGate(registry, nil, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := gate.ReservePipeline(ctx, []workpool.CapacityTarget{
		{NodeID: "available", Address: "https://available"},
		{NodeID: "busy", Address: "https://busy"},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("busy pipeline reservation error = %v, want deadline exceeded", err)
	}
	for _, nodeID := range []string{"available", "busy"} {
		nodeGate := gate.nodeDispatchGate(nodeID, 1)
		nodeGate.mu.Lock()
		active := nodeGate.active
		nodeGate.mu.Unlock()
		if active != 0 {
			t.Fatalf("failed reservation kept %d local slots on %s", active, nodeID)
		}
	}
	if gate.Active() != 0 {
		t.Fatalf("failed reservation leaked global capacity: %d", gate.Active())
	}
}
