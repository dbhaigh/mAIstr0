package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/workpool"
)

func TestDispatchCapacityGateReservesAnyAvailableNodeConcurrently(t *testing.T) {
	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: "http://node-a", MaxConcurrentTasks: 1, AvailableCapacity: 1,
	})
	registry.Upsert(cluster.NodeStatus{
		ID: "node-b", Address: "http://node-b", MaxConcurrentTasks: 1, AvailableCapacity: 1,
	})
	gate := newDispatchCapacityGate(registry, nil, 2)
	candidates := []workpool.CapacityTarget{
		{NodeID: "node-a", Address: "http://node-a"},
		{NodeID: "node-b", Address: "http://node-b"},
	}

	start := make(chan struct{})
	type result struct {
		reservation workpool.CapacityReservation
		err         error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			reservation, err := gate.Reserve(context.Background(), candidates)
			results <- result{reservation: reservation, err: err}
		}()
	}
	close(start)

	reservations := make([]workpool.CapacityReservation, 0, 2)
	targets := make(map[string]bool)
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("reserve capacity: %v", got.err)
			}
			reservations = append(reservations, got.reservation)
			targets[got.reservation.Target().NodeID] = true
		case <-time.After(time.Second):
			t.Fatal("concurrent reservations did not complete")
		}
	}
	defer func() {
		for _, reservation := range reservations {
			reservation.Release()
		}
	}()
	if len(targets) != 2 {
		t.Fatalf("concurrent reservations chose nodes %v, want both available nodes", targets)
	}
	if got := gate.Active(); got != 2 {
		t.Fatalf("active reservations = %d, want global limit 2", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := gate.Reserve(ctx, candidates); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("oversubscribed reservation error = %v, want deadline exceeded", err)
	}
	if got := gate.Active(); got != 2 {
		t.Fatalf("active reservations after rejection = %d, want 2", got)
	}
}

func TestDispatchCapacityGateReleasesNodeReservationOnGlobalWaitCancellation(t *testing.T) {
	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{
		ID: "node", Address: "http://node", MaxConcurrentTasks: 2, AvailableCapacity: 2,
	})
	gate := newDispatchCapacityGate(registry, nil, 1)
	target := []workpool.CapacityTarget{{NodeID: "node", Address: "http://node"}}
	first, err := gate.Reserve(context.Background(), target)
	if err != nil {
		t.Fatalf("reserve first slot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := gate.Reserve(ctx, target)
		result <- err
	}()
	nodeGate := gate.nodeDispatchGate("node", 2)
	deadline := time.Now().Add(time.Second)
	for {
		nodeGate.mu.Lock()
		active := nodeGate.active
		nodeGate.mu.Unlock()
		if active == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second reservation did not acquire its node slot")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled reservation error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reservation did not stop after context cancellation")
	}

	nodeGate.mu.Lock()
	active := nodeGate.active
	nodeGate.mu.Unlock()
	if active != 1 {
		t.Fatalf("node reservations after cancellation = %d, want 1", active)
	}
	if got := gate.Active(); got != 1 {
		t.Fatalf("global reservations after cancellation = %d, want 1", got)
	}

	first.Release()
	next, err := gate.Reserve(context.Background(), target)
	if err != nil {
		t.Fatalf("reserve after cancellation cleanup: %v", err)
	}
	next.Release()
}
