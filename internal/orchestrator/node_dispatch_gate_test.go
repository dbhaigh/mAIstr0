package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeDispatchGateCapacityMath(t *testing.T) {
	tests := []struct {
		name           string
		limit          int
		available      int
		reportedActive int
		localActive    int
		wantLimit      int
		wantExternal   int
	}{
		{"available capacity reports occupancy", 4, 2, 1, 0, 4, 2},
		{"reported activity reports occupancy", 4, 4, 3, 0, 4, 3},
		{"subtracts local activity from reported occupancy", 4, 4, 3, 2, 4, 1},
		{"does not report negative external occupancy", 4, 4, 1, 2, 4, 0},
		{"updates node limit", 3, 2, 0, 0, 3, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gate := newNodeDispatchGate(1)
			gate.active = test.localActive
			gate.setCapacity(test.limit, test.available, test.reportedActive)
			if gate.limit != test.wantLimit {
				t.Fatalf("limit = %d, want %d", gate.limit, test.wantLimit)
			}
			if gate.external != test.wantExternal {
				t.Fatalf("external occupancy = %d, want %d", gate.external, test.wantExternal)
			}
		})
	}
}

func TestNodeDispatchGateRespectsAvailableCapacity(t *testing.T) {
	gate := newNodeDispatchGate(1)
	// available models the changing node-advertised capacity read by the waiter.
	var available atomic.Int64
	capacity := func() (int, int, int, bool) { return 1, int(available.Load()), 0, true }
	acquired := make(chan error, 1)
	go func() { acquired <- gate.acquire(context.Background(), capacity) }()
	select {
	case err := <-acquired:
		t.Fatalf("dispatch acquired despite no available capacity: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	available.Store(1)
	gate.setCapacity(1, 1, 0)
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("dispatch did not acquire refreshed capacity: %v", err)
		}
		gate.release()
	case <-time.After(time.Second):
		t.Fatal("dispatch did not acquire refreshed capacity")
	}
}

func TestNodeDispatchGateRechecksFreshCapacity(t *testing.T) {
	gate := newNodeDispatchGate(1)
	// available models concurrent capacity updates without adding test locks.
	var available atomic.Int64
	capacity := func() (int, int, int, bool) { return 1, int(available.Load()), 0, true }
	acquired := make(chan error, 1)
	go func() {
		acquired <- gate.acquire(context.Background(), capacity)
	}()
	select {
	case err := <-acquired:
		if err == nil {
			gate.release()
		}
		t.Fatalf("dispatch ignored full advertised capacity: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	available.Store(1)
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("dispatch did not proceed after capacity refreshed: %v", err)
		}
		gate.release()
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not recheck the node's refreshed capacity")
	}
}
