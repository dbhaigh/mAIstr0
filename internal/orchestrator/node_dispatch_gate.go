package orchestrator

import (
	"context"
	"errors"
	"sync"
	"time"
)

type nodeDispatchGate struct {
	// mu protects advertised capacity and local/external active counts; capacity probes run before acquiring it.
	mu sync.Mutex
	// refreshMu serializes capacity refreshes, and is never held while acquiring mu.
	refreshMu chan struct{}
	limit     int
	active    int
	external  int
	changed   chan struct{}
}

func newNodeDispatchGate(limit int) *nodeDispatchGate {
	return &nodeDispatchGate{
		limit:     limit,
		refreshMu: make(chan struct{}, 1),
		changed:   make(chan struct{}),
	}
}

func (g *nodeDispatchGate) setCapacity(limit, available, reportedActive int) {
	g.mu.Lock()
	occupied := max(limit-available, reportedActive)
	external := max(0, occupied-g.active)
	if limit != g.limit || external != g.external {
		g.limit, g.external = limit, external
		close(g.changed)
		g.changed = make(chan struct{})
	}
	g.mu.Unlock()
}

func (g *nodeDispatchGate) acquire(ctx context.Context, capacity func() (int, int, int, bool)) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		acquired, healthy, changed := g.tryAcquire(capacity)
		if !healthy {
			return errors.New("node is no longer available")
		}
		if acquired {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}

func (g *nodeDispatchGate) tryAcquire(capacity func() (int, int, int, bool)) (bool, bool, <-chan struct{}) {
	limit, available, reportedActive, healthy := capacity()
	if !healthy {
		return false, false, nil
	}
	g.setCapacity(limit, available, reportedActive)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active+g.external < g.limit {
		g.active++
		return true, true, nil
	}
	return false, true, g.changed
}

func (g *nodeDispatchGate) release() {
	g.mu.Lock()
	g.active--
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()
}

func (g *nodeDispatchGate) occupancy() (active, limit int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active + g.external, g.limit
}
