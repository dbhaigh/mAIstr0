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
	// refreshDone marks the single in-flight node probe; mu is released during I/O.
	refreshDone chan struct{}
	limit       int
	active      int
	external    int
	changed     chan struct{}
}

func newNodeDispatchGate(limit int) *nodeDispatchGate {
	return &nodeDispatchGate{
		limit:   limit,
		changed: make(chan struct{}),
	}
}

func (g *nodeDispatchGate) beginRefresh() (<-chan struct{}, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.refreshDone != nil {
		return g.refreshDone, false
	}
	g.refreshDone = make(chan struct{})
	return g.refreshDone, true
}

func (g *nodeDispatchGate) endRefresh() {
	g.mu.Lock()
	done := g.refreshDone
	g.refreshDone = nil
	close(done)
	g.mu.Unlock()
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
