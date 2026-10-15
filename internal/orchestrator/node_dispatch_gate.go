package orchestrator

import (
	"context"
	"errors"
	"sync"
	"time"
)

type nodeDispatchGate struct {
	// mu protects the advertised capacity and local/external active counts.
	mu        sync.Mutex
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
		limit, available, reportedActive, healthy := capacity()
		if !healthy {
			return errors.New("node is no longer available")
		}
		g.setCapacity(limit, available, reportedActive)
		g.mu.Lock()
		if g.active+g.external < g.limit {
			g.active++
			g.mu.Unlock()
			return nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-ticker.C:
		}
	}
}

func (g *nodeDispatchGate) release() {
	g.mu.Lock()
	g.active--
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()
}
