// Package background owns cancellable, joined service background work.
package background

import (
	"context"
	"sync"
	"time"
)

// Group's zero value is ready for use. Stop prevents further work from starting,
// cancels running work, and waits for it before its owner closes shared state.
type Group struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

func (g *Group) initLocked() {
	if g.ctx == nil {
		g.ctx, g.cancel = context.WithCancel(context.Background())
	}
}

func (g *Group) Context() context.Context {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	return g.ctx
}

func (g *Group) start() (context.Context, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.initLocked()
	if g.closed {
		return g.ctx, false
	}
	g.wg.Add(1)
	return g.ctx, true
}

func (g *Group) Go(run func(context.Context)) {
	ctx, ok := g.start()
	if !ok {
		return
	}
	go func() {
		defer g.wg.Done()
		run(ctx)
	}()
}

// Run joins a synchronous service lifetime, including startup, to the group.
// Its caller may call Stop only after Run returns; other goroutines may stop it.
func (g *Group) Run(run func(context.Context) error) error {
	ctx, ok := g.start()
	if !ok {
		return ctx.Err()
	}
	defer g.wg.Done()
	return run(ctx)
}

func (g *Group) Stop() {
	g.mu.Lock()
	g.initLocked()
	g.closed = true
	g.cancel()
	g.mu.Unlock()
	g.wg.Wait()
}

// Wait returns false when canceled, including when the delay is already zero.
func Wait(ctx context.Context, delay time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// RunEvery runs fn serially after each interval until cancellation.
// The interval must be positive, as required by time.NewTicker.
func RunEvery(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			fn(ctx)
		}
	}
}
