package hub

import (
	"bytes"
	"context"
	"log/slog"
	"time"
)

// Publisher coalesces snapshot requests without building payloads on the
// requesting goroutine. Run must be called exactly once by its service owner.
type Publisher struct {
	hub      *Hub
	build    func(context.Context) ([]byte, error)
	requests chan struct{}
}

func NewPublisher(h *Hub, build func(context.Context) ([]byte, error)) *Publisher {
	return &Publisher{hub: h, build: build, requests: make(chan struct{}, 1)}
}

func (p *Publisher) Request() {
	select {
	case p.requests <- struct{}{}:
	default:
	}
}

func (p *Publisher) Run(ctx context.Context) {
	p.run(ctx, time.Second, 25*time.Millisecond)
}

func (p *Publisher) run(ctx context.Context, interval, debounce time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var previous []byte
	var generation uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.requests:
		case <-p.hub.subscribed:
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return
		}
		if !p.hub.HasSubscribers() {
			previous = nil
			continue
		}
		timer := time.NewTimer(debounce)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		// Requests received during this coalescing window belong to this build.
		select {
		case <-p.requests:
		default:
		}
		select {
		case <-p.hub.subscribed:
		default:
		}
		nextGeneration := p.hub.generation.Load()
		data, err := p.build(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("could not build event snapshot", "component", "hub", "error", err)
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if generation == nextGeneration && bytes.Equal(previous, data) {
			continue
		}
		// Queued clients may still be using the frame during the next build.
		frame := bytes.Clone(data)
		if p.hub.broadcast(frame) {
			previous = frame
			generation = nextGeneration
		}
	}
}
