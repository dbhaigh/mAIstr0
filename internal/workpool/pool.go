package workpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrClosed is returned when a submission observes that the pool has closed.
var ErrClosed = errors.New("workpool: pool is closed")

// Options configures whether accepted jobs inherit the submitter's context.
type Options struct {
	// PropagateSubmitterContext cancels a running job when its Submit context is canceled.
	PropagateSubmitterContext bool
}

type job[J any] struct {
	ctx   context.Context
	value J
}

type Pool[J any] struct {
	ctx                       context.Context
	cancel                    context.CancelFunc
	jobs                      chan job[J]
	handle                    func(context.Context, J)
	propagateSubmitterContext bool
	closeOnce                 sync.Once
	workers                   sync.WaitGroup
	active                    atomic.Int64
}

// New starts workers that run accepted jobs with both submitter and pool cancellation.
func New[J any](workers, queueCap int, handle func(context.Context, J)) *Pool[J] {
	return NewWithOptions(workers, queueCap, handle, Options{PropagateSubmitterContext: true})
}

// NewWithOptions starts a pool with explicit job-context propagation behavior.
func NewWithOptions[J any](workers, queueCap int, handle func(context.Context, J), options Options) *Pool[J] {
	if workers <= 0 {
		panic("workpool: worker count must be positive")
	}
	if queueCap < 0 {
		panic("workpool: queue capacity cannot be negative")
	}
	if handle == nil {
		panic("workpool: handler cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool := &Pool[J]{
		ctx:                       ctx,
		cancel:                    cancel,
		jobs:                      make(chan job[J], queueCap),
		handle:                    handle,
		propagateSubmitterContext: options.PropagateSubmitterContext,
	}
	pool.workers.Add(workers)
	for range workers {
		go pool.worker()
	}
	return pool
}

func (p *Pool[J]) Submit(ctx context.Context, value J) error {
	if ctx == nil {
		return errors.New("workpool: submit context cannot be nil")
	}
	if p.ctx.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-p.ctx.Done():
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- job[J]{ctx: ctx, value: value}:
		return nil
	}
}

func (p *Pool[J]) worker() {
	defer p.workers.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case next := <-p.jobs:
			if p.ctx.Err() != nil {
				return
			}
			p.run(next)
		}
	}
}

func (p *Pool[J]) run(next job[J]) {
	jobCtx, cancel := context.WithCancel(p.ctx)
	var stopSubmitter func() bool
	if p.propagateSubmitterContext {
		stopSubmitter = context.AfterFunc(next.ctx, cancel)
	}
	defer func() {
		if stopSubmitter != nil {
			stopSubmitter()
		}
		cancel()
		p.active.Add(-1)
	}()
	p.active.Add(1)
	p.handle(jobCtx, next.value)
}

func (p *Pool[J]) Close() {
	p.closeOnce.Do(p.cancel)
	p.workers.Wait()
}

func (p *Pool[J]) QueueDepth() int {
	return len(p.jobs)
}

func (p *Pool[J]) Active() int {
	return int(p.active.Load())
}
