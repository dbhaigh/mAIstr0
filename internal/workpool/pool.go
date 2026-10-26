package workpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrClosed is returned when a submission observes that the pool has closed.
var ErrClosed = errors.New("workpool: pool is closed")

// ErrFull is returned by TrySubmit when no queue slot is immediately available.
var ErrFull = errors.New("workpool: queue is full")

// CapacityTarget identifies a node that may accept a unit of work. Pass one
// target to pin a reservation to a node, or multiple eligible targets for
// automatic routing.
type CapacityTarget struct {
	NodeID  string
	Address string
}

// CapacityReservation holds both the selected node's capacity and one global
// dispatch slot. Its owner releases it when the work completes or its work
// context is cancelled.
type CapacityReservation interface {
	Target() CapacityTarget
	// Release returns the reserved capacity. Implementations must make repeated
	// calls safe so completion and cancellation cleanup may race.
	Release()
}

// CapacityGate coordinates per-node and global dispatch limits across work
// submitted to a Pool and work handled directly by an HTTP router. The Pool's
// Options configure job contexts only; they do not reserve dispatch capacity.
// Reserve waits until one candidate can be reserved or ctx is cancelled. A
// single candidate pins the reservation; multiple candidates allow automatic
// routing in the supplied order. The context bounds only the wait for a
// reservation: callers must release the returned reservation on completion or
// cancellation. Active reports currently held global slots, not queued jobs.
type CapacityGate interface {
	Reserve(ctx context.Context, candidates []CapacityTarget) (CapacityReservation, error)
	Active() int
}

// Options configures job context propagation, not capacity reservation.
// Submit always observes the submitter context while waiting to enqueue.
// Pool cancellation cancels running work; PropagateSubmitterContext also links
// accepted jobs to the submitter context, including jobs still queued. The
// zero value does not propagate cancellation after acceptance; New opts in.
type Options struct {
	// PropagateSubmitterContext links the submitter context to the job context
	// after Submit accepts the job. When false, the pool context still controls
	// the job, but later submitter cancellation does not.
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
	// submissions lets Close drain queued references after all submitters leave.
	submissions sync.RWMutex
	// closeOnce serializes pool shutdown; it is not held while workers finish.
	closeOnce sync.Once
	workers   sync.WaitGroup
	// active is a lock-free count of handlers currently executing; it is
	// independent of queue depth and does not participate in a lock hierarchy.
	active atomic.Int64
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
	return p.submit(ctx, value, true)
}

// TrySubmit never waits for queue space. Acceptance and cancellation semantics
// match Submit; a submission racing Close may be accepted and then abandoned.
func (p *Pool[J]) TrySubmit(ctx context.Context, value J) error {
	return p.submit(ctx, value, false)
}

func (p *Pool[J]) submit(ctx context.Context, value J, wait bool) error {
	if ctx == nil {
		return errors.New("workpool: submit context cannot be nil")
	}
	p.submissions.RLock()
	defer p.submissions.RUnlock()
	if p.ctx.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if !wait {
		select {
		case <-p.ctx.Done():
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		case p.jobs <- job[J]{ctx: ctx, value: value}:
			return nil
		default:
			return ErrFull
		}
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
		if next.ctx.Err() != nil {
			cancel()
		}
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
	p.closeOnce.Do(func() {
		p.cancel()
		p.workers.Wait()
		p.submissions.Lock()
		defer p.submissions.Unlock()
		for {
			select {
			case <-p.jobs:
			default:
				return
			}
		}
	})
}

func (p *Pool[J]) QueueDepth() int {
	return len(p.jobs)
}

func (p *Pool[J]) Active() int {
	return int(p.active.Load())
}
