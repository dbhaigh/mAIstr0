package orchestrator

import (
	"context"
	"sync"
	"time"
)

type pipelineStageMetricKey struct {
	logicalID  string
	stageIndex int
	nodeID     string
	startLayer int
	endLayer   int
}

type pipelineStageMetric struct {
	active            int
	hasActivation     bool
	activationBytes   int
	activationLatency time.Duration
	lastForwardAt     time.Time
}

type pipelineStageMetrics struct {
	mu      sync.RWMutex
	metrics map[pipelineStageMetricKey]pipelineStageMetric
}

func newPipelineStageMetrics() *pipelineStageMetrics {
	return &pipelineStageMetrics{metrics: make(map[pipelineStageMetricKey]pipelineStageMetric)}
}

func (metrics *pipelineStageMetrics) begin(key pipelineStageMetricKey) func(int, time.Duration, bool) {
	metrics.mu.Lock()
	sample := metrics.metrics[key]
	sample.active++
	metrics.metrics[key] = sample
	metrics.mu.Unlock()

	return func(activationBytes int, activationLatency time.Duration, succeeded bool) {
		metrics.mu.Lock()
		sample := metrics.metrics[key]
		if sample.active > 0 {
			sample.active--
		}
		if succeeded && activationBytes > 0 {
			sample.hasActivation = true
			sample.activationBytes = activationBytes
			sample.activationLatency = activationLatency
		}
		sample.lastForwardAt = time.Now()
		metrics.metrics[key] = sample
		metrics.mu.Unlock()
	}
}

func (metrics *pipelineStageMetrics) snapshot(key pipelineStageMetricKey) pipelineStageMetric {
	metrics.mu.RLock()
	sample := metrics.metrics[key]
	metrics.mu.RUnlock()
	return sample
}

type pipelinePlanLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newPipelinePlanLocker() *pipelinePlanLocker {
	return &pipelinePlanLocker{locks: make(map[string]*sync.Mutex)}
}

func (locker *pipelinePlanLocker) lock(logicalID string) func() {
	locker.mu.Lock()
	lock := locker.locks[logicalID]
	if lock == nil {
		lock = &sync.Mutex{}
		locker.locks[logicalID] = lock
	}
	locker.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}

type pipelineRequestState struct {
	mu         sync.Mutex
	active     int
	resharding bool
	changed    chan struct{}
}

type pipelineRequestGate struct {
	mu     sync.Mutex
	states map[string]*pipelineRequestState
}

func newPipelineRequestGate() *pipelineRequestGate {
	return &pipelineRequestGate{states: make(map[string]*pipelineRequestState)}
}

func (gate *pipelineRequestGate) state(logicalID string) *pipelineRequestState {
	gate.mu.Lock()
	state := gate.states[logicalID]
	if state == nil {
		state = &pipelineRequestState{changed: make(chan struct{})}
		gate.states[logicalID] = state
	}
	gate.mu.Unlock()
	return state
}

func (gate *pipelineRequestGate) begin(ctx context.Context, logicalID string) (func(), error) {
	state := gate.state(logicalID)
	for {
		state.mu.Lock()
		if !state.resharding {
			state.active++
			state.mu.Unlock()
			return func() {
				state.mu.Lock()
				state.active--
				state.signalLocked()
				state.mu.Unlock()
			}, nil
		}
		changed := state.changed
		state.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (gate *pipelineRequestGate) beginReshard(ctx context.Context, logicalID string) (func(), error) {
	state := gate.state(logicalID)
	for {
		state.mu.Lock()
		if state.resharding {
			changed := state.changed
			state.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
			}
			continue
		}

		state.resharding = true
		for state.active > 0 {
			changed := state.changed
			state.mu.Unlock()
			select {
			case <-ctx.Done():
				state.mu.Lock()
				state.resharding = false
				state.signalLocked()
				state.mu.Unlock()
				return nil, ctx.Err()
			case <-changed:
				state.mu.Lock()
			}
		}
		state.mu.Unlock()
		return func() {
			state.mu.Lock()
			state.resharding = false
			state.signalLocked()
			state.mu.Unlock()
		}, nil
	}
}

func (state *pipelineRequestState) signalLocked() {
	close(state.changed)
	state.changed = make(chan struct{})
}
