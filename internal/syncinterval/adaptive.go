package syncinterval

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

var intervals = [...]time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

const DebounceInterval = 500 * time.Millisecond

// Backoff selects a polling interval based on watermark progress.
type Backoff struct {
	index int
}

func (b *Backoff) Current() time.Duration {
	return intervals[b.index]
}

func (b *Backoff) Observe(watermarkAdvanced bool) {
	if watermarkAdvanced {
		b.index = 0
		return
	}
	if b.index < len(intervals)-1 {
		b.index++
	}
}

func (b *Backoff) Reset() {
	b.index = 0
}

// WatermarksAdvanced reports whether current contains a higher sequence than
// previous for any origin.
func WatermarksAdvanced(previous, current map[string]uint64) bool {
	for origin, sequence := range current {
		if sequence > previous[origin] {
			return true
		}
	}
	return false
}

// Wait sleeps for interval, returning true if a change signal wakes it first.
func Wait(interval time.Duration, changes <-chan struct{}) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-changes:
		return true
	case <-timer.C:
		return false
	}
}

// WaitDebounced waits for a change signal to remain quiet for debounce, or
// returns false when the heartbeat interval expires without a change.
func WaitDebounced(interval, debounce time.Duration, changes <-chan struct{}, refresh func() <-chan struct{}) bool {
	return WaitDebouncedContext(context.Background(), interval, debounce, changes, refresh)
}

// WaitDebouncedContext is WaitDebounced with cancellation. Cancellation returns
// false; callers can inspect ctx.Err() to distinguish it from a heartbeat.
func WaitDebouncedContext(ctx context.Context, interval, debounce time.Duration, changes <-chan struct{}, refresh func() <-chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	heartbeat := time.NewTimer(interval)
	defer heartbeat.Stop()
	var heartbeatC = heartbeat.C
	var debounceTimer *time.Timer
	defer func() {
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
	}()
	currentChanges := changes

	for {
		var debounceC <-chan time.Time
		if debounceTimer != nil {
			debounceC = debounceTimer.C
		}
		select {
		case <-ctx.Done():
			return false
		case <-currentChanges:
			if refresh != nil {
				currentChanges = refresh()
			}
			if debounce <= 0 {
				return true
			}
			if debounceTimer == nil {
				debounceTimer = time.NewTimer(debounce)
			} else {
				if !debounceTimer.Stop() {
					select {
					case <-debounceTimer.C:
					default:
					}
				}
				debounceTimer.Reset(debounce)
			}
		case <-debounceC:
			return true
		case <-heartbeatC:
			return false
		}
	}
}

// Jitter varies heartbeat intervals by up to ten percent in either direction.
func Jitter(interval time.Duration) time.Duration {
	if interval <= 0 {
		return interval
	}
	radius := int64(interval / 10)
	if radius == 0 {
		return interval
	}
	return interval + time.Duration(rand.Int63n(radius*2+1)-radius)
}

// Signal broadcasts a change to all current waiters. A caller should capture
// Changes before starting work so writes during that work are not missed.
type Signal struct {
	// mu protects the current notification channel; it is not held while
	// waiters block on that channel, and no other lock is nested beneath it.
	mu      sync.Mutex
	changed chan struct{}
}

func (s *Signal) Changes() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

func (s *Signal) Notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	close(s.changed)
	s.changed = make(chan struct{})
}
