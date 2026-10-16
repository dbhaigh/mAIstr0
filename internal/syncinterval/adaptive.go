package syncinterval

import (
	"sync"
	"time"
)

var intervals = [...]time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

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
