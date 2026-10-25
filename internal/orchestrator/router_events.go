package orchestrator

import (
	"sync"
	"time"
)

type routerRequestEvent struct {
	RequestID         string    `json:"request_id"`
	AttemptID         string    `json:"attempt_id,omitempty"`
	Phase             string    `json:"phase"`
	Timestamp         time.Time `json:"timestamp"`
	NodeID            string    `json:"node_id,omitempty"`
	Model             string    `json:"model,omitempty"`
	Engine            string    `json:"engine,omitempty"`
	QueueMs           int64     `json:"queue_ms,omitempty"`
	TimeToFirstByteMs int64     `json:"time_to_first_byte_ms,omitempty"`
	DurationMs        int64     `json:"-"`
	Status            int       `json:"status,omitempty"`
	FailureClass      string    `json:"failure_class,omitempty"`
}

type routerEventLog struct {
	// mu protects the bounded event slice; callers must not hold it while doing I/O.
	mu       sync.RWMutex
	capacity int
	events   []routerRequestEvent
}

func newRouterEventLog(capacity int) *routerEventLog {
	return &routerEventLog{capacity: capacity}
}

func (l *routerEventLog) append(event routerRequestEvent) {
	if l == nil || l.capacity <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
	if len(l.events) > l.capacity {
		copy(l.events, l.events[len(l.events)-l.capacity:])
		l.events = l.events[:l.capacity]
	}
}

func (l *routerEventLog) snapshot() []routerRequestEvent {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]routerRequestEvent(nil), l.events...)
}
