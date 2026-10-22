package orchestrator

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var errRouterIdleStreamTimeout = errors.New("router idle-stream timeout")

type routerIdleTimeoutReader struct {
	body    io.ReadCloser
	cancel  context.CancelFunc
	timeout time.Duration
	timer   *time.Timer
	mu      sync.Mutex
	expired bool
	closed  bool
}

func newRouterIdleTimeoutReader(
	body io.ReadCloser,
	timeout time.Duration,
	cancel context.CancelFunc,
) *routerIdleTimeoutReader {
	reader := &routerIdleTimeoutReader{body: body, cancel: cancel, timeout: timeout}
	reader.timer = time.AfterFunc(timeout, reader.expire)
	return reader
}

func (r *routerIdleTimeoutReader) Read(buffer []byte) (int, error) {
	n, err := r.body.Read(buffer)
	r.mu.Lock()
	expired := r.expired
	if !expired {
		if n > 0 {
			r.timer.Reset(r.timeout)
		}
		if err != nil {
			r.timer.Stop()
		}
	}
	r.mu.Unlock()
	if expired {
		return n, errRouterIdleStreamTimeout
	}
	return n, err
}

func (r *routerIdleTimeoutReader) Close() error {
	r.mu.Lock()
	r.closed = true
	r.timer.Stop()
	r.mu.Unlock()
	return r.body.Close()
}

func (r *routerIdleTimeoutReader) expire() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.expired = true
	r.mu.Unlock()
	r.cancel()
	_ = r.body.Close()
}
