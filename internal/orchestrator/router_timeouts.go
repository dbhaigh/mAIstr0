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

// Read reads from the response body and resets the idle timer after progress.
// Once the idle timeout expires, it returns errRouterIdleStreamTimeout.
//
// Parameters: buffer ([]byte) receives the next response bytes.
// Returns: the bytes read and the underlying read error, or the idle-timeout
// error after expiry.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [router_resilience_test.go](router_resilience_test.go).
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

// Close stops the idle timer and closes the underlying response body.
//
// Returns: the error from closing the response body.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [router_resilience_test.go](router_resilience_test.go).
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
