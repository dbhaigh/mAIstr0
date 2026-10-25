package hub

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for publisher")
		var zero T
		return zero
	}
}

func TestPublisherCoalescesAndSkipsIdenticalSnapshots(t *testing.T) {
	h := New()
	ch := h.subscribe()
	defer h.unsubscribe(ch)
	built := make(chan struct{}, 10)
	p := NewPublisher(h, func(context.Context) ([]byte, error) {
		built <- struct{}{}
		return []byte(`{"nodes":[]}`), nil
	})
	for range 1000 {
		p.Request()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.run(ctx, time.Hour, 0)
	}()
	defer func() { cancel(); <-done }()
	receive(t, built)
	if got := string(receive(t, ch)); got != `{"nodes":[]}` {
		t.Fatalf("snapshot = %q", got)
	}
	p.Request()
	receive(t, built)
	// The next build cannot start until the preceding comparison completes.
	p.Request()
	receive(t, built)
	select {
	case <-ch:
		t.Fatal("identical snapshot was sent again")
	default:
	}
	newClient := h.subscribe()
	defer h.unsubscribe(newClient)
	if got := string(receive(t, newClient)); got != `{"nodes":[]}` {
		t.Fatalf("new subscriber snapshot = %q", got)
	}
}

func TestPublisherSkipsIdleWorkAndCancelsBuilder(t *testing.T) {
	h := New()
	var calls atomic.Int32
	started := make(chan struct{})
	p := NewPublisher(h, func(ctx context.Context) ([]byte, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	p.Request()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.run(ctx, time.Hour, 0) }()
	client := h.subscribe()
	defer h.unsubscribe(client)
	receive(t, started)
	for range 1000 {
		p.Request()
	}
	cancel()
	receive(t, done)
	if calls.Load() != 1 {
		t.Fatal("unexpected build count")
	}
}

func TestPublisherAlreadyCanceledDoesNotBuild(t *testing.T) {
	h := New()
	client := h.subscribe()
	defer h.unsubscribe(client)
	p := NewPublisher(h, func(context.Context) ([]byte, error) {
		t.Error("built with an already canceled context")
		return nil, nil
	})
	p.Request()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.run(ctx, time.Hour, 0)
}

func TestPublisherDoesNotBuildWithoutSubscribers(t *testing.T) {
	p := NewPublisher(New(), func(context.Context) ([]byte, error) {
		t.Error("built without subscribers")
		return nil, nil
	})
	p.Request()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	p.run(ctx, time.Millisecond, 0)
}

func TestPublisherOwnsQueuedFrames(t *testing.T) {
	h := New()
	client := h.subscribe()
	defer h.unsubscribe(client)
	built := make(chan struct{}, 2)
	buffer := []byte("0")
	p := NewPublisher(h, func(context.Context) ([]byte, error) {
		buffer[0]++
		built <- struct{}{}
		return buffer, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.run(ctx, time.Hour, 0) }()
	defer func() { cancel(); <-done }()
	receive(t, built)
	p.Request()
	receive(t, built)
	for _, want := range []string{"1", "2"} {
		if got := string(receive(t, client)); got != want {
			t.Fatalf("queued frame = %q, want %q", got, want)
		}
	}
}

func TestPublisherRetriesFullClientQueue(t *testing.T) {
	h := New()
	client := h.subscribe()
	defer h.unsubscribe(client)
	for range cap(client) {
		client <- []byte("old")
	}
	built := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls int
	p := NewPublisher(h, func(ctx context.Context) ([]byte, error) {
		calls++
		built <- struct{}{}
		if calls == 2 {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []byte("new"), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.run(ctx, time.Hour, 0) }()
	defer func() { cancel(); <-done }()
	receive(t, built)
	p.Request()
	// Starting the second build proves the first delivery attempt completed.
	receive(t, built)
	for range cap(client) {
		if got := string(receive(t, client)); got != "old" {
			t.Fatalf("full queue contained %q", got)
		}
	}
	close(release)
	if got := string(receive(t, client)); got != "new" {
		t.Fatalf("retried frame = %q", got)
	}
}

func TestPublisherRetriesBuildErrors(t *testing.T) {
	h := New()
	client := h.subscribe()
	defer h.unsubscribe(client)
	failed := make(chan struct{})
	var calls int
	p := NewPublisher(h, func(context.Context) ([]byte, error) {
		calls++
		if calls == 1 {
			close(failed)
			return nil, errors.New("test snapshot failure")
		}
		return []byte(`{"ok":true}`), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.run(ctx, time.Hour, 0) }()
	defer func() { cancel(); <-done }()
	receive(t, failed)
	p.Request()
	if got := string(receive(t, client)); got != `{"ok":true}` {
		t.Fatalf("retry snapshot = %q", got)
	}
}

func BenchmarkPublisherRequest(b *testing.B) {
	p := NewPublisher(New(), func(context.Context) ([]byte, error) { return nil, nil })
	b.ReportAllocs()
	for range b.N {
		p.Request()
	}
}
