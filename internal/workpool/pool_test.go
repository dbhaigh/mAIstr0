package workpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestPoolMetricsTrackActiveAndQueuedJobs(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	pool := New[int](2, 2, func(ctx context.Context, _ int) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	defer pool.Close()

	for value := range 4 {
		if err := pool.Submit(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	if got := pool.Active(); got != 2 {
		t.Fatalf("active jobs = %d, want 2", got)
	}
	if got := pool.QueueDepth(); got != 2 {
		t.Fatalf("queued jobs = %d, want 2", got)
	}

	close(release)
	waitFor(t, func() bool { return pool.Active() == 0 && pool.QueueDepth() == 0 })
}

func TestSubmitPropagatesContextAndPoolShutdown(t *testing.T) {
	started := make(chan struct{}, 2)
	ctxDone := make(chan struct{}, 2)
	pool := New[int](1, 1, func(ctx context.Context, _ int) {
		started <- struct{}{}
		<-ctx.Done()
		ctxDone <- struct{}{}
	})
	defer pool.Close()
	submitterCtx, cancelSubmitter := context.WithCancel(context.Background())
	if err := pool.Submit(submitterCtx, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	cancelSubmitter()
	select {
	case <-ctxDone:
	case <-time.After(time.Second):
		t.Fatal("submitter cancellation did not cancel the job context")
	}

	if err := pool.Submit(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second job did not start")
	}
	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()
	select {
	case <-ctxDone:
	case <-time.After(time.Second):
		t.Fatal("pool shutdown did not cancel the active job context")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not wait for workers")
	}
}

func TestPoolCanIgnoreSubmitterContextForAcceptedJobs(t *testing.T) {
	started := make(chan context.Context, 1)
	pool := NewWithOptions[int](1, 1, func(ctx context.Context, _ int) {
		started <- ctx
		<-ctx.Done()
	}, Options{})
	defer pool.Close()

	submitterCtx, cancel := context.WithCancel(context.Background())
	if err := pool.Submit(submitterCtx, 1); err != nil {
		t.Fatal(err)
	}
	var jobCtx context.Context
	select {
	case jobCtx = <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}
	cancel()
	select {
	case <-jobCtx.Done():
		t.Fatal("submitter context unexpectedly canceled the accepted job")
	case <-time.After(30 * time.Millisecond):
	}
	pool.Close()
	select {
	case <-jobCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("pool shutdown did not cancel the job")
	}
}

func TestSubmitBlocksForQueueAndReturnsContextError(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	pool := New[int](1, 1, func(ctx context.Context, _ int) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	defer pool.Close()

	if err := pool.Submit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	if err := pool.Submit(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if got := pool.QueueDepth(); got != 1 {
		t.Fatalf("queue depth = %d, want 1 before blocked submit", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	attempting := make(chan struct{})
	go func() {
		close(attempting)
		result <- pool.Submit(ctx, 3)
	}()
	<-attempting
	select {
	case err := <-result:
		t.Fatalf("submit into a full queue returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked submit error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked submit did not return after context cancellation")
	}

	opened := make(chan error, 1)
	opening := make(chan struct{})
	go func() {
		close(opening)
		opened <- pool.Submit(context.Background(), 4)
	}()
	<-opening
	select {
	case err := <-opened:
		t.Fatalf("submit into a full queue returned early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-opened:
		if err != nil {
			t.Fatalf("submit after queue space opened: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked submit did not proceed after a queue slot opened")
	}
}

func TestConcurrentSubmitAndCloseUnblocksSubmitters(t *testing.T) {
	pool := New[int](2, 1, func(ctx context.Context, _ int) { <-ctx.Done() })
	var submitters sync.WaitGroup
	results := make(chan error, 16)
	for value := range 16 {
		submitters.Add(1)
		go func() {
			defer submitters.Done()
			results <- pool.Submit(context.Background(), value)
		}()
	}
	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()

	done := make(chan struct{})
	go func() {
		submitters.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Submit remained blocked while the pool closed")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close remained blocked")
	}
	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("concurrent submit error = %v, want nil or ErrClosed", err)
		}
	}
	if err := pool.Submit(context.Background(), 99); !errors.Is(err, ErrClosed) {
		t.Fatalf("submit after Close = %v, want ErrClosed", err)
	}
}

func TestCloseDoesNotLeakWorkers(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()
	for range 8 {
		pool := New[int](3, 2, func(ctx context.Context, _ int) { <-ctx.Done() })
		for range 3 {
			if err := pool.Submit(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
		}
		pool.Close()
	}
	waitFor(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline+2
	})
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
