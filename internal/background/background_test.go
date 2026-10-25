package background

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupStopJoinsAndPreventsNewWork(t *testing.T) {
	var group Group
	started, stopped := make(chan struct{}), make(chan struct{})
	group.Go(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(stopped)
	})
	<-started
	group.Stop()
	select {
	case <-stopped:
	default:
		t.Fatal("Stop returned before the worker")
	}
	if group.Context().Err() != context.Canceled {
		t.Fatal("group context remains live")
	}
	group.Go(func(context.Context) { t.Error("started after Stop") })
	group.Stop()
}

func TestGroupConcurrentStopAndStart(t *testing.T) {
	var group Group
	var callers sync.WaitGroup
	for range 20 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			group.Go(func(ctx context.Context) { <-ctx.Done() })
			group.Stop()
		}()
	}
	callers.Wait()
}

func TestGroupStopJoinsSynchronousService(t *testing.T) {
	var group Group
	started, returned := make(chan struct{}), make(chan error, 1)
	go func() {
		returned <- group.Run(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-started
	group.Stop()
	if err := <-returned; err != context.Canceled {
		t.Fatalf("Run returned %v", err)
	}
	if err := group.Run(func(context.Context) error {
		t.Error("service started after Stop")
		return nil
	}); err != context.Canceled {
		t.Fatalf("closed group returned %v", err)
	}
}

func TestRunEverySerialAndCancelable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var active atomic.Int32
	var calls int
	RunEvery(ctx, time.Millisecond, func(context.Context) {
		if active.Add(1) != 1 {
			t.Error("overlapping callbacks")
		}
		defer active.Add(-1)
		calls++
		if calls == 3 {
			cancel()
		}
	})
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestWaitCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, delay := range []time.Duration{0, time.Hour} {
		if Wait(ctx, delay) {
			t.Fatalf("Wait(%s) ignored cancellation", delay)
		}
	}
	RunEvery(ctx, time.Millisecond, func(context.Context) {
		t.Error("called with canceled context")
	})
}
