package workpool

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func TestTrySubmitRejectsSaturationAndCloseDrainsQueue(t *testing.T) {
	started := make(chan struct{})
	pool := New[int](1, 1, func(ctx context.Context, _ int) {
		close(started)
		<-ctx.Done()
	})
	defer pool.Close()
	if err := pool.TrySubmit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := pool.TrySubmit(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if err := pool.TrySubmit(context.Background(), 3); !errors.Is(err, ErrFull) {
		t.Fatalf("saturated submission = %v, want ErrFull", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pool.TrySubmit(ctx, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submission = %v", err)
	}
	var closers sync.WaitGroup
	for range 16 {
		closers.Go(pool.Close)
	}
	closers.Wait()
	if pool.QueueDepth() != 0 || pool.Active() != 0 {
		t.Fatalf("closed pool retained jobs: queued=%d active=%d", pool.QueueDepth(), pool.Active())
	}
	if err := pool.TrySubmit(context.Background(), 5); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed submission = %v", err)
	}
}

func TestTrySubmitRacesWithClose(t *testing.T) {
	pool := New[int](2, 2, func(ctx context.Context, _ int) { <-ctx.Done() })
	var calls sync.WaitGroup
	for range 32 {
		calls.Go(func() {
			err := pool.TrySubmit(context.Background(), 1)
			if err != nil && !errors.Is(err, ErrFull) && !errors.Is(err, ErrClosed) {
				t.Errorf("submission = %v", err)
			}
		})
	}
	calls.Go(pool.Close)
	calls.Wait()
	if pool.QueueDepth() != 0 || pool.Active() != 0 {
		t.Fatal("close retained accepted work")
	}
}

func TestWorkerPanicIsNotSilentlyRecovered(t *testing.T) {
	if os.Getenv("MAISTR0_POOL_PANIC_PROBE") == "1" {
		pool := New[int](1, 1, func(context.Context, int) { panic("pool panic probe") })
		_ = pool.Submit(context.Background(), 1)
		select {}
	}
	command := exec.Command(os.Args[0], "-test.run=^TestWorkerPanicIsNotSilentlyRecovered$")
	command.Env = append(os.Environ(), "MAISTR0_POOL_PANIC_PROBE=1")
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "panic: pool panic probe") {
		t.Fatalf("worker panic did not terminate subprocess: err=%v output=%s", err, output)
	}
}

func TestQueuedCanceledJobEntersWithCanceledContext(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	started, release := make(chan struct{}), make(chan struct{})
	observed := make(chan error, 1)
	pool := New[int](1, 1, func(ctx context.Context, value int) {
		if value == 1 {
			close(started)
			<-release
			return
		}
		observed <- ctx.Err()
	})
	defer pool.Close()
	if err := pool.Submit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	if err := pool.Submit(ctx, 2); err != nil {
		t.Fatal(err)
	}
	cancel()
	close(release)
	if err := <-observed; err != context.Canceled {
		t.Fatalf("queued handler context = %v, want canceled at entry", err)
	}
}

func BenchmarkPoolSubmission(b *testing.B) {
	for _, propagate := range []bool{false, true} {
		name := "detached"
		if propagate {
			name = "linked"
		}
		b.Run(name, func(b *testing.B) {
			pool := NewWithOptions[int](1, 64, func(context.Context, int) {}, Options{
				PropagateSubmitterContext: propagate,
			})
			defer pool.Close()
			b.ReportAllocs()
			for b.Loop() {
				if err := pool.Submit(context.Background(), 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
