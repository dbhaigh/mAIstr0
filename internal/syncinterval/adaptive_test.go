package syncinterval

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestBackoffAdvancesAndResetsOnWatermarkProgress(t *testing.T) {
	t.Parallel()
	var backoff Backoff
	want := []time.Duration{
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
		60 * time.Second,
	}
	for i, interval := range want {
		if got := backoff.Current(); got != interval {
			t.Fatalf("interval before idle round %d = %s, want %s", i, got, interval)
		}
		backoff.Observe(false)
	}
	backoff.Observe(true)
	if got := backoff.Current(); got != 5*time.Second {
		t.Fatalf("interval after watermark advancement = %s, want 5s", got)
	}
	backoff.Observe(false)
	backoff.Reset()
	if got := backoff.Current(); got != 5*time.Second {
		t.Fatalf("interval after reset = %s, want 5s", got)
	}
}

func TestWatermarksAdvanced(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		previous map[string]uint64
		current  map[string]uint64
		want     bool
	}{
		{name: "new origin", current: map[string]uint64{"peer": 1}, want: true},
		{name: "higher sequence", previous: map[string]uint64{"peer": 1}, current: map[string]uint64{"peer": 2}, want: true},
		{name: "unchanged", previous: map[string]uint64{"peer": 2}, current: map[string]uint64{"peer": 2}},
		{name: "regressed", previous: map[string]uint64{"peer": 2}, current: map[string]uint64{"peer": 1}},
		{name: "removed origin", previous: map[string]uint64{"peer": 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := WatermarksAdvanced(test.previous, test.current); got != test.want {
				t.Fatalf("WatermarksAdvanced() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestWaitWakesOnSignal(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var signal Signal
		changes := signal.Changes()
		done := make(chan bool, 1)
		go func() {
			done <- Wait(time.Minute, changes)
		}()
		synctest.Wait()
		signal.Notify()
		if !<-done {
			t.Fatal("Wait returned for timer instead of change signal")
		}
	})
}

func TestWaitDebouncedRefreshesSignalChannel(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var signal Signal
		changes := signal.Changes()
		done := make(chan bool, 1)
		go func() {
			done <- WaitDebounced(time.Minute, 40*time.Millisecond, changes, signal.Changes)
		}()
		signal.Notify()
		synctest.Wait()
		time.Sleep(20 * time.Millisecond)
		signal.Notify()
		synctest.Wait()
		time.Sleep(40 * time.Millisecond)
		synctest.Wait()
		if !<-done {
			t.Fatal("debounced wait returned for the heartbeat")
		}
	})
}

func TestWaitDebouncedContextCancellation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		preCancel bool
		change    bool
	}{
		{name: "already cancelled", preCancel: true},
		{name: "heartbeat"},
		{name: "debounce", change: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var signal Signal
			changes := signal.Changes()
			refreshed := make(chan struct{})
			if test.change {
				signal.Notify()
			}
			if test.preCancel {
				cancel()
			}
			done := make(chan bool, 1)
			go func() {
				done <- WaitDebouncedContext(ctx, time.Hour, time.Hour, changes, func() <-chan struct{} {
					close(refreshed)
					return signal.Changes()
				})
			}()
			if test.change {
				select {
				case <-refreshed:
				case <-time.After(time.Second):
					t.Fatal("wait did not start debouncing the change")
				}
			}
			cancel()
			select {
			case changed := <-done:
				if changed {
					t.Fatal("cancelled wait reported a change")
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled wait did not stop promptly")
			}
		})
	}
}

func TestWaitDebouncedContextHeartbeatAndChange(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change bool
	}{
		{name: "heartbeat"},
		{name: "change", change: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			changes := make(chan struct{}, 1)
			if test.change {
				changes <- struct{}{}
			}
			interval := time.Duration(0)
			if test.change {
				interval = time.Hour
			}
			if got := WaitDebouncedContext(context.Background(), interval, 0, changes, nil); got != test.change {
				t.Fatalf("WaitDebouncedContext() = %v, want %v", got, test.change)
			}
		})
	}
}

func TestWaitDebouncedContextHeartbeatBoundsContinuousChanges(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()

		var signal Signal
		stop := make(chan struct{})
		senderDone := make(chan struct{})
		go func() {
			defer close(senderDone)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					signal.Notify()
				}
			}
		}()

		changed := WaitDebouncedContext(ctx, 80*time.Millisecond, 500*time.Millisecond, signal.Changes(), signal.Changes)
		close(stop)
		<-senderDone
		if changed {
			t.Fatal("continuous changes returned as a settled debounce")
		}
		if ctx.Err() != nil {
			t.Fatalf("wait returned only after cancellation: %v", ctx.Err())
		}
	})
}

func TestJitterStaysWithinTenPercent(t *testing.T) {
	t.Parallel()
	const interval = 10 * time.Second
	for range 100 {
		got := Jitter(interval)
		if got < 9*time.Second || got > 11*time.Second {
			t.Fatalf("Jitter(%s) = %s, outside +/-10%%", interval, got)
		}
	}
	if got := Jitter(0); got != 0 {
		t.Fatalf("Jitter(0) = %s, want 0", got)
	}
}
