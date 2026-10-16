package syncinterval

import (
	"testing"
	"time"
)

func TestBackoffAdvancesAndResetsOnWatermarkProgress(t *testing.T) {
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
	var signal Signal
	changes := signal.Changes()
	done := make(chan bool, 1)
	go func() {
		done <- Wait(time.Minute, changes)
	}()
	signal.Notify()
	select {
	case woke := <-done:
		if !woke {
			t.Fatal("Wait returned for timer instead of change signal")
		}
	case <-time.After(time.Second):
		t.Fatal("change signal did not wake Wait promptly")
	}
}
