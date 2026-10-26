package orchestrator

import (
	"context"
	"errors"
	"testing"
)

func TestCanceledPipelineAdmissionDoesNotMutateState(t *testing.T) {
	for _, reshard := range []bool{false, true} {
		t.Run(map[bool]string{false: "inference", true: "reshard"}[reshard], func(t *testing.T) {
			gate := newPipelineRequestGate()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var release func()
			var err error
			if reshard {
				release, err = gate.beginReshard(ctx, "model")
			} else {
				release, err = gate.begin(ctx, "model")
			}
			if release != nil {
				release()
			}
			if !errors.Is(err, context.Canceled) || release != nil {
				t.Fatalf("canceled admission = %v, release present=%t", err, release != nil)
			}
			state := gate.state("model")
			if state.active != 0 || state.resharding {
				t.Fatalf("canceled admission changed state: active=%d resharding=%t", state.active, state.resharding)
			}
		})
	}
}

func BenchmarkPipelineAdmission(b *testing.B) {
	gate := newPipelineRequestGate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.ReportAllocs()
	for b.Loop() {
		release, err := gate.begin(ctx, "model")
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}
