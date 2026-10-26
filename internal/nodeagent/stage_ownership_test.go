package nodeagent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
)

type countedOwnershipStage struct {
	engine.PipelineStageEngine
	engine.PipelineStageEngineV2
	opens    atomic.Int32
	closes   atomic.Int32
	closeErr error
}

func (s *countedOwnershipStage) OpenPipelineRequest(ctx context.Context, assignment engine.StageAssignment, id string) error {
	s.opens.Add(1)
	return s.PipelineStageEngineV2.OpenPipelineRequest(ctx, assignment, id)
}

func (s *countedOwnershipStage) ClosePipelineRequest(ctx context.Context, assignment engine.StageAssignment, id string) error {
	s.closes.Add(1)
	return errors.Join(s.PipelineStageEngineV2.ClosePipelineRequest(ctx, assignment, id), s.closeErr)
}

func TestStageShutdownStopsTimersReleasesCapacityAndRejectsDuplicates(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "engine close error"}[failClose], func(t *testing.T) {
			component := newStageComponent()
			runtime := engine.NewSimulated()
			wrapped := &countedOwnershipStage{PipelineStageEngine: runtime, PipelineStageEngineV2: runtime}
			if failClose {
				wrapped.closeErr = errors.New("close failure probe")
			}
			assignment := engine.StageAssignment{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4}
			ctx := context.Background()
			if err := component.load(ctx, assignment, func(engine.StageAssignment) (engine.PipelineStageEngine, error) {
				return wrapped, nil
			}); err != nil {
				t.Fatal(err)
			}
			var releases atomic.Int32
			if err := component.openPipelineRequest(ctx, assignment, "request", func() { releases.Add(1) }); err != nil {
				t.Fatal(err)
			}
			active, err := component.pipelineRequest(assignment, "request")
			if err != nil {
				t.Fatal(err)
			}
			if err := component.openPipelineRequest(ctx, assignment, "request", nil); err == nil {
				t.Fatal("duplicate request accepted")
			}
			if wrapped.opens.Load() != 1 || wrapped.closes.Load() != 0 {
				t.Fatal("duplicate open touched the engine's existing request")
			}
			if err := component.close(ctx); failClose && !errors.Is(err, wrapped.closeErr) || !failClose && err != nil {
				t.Fatalf("shutdown error = %v, want %v", err, wrapped.closeErr)
			}
			if len(component.requests) != 0 || len(component.stages) != 0 || active.expiryTimer != nil ||
				releases.Load() != 1 || wrapped.closes.Load() != 1 {
				t.Fatalf("stage cleanup incomplete: requests=%d stages=%d releases=%d closes=%d",
					len(component.requests), len(component.stages), releases.Load(), wrapped.closes.Load())
			}
			_ = component.close(ctx)
			if releases.Load() != 1 || wrapped.closes.Load() != 1 {
				t.Fatal("repeated shutdown repeated resource release")
			}
			if err := component.load(ctx, assignment, func(engine.StageAssignment) (engine.PipelineStageEngine, error) {
				return wrapped, nil
			}); !errors.Is(err, errStageUnavailable) {
				t.Fatalf("load after close = %v", err)
			}
		})
	}
}

func TestCanceledStageStepDoesNotWaitForBusyRequest(t *testing.T) {
	component := newStageComponent()
	active := &activePipelineRequest{operation: make(chan struct{}, 1)}
	assignment := engine.StageAssignment{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4}
	active.assignment, active.requestID = assignment, "request"
	component.requests[pipelineRequestKey(assignment, "request")] = active
	if err := active.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer active.unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := component.prefillPipelineStage(ctx, engine.PipelineStageRequest{Assignment: assignment, RequestID: "request"})
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting step error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled step remained blocked behind another operation")
	}
}

func BenchmarkPipelineRequestOwnership(b *testing.B) {
	component := newStageComponent()
	runtime := engine.NewSimulated()
	assignment := engine.StageAssignment{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4}
	ctx := context.Background()
	if err := component.load(ctx, assignment, func(engine.StageAssignment) (engine.PipelineStageEngine, error) {
		return runtime, nil
	}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := component.unload(ctx, assignment); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	for b.Loop() {
		if err := component.openPipelineRequest(ctx, assignment, "request", nil); err != nil {
			b.Fatal(err)
		}
		if err := component.closePipelineRequest(ctx, assignment, "request"); err != nil {
			b.Fatal(err)
		}
	}
}
