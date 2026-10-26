package nodeagent

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestExecutionAccountingIncludesWorkOutsideAdmissionGate(t *testing.T) {
	component := newExecutorComponent(1)
	releaseDirect := component.beginActiveTask()
	releaseLimited, err := component.beginExecution(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := component.activeTaskCount(); got != 2 || len(component.executionSlots) != 1 {
		t.Fatalf("mixed work = active %d slots %d, want 2/1", got, len(component.executionSlots))
	}
	if got := component.availableCapacity(1); got != 0 {
		t.Fatalf("over-limit accounting available capacity = %d", got)
	}
	releaseDirect()
	if component.activeTaskCount() != 1 || len(component.executionSlots) != 1 {
		t.Fatal("ungated release changed admitted work")
	}
	releaseLimited()
	if component.activeTaskCount() != 0 || len(component.executionSlots) != 0 {
		t.Fatal("execution release retained active work or a slot")
	}
}

func TestExecutionCancellationAtSaturationPreservesRunningWork(t *testing.T) {
	component := newExecutorComponent(1)
	release, err := component.beginExecution(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		acquired, err := component.beginExecution(ctx)
		if acquired != nil {
			acquired()
		}
		result <- err
	}()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("saturated canceled admission = %v", err)
	}
	if component.activeTaskCount() != 1 || len(component.executionSlots) != 1 {
		t.Fatal("canceled waiter changed the running reservation")
	}
}

func TestExecutionAccountingConcurrentReleases(t *testing.T) {
	component := newExecutorComponent(2)
	var workers sync.WaitGroup
	for i := range 64 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if i%2 == 0 {
				release := component.beginActiveTask()
				defer release()
			} else {
				release, err := component.beginExecution(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				defer release()
			}
			if component.activeTaskCount() < 1 {
				t.Error("running work was not accounted for")
			}
		}()
	}
	workers.Wait()
	if component.activeTaskCount() != 0 || len(component.executionSlots) != 0 {
		t.Fatal("concurrent execution retained accounting or slots")
	}
}

func TestCanceledExecutionNeverAcquiresAvailableCapacity(t *testing.T) {
	component := newExecutorComponent(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		release, err := component.beginExecution(ctx)
		if release != nil {
			release()
		}
		if !errors.Is(err, context.Canceled) || release != nil {
			t.Fatalf("canceled acquisition = %v, release present=%t", err, release != nil)
		}
	}
	if component.activeTaskCount() != 0 || len(component.executionSlots) != 0 {
		t.Fatal("canceled execution retained capacity")
	}
}

func BenchmarkExecutionAdmission(b *testing.B) {
	component := newExecutorComponent(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.ReportAllocs()
	for b.Loop() {
		release, err := component.beginExecution(ctx)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}
