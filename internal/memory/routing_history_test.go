package memory

import (
	"strings"
	"testing"
	"time"
)

func TestRoutingHistoryExposesMeasuredThroughputAndP95(t *testing.T) {
	store := NewInMemoryStore()
	history, ok := store.(interface {
		TokensPerSecond(nodeID, model, taskType string) float64
		P95Duration(model, taskType string) time.Duration
	})
	if !ok {
		t.Fatal("memory store does not expose routing history estimates")
	}
	for _, duration := range []int64{10, 20, 30, 40, 50} {
		_, err := store.Record(Experience{
			Kind: "subtask", NodeID: "node-a", Model: "model-a", TaskType: "code",
			Output: strings.Repeat("x", 128), Success: true, DurationMs: duration,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := store.Record(Experience{
		Kind: "subtask", NodeID: "node-a", Model: "model-a", TaskType: "code",
		Output: strings.Repeat("x", 128), Success: false, DurationMs: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := history.TokensPerSecond("node-a", "model-a", "code"); got < 1066 || got > 1067 {
		t.Fatalf("median tokens per second = %v, want approximately 1066.67", got)
	}
	if got := history.P95Duration("model-a", "code"); got != 50*time.Millisecond {
		t.Fatalf("p95 duration = %s, want 50ms", got)
	}
	if got := history.P95Duration("model-a", "math"); got != 0 {
		t.Fatalf("p95 without matching samples = %s, want zero", got)
	}
}

func TestRoutingHistoryRequiresEnoughSamplesForP95(t *testing.T) {
	store := NewInMemoryStore()
	history, ok := store.(interface {
		P95Duration(model, taskType string) time.Duration
	})
	if !ok {
		t.Fatal("memory store does not expose routing history estimates")
	}
	for i := int64(1); i <= minimumP95Samples-1; i++ {
		_, err := store.Record(Experience{
			Kind: "subtask", Model: "model-a", TaskType: "general",
			Success: true, DurationMs: i,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := history.P95Duration("model-a", "general"); got != 0 {
		t.Fatalf("p95 with insufficient samples = %s, want zero", got)
	}
}
