package deepseek

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
)

func newMemoryHarness(t *testing.T) (*Harness, *Session) {
	t.Helper()
	store, err := memory.Open(filepath.Join(t.TempDir(), "deepseek.db"), "test")
	if err != nil {
		t.Fatalf("open memory: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	h := New(cluster.NewRegistry(), nil)
	h.SetMemory(store)
	return h, h.CreateSession("memory-test", SessionConfig{})
}

func TestMemoryToolsRegisteredWithStore(t *testing.T) {
	h, _ := newMemoryHarness(t)
	names := map[string]bool{}
	for _, d := range h.ListTools() {
		names[d.Name] = true
	}
	for _, want := range []string{"recall_memory", "remember_fact", "memory_insights", "node_to_node_conversation"} {
		if !names[want] {
			t.Errorf("expected tool %q to be registered once memory is attached", want)
		}
	}
}

func TestRememberAndRecallToolsShareState(t *testing.T) {
	h, s := newMemoryHarness(t)
	ctx := context.Background()

	remember := &RememberFactTool{}
	if _, _, _, _, err := remember.Execute(ctx, h, s, map[string]any{
		"key":   "preferred-code-model",
		"value": "codellama handles Go best on node-a",
	}); err != nil {
		t.Fatalf("remember_fact: %v", err)
	}

	facts := h.mem.Facts("cluster")
	if len(facts) != 1 || facts[0].Key != "preferred-code-model" {
		t.Fatalf("fact was not persisted: %+v", facts)
	}

	_, _ = h.mem.Record(memory.Experience{
		TaskType: "code", NodeID: "node-a", Model: "codellama",
		Description: "implement an exponential backoff retry loop",
		Output:      "func retry() {}", Success: true, DurationMs: 600,
	})

	recall := &RecallMemoryTool{}
	out, _, _, _, err := recall.Execute(ctx, h, s, map[string]any{"query": "exponential backoff retry"})
	if err != nil {
		t.Fatalf("recall_memory: %v", err)
	}
	result := out.(map[string]any)
	if result["recalled"].(int) != 1 {
		t.Errorf("expected 1 recalled memory, got %v", result["recalled"])
	}
}

func TestMemoryInsightsToolReportsLearning(t *testing.T) {
	h, s := newMemoryHarness(t)
	for i := 0; i < 4; i++ {
		_, _ = h.mem.Record(memory.Experience{
			TaskType: "code", NodeID: "node-a", Model: "coder", Success: true, DurationMs: 500,
		})
	}

	out, _, _, _, err := (&MemoryInsightsTool{}).Execute(context.Background(), h, s, map[string]any{"task_type": "code"})
	if err != nil {
		t.Fatalf("memory_insights: %v", err)
	}
	res := out.(map[string]any)
	if res["experiences_recorded"].(int) != 4 {
		t.Errorf("unexpected experience count: %v", res["experiences_recorded"])
	}
	if res["success_rate"].(float64) != 1.0 {
		t.Errorf("unexpected success rate: %v", res["success_rate"])
	}
}

func TestMemoryToolsFailClosedWithoutStore(t *testing.T) {
	h := New(cluster.NewRegistry(), nil)
	s := h.CreateSession("no-memory", SessionConfig{})

	if _, _, _, _, err := (&RecallMemoryTool{}).Execute(context.Background(), h, s, map[string]any{"query": "x"}); err == nil {
		t.Error("expected recall_memory to fail without a memory store")
	}
}

func TestNodeToNodeToolRejectsSameNode(t *testing.T) {
	h, s := newMemoryHarness(t)
	_, _, _, _, err := (&NodeToNodeConversationTool{}).Execute(context.Background(), h, s, map[string]any{
		"node_a": "n1", "node_b": "n1", "topic": "anything",
	})
	if err == nil {
		t.Error("expected a node pairing with itself to be rejected")
	}
}
