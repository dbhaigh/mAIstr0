package memory

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"), "test-owner")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestRecordAndRecall(t *testing.T) {
	s := newTestStore(t)

	if _, err := s.Record(Experience{
		Kind:        "subtask",
		TaskType:    "code",
		Description: "write a go worker pool with bounded concurrency",
		Output:      "func Pool() {}",
		NodeID:      "node-a",
		Model:       "codellama",
		Success:     true,
		DurationMs:  1200,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if _, err := s.Record(Experience{
		Kind:        "subtask",
		TaskType:    "math",
		Description: "integrate a polynomial",
		NodeID:      "node-b",
		Model:       "mathstral",
		Success:     true,
		DurationMs:  400,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	all := s.Experiences(10)
	if len(all) != 2 {
		t.Fatalf("expected 2 experiences, got %d", len(all))
	}

	hits := s.Recall(SearchFilter{Query: "worker pool concurrency", Limit: 5})
	if len(hits) != 1 {
		t.Fatalf("expected 1 keyword match, got %d", len(hits))
	}
	if hits[0].NodeID != "node-a" {
		t.Errorf("expected node-a recall, got %s", hits[0].NodeID)
	}

	filtered := s.Recall(SearchFilter{Query: "polynomial", TaskType: "math", Limit: 5})
	if len(filtered) != 1 || filtered[0].Model != "mathstral" {
		t.Errorf("task-type filtered recall wrong: %+v", filtered)
	}
}

func TestLearnedBiasFavoursProvenPairings(t *testing.T) {
	s := newTestStore(t)

	for i := 0; i < 8; i++ {
		_, _ = s.Record(Experience{TaskType: "code", NodeID: "good", Model: "m", Success: true, DurationMs: 800})
		_, _ = s.Record(Experience{TaskType: "code", NodeID: "bad", Model: "m", Success: false, DurationMs: 45000, Error: "timeout"})
	}

	good := s.LearnedBias("good", "m", "code")
	bad := s.LearnedBias("bad", "m", "code")

	if good <= 0 {
		t.Errorf("expected positive bias for the reliable pairing, got %.2f", good)
	}
	if bad >= 0 {
		t.Errorf("expected negative bias for the failing pairing, got %.2f", bad)
	}
	if good <= bad {
		t.Errorf("reliable pairing must outrank failing one: good=%.2f bad=%.2f", good, bad)
	}

	// An unseen pairing carries no opinion either way.
	if bias := s.LearnedBias("unknown", "m", "code"); bias != 0 {
		t.Errorf("expected neutral bias for unseen pairing, got %.2f", bias)
	}
}

func TestRecommendAndInsights(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 5; i++ {
		_, _ = s.Record(Experience{TaskType: "code", NodeID: "node-a", Model: "coder", Success: true, DurationMs: 500})
	}
	for i := 0; i < 5; i++ {
		_, _ = s.Record(Experience{TaskType: "code", NodeID: "node-b", Model: "coder", Success: false, DurationMs: 9000})
	}

	recs := s.Recommend("code", 5)
	if len(recs) == 0 {
		t.Fatal("expected recommendations")
	}
	if recs[0].NodeID != "node-a" {
		t.Errorf("expected node-a recommended first, got %s", recs[0].NodeID)
	}

	ins := s.Insights()
	if ins.TotalExperiences != 10 {
		t.Errorf("expected 10 experiences, got %d", ins.TotalExperiences)
	}
	if ins.SuccessRate != 0.5 {
		t.Errorf("expected 0.5 success rate, got %.2f", ins.SuccessRate)
	}
	if len(ins.Lessons) == 0 {
		t.Error("expected at least one learned lesson")
	}
}

func TestFactsRoundTrip(t *testing.T) {
	s := newTestStore(t)

	if _, err := s.PutFact(Fact{Key: "preferred-code-model", Value: "codellama", Source: "test"}); err != nil {
		t.Fatalf("put fact: %v", err)
	}

	got, err := s.GetFact("", "preferred-code-model")
	if err != nil {
		t.Fatalf("get fact: %v", err)
	}
	if got.Value != "codellama" || got.Hits != 1 {
		t.Errorf("unexpected fact: %+v", got)
	}

	if _, err := s.GetFact("", "missing"); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	if err := s.DeleteFact("", "preferred-code-model"); err != nil {
		t.Fatalf("delete fact: %v", err)
	}
	if len(s.Facts("")) != 0 {
		t.Error("expected fact to be forgotten")
	}
}

func TestDialoguePersistence(t *testing.T) {
	s := newTestStore(t)
	participants := []string{"node:a", "node:b"}

	_ = s.AppendDialogueTurn("d1", participants, "caching", Turn{Speaker: "node:a", Content: "propose LRU"})
	_ = s.AppendDialogueTurn("d1", participants, "caching", Turn{Speaker: "node:b", Content: "prefer ARC"})

	d, err := s.Dialogue("d1")
	if err != nil {
		t.Fatalf("dialogue: %v", err)
	}
	if len(d.Turns) != 2 || len(d.Participants) != 2 {
		t.Errorf("unexpected dialogue: %+v", d)
	}
	if list := s.Dialogues(10); len(list) != 1 {
		t.Errorf("expected 1 stored dialogue, got %d", len(list))
	}
}

func TestMergePerformancesKeepsStrongerEvidence(t *testing.T) {
	s := newTestStore(t)
	_, _ = s.Record(Experience{TaskType: "code", NodeID: "n1", Model: "m", Success: true, DurationMs: 100})

	s.MergePerformances([]Performance{{
		NodeID: "n1", Model: "m", TaskType: "code",
		Attempts: 50, Successes: 45, SuccessRate: 0.9, AvgDurationMs: 700, LastUsed: time.Now(),
	}})

	for _, p := range s.Performances() {
		if p.NodeID == "n1" && p.Attempts != 50 {
			t.Errorf("expected merged record with 50 attempts, got %d", p.Attempts)
		}
	}

	// A weaker report must not overwrite richer local evidence.
	s.MergePerformances([]Performance{{NodeID: "n1", Model: "m", TaskType: "code", Attempts: 2}})
	for _, p := range s.Performances() {
		if p.NodeID == "n1" && p.Attempts != 50 {
			t.Errorf("weaker report should not overwrite, got %d attempts", p.Attempts)
		}
	}
}

func TestContextBriefSummarisesHistory(t *testing.T) {
	s := newTestStore(t)
	_, _ = s.Record(Experience{
		TaskType: "code", NodeID: "node-a", Model: "coder",
		Description: "build a rate limiter", Output: "token bucket implementation",
		Success: true, DurationMs: 900,
	})
	_, _ = s.PutFact(Fact{Key: "gpu-node", Value: "node-a has the only GPU"})

	brief := s.ContextBrief("rate limiter", "code", 5)
	if brief == "" {
		t.Fatal("expected a non-empty context brief")
	}
	for _, want := range []string{"<cluster_memory>", "rate limiter", "gpu-node"} {
		if !strings.Contains(brief, want) {
			t.Errorf("brief missing %q:\n%s", want, brief)
		}
	}
}

func TestPruneDropsOldExperiences(t *testing.T) {
	s := newTestStore(t)
	_, _ = s.Record(Experience{Description: "old", CreatedAt: time.Now().Add(-72 * time.Hour), NodeID: "n", Model: "m"})
	_, _ = s.Record(Experience{Description: "fresh", NodeID: "n", Model: "m"})

	removed, err := s.Prune(24 * time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed != 1 {
		t.Errorf("expected 1 pruned record, got %d", removed)
	}
	if len(s.Experiences(10)) != 1 {
		t.Error("expected only the fresh experience to remain")
	}
	// Learned aggregates survive pruning.
	if len(s.Performances()) == 0 {
		t.Error("performance aggregates must outlive pruned experiences")
	}
}
