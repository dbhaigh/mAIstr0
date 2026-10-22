package memory

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) Store {
	t.Helper()
	return NewInMemoryStore()
}

func TestMergeSnapshotReplayIsIdempotent(t *testing.T) {
	source := newTestStore(t)
	if _, err := source.Record(Experience{
		Description: "record once", NodeID: "node", Model: "model", Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PutFact(Fact{Key: "fact", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	if err := source.AppendDialogueTurn("dialogue", []string{"node"}, "topic", Turn{
		Speaker: "node", Content: "turn", At: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := source.Snapshot()

	destination := newTestStore(t)
	if err := destination.MergeSnapshotFrom(snapshot, "source-node"); err != nil {
		t.Fatal(err)
	}
	before := destination.Snapshot()
	if err := destination.MergeSnapshotFrom(snapshot, "source-node"); err != nil {
		t.Fatal(err)
	}
	after := destination.Snapshot()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("replaying snapshot changed state:\nbefore: %+v\nafter:  %+v", before, after)
	}
}

func TestOutOfOrderSnapshotsConverge(t *testing.T) {
	store := newTestStore(t)
	origin := "origin-a"
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := Snapshot{
		Experiences: []Experience{{
			Origin: origin, Sequence: 2, Description: "second", CreatedAt: base.Add(time.Second),
		}},
		Facts: []Fact{{
			Key: "fact", Value: "new", Scope: "cluster", Source: "source-a",
			Origin: origin, Sequence: 4, CreatedAt: base, UpdatedAt: base.Add(time.Second),
		}},
		Performances: []Performance{{
			NodeID: "node", Model: "model", TaskType: "general", Attempts: 5,
			Origin: origin, Sequence: 6,
		}},
		Dialogues: []DialogueRecord{{
			ID: "dialogue", Participants: []string{"node"}, Topic: "topic",
			Turns:  []Turn{{Speaker: "node", Content: "new", At: base.Add(time.Second)}},
			Origin: origin, Sequence: 8, CreatedAt: base, UpdatedAt: base.Add(time.Second),
		}},
		Watermarks: map[string]uint64{origin: 8},
	}
	older := Snapshot{
		Experiences: []Experience{{
			Origin: origin, Sequence: 1, Description: "first", CreatedAt: base,
		}},
		Facts: []Fact{{
			Key: "fact", Value: "old", Scope: "cluster", Source: "source-a",
			Origin: origin, Sequence: 3, CreatedAt: base, UpdatedAt: base,
		}},
		Performances: []Performance{{
			NodeID: "node", Model: "model", TaskType: "general", Attempts: 2,
			Origin: origin, Sequence: 5,
		}},
		Dialogues: []DialogueRecord{{
			ID: "dialogue", Participants: []string{"node"}, Topic: "topic",
			Turns:  []Turn{{Speaker: "node", Content: "old", At: base}},
			Origin: origin, Sequence: 7, CreatedAt: base, UpdatedAt: base,
		}},
		Watermarks: map[string]uint64{origin: 7},
	}
	if err := store.MergeSnapshotFrom(newer, "peer"); err != nil {
		t.Fatal(err)
	}
	if err := store.MergeSnapshotFrom(older, "peer"); err != nil {
		t.Fatal(err)
	}

	snapshot := store.Snapshot()
	if len(snapshot.Experiences) != 2 {
		t.Fatalf("experiences after out-of-order merge = %d, want 2", len(snapshot.Experiences))
	}
	fact, err := store.GetFact("cluster", "fact")
	if err != nil {
		t.Fatal(err)
	}
	if fact.Value != "new" {
		t.Fatalf("stale fact won out-of-order merge: %+v", fact)
	}
	if len(snapshot.Performances) != 1 || snapshot.Performances[0].Attempts != 5 {
		t.Fatalf("stale performance won out-of-order merge: %+v", snapshot.Performances)
	}
	if len(snapshot.Dialogues) != 1 || len(snapshot.Dialogues[0].Turns) != 2 {
		t.Fatalf("dialogue turns did not converge: %+v", snapshot.Dialogues)
	}
}

func TestSnapshotForPeerNegotiatesIncrementalAndFullFallback(t *testing.T) {
	store := newTestStore(t)
	for range 3 {
		if _, err := store.Record(Experience{Description: "record"}); err != nil {
			t.Fatal(err)
		}
	}
	full := store.Snapshot()
	if len(full.Experiences) != 3 || full.SyncVersion != SyncProtocolVersion {
		t.Fatalf("full snapshot = %+v", full)
	}
	origin := full.Experiences[0].Origin
	incremental := SnapshotForPeer(store, SyncProtocolVersion, map[string]uint64{origin: 2})
	if len(incremental.Experiences) != 1 {
		t.Fatalf("incremental experiences = %d, want 1", len(incremental.Experiences))
	}
	if versionTwo := SnapshotForPeer(store, SyncPageProtocolVersion, map[string]uint64{origin: 2}); len(versionTwo.Experiences) != 1 {
		t.Fatalf("version 2 peer got %d experiences, want 1", len(versionTwo.Experiences))
	}
	if legacyIncremental := SnapshotForPeer(store, FirstSyncProtocolVersion, map[string]uint64{origin: 2}); len(legacyIncremental.Experiences) != 1 {
		t.Fatalf("version 1 peer got %d experiences, want the incremental record", len(legacyIncremental.Experiences))
	}
	if legacy := SnapshotForPeer(store, 0, map[string]uint64{origin: 2}); len(legacy.Experiences) != 3 {
		t.Fatalf("legacy peer got %d experiences, want full snapshot", len(legacy.Experiences))
	}
	if reset := SnapshotForPeer(store, SyncProtocolVersion, map[string]uint64{origin: 99}); len(reset.Experiences) != 3 {
		t.Fatalf("peer with ahead watermark got %d experiences, want full fallback", len(reset.Experiences))
	}
	if future := SnapshotForPeer(store, SyncProtocolVersion+1, map[string]uint64{origin: 2}); len(future.Experiences) != 3 {
		t.Fatalf("unsupported future peer got %d experiences, want full fallback", len(future.Experiences))
	}

	request, err := http.NewRequest(http.MethodGet, "https://example.test/memory/sync", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetSyncRequestHeaders(request, full.Watermarks); err != nil {
		t.Fatal(err)
	}
	version, watermarks, err := ReadSyncRequestHeaders(request)
	if err != nil {
		t.Fatal(err)
	}
	if version != SyncProtocolVersion || !reflect.DeepEqual(watermarks, full.Watermarks) {
		t.Fatalf("decoded sync headers = version %d, watermarks %v", version, watermarks)
	}
	legacyRequest, _ := http.NewRequest(http.MethodGet, "https://example.test/memory/sync", nil)
	if version, marks, err := ReadSyncRequestHeaders(legacyRequest); err != nil || version != 0 || marks != nil {
		t.Fatalf("legacy sync headers = version %d, watermarks %v, err %v", version, marks, err)
	}

	encoded, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	var legacyWire struct {
		Experiences []Experience `json:"experiences"`
	}
	if err := json.Unmarshal(encoded, &legacyWire); err != nil {
		t.Fatalf("legacy peer could not decode extended snapshot: %v", err)
	}
	if len(legacyWire.Experiences) != len(full.Experiences) {
		t.Fatalf("legacy peer decoded %d experiences, want %d", len(legacyWire.Experiences), len(full.Experiences))
	}
}

func TestWatermarksPersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watermarks.db")
	store, err := Open(path, "local-node")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record(Experience{Description: "persisted"}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetPeerWatermarks("peer-node", map[string]uint64{"remote-node": 42}); err != nil {
		t.Fatal(err)
	}
	wantWatermarks := store.Watermarks()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, "local-node")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Watermarks(); !reflect.DeepEqual(got, wantWatermarks) {
		t.Fatalf("watermarks after restart = %v, want %v", got, wantWatermarks)
	}
	if got := reopened.PeerWatermarks("peer-node"); !reflect.DeepEqual(got, map[string]uint64{"remote-node": 42}) {
		t.Fatalf("peer watermarks after restart = %v", got)
	}
}

func TestSnapshotWatermarksNeverOutrunIncludedRecords(t *testing.T) {
	stores := []struct {
		name  string
		store Store
	}{
		{name: "in-memory", store: NewInMemoryStore()},
	}
	boltStore, err := Open(filepath.Join(t.TempDir(), "snapshot.db"), "snapshot-node")
	if err != nil {
		t.Fatal(err)
	}
	stores = append(stores, struct {
		name  string
		store Store
	}{name: "bbolt", store: boltStore})

	for _, test := range stores {
		t.Run(test.name, func(t *testing.T) {
			defer test.store.Close()
			done := make(chan struct{})
			errs := make(chan error, 1)
			go func() {
				defer close(done)
				for range 250 {
					if _, err := test.store.Record(Experience{Description: "record"}); err != nil {
						errs <- err
						return
					}
				}
			}()
			for {
				snapshot := test.store.Snapshot()
				maxSequence := make(map[string]uint64)
				for _, experience := range snapshot.Experiences {
					if experience.Sequence > maxSequence[experience.Origin] {
						maxSequence[experience.Origin] = experience.Sequence
					}
				}
				for origin, sequence := range snapshot.Watermarks {
					if sequence > maxSequence[origin] {
						t.Fatalf("snapshot watermark for %s is %d, but included records reach only %d",
							origin, sequence, maxSequence[origin])
					}
				}
				select {
				case err := <-errs:
					t.Fatal(err)
				case <-done:
					select {
					case err := <-errs:
						t.Fatal(err)
					default:
					}
					return
				default:
				}
			}
		})
	}
}

func BenchmarkSnapshotPayloadSizes10KExperiences(b *testing.B) {
	store := NewInMemoryStore()
	defer store.Close()
	for i := range 10_000 {
		if _, err := store.Record(Experience{Description: strings.Repeat("x", 80) + string(rune(i))}); err != nil {
			b.Fatal(err)
		}
	}
	full := store.Snapshot()
	origin := full.Experiences[0].Origin
	incremental := store.SnapshotSince(map[string]uint64{origin: 9_990})
	fullPayload, err := json.Marshal(full)
	if err != nil {
		b.Fatal(err)
	}
	incrementalPayload, err := json.Marshal(incremental)
	if err != nil {
		b.Fatal(err)
	}
	if len(incrementalPayload) >= len(fullPayload) {
		b.Fatalf("incremental payload size %d is not smaller than full payload %d", len(incrementalPayload), len(fullPayload))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(full)
		_, _ = json.Marshal(incremental)
	}
	b.ReportMetric(float64(len(fullPayload)), "full-bytes")
	b.ReportMetric(float64(len(incrementalPayload)), "incremental-bytes")
}

func TestConcurrentRecordPersistsPerformanceAggregate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path, "concurrent-test")
	if err != nil {
		t.Fatal(err)
	}

	const writers, recordsPerWriter = 8, 40
	var wg sync.WaitGroup
	errs := make(chan error, writers*recordsPerWriter)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range recordsPerWriter {
				_, err := store.Record(Experience{
					TaskType: "code", NodeID: "node", Model: "model", Success: true,
				})
				if err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, "concurrent-test")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	performances := reopened.Performances()
	if len(performances) != 1 || performances[0].Attempts != writers*recordsPerWriter {
		t.Fatalf("persisted performances = %+v, want %d attempts", performances, writers*recordsPerWriter)
	}
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

func TestFactsScopeUsesExactPrefix(t *testing.T) {
	s := newTestStore(t)
	for _, fact := range []Fact{
		{Scope: "team", Key: "one", Value: "included"},
		{Scope: "team-archive", Key: "one", Value: "excluded"},
	} {
		if _, err := s.PutFact(fact); err != nil {
			t.Fatalf("put fact in scope %q: %v", fact.Scope, err)
		}
	}

	got := s.Facts("team")
	if len(got) != 1 || got[0].Scope != "team" || got[0].Value != "included" {
		t.Fatalf("facts in exact scope = %+v", got)
	}
}

func TestMergeSnapshotDoesNotReplaceNewerFactWithStaleReplica(t *testing.T) {
	s := newTestStore(t)
	newerAt := time.Now()
	if _, err := s.PutFact(Fact{
		Key: "preferred-model", Value: "new-model", Scope: "cluster",
		Source: "node-a", UpdatedAt: newerAt,
	}); err != nil {
		t.Fatalf("put current fact: %v", err)
	}
	if err := s.MergeSnapshot(Snapshot{Facts: []Fact{{
		Key: "preferred-model", Value: "old-model", Scope: "cluster",
		Source: "node-b", UpdatedAt: newerAt.Add(-time.Minute),
	}}}); err != nil {
		t.Fatalf("merge stale snapshot: %v", err)
	}
	fact, err := s.GetFact("cluster", "preferred-model")
	if err != nil {
		t.Fatalf("get merged fact: %v", err)
	}
	if fact.Value != "new-model" {
		t.Fatalf("stale replica replaced newer fact: %+v", fact)
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

func TestAppendDialogueTurnsPersistsTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dialogues.db")
	store, err := Open(path, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	turns := []Turn{
		{Speaker: "user", Content: "question", At: time.Unix(1, 0).UTC()},
		{Speaker: "node", Content: "answer", At: time.Unix(2, 0).UTC()},
	}
	appender, ok := store.(DialogueBatchAppender)
	if !ok {
		t.Fatal("store does not support atomic dialogue appends")
	}
	if err := appender.AppendDialogueTurns("dialogue", []string{"user", "node"}, "topic", turns); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	dialogue, err := reopened.Dialogue("dialogue")
	if err != nil {
		t.Fatal(err)
	}
	if len(dialogue.Turns) != 2 ||
		dialogue.Turns[0].Speaker != "user" || dialogue.Turns[0].Content != "question" ||
		dialogue.Turns[1].Speaker != "node" || dialogue.Turns[1].Content != "answer" {
		t.Fatalf("persisted dialogue turns = %+v", dialogue.Turns)
	}
}

func TestAgentSessionPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.db")
	store, err := Open(path, "test-owner")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	payload := []byte(`{"id":"session-1","harness":"pi","messages":[]}`)
	if err := store.SaveAgentSession("pi", "session-1", payload); err != nil {
		t.Fatalf("save session: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store, err = Open(path, "test-owner")
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()
	sessions, err := store.AgentSessions()
	if err != nil {
		t.Fatalf("load sessions: %v", err)
	}
	if len(sessions) != 1 || string(sessions[0]) != string(payload) {
		t.Fatalf("restored sessions = %q, want %q", sessions, payload)
	}
	if err := store.DeleteAgentSession("pi", "session-1"); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	sessions, err = store.AgentSessions()
	if err != nil {
		t.Fatalf("list sessions after delete: %v", err)
	}
	if len(sessions) != 0 {
		t.Fatalf("sessions after delete = %d, want 0", len(sessions))
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
