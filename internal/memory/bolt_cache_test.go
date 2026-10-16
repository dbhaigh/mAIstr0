package memory

import (
	"path/filepath"
	"testing"
	"time"
)

func TestBoltHotCachesRefreshAfterWritesAndMerges(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "memory.db"), "cache-test")
	if err != nil {
		t.Fatal(err)
	}
	boltStore := store.(*boltStore)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = boltStore.Close()
		}
	})

	if err := boltStore.SetPeerWatermarks("peer", map[string]uint64{"remote": 1}); err != nil {
		t.Fatal(err)
	}
	if got := boltStore.PeerWatermarks("peer")["remote"]; got != 1 {
		t.Fatalf("initial peer watermark = %d, want 1", got)
	}
	if err := boltStore.SetPeerWatermarks("peer", map[string]uint64{"remote": 2, "other": 4}); err != nil {
		t.Fatal(err)
	}
	if got := boltStore.PeerWatermarks("peer"); got["remote"] != 2 || got["other"] != 4 {
		t.Fatalf("peer watermark cache after update = %#v", got)
	}
	peerCopy := boltStore.PeerWatermarks("peer")
	peerCopy["remote"] = 99
	if got := boltStore.PeerWatermarks("peer")["remote"]; got != 2 {
		t.Fatalf("mutating returned peer watermarks changed the cache: %d", got)
	}

	previousWatermarks := boltStore.Watermarks()
	for i := range 2 {
		if _, err := boltStore.Record(Experience{
			TaskType: "code", Description: "successful run",
			NodeID: "node", Model: "model", Success: true,
			CreatedAt: time.Now().Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := boltStore.Watermarks()[boltStore.owner]; got <= previousWatermarks[boltStore.owner] {
		t.Fatalf("local watermark after Record = %d, did not advance past %d", got, previousWatermarks[boltStore.owner])
	}
	if got := boltStore.LearnedBias("node", "model", "code"); got <= 0 {
		t.Fatalf("learned bias after two successful records = %v, want positive", got)
	}
	if experiences := boltStore.Experiences(10); len(experiences) != 2 || experiences[0].ID < experiences[1].ID {
		t.Fatalf("recent experience cache = %#v, want two newest-first records", experiences)
	}

	boltStore.Snapshot()
	if _, err := boltStore.PutFact(Fact{Key: "local", Value: "before merge"}); err != nil {
		t.Fatal(err)
	}
	if got := boltStore.Snapshot(); len(got.Facts) != 1 || got.Facts[0].Key != "local" {
		t.Fatalf("snapshot cache after PutFact = %+v, want local fact", got.Facts)
	}
	beforeMerge := boltStore.Snapshot()
	merged := Snapshot{
		Experiences: []Experience{{
			Origin: "remote", Sequence: 1, TaskType: "code", Description: "merged run",
			NodeID: "node", Model: "model", CreatedAt: time.Now().Add(time.Minute),
		}},
		Facts: []Fact{{
			Key: "merged", Value: "visible", Scope: "cluster",
			Origin: "remote", Sequence: 2, UpdatedAt: time.Now().Add(time.Minute),
		}},
		Performances: []Performance{{
			NodeID: "node", Model: "model", TaskType: "code", Attempts: 10,
			Successes: 1, Failures: 9, SuccessRate: 0.1, AvgDurationMs: 60000,
			Origin: "remote", Sequence: 3,
		}},
		Watermarks: map[string]uint64{"remote": 3},
	}
	if err := boltStore.MergeSnapshotFrom(merged, "peer"); err != nil {
		t.Fatal(err)
	}
	if got := boltStore.LearnedBias("node", "model", "code"); got >= 0 {
		t.Fatalf("learned bias after merged performance = %v, want negative", got)
	}
	if got := boltStore.PeerWatermarks("peer")["remote"]; got != 3 {
		t.Fatalf("peer watermark after merge = %d, want 3", got)
	}
	afterMerge := boltStore.Snapshot()
	if len(afterMerge.Experiences) != len(beforeMerge.Experiences)+1 ||
		len(afterMerge.Facts) != len(beforeMerge.Facts)+1 {
		t.Fatalf("snapshot cache was not invalidated after merge: before=%+v after=%+v", beforeMerge, afterMerge)
	}
	afterMerge.Experiences[0].Description = "caller mutation"
	if got := boltStore.Snapshot().Experiences[0].Description; got != "merged run" {
		t.Fatalf("mutating returned snapshot changed cache: %q", got)
	}
	if len(boltStore.Experiences(10)) != 3 {
		t.Fatalf("recent experience cache after merge = %#v, want three records", boltStore.Experiences(10))
	}

	if err := boltStore.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if got := boltStore.PeerWatermarks("peer"); got["remote"] != 3 {
		t.Fatalf("peer watermark read missed cache after close: %#v", got)
	}
	if got := boltStore.LearnedBias("node", "model", "code"); got >= 0 {
		t.Fatalf("learned bias read missed cache after close: %v", got)
	}
	if got := boltStore.Experiences(10); len(got) != 3 {
		t.Fatalf("recent experience read missed cache after close: %#v", got)
	}
	if got := boltStore.Snapshot(); len(got.Experiences) != 3 || len(got.Facts) != 2 {
		t.Fatalf("snapshot read missed cache after close: %+v", got)
	}
	if got := boltStore.SnapshotSince(map[string]uint64{"remote": 3}); len(got.Experiences) != 2 {
		t.Fatalf("incremental snapshot cache result = %+v, want two local records", got)
	}
}
