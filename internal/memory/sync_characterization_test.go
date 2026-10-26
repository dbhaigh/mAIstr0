package memory

import (
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	bolt "go.etcd.io/bbolt"
)

type syncStoreFactory struct {
	name string
	open func(t *testing.T, owner string) Store
}

func syncStoreFactories() []syncStoreFactory {
	return []syncStoreFactory{
		{name: "in-memory", open: func(*testing.T, string) Store { return NewInMemoryStore() }},
		{name: "bbolt", open: func(t *testing.T, owner string) Store {
			t.Helper()
			return openCharacterizationBolt(t, filepath.Join(t.TempDir(), owner+".db"), owner)
		}},
	}
}

func openCharacterizationBolt(t *testing.T, path, owner string) Store {
	t.Helper()
	store, err := Open(path, owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeutil.Discard(store) })
	return store
}

var characterizationBase = time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)

// characterizationBatch is a peer batch containing every replicated record
// kind from one origin with contiguous sequences 1..8.
func characterizationBatch(origin string) Snapshot {
	batch := Snapshot{SyncVersion: SyncProtocolVersion, Watermarks: map[string]uint64{origin: 8}}
	for index := range 4 {
		batch.Experiences = append(batch.Experiences, Experience{
			Origin: origin, Sequence: uint64(index + 1), Kind: "task", NodeID: "worker", Model: "model",
			Description: fmt.Sprintf("experience %d %s", index, strings.Repeat("d", 120)),
			Success:     index%2 == 0, CreatedAt: characterizationBase.Add(time.Duration(index) * time.Second),
		})
	}
	for index := range 2 {
		batch.Facts = append(batch.Facts, Fact{
			Key: fmt.Sprintf("fact-%d", index), Value: "current", Scope: "cluster", Source: "source",
			Origin: origin, Sequence: uint64(index + 5), CreatedAt: characterizationBase,
			UpdatedAt: characterizationBase.Add(time.Duration(index+10) * time.Second),
		})
	}
	batch.Performances = []Performance{{
		NodeID: "worker", Model: "model", TaskType: "general", Attempts: 5, Successes: 4, Failures: 1,
		SuccessRate: 0.8, Origin: origin, Sequence: 7, LastUsed: characterizationBase,
	}}
	batch.Dialogues = []DialogueRecord{{
		ID: "dialogue", Participants: []string{"worker"}, Topic: "topic",
		Turns:  []Turn{{Speaker: "worker", Content: "hello", At: characterizationBase}},
		Origin: origin, Sequence: 8, CreatedAt: characterizationBase, UpdatedAt: characterizationBase,
	}}
	return batch
}

type syncState struct {
	RecordIDs  []string
	Counts     [4]int
	FactValues map[string]string
	Attempts   map[string]int
	Watermarks map[string]uint64
	Peer       map[string]uint64
}

func captureSyncState(t *testing.T, store Store, peer string) syncState {
	t.Helper()
	snapshot := store.Snapshot()
	ids, err := RecordIDs(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	state := syncState{
		RecordIDs:  ids,
		Counts:     [4]int{len(snapshot.Experiences), len(snapshot.Facts), len(snapshot.Performances), len(snapshot.Dialogues)},
		FactValues: make(map[string]string),
		Attempts:   make(map[string]int),
		Watermarks: store.Watermarks(),
		Peer:       store.PeerWatermarks(peer),
	}
	for _, fact := range snapshot.Facts {
		state.FactValues[fact.Scope+"|"+fact.Key] = fact.Value
	}
	for _, performance := range snapshot.Performances {
		state.Attempts[performance.NodeID+"|"+performance.Model+"|"+performance.TaskType] = performance.Attempts
	}
	return state
}

func TestSyncCharacterizationReplayedBatchIsIdempotent(t *testing.T) {
	for _, factory := range syncStoreFactories() {
		for _, test := range []struct {
			name         string
			replaySource string
		}{
			{name: "same peer replays batch", replaySource: "peer-a"},
			{name: "second peer relays batch", replaySource: "peer-b"},
		} {
			t.Run(factory.name+"/"+test.name, func(t *testing.T) {
				store := factory.open(t, "local")
				batch := characterizationBatch("origin-a")
				if err := store.MergeSnapshotFrom(batch, "peer-a"); err != nil {
					t.Fatal(err)
				}
				before := captureSyncState(t, store, "peer-a")
				beforeSnapshot := store.Snapshot()
				if before.Counts != [4]int{4, 2, 1, 1} || before.Watermarks["origin-a"] != 8 {
					t.Fatalf("first apply state = %+v", before)
				}

				if err := store.MergeSnapshotFrom(batch, test.replaySource); err != nil {
					t.Fatal(err)
				}
				if after := captureSyncState(t, store, "peer-a"); !reflect.DeepEqual(after, before) {
					t.Fatalf("replay changed state:\nbefore %+v\nafter  %+v", before, after)
				}
				if after := store.Snapshot(); !reflect.DeepEqual(after, beforeSnapshot) {
					t.Fatalf("replay changed snapshot:\nbefore %+v\nafter  %+v", beforeSnapshot, after)
				}
				if got := store.PeerWatermarks(test.replaySource); got["origin-a"] != 8 {
					t.Fatalf("replay source %s watermarks = %v, want origin-a=8", test.replaySource, got)
				}
			})
		}
	}
}

func TestSyncCharacterizationReorderedPagesConverge(t *testing.T) {
	batch := characterizationBatch("origin-a")
	pages, err := SplitSnapshot(batch, 900, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) < 3 {
		t.Fatalf("batch split into %d pages, want at least 3 for reordering", len(pages))
	}
	stale := Snapshot{
		Facts: []Fact{{
			Key: "fact-0", Value: "stale", Scope: "cluster", Source: "source",
			Origin: "origin-a", Sequence: 2, CreatedAt: characterizationBase, UpdatedAt: characterizationBase,
		}},
		Performances: []Performance{{
			NodeID: "worker", Model: "model", TaskType: "general", Attempts: 2, Successes: 1, Failures: 1,
			SuccessRate: 0.5, Origin: "origin-a", Sequence: 3, LastUsed: characterizationBase,
		}},
	}
	updates := append(slices.Clone(pages), stale)
	last := len(updates) - 1
	orders := []struct {
		name  string
		order []int
	}{
		{name: "forward", order: indexes(len(updates))},
		{name: "reverse", order: reversed(indexes(len(updates)))},
		{name: "stale first then interleaved", order: append([]int{last}, interleaved(len(pages))...)},
		{name: "every update twice", order: doubled(indexes(len(updates)))},
	}

	for _, factory := range syncStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			var want syncState
			for index, order := range orders {
				t.Run(order.name, func(t *testing.T) {
					store := factory.open(t, "local")
					for _, update := range order.order {
						if err := store.MergeSnapshotFrom(updates[update], "peer-a"); err != nil {
							t.Fatal(err)
						}
					}
					got := captureSyncState(t, store, "peer-a")
					if got.FactValues["cluster|fact-0"] != "current" || got.Attempts["worker|model|general"] != 5 {
						t.Fatalf("stale update won: facts %v attempts %v", got.FactValues, got.Attempts)
					}
					if got.Peer["origin-a"] != 8 || got.Watermarks["origin-a"] != 8 {
						t.Fatalf("watermarks after reordered apply = local %v peer %v", got.Watermarks, got.Peer)
					}
					if index == 0 {
						want = got
						return
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("order %v diverged:\nwant %+v\ngot  %+v", order.order, want, got)
					}
				})
			}
		})
	}
}

func indexes(n int) []int {
	order := make([]int, n)
	for index := range order {
		order[index] = index
	}
	return order
}

func reversed(order []int) []int {
	order = slices.Clone(order)
	slices.Reverse(order)
	return order
}

func interleaved(n int) []int {
	order := make([]int, 0, n)
	for index := 1; index < n; index += 2 {
		order = append(order, index)
	}
	for index := 0; index < n; index += 2 {
		order = append(order, index)
	}
	return order
}

func doubled(order []int) []int {
	out := make([]int, 0, len(order)*2)
	for _, index := range order {
		out = append(out, index, index)
	}
	return out
}

func gapHolder(t *testing.T) Store {
	t.Helper()
	holder := NewInMemoryStore()
	batch := Snapshot{Watermarks: map[string]uint64{"origin-a": 3}}
	for sequence := uint64(1); sequence <= 3; sequence++ {
		batch.Experiences = append(batch.Experiences, Experience{
			Origin: "origin-a", Sequence: sequence, Description: fmt.Sprintf("sequence %d", sequence),
			CreatedAt: characterizationBase.Add(time.Duration(sequence) * time.Second),
		})
	}
	if err := holder.MergeSnapshotFrom(batch, "origin-a"); err != nil {
		t.Fatal(err)
	}
	return holder
}

// A received batch missing an intermediate sequence advances the local
// per-origin watermark to the highest sequence seen; only the advertised
// snapshot watermarks advance the per-peer resume point. Whether the gap is
// recovered incrementally therefore depends on what the sender advertised.
func TestSyncCharacterizationSequenceGapRecovery(t *testing.T) {
	for _, factory := range syncStoreFactories() {
		for _, test := range []struct {
			name             string
			advertised       map[string]uint64
			wantPeerAfterGap uint64
			wantIncremental  int
			needsFull        bool
		}{
			{name: "gap without advertised watermarks", advertised: nil, wantPeerAfterGap: 0, wantIncremental: 3},
			{name: "gap with advertised watermark past it", advertised: map[string]uint64{"origin-a": 3}, wantPeerAfterGap: 3, wantIncremental: 0, needsFull: true},
		} {
			t.Run(factory.name+"/"+test.name, func(t *testing.T) {
				holder := gapHolder(t)
				full := holder.Snapshot()
				store := factory.open(t, "local")
				gapped := Snapshot{Watermarks: test.advertised}
				for _, experience := range full.Experiences {
					if experience.Sequence != 2 {
						gapped.Experiences = append(gapped.Experiences, experience)
					}
				}
				if err := store.MergeSnapshotFrom(gapped, "holder"); err != nil {
					t.Fatal(err)
				}
				if got := store.Watermarks()["origin-a"]; got != 3 {
					t.Fatalf("local watermark after gap = %d, want highest seen sequence 3", got)
				}
				if got := store.PeerWatermarks("holder")["origin-a"]; got != test.wantPeerAfterGap {
					t.Fatalf("peer resume point after gap = %d, want %d", got, test.wantPeerAfterGap)
				}

				next := SnapshotForPeer(holder, SyncProtocolVersion, store.PeerWatermarks("holder"))
				if len(next.Experiences) != test.wantIncremental {
					t.Fatalf("next incremental sync carried %d experiences, want %d", len(next.Experiences), test.wantIncremental)
				}
				if err := store.MergeSnapshotFrom(next, "holder"); err != nil {
					t.Fatal(err)
				}
				recovered := len(store.Snapshot().Experiences) == 3
				if recovered == test.needsFull {
					t.Fatalf("incremental recovery = %t, want %t", recovered, !test.needsFull)
				}
				if test.needsFull {
					if err := store.MergeSnapshotFrom(SnapshotForPeer(holder, 0, nil), "holder"); err != nil {
						t.Fatal(err)
					}
				}
				if got := sortedIDs(t, store.Snapshot()); !reflect.DeepEqual(got, sortedIDs(t, full)) {
					t.Fatalf("store did not converge after recovery: %v", got)
				}
				if got := store.PeerWatermarks("holder")["origin-a"]; got != 3 {
					t.Fatalf("peer resume point after recovery = %d, want 3", got)
				}
			})
		}
	}
}

func sortedIDs(t *testing.T, snapshot Snapshot) []string {
	t.Helper()
	ids, err := RecordIDs(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	return ids
}

// Only the final page carries watermarks, so a consumer interrupted after
// merging a partial page keeps its old per-peer resume point and retries
// from there.
func TestSyncCharacterizationPartialPageKeepsPeerResumePoint(t *testing.T) {
	for _, factory := range syncStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			holder := factory.open(t, "holder")
			if err := holder.MergeSnapshotFrom(characterizationBatch("origin-a"), "origin-a"); err != nil {
				t.Fatal(err)
			}
			store := factory.open(t, "local")
			outgoing := SnapshotForPeer(holder, SyncProtocolVersion, store.PeerWatermarks("holder"))
			first, _, more, err := PageSnapshot(outgoing, map[string]uint64{}, outgoing.Watermarks, 900, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !more || first.Watermarks != nil {
				t.Fatalf("first page more=%t watermarks=%v, want a non-final page without watermarks", more, first.Watermarks)
			}
			if err := store.MergeSnapshotFrom(first, "holder"); err != nil {
				t.Fatal(err)
			}
			if got := store.PeerWatermarks("holder"); len(got) != 0 {
				t.Fatalf("partial page advanced peer resume point to %v", got)
			}

			retry := SnapshotForPeer(holder, SyncProtocolVersion, store.PeerWatermarks("holder"))
			if err := store.MergeSnapshotFrom(retry, "holder"); err != nil {
				t.Fatal(err)
			}
			if got, want := sortedIDs(t, store.Snapshot()), sortedIDs(t, holder.Snapshot()); !reflect.DeepEqual(got, want) {
				t.Fatalf("retry after partial page = %v, want %v", got, want)
			}
			if got, want := FilterMemoryWatermarks(store.PeerWatermarks("holder")), FilterMemoryWatermarks(holder.Watermarks()); !reflect.DeepEqual(got, want) {
				t.Fatalf("peer resume point after retry = %v, want %v", got, want)
			}
		})
	}
}

func TestSyncCharacterizationResumePointSurvivesReopen(t *testing.T) {
	holder := NewInMemoryStore()
	for index := range 3 {
		if _, err := holder.Record(Experience{Description: fmt.Sprintf("before restart %d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "resume.db")
	store, err := Open(path, "local")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MergeSnapshotFrom(SnapshotForPeer(holder, SyncProtocolVersion, store.PeerWatermarks("holder")), "holder"); err != nil {
		t.Fatal(err)
	}
	wantPeer := store.PeerWatermarks("holder")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openCharacterizationBolt(t, path, "local")
	if got := reopened.PeerWatermarks("holder"); !reflect.DeepEqual(got, wantPeer) {
		t.Fatalf("peer resume point after reopen = %v, want %v", got, wantPeer)
	}
	for index := range 2 {
		if _, err := holder.Record(Experience{Description: fmt.Sprintf("after restart %d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	next := SnapshotForPeer(holder, SyncProtocolVersion, reopened.PeerWatermarks("holder"))
	if len(next.Experiences) != 2 {
		t.Fatalf("resumed sync carried %d experiences, want only the 2 new records", len(next.Experiences))
	}
	if err := reopened.MergeSnapshotFrom(next, "holder"); err != nil {
		t.Fatal(err)
	}
	if got, want := sortedIDs(t, reopened.Snapshot()), sortedIDs(t, holder.Snapshot()); !reflect.DeepEqual(got, want) {
		t.Fatalf("resumed store = %v, want %v", got, want)
	}
}

// A wiped bbolt peer keeps its owner origin but restarts its sequences. While
// it is behind the receiver's resume point, every sync falls back to a full
// snapshot; once it has overtaken that point before the next sync, records
// whose new sequences are at or below the old watermark are not sent
// incrementally and only a full snapshot recovers them.
func TestSyncCharacterizationWipedPeerSequenceRegression(t *testing.T) {
	for _, factory := range syncStoreFactories() {
		for _, test := range []struct {
			name            string
			recordsAfter    int
			wantIncremental int
			needsFull       bool
		}{
			{name: "wiped peer behind old resume point", recordsAfter: 2, wantIncremental: 2},
			{name: "wiped peer overtook old resume point", recordsAfter: 6, wantIncremental: 1, needsFull: true},
		} {
			t.Run(factory.name+"/"+test.name, func(t *testing.T) {
				dir := t.TempDir()
				before, err := Open(filepath.Join(dir, "before.db"), "holder")
				if err != nil {
					t.Fatal(err)
				}
				for index := range 5 {
					if _, err := before.Record(Experience{Description: fmt.Sprintf("before wipe %d", index)}); err != nil {
						t.Fatal(err)
					}
				}
				store := factory.open(t, "local")
				if err := store.MergeSnapshotFrom(SnapshotForPeer(before, SyncProtocolVersion, nil), "holder"); err != nil {
					t.Fatal(err)
				}
				if err := before.Close(); err != nil {
					t.Fatal(err)
				}
				if got := store.PeerWatermarks("holder")["holder"]; got != 5 {
					t.Fatalf("resume point before wipe = %d, want 5", got)
				}

				wiped := openCharacterizationBolt(t, filepath.Join(dir, "wiped.db"), "holder")
				for index := range test.recordsAfter {
					if _, err := wiped.Record(Experience{Description: fmt.Sprintf("after wipe %d", index)}); err != nil {
						t.Fatal(err)
					}
				}
				next := SnapshotForPeer(wiped, SyncProtocolVersion, store.PeerWatermarks("holder"))
				if len(next.Experiences) != test.wantIncremental {
					t.Fatalf("sync after wipe carried %d experiences, want %d", len(next.Experiences), test.wantIncremental)
				}
				if err := store.MergeSnapshotFrom(next, "holder"); err != nil {
					t.Fatal(err)
				}
				want := 5 + test.recordsAfter
				if got := len(store.Snapshot().Experiences); (got == want) == test.needsFull {
					t.Fatalf("experiences after incremental sync = %d, want converged=%t", got, !test.needsFull)
				}
				if test.needsFull {
					if err := store.MergeSnapshotFrom(SnapshotForPeer(wiped, 0, nil), "holder"); err != nil {
						t.Fatal(err)
					}
				}
				if got := len(store.Snapshot().Experiences); got != want {
					t.Fatalf("experiences after recovery = %d, want %d", got, want)
				}
				if got := store.PeerWatermarks("holder")["holder"]; got != uint64(max(5, test.recordsAfter)) {
					t.Fatalf("resume point after recovery = %d, want %d", got, max(5, test.recordsAfter))
				}
			})
		}
	}
}

// LoadSnapshotForPeer must select exactly what SnapshotForPeer selects when
// the store is readable.
func TestLoadSnapshotForPeerMatchesSnapshotForPeer(t *testing.T) {
	for _, factory := range syncStoreFactories() {
		t.Run(factory.name, func(t *testing.T) {
			store := factory.open(t, "local")
			if err := store.MergeSnapshotFrom(characterizationBatch("origin-a"), "peer-a"); err != nil {
				t.Fatal(err)
			}
			for _, version := range []int{0, FirstSyncProtocolVersion, SyncPageProtocolVersion, SyncProtocolVersion, SyncProtocolVersion + 1} {
				for _, watermarks := range []map[string]uint64{nil, {"origin-a": 4}, {"origin-a": 99}} {
					want := SnapshotForPeer(store, version, watermarks)
					got, err := LoadSnapshotForPeer(store, version, watermarks)
					if err != nil {
						t.Fatalf("version %d watermarks %v: %v", version, watermarks, err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("version %d watermarks %v:\nload     %+v\nsnapshot %+v", version, watermarks, got, want)
					}
				}
			}
		})
	}
}

// Regression for F8: bbolt Snapshot hides read and decode failures behind an
// empty snapshot. The public Snapshot and SnapshotForPeer keep that behavior;
// LoadSnapshotForPeer, used by every sync path, must report the failure.
func TestLoadSnapshotForPeerReportsUnreadableBoltStore(t *testing.T) {
	corrupt := func(bucket []byte, key, value []byte) func(*testing.T, *boltStore) {
		return func(t *testing.T, store *boltStore) {
			t.Helper()
			if err := store.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucket).Put(key, value)
			}); err != nil {
				t.Fatal(err)
			}
			store.invalidateSnapshotCache()
		}
	}
	for _, test := range []struct {
		name       string
		breakStore func(*testing.T, *boltStore)
	}{
		{name: "corrupt experience", breakStore: corrupt(bucketExperiences, itob(1<<40), []byte("{not json"))},
		{name: "corrupt fact", breakStore: corrupt(bucketFacts, []byte("cluster|corrupt"), []byte("{"))},
		{name: "corrupt performance", breakStore: corrupt(bucketPerf, []byte("corrupt"), []byte("{"))},
		{name: "corrupt dialogue", breakStore: corrupt(bucketDialogues, []byte("corrupt"), []byte("{"))},
		{name: "corrupt watermark", breakStore: corrupt(bucketMeta, []byte("origin-watermark:corrupt"), []byte{1, 2, 3})},
		{name: "closed database", breakStore: func(t *testing.T, store *boltStore) {
			t.Helper()
			store.invalidateSnapshotCache()
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openCharacterizationBolt(t, filepath.Join(t.TempDir(), "broken.db"), "local")
			if err := store.MergeSnapshotFrom(characterizationBatch("origin-a"), "peer-a"); err != nil {
				t.Fatal(err)
			}
			if got := len(store.Snapshot().Experiences); got != 4 {
				t.Fatalf("healthy snapshot experiences = %d, want 4", got)
			}
			test.breakStore(t, store.(*boltStore))

			if got := store.Snapshot(); memoryRecordCount(got) != 0 || got.SyncVersion != SyncProtocolVersion || got.Watermarks != nil {
				t.Fatalf("public Snapshot on unreadable store = %+v, want the legacy empty snapshot", got)
			}
			if got := SnapshotForPeer(store, SyncProtocolVersion, nil); memoryRecordCount(got) != 0 {
				t.Fatalf("SnapshotForPeer on unreadable store returned %d records", memoryRecordCount(got))
			}
			for _, request := range []struct {
				version    int
				watermarks map[string]uint64
			}{
				{version: 0},
				{version: SyncProtocolVersion, watermarks: map[string]uint64{"origin-a": 2}},
				{version: SyncProtocolVersion, watermarks: map[string]uint64{"origin-a": 99}},
			} {
				got, err := LoadSnapshotForPeer(store, request.version, request.watermarks)
				if err == nil || !strings.Contains(err.Error(), "memory: create snapshot") {
					t.Fatalf("LoadSnapshotForPeer(version %d, %v) error = %v, want snapshot read failure", request.version, request.watermarks, err)
				}
				if !reflect.DeepEqual(got, Snapshot{}) {
					t.Fatalf("LoadSnapshotForPeer returned data with its error: %+v", got)
				}
			}
		})
	}
}

func memoryRecordCount(snapshot Snapshot) int {
	return len(snapshot.Experiences) + len(snapshot.Facts) + len(snapshot.Performances) + len(snapshot.Dialogues)
}
