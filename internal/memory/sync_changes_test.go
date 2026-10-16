package memory

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/syncinterval"
)

func TestChangeSignalWakesAfterLocalWriteAndPeerMerge(t *testing.T) {
	boltStore, err := Open(filepath.Join(t.TempDir(), "memory.db"), "node")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := boltStore.Close(); err != nil {
			t.Errorf("close bbolt store: %v", err)
		}
	})

	stores := map[string]Store{
		"in-memory": NewInMemoryStore(),
		"bbolt":     boltStore,
	}
	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			source, ok := store.(ChangeSource)
			if !ok {
				t.Fatal("store does not expose a change signal")
			}
			changes := source.Changes()
			if _, err := store.Record(Experience{Description: "local write"}); err != nil {
				t.Fatal(err)
			}
			if !syncinterval.Wait(time.Second, changes) {
				t.Fatal("local write did not wake the sync wait")
			}

			changes = source.Changes()
			if err := store.MergeSnapshotFrom(Snapshot{
				Experiences: []Experience{{Origin: "peer", Sequence: 1, Description: "peer write"}},
				Watermarks:  map[string]uint64{"peer": 1},
			}, "peer"); err != nil {
				t.Fatal(err)
			}
			if !syncinterval.Wait(time.Second, changes) {
				t.Fatal("peer merge did not wake the sync wait")
			}
		})
	}
}
