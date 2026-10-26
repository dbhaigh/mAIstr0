package memory

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/testutil"
)

func BenchmarkProfileMemoryOperations(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "profile.db"), "synthetic")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	snapshot := benchmarkHistorySnapshot(256)
	latencies := &testutil.Latencies{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		start := time.Now()
		if err := store.MergeSnapshotFrom(snapshot, "synthetic"); err != nil {
			b.Fatal(err)
		}
		if _, err := store.PutFact(Fact{Key: "synthetic", Value: "updated"}); err != nil {
			b.Fatal(err)
		}
		if _, err := store.GetFact("", "synthetic"); err != nil {
			b.Fatal(err)
		}
		if got := store.SnapshotSince(map[string]uint64{"benchmark": 250}); len(got.Experiences) != 6 {
			b.Fatalf("incremental snapshot: %d experiences", len(got.Experiences))
		}
		latencies.Record(time.Since(start))
	}
	latencies.Report(b, 1)
}

func BenchmarkProfileSyncPage(b *testing.B) {
	for _, total := range []int{10000, 100000} {
		for _, consumed := range []int{0, 90, 99, 100} {
			b.Run(strconv.Itoa(total)+"/"+strconv.Itoa(consumed), func(b *testing.B) {
				snapshot := benchmarkHistorySnapshot(total)
				cursor := map[string]uint64{"benchmark": uint64(total * consumed / 100)}
				latencies := &testutil.Latencies{}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					start := time.Now()
					page, _, _, err := PageSnapshot(snapshot, cursor, snapshot.Watermarks, 1<<20, 0)
					if err != nil {
						b.Fatal(err)
					}
					body, err := json.Marshal(page)
					if err != nil || len(body) > 1<<20 {
						b.Fatalf("invalid sync page: %d bytes, %v", len(body), err)
					}
					latencies.Record(time.Since(start))
				}
				latencies.Report(b, 1)
			})
		}
	}
}
