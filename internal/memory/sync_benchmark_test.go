package memory

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
)

func benchmarkHistorySnapshot(count int) Snapshot {
	const origin = "benchmark"
	snapshot := Snapshot{
		Experiences: make([]Experience, count),
		SyncVersion: SyncProtocolVersion,
		Watermarks:  map[string]uint64{origin: uint64(count)},
	}
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	for i := range snapshot.Experiences {
		snapshot.Experiences[i] = Experience{
			ID: uint64(i + 1), Origin: origin, Sequence: uint64(i + 1),
			Kind: "result", Description: strings.Repeat("x", 80) + strconv.Itoa(i), CreatedAt: createdAt,
		}
	}
	return snapshot
}

func BenchmarkMemoryHistorySnapshotGeneration(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		count int
	}{{"10K", 10_000}, {"100K", 100_000}} {
		b.Run(fixture.name, func(b *testing.B) {
			snapshot := benchmarkHistorySnapshot(fixture.count)
			store := NewInMemoryStore()
			if err := store.MergeSnapshotFrom(snapshot, "benchmark"); err != nil {
				b.Fatal(err)
			}
			defer closeutil.Discard(store)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = store.Snapshot()
			}
		})
	}
}

func BenchmarkBboltSnapshotScanSort(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "snapshot.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})

	const (
		experienceCount = 10_000
		otherCount      = 1_000
	)
	snapshot := benchmarkHistorySnapshot(experienceCount)
	at := time.Unix(1_700_000_000, 0).UTC()
	snapshot.Facts = make([]Fact, otherCount)
	snapshot.Performances = make([]Performance, otherCount)
	snapshot.Dialogues = make([]DialogueRecord, otherCount)
	for i := range otherCount {
		sequence := uint64(experienceCount + i + 1)
		updatedAt := at.Add(time.Duration(i) * time.Second)
		snapshot.Facts[i] = Fact{
			Key: strconv.Itoa(i), Origin: "benchmark", Sequence: sequence,
			Value: "synthetic fact", Scope: "benchmark", CreatedAt: at, UpdatedAt: updatedAt,
		}
		snapshot.Performances[i] = Performance{
			NodeID: strconv.Itoa(i), Origin: "benchmark", Sequence: sequence,
			Model: "synthetic-model", TaskType: "general", AvgDurationMs: int64(otherCount - i),
			SuccessRate: float64(i) / otherCount, LastUsed: updatedAt,
		}
		snapshot.Dialogues[i] = DialogueRecord{
			ID: strconv.Itoa(i), Origin: "benchmark", Sequence: sequence,
			Participants: []string{"benchmark"}, Topic: "synthetic",
			Turns:     []Turn{{Speaker: "benchmark", Content: "synthetic dialogue", At: updatedAt}},
			CreatedAt: at, UpdatedAt: updatedAt,
		}
	}
	snapshot.Watermarks["benchmark"] = uint64(experienceCount + otherCount)
	if err := store.MergeSnapshotFrom(snapshot, "benchmark"); err != nil {
		b.Fatal(err)
	}
	boltStore := store.(*boltStore)

	b.ReportAllocs()
	b.ReportMetric(experienceCount+3*otherCount, "records/op")
	b.ResetTimer()
	for range b.N {
		boltStore.invalidateSnapshotCache()
		got, err := boltStore.loadSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		if len(got.Experiences) != experienceCount || len(got.Facts) != otherCount ||
			len(got.Performances) != otherCount || len(got.Dialogues) != otherCount {
			b.Fatalf("snapshot record counts = %d/%d/%d/%d",
				len(got.Experiences), len(got.Facts), len(got.Performances), len(got.Dialogues))
		}
	}
}

func BenchmarkMemoryHistoryMerge(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		count int
	}{{"10K", 10_000}, {"100K", 100_000}} {
		b.Run(fixture.name, func(b *testing.B) {
			snapshot := benchmarkHistorySnapshot(fixture.count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				store := NewInMemoryStore()
				if err := store.MergeSnapshotFrom(snapshot, "benchmark"); err != nil {
					b.Fatal(err)
				}
				if err := store.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMemoryHistoryPageTransfer(b *testing.B) {
	for _, fixture := range []struct {
		name  string
		count int
	}{{"10K", 10_000}, {"100K", 100_000}} {
		b.Run(fixture.name, func(b *testing.B) {
			snapshot := benchmarkHistorySnapshot(fixture.count)
			snapshotBody, err := json.Marshal(snapshot)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cursor := map[string]uint64{}
				for {
					page, next, more, err := PageSnapshot(snapshot, cursor, snapshot.Watermarks, 1<<20, 0)
					if err != nil {
						b.Fatal(err)
					}
					body, err := json.Marshal(page)
					if err != nil {
						b.Fatal(err)
					}
					if len(body) > 1<<20 {
						b.Fatalf("page size = %d bytes, exceeds 1 MiB", len(body))
					}
					if !more {
						break
					}
					cursor = next
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(fixture.count), "records/op")
			b.ReportMetric(float64(len(snapshotBody)), "snapshot-bytes")
		})
	}
}
