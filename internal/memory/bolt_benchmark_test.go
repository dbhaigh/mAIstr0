package memory

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func BenchmarkBboltExperienceRecord(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})

	experience := Experience{
		Kind: "subtask", TaskType: "general", Description: "bbolt record benchmark",
		Prompt: "simulated prompt", Output: "simulated response", Model: "simulated-general",
		NodeID: "simulated-node", Success: true, CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := store.Record(experience); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBboltAgentSessionSave(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	payload := []byte(`{"id":"benchmark-session","harness":"pi","messages":[]}`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := store.SaveAgentSession("pi", "benchmark-session", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBboltAgentSessionSaveParallel(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	payload := []byte(`{"id":"benchmark-session","harness":"pi","messages":[]}`)
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := "session-" + strconv.FormatUint(sequence.Add(1), 10)
			if err := store.SaveAgentSession("pi", id, payload); err != nil {
				b.Errorf("save benchmark agent session: %v", err)
				return
			}
		}
	})
}

func BenchmarkBboltDialogueAppend(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	turn := Turn{
		Speaker: "node:benchmark", Content: "simulated dialogue turn",
		At: time.Unix(1_700_000_000, 0).UTC(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := "dialogue-" + strconv.Itoa(i)
		if err := store.AppendDialogueTurn(id, []string{"node:benchmark"}, "benchmark", turn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBboltDialogueAppendParallel(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	turn := Turn{
		Speaker: "node:benchmark", Content: "simulated dialogue turn",
		At: time.Unix(1_700_000_000, 0).UTC(),
	}
	var sequence atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := "dialogue-" + strconv.FormatUint(sequence.Add(1), 10)
			if err := store.AppendDialogueTurn(id, []string{"node:benchmark"}, "benchmark", turn); err != nil {
				b.Errorf("append benchmark dialogue turn: %v", err)
				return
			}
		}
	})
}

func BenchmarkBboltDialogueTurnPair(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	turns := []Turn{
		{
			Speaker: "user", Content: "simulated question",
			At: time.Unix(1_700_000_000, 0).UTC(),
		},
		{
			Speaker: "node:benchmark", Content: "simulated answer",
			At: time.Unix(1_700_000_001, 0).UTC(),
		},
	}
	batchAppender := store.(DialogueBatchAppender)
	b.Run("SeparateTransactions", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			id := "dialogue-" + strconv.Itoa(i)
			for _, turn := range turns {
				if err := store.AppendDialogueTurn(id, []string{"node:benchmark"}, "benchmark", turn); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("OneTransaction", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			id := "dialogue-" + strconv.Itoa(i)
			if err := batchAppender.AppendDialogueTurns(id, []string{"node:benchmark"}, "benchmark", turns); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBboltFactsScoped10K(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	boltStore := store.(*boltStore)
	const (
		factCount   = 10_000
		scopeCount  = 100
		targetCount = factCount / scopeCount
	)
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	if err := boltStore.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketFacts)
		for i := range factCount {
			fact := Fact{
				Key: fmt.Sprintf("fact-%05d", i), Scope: fmt.Sprintf("scope-%02d", i%scopeCount),
				Value: "benchmark fact", CreatedAt: createdAt, UpdatedAt: createdAt,
			}
			encoded, err := json.Marshal(fact)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(fact.Scope+"|"+fact.Key), encoded); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.Run("ScopeSeek", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(targetCount), "facts/op")
		b.ResetTimer()
		for range b.N {
			facts := store.Facts("scope-42")
			if len(facts) != targetCount {
				b.Fatalf("scoped facts = %d, want %d", len(facts), targetCount)
			}
		}
	})
	b.Run("FullScanReference", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(targetCount), "facts/op")
		b.ResetTimer()
		for range b.N {
			count := 0
			if err := boltStore.db.View(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketFacts).ForEach(func(_, value []byte) error {
					var fact Fact
					if err := json.Unmarshal(value, &fact); err != nil {
						return err
					}
					if fact.Scope == "scope-42" {
						count++
					}
					return nil
				})
			}); err != nil {
				b.Fatal(err)
			}
			if count != targetCount {
				b.Fatalf("full-scan facts = %d, want %d", count, targetCount)
			}
		}
	})
}

func BenchmarkBboltDialogues10KRecent(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "memory.db"), "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	boltStore := store.(*boltStore)
	const dialogueCount = 10_000
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	content := strings.Repeat("dialogue transcript payload ", 8)
	if err := boltStore.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketDialogues)
		for i := range dialogueCount {
			updatedAt := createdAt.Add(time.Duration(i) * time.Second)
			dialogue := DialogueRecord{
				ID: fmt.Sprintf("dialogue-%05d", i), Participants: []string{"node"},
				Topic: "benchmark", Turns: []Turn{{
					Speaker: "node", Content: content, At: updatedAt,
				}},
				CreatedAt: createdAt, UpdatedAt: updatedAt,
			}
			encoded, err := json.Marshal(dialogue)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(dialogue.ID), encoded); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.Run("MetadataIndex", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(50, "dialogues/op")
		b.ResetTimer()
		for range b.N {
			dialogues := store.Dialogues(50)
			if len(dialogues) != 50 {
				b.Fatalf("recent dialogues = %d, want 50", len(dialogues))
			}
		}
	})
	b.Run("FullDecodeReference", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(50, "dialogues/op")
		b.ResetTimer()
		for range b.N {
			dialogues := make([]DialogueRecord, 0)
			if err := boltStore.db.View(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketDialogues).ForEach(func(_, value []byte) error {
					var dialogue DialogueRecord
					if err := json.Unmarshal(value, &dialogue); err != nil {
						return err
					}
					dialogues = append(dialogues, dialogue)
					return nil
				})
			}); err != nil {
				b.Fatal(err)
			}
			sort.Slice(dialogues, func(i, j int) bool {
				return dialogues[i].UpdatedAt.After(dialogues[j].UpdatedAt)
			})
			if len(dialogues) > 50 {
				dialogues = dialogues[:50]
			}
			if len(dialogues) != 50 {
				b.Fatalf("reference recent dialogues = %d, want 50", len(dialogues))
			}
		}
	})
}
