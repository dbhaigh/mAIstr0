package memory

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInMemoryExperiencePagesCoverResultsAndEmptyPages(t *testing.T) {
	store := NewInMemoryStore().(*memoryStore)
	for i := 0; i < 3; i++ {
		if _, err := store.Record(Experience{
			Kind: "task", Description: "websocket handler", Success: true,
			CreatedAt: time.Date(2025, time.January, 1, 0, 0, i, 0, time.UTC),
		}); err != nil {
			t.Fatal(err)
		}
	}

	first, err := store.ExperiencesPage(SearchFilter{Query: "websocket"}, PageOptions{Limit: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || !first.HasMore || first.NextCursor.ID == 0 {
		t.Fatalf("unexpected first page: %+v", first)
	}
	last, err := store.ExperiencesPage(SearchFilter{Query: "websocket"}, PageOptions{
		Cursor: first.NextCursor, Limit: 2, MaxBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(last.Items) != 1 || last.HasMore || last.Items[0].ID == first.Items[0].ID || last.Items[0].ID == first.Items[1].ID {
		t.Fatalf("unexpected final page: %+v", last)
	}

	empty, err := store.ExperiencesPage(SearchFilter{Query: "absent"}, PageOptions{Limit: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Items) != 0 || empty.HasMore {
		t.Fatalf("expected an empty terminal page, got %+v", empty)
	}
}

func TestInMemoryHistoryPagesRejectOversizedSingleRecord(t *testing.T) {
	store := NewInMemoryStore().(*memoryStore)
	if _, err := store.Record(Experience{Description: strings.Repeat("x", 128)}); err != nil {
		t.Fatal(err)
	}
	_, err := store.ExperiencesPage(SearchFilter{}, PageOptions{Limit: 1, MaxBytes: 64})
	if !errors.Is(err, ErrPageRecordTooLarge) {
		t.Fatalf("oversized experience error = %v, want %v", err, ErrPageRecordTooLarge)
	}
}

func TestHistoryPageReturnsPriorRecordsBeforeOversizedNextRecord(t *testing.T) {
	store := NewInMemoryStore().(*memoryStore)
	if _, err := store.Record(Experience{
		Description: strings.Repeat("x", 256),
		CreatedAt:   time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record(Experience{
		Description: "small record",
		CreatedAt:   time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}

	first, err := store.ExperiencesPage(SearchFilter{}, PageOptions{Limit: 2, MaxBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || !first.HasMore || first.Items[0].Description != "small record" {
		t.Fatalf("expected the fitting record before the oversized record, got %+v", first)
	}
	_, err = store.ExperiencesPage(SearchFilter{}, PageOptions{
		Cursor: first.NextCursor, Limit: 2, MaxBytes: 256,
	})
	if !errors.Is(err, ErrPageRecordTooLarge) {
		t.Fatalf("oversized next-page record error = %v, want %v", err, ErrPageRecordTooLarge)
	}
}

func TestInMemoryFactsAndDialoguePages(t *testing.T) {
	store := NewInMemoryStore().(*memoryStore)
	for _, key := range []string{"alpha", "beta"} {
		if _, err := store.PutFact(Fact{Key: key, Value: key}); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveDialogue(DialogueRecord{ID: key, Topic: key}); err != nil {
			t.Fatal(err)
		}
	}

	facts, err := store.FactsPage("", PageOptions{Limit: 1, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Items) != 1 || !facts.HasMore {
		t.Fatalf("unexpected first facts page: %+v", facts)
	}
	nextFacts, err := store.FactsPage("", PageOptions{Cursor: facts.NextCursor, Limit: 1, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(nextFacts.Items) != 1 || nextFacts.HasMore || nextFacts.Items[0].Key == facts.Items[0].Key {
		t.Fatalf("unexpected final facts page: %+v", nextFacts)
	}

	dialogues, err := store.DialoguesPage(PageOptions{Limit: 1, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(dialogues.Items) != 1 || !dialogues.HasMore {
		t.Fatalf("unexpected first dialogue page: %+v", dialogues)
	}
	nextDialogues, err := store.DialoguesPage(PageOptions{Cursor: dialogues.NextCursor, Limit: 1, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(nextDialogues.Items) != 1 || nextDialogues.HasMore || nextDialogues.Items[0].ID == dialogues.Items[0].ID {
		t.Fatalf("unexpected final dialogue page: %+v", nextDialogues)
	}
}

func TestBoltFactsPageUsesExactScopePrefix(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "facts.db"), "facts-test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, fact := range []Fact{
		{Scope: "team", Key: "one", Value: "included"},
		{Scope: "team-archive", Key: "one", Value: "excluded"},
	} {
		if _, err := store.PutFact(fact); err != nil {
			t.Fatal(err)
		}
	}

	page, err := store.(HistoryPager).FactsPage("team", PageOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Scope != "team" || page.Items[0].Value != "included" {
		t.Fatalf("facts in exact scope = %+v", page.Items)
	}
}

func TestBoltHistoryPagesUsePersistedCursorPositions(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "history.db"), "history-test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < 3; i++ {
		if _, err := store.Record(Experience{Description: "websocket persistence", Success: i%2 == 0}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutFact(Fact{Key: string(rune('a' + i)), Value: "value"}); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveDialogue(DialogueRecord{ID: string(rune('a' + i)), Topic: "topic"}); err != nil {
			t.Fatal(err)
		}
	}
	pager := store.(HistoryPager)

	first, err := pager.ExperiencesPage(SearchFilter{Query: "websocket"}, PageOptions{Limit: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	last, err := pager.ExperiencesPage(SearchFilter{Query: "websocket"}, PageOptions{
		Cursor: first.NextCursor, Limit: 2, MaxBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || !first.HasMore || len(last.Items) != 1 || last.HasMore {
		t.Fatalf("unexpected bolt experience pages: first=%+v last=%+v", first, last)
	}

	facts, err := pager.FactsPage("", PageOptions{Limit: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	lastFacts, err := pager.FactsPage("", PageOptions{Cursor: facts.NextCursor, Limit: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Items) != 2 || !facts.HasMore || len(lastFacts.Items) != 1 || lastFacts.HasMore {
		t.Fatalf("unexpected bolt fact pages: first=%+v last=%+v", facts, lastFacts)
	}

	dialogues, err := pager.DialoguesPage(PageOptions{Limit: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	lastDialogues, err := pager.DialoguesPage(PageOptions{Cursor: dialogues.NextCursor, Limit: 2, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if len(dialogues.Items) != 2 || !dialogues.HasMore || len(lastDialogues.Items) != 1 || lastDialogues.HasMore {
		t.Fatalf("unexpected bolt dialogue pages: first=%+v last=%+v", dialogues, lastDialogues)
	}
}
