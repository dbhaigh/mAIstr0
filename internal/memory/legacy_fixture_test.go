package memory

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteLegacyMemoryFixture(t *testing.T) {
	if os.Getenv("MAISTRO_WRITE_FIXTURES") != "1" {
		t.Skip("set MAISTRO_WRITE_FIXTURES=1 to regenerate the legacy fixture")
	}
	path := filepath.Join("testdata", "legacy-memory.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, "fixture-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	createdAt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err := store.Record(Experience{
		Kind: "task", TaskType: "code", Description: "fixture experience",
		Prompt: "fixture prompt", Output: "fixture output", NodeID: "node-fixture",
		Model: "fixture-model", Success: true, DurationMs: 1200, CreatedAt: createdAt,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutFact(Fact{
		Key: "fixture-fact", Value: "preserved", Scope: "cluster", Source: "fixture",
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}); err != nil {
		t.Fatal(err)
	}
	store.MergePerformances([]Performance{{
		NodeID: "node-merged", Model: "merged-model", TaskType: "general",
		Attempts: 3, Successes: 2, Failures: 1, TotalDuration: 900,
		AvgDurationMs: 300, SuccessRate: 2.0 / 3.0, LastUsed: createdAt,
	}})
	if err := store.SaveDialogue(DialogueRecord{
		ID: "fixture-dialogue", Participants: []string{"node-fixture"},
		Topic: "fixture topic", Turns: []Turn{{
			Speaker: "node-fixture", Content: "fixture turn", At: createdAt,
		}},
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveAgentSession("pi", "fixture-session", []byte(
		`{"id":"fixture-session","harness":"pi","messages":[]}`,
	)); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyMemoryFixtureLoadsAllRecords(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "legacy-memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy-memory.db")
	if err := os.WriteFile(path, fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, "fixture-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	experiences := store.Experiences(10)
	if len(experiences) != 1 || experiences[0].Description != "fixture experience" {
		t.Fatalf("legacy experiences = %+v", experiences)
	}
	experience := experiences[0]
	if experience.Kind != "task" || experience.TaskType != "code" ||
		experience.Prompt != "fixture prompt" || experience.Output != "fixture output" ||
		experience.NodeID != "node-fixture" || experience.Model != "fixture-model" ||
		!experience.Success || experience.DurationMs != 1200 ||
		!experience.CreatedAt.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) ||
		experience.Origin != "fixture-owner" || experience.Sequence == 0 {
		t.Fatalf("legacy experience fields = %+v", experience)
	}
	facts := store.Facts("cluster")
	if len(facts) != 1 || facts[0].Key != "fixture-fact" || facts[0].Value != "preserved" {
		t.Fatalf("legacy facts = %+v", facts)
	}
	if facts[0].Source != "fixture" || facts[0].Origin != "fixture-owner" || facts[0].Sequence == 0 ||
		!facts[0].CreatedAt.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) ||
		facts[0].UpdatedAt.IsZero() || facts[0].UpdatedAt.Before(facts[0].CreatedAt) {
		t.Fatalf("legacy fact fields = %+v", facts[0])
	}
	performances := store.Performances()
	if len(performances) != 2 {
		t.Fatalf("legacy performances = %+v, want two rows", performances)
	}
	var foundPerformance bool
	for _, performance := range performances {
		if performance.NodeID == "node-merged" {
			foundPerformance = performance.Model == "merged-model" &&
				performance.TaskType == "general" && performance.Attempts == 3 &&
				performance.Successes == 2 && performance.Failures == 1 &&
				performance.TotalDuration == 900 && performance.AvgDurationMs == 300 &&
				performance.SuccessRate == 2.0/3.0 &&
				performance.LastUsed.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))
		}
	}
	if !foundPerformance {
		t.Fatalf("legacy performance fields = %+v", performances)
	}
	for _, performance := range performances {
		if performance.Origin != "fixture-owner" || performance.Sequence == 0 {
			t.Fatalf("legacy performance metadata = %+v", performance)
		}
	}
	dialogues := store.Dialogues(10)
	if len(dialogues) != 1 || dialogues[0].ID != "fixture-dialogue" ||
		len(dialogues[0].Participants) != 1 || dialogues[0].Participants[0] != "node-fixture" ||
		dialogues[0].Topic != "fixture topic" || len(dialogues[0].Turns) != 1 ||
		dialogues[0].Turns[0].Speaker != "node-fixture" ||
		dialogues[0].Turns[0].Content != "fixture turn" ||
		!dialogues[0].Turns[0].At.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("legacy dialogues = %+v", dialogues)
	}
	if dialogues[0].Origin != "fixture-owner" || dialogues[0].Sequence == 0 {
		t.Fatalf("legacy dialogue metadata = %+v", dialogues[0])
	}
	sessions, err := store.AgentSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || string(sessions[0]) != `{"id":"fixture-session","harness":"pi","messages":[]}` {
		t.Fatalf("legacy agent sessions = %q", sessions)
	}
	snapshot := store.Snapshot()
	if len(snapshot.Experiences) != 1 || len(snapshot.Facts) != 1 ||
		len(snapshot.Performances) != 2 || len(snapshot.Dialogues) != 1 {
		t.Fatalf("legacy snapshot contents = experiences:%d facts:%d performances:%d dialogues:%d",
			len(snapshot.Experiences), len(snapshot.Facts), len(snapshot.Performances), len(snapshot.Dialogues))
	}
}
