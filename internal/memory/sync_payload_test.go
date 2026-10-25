package memory

import "testing"

func TestCombinePayloadsDeduplicatesRecordIDs(t *testing.T) {
	shared := Fact{Key: "shared", Scope: "cluster", Value: "same", Origin: "node-a", Sequence: 1}
	updated := Fact{Key: "shared", Scope: "cluster", Value: "updated", Origin: "node-b", Sequence: 1}
	first, err := PayloadFromSnapshot(Snapshot{Facts: []Fact{shared}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := PayloadFromSnapshot(Snapshot{Facts: []Fact{shared, updated}})
	if err != nil {
		t.Fatal(err)
	}

	combined, err := CombinePayloads([]Payload{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(combined.Snapshot.Facts); got != 2 {
		t.Fatalf("combined facts = %d, want 2 distinct updates", got)
	}
	if got := len(combined.RecordIDs); got != 2 {
		t.Fatalf("combined RecordIDs = %d, want 2", got)
	}
}

func TestSnapshotDifferenceUsesRecordIDs(t *testing.T) {
	known := Fact{Key: "existing", Scope: "cluster", Value: "present", Origin: "node-a", Sequence: 1}
	newFact := Fact{Key: "new", Scope: "cluster", Value: "missing", Origin: "node-b", Sequence: 2}
	difference, err := SnapshotDifference(
		Snapshot{Facts: []Fact{known, newFact}, Watermarks: map[string]uint64{"node-b": 2}},
		Snapshot{Facts: []Fact{known}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(difference.Facts) != 1 || difference.Facts[0].Key != newFact.Key {
		t.Fatalf("snapshot difference facts = %+v, want only %q", difference.Facts, newFact.Key)
	}
	if difference.Watermarks["node-b"] != 2 {
		t.Fatalf("snapshot difference watermarks = %v, want node-b at 2", difference.Watermarks)
	}
}
