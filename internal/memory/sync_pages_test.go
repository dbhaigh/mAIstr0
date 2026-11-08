package memory

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestSyncPageHeadersRoundTrip(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/memory/sync", nil)
	cursor := map[string]uint64{"node-a": 5}
	upper := map[string]uint64{"node-a": 10}
	if err := SetSyncPageRequestHeaders(request, cursor, upper); err != nil {
		t.Fatal(err)
	}
	gotCursor, gotUpper, err := ReadSyncPageRequestHeaders(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotCursor, cursor) || !reflect.DeepEqual(gotUpper, upper) {
		t.Fatalf("page request headers = (%v, %v), want (%v, %v)", gotCursor, gotUpper, cursor, upper)
	}

	recorder := httptest.NewRecorder()
	if err := SetSyncPageResponseHeader(recorder, cursor); err != nil {
		t.Fatal(err)
	}
	response := &http.Response{Header: recorder.Header()}
	gotCursor, more, err := ReadSyncPageResponseHeader(response)
	if err != nil {
		t.Fatal(err)
	}
	if !more || !reflect.DeepEqual(gotCursor, cursor) {
		t.Fatalf("page response = (%v, %v), want (%v, true)", gotCursor, more, cursor)
	}
}

func TestSnapshotPagesStayWithin64MiBAndFinishWatermarks(t *testing.T) {
	const recordBytes = 24 << 20
	snapshot := Snapshot{
		Experiences: []Experience{
			{ID: 1, Origin: "node-a", Sequence: 1, Description: strings.Repeat("a", recordBytes)},
			{ID: 2, Origin: "node-a", Sequence: 2, Description: strings.Repeat("b", recordBytes)},
			{ID: 3, Origin: "node-a", Sequence: 3, Description: strings.Repeat("c", recordBytes)},
		},
		SyncVersion: SyncProtocolVersion,
		Watermarks:  map[string]uint64{"node-a": 3},
	}
	const limit = int64(64 << 20)

	pages, err := SplitSnapshot(snapshot, limit, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) < 2 {
		t.Fatalf("snapshot produced %d pages, want multiple pages", len(pages))
	}
	for index, page := range pages {
		encoded, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(encoded)) > limit {
			t.Fatalf("page %d encoded to %d bytes, over the %d-byte limit", index, len(encoded), limit)
		}
		if index < len(pages)-1 && len(page.Watermarks) != 0 {
			t.Fatalf("intermediate page %d advanced watermarks: %v", index, page.Watermarks)
		}
	}
	if !reflect.DeepEqual(pages[len(pages)-1].Watermarks, snapshot.Watermarks) {
		t.Fatalf("final page watermarks = %v, want %v", pages[len(pages)-1].Watermarks, snapshot.Watermarks)
	}

	cursor := map[string]uint64(nil)
	upper := snapshot.Watermarks
	var received []uint64
	for {
		page, nextCursor, more, err := PageSnapshot(snapshot, cursor, upper, limit, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, experience := range page.Experiences {
			received = append(received, experience.Sequence)
		}
		if !more {
			if !reflect.DeepEqual(page.Watermarks, snapshot.Watermarks) {
				t.Fatalf("final GET page watermarks = %v, want %v", page.Watermarks, snapshot.Watermarks)
			}
			break
		}
		if len(page.Watermarks) != 0 {
			t.Fatalf("intermediate GET page advanced watermarks: %v", page.Watermarks)
		}
		cursor = nextCursor
	}
	if !reflect.DeepEqual(received, []uint64{1, 2, 3}) {
		t.Fatalf("received sequences = %v, want [1 2 3]", received)
	}
}

func TestPageSnapshotRejectsOversizedRecord(t *testing.T) {
	snapshot := Snapshot{
		Experiences: []Experience{{ID: 1, Origin: "node-a", Sequence: 1, Description: strings.Repeat("x", 256)}},
		Watermarks:  map[string]uint64{"node-a": 1},
	}
	if _, _, _, err := PageSnapshot(snapshot, nil, snapshot.Watermarks, 128, 0); err == nil {
		t.Fatal("expected an oversized single record to fail explicitly")
	}
}

func TestJSONSizeEncoderMatchesMarshal(t *testing.T) {
	sizeEncoder := newJSONSizeEncoder()
	values := []any{
		&Experience{ID: 1, Description: "<>&\"\u2028\u2029\n"},
		&Fact{Scope: "scope", Key: "key", Value: `{"nested":"<>&"}`},
		&Performance{NodeID: "node", Model: "model", TaskType: "general", SuccessRate: 0.75},
		&DialogueRecord{
			ID: "dialogue", Participants: []string{"<>&"},
			Turns: []Turn{{Speaker: "speaker", Content: "line\n\u2028"}},
		},
	}
	for index, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal value %d: %v", index, err)
		}
		got, err := sizeEncoder.Size(value)
		if err != nil {
			t.Fatalf("measure value %d: %v", index, err)
		}
		if got != len(encoded) {
			t.Fatalf("measured value %d as %d bytes, json.Marshal encoded %d", index, got, len(encoded))
		}
	}
}

func TestSnapshotSincePreservesFilteringAndWatermarkSemantics(t *testing.T) {
	snapshot := Snapshot{
		Experiences: []Experience{
			{ID: 1, Origin: "node-a", Sequence: 2},
			{ID: 2, Origin: "node-a", Sequence: 3},
			{ID: 3, Sequence: 10},
			{ID: 4, Origin: "node-b", Sequence: 4},
		},
		Facts: []Fact{
			{Key: "equal", Origin: "node-a", Sequence: 2},
			{Key: "new", Origin: "node-a", Sequence: 4},
			{Key: "legacy", Origin: "legacy", Sequence: 0},
		},
		Performances: []Performance{
			{NodeID: "equal", Origin: "node-b", Sequence: 1},
			{NodeID: "new", Origin: "node-b", Sequence: 5},
			{NodeID: "legacy", Origin: "legacy", Sequence: 0},
		},
		Dialogues: []DialogueRecord{
			{ID: "equal", Origin: "node-c", Sequence: 1},
			{ID: "new", Origin: "node-c", Sequence: 2},
			{ID: "legacy", Sequence: 0},
		},
		Watermarks: map[string]uint64{"snapshot-origin": 7},
	}
	watermarks := map[string]uint64{"node-a": 2, "node-b": 1, "node-c": 1, "legacy": 100}

	got := snapshotSince(snapshot, watermarks)
	want := Snapshot{
		Experiences: []Experience{
			{ID: 2, Origin: "node-a", Sequence: 3},
			{ID: 3, Sequence: 10},
			{ID: 4, Origin: "node-b", Sequence: 4},
		},
		Facts: []Fact{
			{Key: "new", Origin: "node-a", Sequence: 4},
			{Key: "legacy", Origin: "legacy", Sequence: 0},
		},
		Performances: []Performance{
			{NodeID: "new", Origin: "node-b", Sequence: 5},
			{NodeID: "legacy", Origin: "legacy", Sequence: 0},
		},
		Dialogues: []DialogueRecord{
			{ID: "new", Origin: "node-c", Sequence: 2},
			{ID: "legacy", Sequence: 0},
		},
		SyncVersion: SyncProtocolVersion,
		Watermarks:  map[string]uint64{"snapshot-origin": 7},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incremental snapshot = %+v, want %+v", got, want)
	}
	got.Watermarks["snapshot-origin"] = 99
	if snapshot.Watermarks["snapshot-origin"] != 7 {
		t.Fatal("mutating incremental watermarks changed the source snapshot")
	}
}

func TestSnapshotSinceEmptyResultKeepsNilRecordSlices(t *testing.T) {
	snapshot := Snapshot{
		Experiences:  []Experience{{Origin: "node-a", Sequence: 1}},
		Facts:        []Fact{{Origin: "node-a", Sequence: 1}},
		Performances: []Performance{{Origin: "node-a", Sequence: 1}},
		Dialogues:    []DialogueRecord{{Origin: "node-a", Sequence: 1}},
	}
	got := snapshotSince(snapshot, map[string]uint64{"node-a": 1})
	if got.Experiences != nil || got.Facts != nil || got.Performances != nil || got.Dialogues != nil {
		t.Fatalf("empty incremental record slices = %+v, want nil slices", got)
	}
}

func TestSnapshotSinceZeroWatermarksCopiesAllRecords(t *testing.T) {
	snapshot := Snapshot{
		Experiences:  []Experience{{ID: 1, Origin: "node-a", Sequence: 1}},
		Facts:        []Fact{{Key: "fact", Origin: "node-a", Sequence: 2}},
		Performances: []Performance{{NodeID: "node-a", Origin: "node-a", Sequence: 3}},
		Dialogues:    []DialogueRecord{{ID: "dialogue", Origin: "node-a", Sequence: 4}},
		Watermarks:   map[string]uint64{"node-a": 4},
	}

	got := snapshotSince(snapshot, map[string]uint64{"node-a": 0})
	if !reflect.DeepEqual(got.Experiences, snapshot.Experiences) ||
		!reflect.DeepEqual(got.Facts, snapshot.Facts) ||
		!reflect.DeepEqual(got.Performances, snapshot.Performances) ||
		!reflect.DeepEqual(got.Dialogues, snapshot.Dialogues) {
		t.Fatalf("zero-watermark snapshot = %+v, want all source records", got)
	}
	got.Experiences[0].ID = 99
	if snapshot.Experiences[0].ID != 1 {
		t.Fatal("mutating full incremental snapshot changed the source slice")
	}
}
