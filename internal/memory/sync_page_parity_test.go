package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/syncpage"
)

// Keep value boxing here as an oracle for the pre-optimization wire behavior.
func boxedSnapshotPage(snapshot Snapshot, cursor, upper map[string]uint64, maxBytes int64, envelopeBytes int) (Snapshot, map[string]uint64, bool, error) {
	records := make([]syncpage.Record[any], 0, len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Performances)+len(snapshot.Dialogues))
	for _, value := range snapshot.Experiences {
		records = append(records, syncpage.Record[any]{Value: value, Origin: value.Origin, Sequence: value.Sequence, Key: fmt.Sprintf("experience:%020d", value.ID)})
	}
	for _, value := range snapshot.Facts {
		records = append(records, syncpage.Record[any]{Value: value, Origin: value.Origin, Sequence: value.Sequence, Key: "fact:" + value.Scope + "\x00" + value.Key})
	}
	for _, value := range snapshot.Performances {
		records = append(records, syncpage.Record[any]{Value: value, Origin: value.Origin, Sequence: value.Sequence, Key: "performance:" + value.NodeID + "\x00" + value.Model + "\x00" + value.TaskType})
	}
	for _, value := range snapshot.Dialogues {
		records = append(records, syncpage.Record[any]{Value: value, Origin: value.Origin, Sequence: value.Sequence, Key: "dialogue:" + value.ID})
	}
	base := Snapshot{Experiences: []Experience{}, Facts: []Fact{}, Performances: []Performance{},
		Dialogues: []DialogueRecord{}, SyncVersion: SyncProtocolVersion, Watermarks: cloneWatermarks(upper)}
	encoded, err := json.Marshal(base)
	if err != nil {
		return Snapshot{}, nil, false, err
	}
	values, next, more, err := syncpage.Next(records, cursor, upper, maxBytes, len(encoded)+envelopeBytes, json.Marshal)
	if err != nil {
		return Snapshot{}, nil, false, err
	}
	page := Snapshot{Experiences: []Experience{}, Facts: []Fact{}, Performances: []Performance{},
		Dialogues: []DialogueRecord{}, SyncVersion: SyncProtocolVersion}
	for _, value := range values {
		switch record := value.(type) {
		case Experience:
			page.Experiences = append(page.Experiences, record)
		case Fact:
			page.Facts = append(page.Facts, record)
		case Performance:
			page.Performances = append(page.Performances, record)
		case DialogueRecord:
			page.Dialogues = append(page.Dialogues, record)
		}
	}
	if !more {
		page.Watermarks = cloneWatermarks(upper)
	}
	return page, next, more, nil
}

func TestPageSnapshotEncodingFailureParity(t *testing.T) {
	snapshot := Snapshot{Experiences: []Experience{{
		ID: 1, Origin: "synthetic", Sequence: 1,
		CreatedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
	}}}
	for _, consumed := range []uint64{0, 1} {
		t.Run(fmt.Sprint(consumed), func(t *testing.T) {
			cursor, upper := map[string]uint64{"synthetic": consumed}, map[string]uint64{"synthetic": 1}
			_, _, _, wantErr := boxedSnapshotPage(snapshot, cursor, upper, 1024, 0)
			_, _, _, gotErr := PageSnapshot(snapshot, cursor, upper, 1024, 0)
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || (gotErr != nil) != (consumed == 0) {
				t.Fatalf("encoding error = %v, reference = %v, cursor = %d", gotErr, wantErr, consumed)
			}
		})
	}
}

func TestPageSnapshotBoxedReferenceParity(t *testing.T) {
	snapshot := benchmarkHistorySnapshot(80)
	snapshot.Experiences = append(snapshot.Experiences,
		Experience{ID: 81, Description: "legacy empty origin"},
		Experience{ID: 82, Origin: "benchmark", Description: "legacy zero sequence"},
		Experience{ID: 83, Origin: "legacy", Sequence: 2, Description: "explicit legacy"},
		Experience{ID: 20, Origin: "benchmark", Sequence: 20, Description: "duplicate sort key"},
		Experience{ID: 90, Origin: "unknown", Sequence: 1, Description: "missing upper"},
	)
	snapshot.Facts = []Fact{
		{Scope: "synthetic", Key: "first", Value: "value", Origin: "other", Sequence: 1},
		{Scope: "synthetic", Key: "second", Value: `{"nested":[true,1,null]}`, Origin: "other", Sequence: 2},
		{Key: "legacy", Value: "legacy"},
	}

	snapshot.Performances = []Performance{
		{NodeID: "synthetic", Model: "synthetic", TaskType: "general", Origin: "other", Sequence: 3},
		{NodeID: "legacy", TaskType: "general"},
	}
	snapshot.Dialogues = []DialogueRecord{
		{ID: "synthetic", Origin: "other", Sequence: 4},
		{ID: "legacy"},
	}
	rng := rand.New(rand.NewPCG(17, 29))
	rng.Shuffle(len(snapshot.Experiences), func(i, j int) {
		snapshot.Experiences[i], snapshot.Experiences[j] = snapshot.Experiences[j], snapshot.Experiences[i]
	})
	for _, test := range []struct {
		name          string
		cursor, upper map[string]uint64
		limit         int64
		envelope      int
	}{
		{"full", nil, map[string]uint64{"benchmark": 80, "other": 4}, 1 << 20, 0},
		{"many-pages", nil, map[string]uint64{"benchmark": 80, "other": 4}, 1024, 128},
		{"window", map[string]uint64{"benchmark": 70, "other": 1, "legacy": 2}, map[string]uint64{"benchmark": 75, "other": 3}, 1024, 0},
		{"legacy-upper", map[string]uint64{"legacy": 2}, map[string]uint64{"legacy": 1}, 1024, 0},
		{"past-end", map[string]uint64{"benchmark": 100, "other": 10, "legacy": 100}, map[string]uint64{"benchmark": 80, "other": 4}, 1024, 0},
		{"nil-upper", nil, nil, 1024, 0},
		{"oversized", nil, map[string]uint64{"benchmark": 80}, 128, 0},
		{"envelope-oversized", nil, map[string]uint64{"benchmark": 80}, 1024, 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			before, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			cursor := maps.Clone(test.cursor)
			for pageIndex := range 100 {
				cursorBefore, upperBefore := maps.Clone(cursor), maps.Clone(test.upper)
				want, wantCursor, wantMore, wantErr := boxedSnapshotPage(snapshot, cursor, test.upper, test.limit, test.envelope)
				got, gotCursor, gotMore, gotErr := PageSnapshot(snapshot, cursor, test.upper, test.limit, test.envelope)
				if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || gotMore != wantMore ||
					!reflect.DeepEqual(gotCursor, wantCursor) || !reflect.DeepEqual(got, want) {
					t.Fatalf("page %d differs from boxed reference:\n got %#v, %v, %t, %v\nwant %#v, %v, %t, %v",
						pageIndex, got, gotCursor, gotMore, gotErr, want, wantCursor, wantMore, wantErr)
				}
				after, err := json.Marshal(snapshot)
				if err != nil || !bytes.Equal(before, after) || !maps.Equal(cursorBefore, cursor) || !maps.Equal(upperBefore, test.upper) {
					t.Fatal("pagination mutated input")
				}
				if gotErr != nil || !gotMore {
					return
				}
				if maps.Equal(cursor, gotCursor) {
					t.Fatal("pagination made no progress")
				}
				cursor = gotCursor
			}
			t.Fatal("pagination did not terminate")
		})
	}
}
