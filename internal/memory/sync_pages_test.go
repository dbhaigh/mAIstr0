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
