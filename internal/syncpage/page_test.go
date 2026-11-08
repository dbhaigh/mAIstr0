package syncpage

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNextTracksProgressIndependentlyPerOrigin(t *testing.T) {
	records := []Record[string]{
		{Value: "b2", Origin: "node-b", Sequence: 2},
		{Value: "a2", Origin: "node-a", Sequence: 2},
		{Value: "a1", Origin: "node-a", Sequence: 1},
		{Value: "b1", Origin: "node-b", Sequence: 1},
		{Value: "a3", Origin: "node-a", Sequence: 3},
	}
	upper := map[string]uint64{"node-a": 2, "node-b": 2}
	var received []string
	var cursor map[string]uint64
	for {
		page, next, more, err := Next(records, cursor, upper, 12, 2, func(value string) ([]byte, error) {
			return json.Marshal(value)
		})
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, page...)
		if !more {
			break
		}
		cursor = next
	}
	if !reflect.DeepEqual(received, []string{"a1", "a2", "b1", "b2"}) {
		t.Fatalf("received records = %v", received)
	}
}

func TestNextWithKeySizeInPlaceMatchesEncodedPaging(t *testing.T) {
	records := []Record[string]{
		{Value: "b1", Origin: "node-b", Sequence: 1},
		{Value: "legacy", Sequence: 0},
		{Value: "a1", Origin: "node-a", Sequence: 1},
		{Value: "a2", Origin: "node-a", Sequence: 2},
	}
	cursor := map[string]uint64(nil)
	upper := map[string]uint64{"node-a": 2, "node-b": 1}
	key := KeyFuncs[string]{
		Compare: func(left, right string) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		Format: func(value string) string { return value },
	}
	encode := func(value string) ([]byte, error) {
		return json.Marshal(value)
	}
	encodedSize := func(value string) (int, error) {
		encoded, err := json.Marshal(value)
		return len(encoded), err
	}

	want, wantCursor, wantMore, wantErr := NextWithKey(records, cursor, upper, 12, 2, encode, key)
	inPlaceRecords := append([]Record[string](nil), records...)
	got, gotCursor, gotMore, gotErr := NextWithKeySizeInPlace(
		inPlaceRecords, cursor, upper, 12, 2, encodedSize, key,
	)
	if gotErr != nil || wantErr != nil || gotMore != wantMore ||
		!reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotCursor, wantCursor) {
		t.Fatalf("in-place page = %v, %v, %t, %v; encoded page = %v, %v, %t, %v",
			got, gotCursor, gotMore, gotErr, want, wantCursor, wantMore, wantErr)
	}
}
