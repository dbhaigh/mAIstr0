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
