package taskmgr

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestTaskPagesStayWithin64MiBAndConverge(t *testing.T) {
	const recordBytes = 24 << 20
	tasks := []*Task{
		{ID: "task-1", Origin: "task:node-a", Sequence: 1, Description: strings.Repeat("a", recordBytes)},
		{ID: "task-2", Origin: "task:node-a", Sequence: 2, Description: strings.Repeat("b", recordBytes)},
		{ID: "task-3", Origin: "task:node-a", Sequence: 3, Description: strings.Repeat("c", recordBytes)},
	}
	const limit = int64(64 << 20)

	pages, err := SplitTasks(tasks, limit)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) < 2 {
		t.Fatalf("snapshot produced %d pages, want multiple pages", len(pages))
	}
	var splitIDs []string
	for index, page := range pages {
		encoded, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(encoded)) > limit {
			t.Fatalf("page %d encoded to %d bytes, over the %d-byte limit", index, len(encoded), limit)
		}
		for _, task := range page {
			splitIDs = append(splitIDs, task.ID)
		}
	}
	if !reflect.DeepEqual(splitIDs, []string{"task-1", "task-2", "task-3"}) {
		t.Fatalf("split task IDs = %v", splitIDs)
	}

	upper := SnapshotWatermarks(tasks)
	cursor := map[string]uint64(nil)
	var received []string
	for {
		page, nextCursor, more, err := PageTasks(tasks, cursor, upper, limit)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range page {
			received = append(received, task.ID)
		}
		if !more {
			break
		}
		cursor = nextCursor
	}
	if !reflect.DeepEqual(received, splitIDs) {
		t.Fatalf("paged task IDs = %v, want %v", received, splitIDs)
	}
}

func TestPageTasksRejectsOversizedTask(t *testing.T) {
	tasks := []*Task{{ID: "task-1", Origin: "task:node-a", Sequence: 1, Description: strings.Repeat("x", 256)}}
	if _, _, _, err := PageTasks(tasks, nil, SnapshotWatermarks(tasks), 128); err == nil {
		t.Fatal("expected an oversized task to fail explicitly")
	}
}
