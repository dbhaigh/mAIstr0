package memory

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestExperiencePageKeyMatchesFixedWidthFormat(t *testing.T) {
	for _, id := range []uint64{0, 1, 42, math.MaxUint64} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			got := experiencePageKey(id)
			want := fmt.Sprintf("experience:%020d", id)
			if got != want {
				t.Fatalf("experiencePageKey(%d) = %q, want %q", id, got, want)
			}
		})
	}
}

func TestSnapshotKeyComparisonMatchesFormattedKeys(t *testing.T) {
	values := []any{
		&DialogueRecord{ID: "z"},
		&DialogueRecord{ID: "a"},
		&Experience{ID: math.MaxUint64},
		&Experience{ID: 0},
		&Experience{ID: 42},
		&Fact{Scope: "a", Key: "\x00z"},
		&Fact{Scope: "a\x00", Key: "z"},
		&Fact{Scope: "a", Key: "b"},
		&Performance{NodeID: "node", Model: "model", TaskType: "task"},
		&Performance{NodeID: "node\x00model", Model: "", TaskType: "task"},
	}
	for _, left := range values {
		for _, right := range values {
			got := compareSnapshotKeys(left, right)
			want := strings.Compare(formatSnapshotKey(left), formatSnapshotKey(right))
			if sign(got) != sign(want) {
				t.Fatalf("compareSnapshotKeys(%q, %q) = %d, want sign %d",
					formatSnapshotKey(left), formatSnapshotKey(right), got, want)
			}
		}
	}
}

func sign(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}
