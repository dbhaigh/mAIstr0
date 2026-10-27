package memory

import (
	"fmt"
	"math"
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
