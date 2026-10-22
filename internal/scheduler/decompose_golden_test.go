package scheduler

import (
	"flag"
	"path/filepath"
	"testing"

	"github.com/maistr0/maistr0/internal/testutil"
)

var updateDecompositionGoldens = flag.Bool("update-decomposition", false, "update task decomposition golden files")

func TestDecomposePlannerGoldens(t *testing.T) {
	tests := []struct {
		name        string
		description string
		decompose   func(string) []Subtask
	}{
		{
			name:        "enumeration",
			description: "Review these options: Alpha, Beta, and Gamma.",
			decompose:   Decompose,
		},
		{
			name:        "compare",
			description: "Compare Alpha and Beta.",
			decompose:   Decompose,
		},
		{
			name:        "for-each",
			description: "For each of Alpha, Beta, and Gamma, summarize the risk.",
			decompose:   Decompose,
		},
		{
			name:        "heuristic-v1",
			description: "Compare Alpha and Beta.",
			decompose:   DecomposeV1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testutil.AssertJSONGolden(
				t,
				filepath.Join("testdata", "contracts", "decompose-"+test.name+".json"),
				test.decompose(test.description),
				*updateDecompositionGoldens,
			)
		})
	}
}
