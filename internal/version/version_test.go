package version

import "testing"

func TestStringPreservesSemanticVersion(t *testing.T) {
	if got := String(); got != "0.0.1" {
		t.Fatalf("String() = %q, want 0.0.1", got)
	}
}

func TestCompareSemanticAndLegacyVersions(t *testing.T) {
	tests := []struct {
		left, right string
		want        int
	}{
		{"0.0.1", "0.0.2", -1},
		{"1.2.0", "1.1.9", 1},
		{"0.0.1", "0.0008", 1},
		{"0.0008", "0.0.1", -1},
		{"0.0.1", "0.0.1", 0},
	}
	for _, test := range tests {
		if got := Compare(test.left, test.right); got != test.want {
			t.Errorf("Compare(%q, %q) = %d, want %d", test.left, test.right, got, test.want)
		}
	}
}
