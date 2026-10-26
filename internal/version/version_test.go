package version

import "testing"

func TestStringPreservesSemanticVersion(t *testing.T) {
	if got := String(); got != "0.1.0" {
		t.Fatalf("String() = %q, want 0.1.0", got)
	}
}

func TestCompareSemanticAndLegacyVersions(t *testing.T) {
	tests := []struct {
		left, right string
		want        int
	}{
		{"0.0.24.4", "0.1.0", -1},
		{"0.1.0", "0.0.24.4", 1},
		{"0.0.24", "0.0.24.1", -1},
		{"0.0.24.1", "0.0.24", 1},
		{"0.0.24.9", "0.0.24.10", -1},
		{"0.0.24.999", "0.0.25", -1},
		{"0.0.25", "0.0.24.1", 1},
		{"0.0.24.999999999999999999999999", "0.0.24.10", 1},
		{"0.0.24.001", "0.0.24.1", 0},
		{"0.0.24.0", "0.0.24", 0},
		{"0.0.24.bad", "0.0.23", 0},
		{"0.0.1", "0.0.2", -1},
		{"1.2.0", "1.1.9", 1},
		{"0.0.3", "0.0.3.1", -1},
		{"0.0.3.1", "0.0.3", 1},
		{"0.0.3.1", "0.0.3.2", -1},
		{"0.0.3.2", "0.0.3.1", 1},
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
