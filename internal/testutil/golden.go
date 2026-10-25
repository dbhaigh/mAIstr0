package testutil

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func AssertJSONGolden(t testing.TB, path string, value any, update bool) {
	t.Helper()
	actual, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("encode JSON golden %s: %v", path, err)
	}
	actual = append(actual, '\n')
	if update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create JSON golden directory for %s: %v", path, err)
		}
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatalf("write JSON golden %s: %v", path, err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON golden %s (use -update to create it): %v", path, err)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("JSON golden %s changed\n--- got ---\n%s\n--- want ---\n%s", path, actual, expected)
	}
}
