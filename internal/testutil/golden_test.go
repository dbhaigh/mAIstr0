package testutil

import (
	"path/filepath"
	"testing"
)

func TestAssertJSONGoldenCreatesAndComparesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contract.json")
	value := map[string]string{"result": "stable"}
	AssertJSONGolden(t, path, value, true)
	AssertJSONGolden(t, path, value, false)
}
