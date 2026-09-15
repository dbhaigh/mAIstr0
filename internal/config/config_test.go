package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOrchestratorHarnessConfig(t *testing.T) {
	if got := DefaultOrchestratorConfig().Harness; got != "deepseek" {
		t.Fatalf("default harness = %q, want deepseek", got)
	}

	path := filepath.Join(t.TempDir(), "orchestrator.json")
	if err := os.WriteFile(path, []byte(`{"harness":"deepseek"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadOrchestratorConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Harness != "deepseek" {
		t.Fatalf("configured harness = %q, want deepseek", cfg.Harness)
	}
}
