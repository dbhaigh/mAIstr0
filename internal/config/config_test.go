package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOrchestratorHarnessConfig(t *testing.T) {
	defaults := DefaultOrchestratorConfig()
	if defaults.Harness != "pi" {
		t.Fatalf("default harness = %q, want pi", defaults.Harness)
	}
	path := filepath.Join(t.TempDir(), "orchestrator.json")
	if err := os.WriteFile(path, []byte(`{"harness":"deepseek","pi_base_url":"https://ignored.example/v1","pi_api_key_env":"UNUSED_KEY"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadOrchestratorConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Harness != "deepseek" {
		t.Fatalf("configured backend = %#v", cfg)
	}
}
