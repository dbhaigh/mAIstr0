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
	if defaults.PiBaseURL == "" || defaults.PiModel == "" || defaults.PiAPIKeyEnv == "" {
		t.Fatalf("Pi provider defaults are incomplete: %#v", defaults)
	}

	path := filepath.Join(t.TempDir(), "orchestrator.json")
	if err := os.WriteFile(path, []byte(`{"harness":"pi","pi_base_url":"http://localhost:1234/v1","pi_model":"qwen","pi_api_key_env":"LOCAL_LLM_KEY"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadOrchestratorConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Harness != "pi" || cfg.PiBaseURL != "http://localhost:1234/v1" ||
		cfg.PiModel != "qwen" || cfg.PiAPIKeyEnv != "LOCAL_LLM_KEY" {
		t.Fatalf("configured Pi provider = %#v", cfg)
	}
}
