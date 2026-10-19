package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOrchestratorHarnessConfig(t *testing.T) {
	defaults := DefaultOrchestratorConfig()
	if defaults.Harness != "pi" {
		t.Fatalf("default harness = %q, want pi", defaults.Harness)
	}
	if defaults.Mode != OrchestratorModeFull {
		t.Fatalf("default mode = %q, want %q", defaults.Mode, OrchestratorModeFull)
	}
	if defaults.MaxConcurrentDispatches != DefaultDispatchConcurrency {
		t.Fatalf("default dispatch concurrency = %d, want %d", defaults.MaxConcurrentDispatches, DefaultDispatchConcurrency)
	}
	if defaults.DispatchTimeoutSeconds != DefaultDispatchTimeoutSeconds {
		t.Fatalf("default dispatch timeout = %d, want %d seconds", defaults.DispatchTimeoutSeconds, DefaultDispatchTimeoutSeconds)
	}
	path := filepath.Join(t.TempDir(), "orchestrator.json")
	if err := os.WriteFile(path, []byte(`{"mode":"minimal","harness":"deepseek","max_concurrent_dispatches":7,"dispatch_timeout_seconds":420,"pi_base_url":"https://ignored.example/v1","pi_api_key_env":"UNUSED_KEY"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadOrchestratorConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Harness != "deepseek" {
		t.Fatalf("configured backend = %#v", cfg)
	}
	if cfg.Mode != OrchestratorModeMinimal {
		t.Fatalf("configured mode = %q, want %q", cfg.Mode, OrchestratorModeMinimal)
	}
	if cfg.MaxConcurrentDispatches != 7 {
		t.Fatalf("dispatch concurrency = %d, want 7", cfg.MaxConcurrentDispatches)
	}
	if cfg.DispatchTimeoutSeconds != 420 {
		t.Fatalf("dispatch timeout = %d, want 420 seconds", cfg.DispatchTimeoutSeconds)
	}
}

func TestNodeConcurrencyConfigDefaultsAndLoads(t *testing.T) {
	defaults := DefaultNodeConfig()
	if defaults.MaxConcurrentTasks != DefaultNodeConcurrency {
		t.Fatalf("default node concurrency = %d, want %d", defaults.MaxConcurrentTasks, DefaultNodeConcurrency)
	}
	path := filepath.Join(t.TempDir(), "node.json")
	if err := os.WriteFile(path, []byte(`{"max_concurrent_tasks":5}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadNodeConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConcurrentTasks != 5 {
		t.Fatalf("node concurrency = %d, want 5", cfg.MaxConcurrentTasks)
	}
}

func TestNodeConfigNormalizeAndValidate(t *testing.T) {
	cfg := NodeConfig{}
	cfg.Normalize()
	if cfg.ListenAddr != DefaultNodeConfig().ListenAddr {
		t.Fatalf("normalized listen address = %q, want %q", cfg.ListenAddr, DefaultNodeConfig().ListenAddr)
	}
	if cfg.MaxConcurrentTasks != DefaultNodeConcurrency {
		t.Fatalf("normalized concurrency = %d, want %d", cfg.MaxConcurrentTasks, DefaultNodeConcurrency)
	}
	if cfg.TrayMode != "taskbar" {
		t.Fatalf("normalized tray mode = %q, want taskbar", cfg.TrayMode)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("normalized defaults are invalid: %v", err)
	}
	validURLs := DefaultNodeConfig()
	validURLs.OrchestratorAddr = "https://orchestrator.example:7450"
	validURLs.AdvertiseAddr = "HTTPS://node.example:7451"
	validURLs.PairingPIN = "012345"
	for index, name := range []string{"ollama", "vllm", "lmstudio", "llamacpp", "fastflowlm"} {
		scheme := "http"
		if index%2 == 1 {
			scheme = "https"
		}
		validURLs.Engines = append(validURLs.Engines, EngineConfig{
			Name: name,
			URL:  scheme + "://engine.example:1234",
		})
	}
	if err := validURLs.Validate(); err != nil {
		t.Fatalf("valid node URLs and engines were rejected: %v", err)
	}

	tests := []struct {
		name   string
		change func(*NodeConfig, *testing.T)
		want   string
	}{
		{"listen address", func(cfg *NodeConfig, _ *testing.T) { cfg.ListenAddr = "localhost" }, "listen_addr"},
		{"empty listen port", func(cfg *NodeConfig, _ *testing.T) { cfg.ListenAddr = "localhost:" }, "listen_addr"},
		{"concurrency", func(cfg *NodeConfig, _ *testing.T) { cfg.MaxConcurrentTasks = -1 }, "max_concurrent_tasks"},
		{"tray mode", func(cfg *NodeConfig, _ *testing.T) { cfg.TrayMode = "minimized" }, "tray_mode"},
		{"orchestrator URL", func(cfg *NodeConfig, _ *testing.T) { cfg.OrchestratorAddr = "http://orchestrator:7450" }, "orchestrator_addr"},
		{"advertise URL", func(cfg *NodeConfig, _ *testing.T) { cfg.AdvertiseAddr = "http://node:7451" }, "advertise_addr"},
		{"engine name", func(cfg *NodeConfig, _ *testing.T) {
			cfg.Engines = []EngineConfig{{Name: "unknown", URL: "http://engine:1234"}}
		}, "engines[0].name"},
		{"engine URL", func(cfg *NodeConfig, _ *testing.T) {
			cfg.Engines = []EngineConfig{{Name: "ollama", URL: "ftp://engine:1234"}}
		}, "engines[0].url"},
		{"missing engine URL", func(cfg *NodeConfig, _ *testing.T) {
			cfg.Engines = []EngineConfig{{Name: "ollama"}}
		}, "engines[0].url"},
		{"pairing PIN", func(cfg *NodeConfig, _ *testing.T) { cfg.PairingPIN = "12a456" }, "pairing_pin"},
		{"pairing PIN length", func(cfg *NodeConfig, _ *testing.T) { cfg.PairingPIN = "12345" }, "pairing_pin"},
		{"memory parent", func(cfg *NodeConfig, t *testing.T) {
			parent := filepath.Join(t.TempDir(), "not-a-directory")
			if err := os.WriteFile(parent, []byte("file"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg.MemoryPath = filepath.Join(parent, "memory.db")
		}, "memory_path"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultNodeConfig()
			test.change(&cfg, t)
			cfg.Normalize()
			if test.name == "concurrency" && cfg.MaxConcurrentTasks != DefaultNodeConcurrency {
				t.Fatalf("normalized negative concurrency = %d, want %d", cfg.MaxConcurrentTasks, DefaultNodeConcurrency)
			}
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want it to contain %q", err, test.want)
			}
		})
	}
}

func TestOrchestratorConfigNormalizeAndValidate(t *testing.T) {
	defaulted := OrchestratorConfig{DiscoveryEnabled: true}
	defaulted.Normalize()
	if defaulted.ListenAddr != DefaultOrchestratorConfig().ListenAddr || defaulted.Harness != "pi" ||
		defaulted.Mode != OrchestratorModeFull || defaulted.MaxConcurrentDispatches != DefaultDispatchConcurrency ||
		defaulted.DispatchTimeoutSeconds != DefaultDispatchTimeoutSeconds ||
		defaulted.TrayMode != "taskbar" {
		t.Fatalf("normalized orchestrator defaults = %+v", defaulted)
	}
	if err := defaulted.Validate(); err != nil {
		t.Fatalf("normalized orchestrator defaults are invalid: %v", err)
	}

	cfg := OrchestratorConfig{
		ListenAddr:       ":7450",
		Mode:             " MINIMAL ",
		Harness:          "  DEEPSEEK ",
		DiscoveryEnabled: true,
		TrayMode:         "hidden",
	}
	cfg.Normalize()
	if cfg.Mode != OrchestratorModeMinimal {
		t.Fatalf("normalized mode = %q, want %q", cfg.Mode, OrchestratorModeMinimal)
	}
	if cfg.Harness != "deepseek" {
		t.Fatalf("normalized harness = %q, want deepseek", cfg.Harness)
	}
	if cfg.MaxConcurrentDispatches != DefaultDispatchConcurrency {
		t.Fatalf("normalized concurrency = %d, want %d", cfg.MaxConcurrentDispatches, DefaultDispatchConcurrency)
	}
	if cfg.DispatchTimeoutSeconds != DefaultDispatchTimeoutSeconds {
		t.Fatalf("normalized dispatch timeout = %d, want %d seconds", cfg.DispatchTimeoutSeconds, DefaultDispatchTimeoutSeconds)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("normalized config is invalid: %v", err)
	}

	tests := []struct {
		name   string
		change func(*OrchestratorConfig, *testing.T)
		want   string
	}{
		{"listen address", func(cfg *OrchestratorConfig, _ *testing.T) { cfg.ListenAddr = "localhost" }, "listen_addr"},
		{"mode", func(cfg *OrchestratorConfig, _ *testing.T) { cfg.Mode = "lite" }, "mode"},
		{"harness", func(cfg *OrchestratorConfig, _ *testing.T) { cfg.Harness = "unknown" }, "harness"},
		{"concurrency", func(cfg *OrchestratorConfig, _ *testing.T) { cfg.MaxConcurrentDispatches = -1 }, "max_concurrent_dispatches"},
		{"dispatch timeout", func(cfg *OrchestratorConfig, _ *testing.T) { cfg.DispatchTimeoutSeconds = -1 }, "dispatch_timeout_seconds"},
		{"tray mode", func(cfg *OrchestratorConfig, _ *testing.T) { cfg.TrayMode = "minimized" }, "tray_mode"},
		{"discovery", func(cfg *OrchestratorConfig, _ *testing.T) { cfg.DiscoveryEnabled = false }, "discovery_enabled"},
		{"memory parent", func(cfg *OrchestratorConfig, t *testing.T) {
			parent := filepath.Join(t.TempDir(), "not-a-directory")
			if err := os.WriteFile(parent, []byte("file"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg.MemoryPath = filepath.Join(parent, "memory.db")
		}, "memory_path"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultOrchestratorConfig()
			test.change(&cfg, t)
			cfg.Normalize()
			if test.name == "concurrency" && cfg.MaxConcurrentDispatches != DefaultDispatchConcurrency {
				t.Fatalf("normalized negative concurrency = %d, want %d", cfg.MaxConcurrentDispatches, DefaultDispatchConcurrency)
			}
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want it to contain %q", err, test.want)
			}
		})
	}
}

func TestConfigValidationReportsAllViolations(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parent, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := OrchestratorConfig{
		ListenAddr:              "localhost",
		Harness:                 "invalid",
		MaxConcurrentDispatches: -1,
		DiscoveryEnabled:        false,
		TrayMode:                "invalid",
		MemoryPath:              filepath.Join(parent, "memory.db"),
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("invalid config passed validation")
	}
	for _, want := range []string{
		"listen_addr",
		"harness",
		"max_concurrent_dispatches",
		"dispatch_timeout_seconds",
		"tray_mode",
		"discovery_enabled",
		"memory_path",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error %q does not contain %q", err, want)
		}
	}
}
