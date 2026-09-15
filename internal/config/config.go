// Package config loads JSON configuration for the node agent and the
// orchestrator, with sane defaults so the binary runs out of the box.
package config

import (
	"encoding/json"
	"os"
)

type EngineConfig struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	RegistryURL string `json:"registry_url,omitempty"`
}

type NodeConfig struct {
	NodeID           string         `json:"node_id"`
	ListenAddr       string         `json:"listen_addr"`
	AdvertiseAddr    string         `json:"advertise_addr"`
	OrchestratorAddr string         `json:"orchestrator_addr"` // empty = auto-discover on the LAN
	DisabledModels   []string       `json:"disabled_models"`
	DefaultModel     string         `json:"default_model"`
	Engines          []EngineConfig `json:"engines,omitempty"`
	DiscoveryEnabled bool           `json:"discovery_enabled"`
	AutoOpenBrowser  bool           `json:"auto_open_browser"`
	TrayMode         string         `json:"tray_mode"`   // "taskbar" or "hidden" (Windows only)
	MemoryPath       string         `json:"memory_path"` // empty = default per-user data dir
}

func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		ListenAddr:       ":7451",
		AdvertiseAddr:    "",
		OrchestratorAddr: "",
		DiscoveryEnabled: true,
		AutoOpenBrowser:  true,
		TrayMode:         "taskbar",
	}
}

type OrchestratorConfig struct {
	ListenAddr       string `json:"listen_addr"`
	Harness          string `json:"harness"` // "hermes" or "deepseek"
	DiscoveryEnabled bool   `json:"discovery_enabled"`
	AutoOpenBrowser  bool   `json:"auto_open_browser"`
	TrayMode         string `json:"tray_mode"`   // "taskbar" or "hidden" (Windows only)
	MemoryPath       string `json:"memory_path"` // empty = default per-user data dir
}

func DefaultOrchestratorConfig() OrchestratorConfig {
	return OrchestratorConfig{
		ListenAddr:       ":7450",
		Harness:          "deepseek",
		DiscoveryEnabled: true,
		AutoOpenBrowser:  true,
		TrayMode:         "taskbar",
	}
}

// LoadNodeConfig reads a JSON node config file if present, overlaying it on
// top of the defaults. A missing file is not an error.
func LoadNodeConfig(path string) (NodeConfig, error) {
	cfg := DefaultNodeConfig()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func LoadOrchestratorConfig(path string) (OrchestratorConfig, error) {
	cfg := DefaultOrchestratorConfig()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
