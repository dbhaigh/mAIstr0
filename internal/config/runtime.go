package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type RuntimeSettings struct {
	Role                       *string `json:"role,omitempty"`
	NodeConfigPath             *string `json:"node_config_path,omitempty"`
	OrchestratorConfigPath     *string `json:"orchestrator_config_path,omitempty"`
	NodeListenAddr             *string `json:"node_listen_addr,omitempty"`
	OrchestratorListenAddr     *string `json:"orchestrator_listen_addr,omitempty"`
	OrchestratorAddr           *string `json:"orchestrator_addr,omitempty"`
	TrayMode                   *string `json:"tray_mode,omitempty"`
	AutoOpenBrowser            *bool   `json:"auto_open_browser,omitempty"`
	DiscoveryEnabled           *bool   `json:"discovery_enabled,omitempty"`
	Harness                    *string `json:"harness,omitempty"`
	MemoryPath                 *string `json:"memory_path,omitempty"`
	AllowPeerUpdates           *bool   `json:"allow_peer_updates,omitempty"`
	NoPeerUpdates              *bool   `json:"no_peer_updates,omitempty"`
	RequirePairingVerification *bool   `json:"require_pairing_verification,omitempty"`
}

func DefaultRuntimeSettingsPath() string {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		base = "."
	}
	return filepath.Join(base, "maistr0", "runtime.json")
}

func LoadRuntimeSettings(path string) (RuntimeSettings, error) {
	if strings.TrimSpace(path) == "" {
		return RuntimeSettings{}, errors.New("runtime settings path is required")
	}
	settings := RuntimeSettings{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return settings, nil
		}
		return settings, fmt.Errorf("read runtime settings: %w", err)
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		return RuntimeSettings{}, fmt.Errorf("decode runtime settings: %w", err)
	}
	if err := settings.Validate(); err != nil {
		return RuntimeSettings{}, err
	}
	return settings, nil
}

func SaveRuntimeSettings(path string, settings RuntimeSettings) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("runtime settings path is required")
	}
	if err := settings.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("encode runtime settings: %w", err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create runtime settings directory: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".runtime-*.json")
	if err != nil {
		return fmt.Errorf("create temporary runtime settings file: %w", err)
	}
	tempPath := file.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("restrict runtime settings permissions: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write runtime settings: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync runtime settings: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close runtime settings: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace runtime settings: %w", err)
	}
	return nil
}

func (settings RuntimeSettings) Validate() error {
	if settings.Role != nil && *settings.Role != "auto" && *settings.Role != "node" &&
		*settings.Role != "orchestrator" && *settings.Role != "both" {
		return errors.New("role must be auto, node, orchestrator, or both")
	}
	if settings.TrayMode != nil && *settings.TrayMode != "taskbar" && *settings.TrayMode != "hidden" {
		return errors.New("tray_mode must be taskbar or hidden")
	}
	if settings.Harness != nil && *settings.Harness != "pi" && *settings.Harness != "deepseek" {
		return errors.New("harness must be pi or deepseek")
	}
	for name, value := range map[string]*string{
		"node_config_path":         settings.NodeConfigPath,
		"orchestrator_config_path": settings.OrchestratorConfigPath,
		"node_listen_addr":         settings.NodeListenAddr,
		"orchestrator_listen_addr": settings.OrchestratorListenAddr,
		"orchestrator_addr":        settings.OrchestratorAddr,
		"memory_path":              settings.MemoryPath,
	} {
		if value != nil && strings.ContainsAny(*value, "\x00\r\n") {
			return fmt.Errorf("%s must not contain control characters", name)
		}
	}
	return nil
}
