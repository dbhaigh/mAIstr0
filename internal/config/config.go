// Package config loads JSON configuration for the node agent and the
// orchestrator, with sane defaults so the binary runs out of the box.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
)

const (
	DefaultNodeConcurrency                = 2
	DefaultLogLevel                       = "info"
	DefaultDispatchConcurrency            = 16
	DefaultDispatchTimeoutSeconds         = 600
	DefaultMaxConcurrentHedges            = 1
	MaxAllowedConcurrentHedges            = 4
	DefaultRouterRequestTimeoutSeconds    = 120
	DefaultRouterTTFBTimeoutSeconds       = 60
	DefaultRouterIdleStreamTimeoutSeconds = 30
	MaxRouterRequestsPerMinute            = 100000
	MaxRouterConcurrentPerClient          = 1024
	DefaultSyncMaxBytes                   = int64(64 << 20)
	DefaultSyncTimeoutSeconds             = 600
	MaxSyncMaxBytes                       = int64(256 << 20)
	MaxSyncTimeoutSeconds                 = 3600

	OrchestratorModeFull    = "full"
	OrchestratorModeMinimal = "minimal"
)

type EngineConfig struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	RegistryURL string `json:"registry_url,omitempty"`
	APIKey      string `json:"api_key,omitempty"`
}

type NodeConfig struct {
	NodeID                     string         `json:"node_id"`
	ListenAddr                 string         `json:"listen_addr"`
	LogLevel                   string         `json:"log_level,omitempty"`
	AdvertiseAddr              string         `json:"advertise_addr"`
	OrchestratorAddr           string         `json:"orchestrator_addr"` // empty = auto-discover on the LAN
	MaxConcurrentTasks         int            `json:"max_concurrent_tasks"`
	PairingPIN                 string         `json:"pairing_pin,omitempty"`
	DisabledModels             []string       `json:"disabled_models"`
	DefaultModel               string         `json:"default_model"`
	Engines                    []EngineConfig `json:"engines,omitempty"`
	SupportedShardModels       []string       `json:"supported_shard_models,omitempty"`
	MaxLayersItCanHost         int            `json:"max_layers_it_can_host,omitempty"`
	DiscoveryEnabled           bool           `json:"discovery_enabled"`
	AutoOpenBrowser            bool           `json:"auto_open_browser"`
	TrayMode                   string         `json:"tray_mode"`   // "taskbar" or "hidden" (Windows only)
	MemoryPath                 string         `json:"memory_path"` // empty = default per-user data dir
	explicitInvalidConcurrency bool
	SyncMaxBytes               int64 `json:"sync_max_bytes,omitempty"`
	SyncTimeoutSeconds         int   `json:"sync_timeout_seconds,omitempty"`
	explicitInvalidSyncMax     bool
	explicitInvalidSyncTimeout bool
}

func (cfg *NodeConfig) Normalize() {
	cfg.LogLevel = normalizeLogLevel(cfg.LogLevel)
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = DefaultNodeConfig().ListenAddr
	}
	if cfg.MaxConcurrentTasks <= 0 {
		cfg.explicitInvalidConcurrency = cfg.explicitInvalidConcurrency || cfg.MaxConcurrentTasks < 0
		cfg.MaxConcurrentTasks = DefaultNodeConcurrency
	}
	if cfg.SyncMaxBytes <= 0 {
		cfg.explicitInvalidSyncMax = cfg.explicitInvalidSyncMax || cfg.SyncMaxBytes < 0
		cfg.SyncMaxBytes = DefaultSyncMaxBytes
	}
	if cfg.SyncTimeoutSeconds <= 0 {
		cfg.explicitInvalidSyncTimeout = cfg.explicitInvalidSyncTimeout || cfg.SyncTimeoutSeconds < 0
		cfg.SyncTimeoutSeconds = DefaultSyncTimeoutSeconds
	}
	seenShardModels := make(map[string]struct{}, len(cfg.SupportedShardModels))
	shardModels := cfg.SupportedShardModels[:0]
	for _, modelID := range cfg.SupportedShardModels {
		modelID = strings.TrimSpace(modelID)
		key := strings.ToLower(modelID)
		if modelID == "" {
			continue
		}
		if _, exists := seenShardModels[key]; exists {
			continue
		}
		seenShardModels[key] = struct{}{}
		shardModels = append(shardModels, modelID)
	}
	cfg.SupportedShardModels = shardModels
	if cfg.TrayMode == "" {
		cfg.TrayMode = "taskbar"
	}
}

func (cfg NodeConfig) Validate() error {
	var violations []error
	if err := validateLogLevel(cfg.LogLevel); err != nil {
		violations = append(violations, err)
	}
	if err := validateListenAddr(cfg.ListenAddr); err != nil {
		violations = append(violations, fmt.Errorf("listen_addr: %w", err))
	}
	if cfg.MaxConcurrentTasks <= 0 || cfg.explicitInvalidConcurrency {
		violations = append(violations, fmt.Errorf("max_concurrent_tasks must be greater than zero"))
	}
	if cfg.MaxLayersItCanHost < 0 {
		violations = append(violations, errors.New("max_layers_it_can_host must not be negative"))
	}
	if len(cfg.SupportedShardModels) > 0 && cfg.MaxLayersItCanHost <= 0 {
		violations = append(violations, errors.New("max_layers_it_can_host must be greater than zero when supported_shard_models is set"))
	}
	if cfg.SyncMaxBytes < 1 || cfg.SyncMaxBytes > MaxSyncMaxBytes || cfg.explicitInvalidSyncMax {
		violations = append(violations, fmt.Errorf("sync_max_bytes must be between 1 and %d", MaxSyncMaxBytes))
	}
	if cfg.SyncTimeoutSeconds < 1 || cfg.SyncTimeoutSeconds > MaxSyncTimeoutSeconds || cfg.explicitInvalidSyncTimeout {
		violations = append(violations, fmt.Errorf("sync_timeout_seconds must be between 1 and %d", MaxSyncTimeoutSeconds))
	}
	if cfg.TrayMode != "taskbar" && cfg.TrayMode != "hidden" {
		violations = append(violations, fmt.Errorf("tray_mode must be \"taskbar\" or \"hidden\""))
	}
	if err := validateHTTPSURL("orchestrator_addr", cfg.OrchestratorAddr); err != nil {
		violations = append(violations, err)
	}
	if err := validateHTTPSURL("advertise_addr", cfg.AdvertiseAddr); err != nil {
		violations = append(violations, err)
	}
	for index, configured := range cfg.Engines {
		if engine.NewConfigured(configured.Name, configured.URL) == nil {
			violations = append(violations, fmt.Errorf("engines[%d].name %q is not supported", index, configured.Name))
		}
		if configured.URL == "" {
			violations = append(violations, fmt.Errorf("engines[%d].url must be a valid HTTP or HTTPS URL", index))
		} else if err := validateHTTPURL(fmt.Sprintf("engines[%d].url", index), configured.URL); err != nil {
			violations = append(violations, err)
		}
	}
	if cfg.PairingPIN != "" && !validPairingPIN(cfg.PairingPIN) {
		violations = append(violations, fmt.Errorf("pairing_pin must be empty or exactly six digits"))
	}
	if err := validateMemoryPath(cfg.MemoryPath); err != nil {
		violations = append(violations, err)
	}
	return errors.Join(violations...)
}

func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		ListenAddr:         ":7451",
		LogLevel:           DefaultLogLevel,
		AdvertiseAddr:      "",
		OrchestratorAddr:   "",
		MaxConcurrentTasks: DefaultNodeConcurrency,
		DiscoveryEnabled:   true,
		AutoOpenBrowser:    true,
		TrayMode:           "taskbar",
		SyncMaxBytes:       DefaultSyncMaxBytes,
		SyncTimeoutSeconds: DefaultSyncTimeoutSeconds,
	}
}

type OrchestratorConfig struct {
	ListenAddr                          string               `json:"listen_addr"`
	LogLevel                            string               `json:"log_level,omitempty"`
	Mode                                string               `json:"mode,omitempty"` // "full" or "minimal"
	Harness                             string               `json:"harness"`        // "deepseek" or "pi"
	MaxConcurrentDispatches             int                  `json:"max_concurrent_dispatches"`
	DispatchTimeoutSeconds              int                  `json:"dispatch_timeout_seconds"`
	ShardedModels                       []ShardedModelConfig `json:"sharded_models,omitempty"`
	DiscoveryEnabled                    bool                 `json:"discovery_enabled"`
	AutoOpenBrowser                     bool                 `json:"auto_open_browser"`
	TrayMode                            string               `json:"tray_mode"`   // "taskbar" or "hidden" (Windows only)
	MemoryPath                          string               `json:"memory_path"` // empty = default per-user data dir
	SyncMaxBytes                        int64                `json:"sync_max_bytes,omitempty"`
	SyncTimeoutSeconds                  int                  `json:"sync_timeout_seconds,omitempty"`
	SpeculativeHedgingEnabled           bool                 `json:"speculative_hedging_enabled,omitempty"`
	MaxConcurrentHedges                 int                  `json:"max_concurrent_hedges,omitempty"`
	RouterTimeToFirstByteTimeoutSeconds int                  `json:"router_time_to_first_byte_timeout_seconds"`
	RouterIdleStreamTimeoutSeconds      int                  `json:"router_idle_stream_timeout_seconds"`
	RouterRequestsPerMinute             int                  `json:"router_requests_per_minute,omitempty"`
	RouterMaxConcurrentPerClient        int                  `json:"router_max_concurrent_per_client,omitempty"`
	explicitInvalidConcurrency          bool
	explicitInvalidDispatchTimeout      bool
	explicitInvalidHedgeCap             bool
	explicitInvalidRouterTTFBTimeout    bool
	explicitInvalidRouterIdleTimeout    bool
	explicitInvalidSyncMax              bool
	explicitInvalidSyncTimeout          bool
}

func (cfg *OrchestratorConfig) Normalize() {
	cfg.LogLevel = normalizeLogLevel(cfg.LogLevel)
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = DefaultOrchestratorConfig().ListenAddr
	}
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	if cfg.Mode == "" {
		cfg.Mode = OrchestratorModeFull
	}
	cfg.Harness = strings.ToLower(strings.TrimSpace(cfg.Harness))
	if cfg.Harness == "" {
		cfg.Harness = "pi"
	}
	if cfg.MaxConcurrentDispatches <= 0 {
		cfg.explicitInvalidConcurrency = cfg.explicitInvalidConcurrency || cfg.MaxConcurrentDispatches < 0
		cfg.MaxConcurrentDispatches = DefaultDispatchConcurrency
	}
	if cfg.DispatchTimeoutSeconds <= 0 {
		cfg.explicitInvalidDispatchTimeout = cfg.explicitInvalidDispatchTimeout || cfg.DispatchTimeoutSeconds < 0
		cfg.DispatchTimeoutSeconds = DefaultDispatchTimeoutSeconds
	}
	if cfg.MaxConcurrentHedges <= 0 {
		cfg.explicitInvalidHedgeCap = cfg.explicitInvalidHedgeCap || cfg.MaxConcurrentHedges < 0
		cfg.MaxConcurrentHedges = DefaultMaxConcurrentHedges
	}
	if cfg.RouterTimeToFirstByteTimeoutSeconds <= 0 {
		cfg.explicitInvalidRouterTTFBTimeout = cfg.explicitInvalidRouterTTFBTimeout || cfg.RouterTimeToFirstByteTimeoutSeconds < 0
		cfg.RouterTimeToFirstByteTimeoutSeconds = DefaultRouterTTFBTimeoutSeconds
	}
	if cfg.RouterIdleStreamTimeoutSeconds <= 0 {
		cfg.explicitInvalidRouterIdleTimeout = cfg.explicitInvalidRouterIdleTimeout || cfg.RouterIdleStreamTimeoutSeconds < 0
		cfg.RouterIdleStreamTimeoutSeconds = DefaultRouterIdleStreamTimeoutSeconds
	}
	if cfg.SyncMaxBytes <= 0 {
		cfg.explicitInvalidSyncMax = cfg.explicitInvalidSyncMax || cfg.SyncMaxBytes < 0
		cfg.SyncMaxBytes = DefaultSyncMaxBytes
	}
	if cfg.SyncTimeoutSeconds <= 0 {
		cfg.explicitInvalidSyncTimeout = cfg.explicitInvalidSyncTimeout || cfg.SyncTimeoutSeconds < 0
		cfg.SyncTimeoutSeconds = DefaultSyncTimeoutSeconds
	}
	for i := range cfg.ShardedModels {
		cfg.ShardedModels[i].Normalize()
	}
	if cfg.TrayMode == "" {
		cfg.TrayMode = "taskbar"
	}
}

func (cfg OrchestratorConfig) Validate() error {
	var violations []error
	if err := validateLogLevel(cfg.LogLevel); err != nil {
		violations = append(violations, err)
	}
	if err := validateListenAddr(cfg.ListenAddr); err != nil {
		violations = append(violations, fmt.Errorf("listen_addr: %w", err))
	}
	if cfg.Mode != "" && cfg.Mode != OrchestratorModeFull && cfg.Mode != OrchestratorModeMinimal {
		violations = append(violations, fmt.Errorf("mode must be %q or %q", OrchestratorModeFull, OrchestratorModeMinimal))
	}
	if cfg.Harness != "pi" && cfg.Harness != "deepseek" {
		violations = append(violations, fmt.Errorf("harness must be \"pi\" or \"deepseek\""))
	}
	if cfg.MaxConcurrentDispatches <= 0 || cfg.explicitInvalidConcurrency {
		violations = append(violations, fmt.Errorf("max_concurrent_dispatches must be greater than zero"))
	}
	if cfg.DispatchTimeoutSeconds <= 0 || cfg.explicitInvalidDispatchTimeout {
		violations = append(violations, fmt.Errorf("dispatch_timeout_seconds must be greater than zero"))
	}
	if cfg.MaxConcurrentHedges < 1 || cfg.MaxConcurrentHedges > MaxAllowedConcurrentHedges ||
		cfg.MaxConcurrentHedges > cfg.MaxConcurrentDispatches || cfg.explicitInvalidHedgeCap {
		violations = append(violations, fmt.Errorf(
			"max_concurrent_hedges must be between 1 and min(%d, max_concurrent_dispatches)",
			MaxAllowedConcurrentHedges,
		))
	}
	if cfg.RouterTimeToFirstByteTimeoutSeconds < 1 ||
		cfg.RouterTimeToFirstByteTimeoutSeconds > DefaultRouterRequestTimeoutSeconds ||
		cfg.explicitInvalidRouterTTFBTimeout {
		violations = append(violations, fmt.Errorf(
			"router_time_to_first_byte_timeout_seconds must be between 1 and %d",
			DefaultRouterRequestTimeoutSeconds,
		))
	}
	if cfg.RouterIdleStreamTimeoutSeconds < 1 ||
		cfg.RouterIdleStreamTimeoutSeconds > DefaultRouterRequestTimeoutSeconds ||
		cfg.explicitInvalidRouterIdleTimeout {
		violations = append(violations, fmt.Errorf(
			"router_idle_stream_timeout_seconds must be between 1 and %d",
			DefaultRouterRequestTimeoutSeconds,
		))
	}
	if cfg.RouterRequestsPerMinute < 0 || cfg.RouterRequestsPerMinute > MaxRouterRequestsPerMinute {
		violations = append(violations, fmt.Errorf(
			"router_requests_per_minute must be between 0 and %d",
			MaxRouterRequestsPerMinute,
		))
	}
	if cfg.RouterMaxConcurrentPerClient < 0 ||
		cfg.RouterMaxConcurrentPerClient > MaxRouterConcurrentPerClient {
		violations = append(violations, fmt.Errorf(
			"router_max_concurrent_per_client must be between 0 and %d",
			MaxRouterConcurrentPerClient,
		))
	}
	if cfg.SyncMaxBytes < 1 || cfg.SyncMaxBytes > MaxSyncMaxBytes || cfg.explicitInvalidSyncMax {
		violations = append(violations, fmt.Errorf("sync_max_bytes must be between 1 and %d", MaxSyncMaxBytes))
	}
	if cfg.SyncTimeoutSeconds < 1 || cfg.SyncTimeoutSeconds > MaxSyncTimeoutSeconds || cfg.explicitInvalidSyncTimeout {
		violations = append(violations, fmt.Errorf("sync_timeout_seconds must be between 1 and %d", MaxSyncTimeoutSeconds))
	}
	if cfg.TrayMode != "taskbar" && cfg.TrayMode != "hidden" {
		violations = append(violations, fmt.Errorf("tray_mode must be \"taskbar\" or \"hidden\""))
	}
	if !cfg.DiscoveryEnabled {
		violations = append(violations, fmt.Errorf("discovery_enabled must be true for an orchestrator"))
	}
	if err := validateMemoryPath(cfg.MemoryPath); err != nil {
		violations = append(violations, err)
	}
	modelIDs := make(map[string]struct{}, len(cfg.ShardedModels))
	for i, model := range cfg.ShardedModels {
		if err := model.Validate(); err != nil {
			violations = append(violations, fmt.Errorf("sharded_models[%d]: %w", i, err))
		}
		if _, exists := modelIDs[model.LogicalID]; exists {
			violations = append(violations, fmt.Errorf(
				"sharded_models[%d].logical_id %q is duplicated", i, model.LogicalID,
			))
		}
		modelIDs[model.LogicalID] = struct{}{}
	}
	return errors.Join(violations...)
}

func DefaultOrchestratorConfig() OrchestratorConfig {
	return OrchestratorConfig{
		ListenAddr:                          ":7450",
		LogLevel:                            DefaultLogLevel,
		Mode:                                OrchestratorModeFull,
		Harness:                             "pi",
		MaxConcurrentDispatches:             DefaultDispatchConcurrency,
		DispatchTimeoutSeconds:              DefaultDispatchTimeoutSeconds,
		MaxConcurrentHedges:                 DefaultMaxConcurrentHedges,
		RouterTimeToFirstByteTimeoutSeconds: DefaultRouterTTFBTimeoutSeconds,
		RouterIdleStreamTimeoutSeconds:      DefaultRouterIdleStreamTimeoutSeconds,
		DiscoveryEnabled:                    true,
		AutoOpenBrowser:                     true,
		TrayMode:                            "taskbar",
		SyncMaxBytes:                        DefaultSyncMaxBytes,
		SyncTimeoutSeconds:                  DefaultSyncTimeoutSeconds,
	}
}

func normalizeLogLevel(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return DefaultLogLevel
	}
	return value
}

func validateLogLevel(value string) error {
	switch value {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return fmt.Errorf("log_level must be debug, info, warn, or error")
	}
}

func validateListenAddr(address string) error {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("must be a valid host:port address: %w", err)
	}
	if port == "" {
		return fmt.Errorf("must include a port")
	}
	return nil
}

func validateHTTPSURL(field, value string) error {
	return validateURL(field, value, "https")
}

func validateHTTPURL(field, value string) error {
	return validateURL(field, value, "http", "https")
}

func validateURL(field, value string, schemes ...string) error {
	if value == "" {
		return nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("%s must be a valid URL: %w", field, err)
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("%s must be an absolute URL with a host", field)
	}
	for _, scheme := range schemes {
		if strings.EqualFold(parsed.Scheme, scheme) {
			return nil
		}
	}
	return fmt.Errorf("%s must use %s", field, strings.Join(schemes, " or "))
}

func validPairingPIN(pin string) bool {
	if len(pin) != 6 {
		return false
	}
	for _, digit := range pin {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func validateMemoryPath(path string) error {
	if path == "" {
		return nil
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("memory_path parent %q is not creatable: %w", parent, err)
	}
	return nil
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
