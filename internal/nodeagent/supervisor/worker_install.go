package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/hardware"
)

const installedWorkerID = "maistr0-auto-rpc"

var ErrWorkerBusy = errors.New("worker installation requires all managed engines to be stopped")

type WorkerInstallResult struct {
	Source  string            `json:"source"`
	Version string            `json:"version"`
	Updated bool              `json:"updated"`
	Engines []Status          `json:"engines"`
	Warning string            `json:"warning,omitempty"`
	Devices []hardware.Device `json:"devices,omitempty"`
}

func installedWorkerRoot(cfg config.NodeConfig) (string, error) {
	if cfg.MemoryPath != "" {
		return filepath.Abs(cfg.MemoryPath + ".workers")
	}
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locate installed worker directory: %w", err)
	}
	nodeKey := sha256.Sum256([]byte(cfg.NodeID))
	return filepath.Join(root, "maistr0", "workers", hex.EncodeToString(nodeKey[:8])), nil
}

func loadWorkerInstallation(root string) (*workerInstallation, error) {
	data, err := os.ReadFile(filepath.Join(root, "installation.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read installed worker manifest: %w", err)
	}
	var installed workerInstallation
	if err := json.Unmarshal(data, &installed); err != nil {
		return nil, fmt.Errorf("decode installed worker manifest: %w", err)
	}
	if _, err := workerRepository(installed.Source); err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(installed.Binary)
	if err != nil {
		return nil, fmt.Errorf("resolve installed worker: %w", err)
	}
	relative, err := filepath.Rel(root, resolved)
	if err != nil || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("installed worker is outside the approved directory")
	}
	return &installed, nil
}

func withInstalledWorker(cfg config.NodeConfig, root string, installed workerInstallation) config.NodeConfig {
	for _, engine := range cfg.ManagedEngines {
		if engine.Kind == config.ManagedEngineGGMLRPCServer {
			return cfg
		}
	}
	cfg.ManagedEngines = append(append([]config.ManagedEngineConfig(nil), cfg.ManagedEngines...), config.ManagedEngineConfig{
		ID:         installedWorkerID,
		Kind:       config.ManagedEngineGGMLRPCServer,
		BinaryPath: installed.Binary,
	})
	cfg.ManagedEngineBinaryAllowlist = append(append([]string(nil), cfg.ManagedEngineBinaryAllowlist...), root)
	if installed.Head != nil {
		hasHead := false
		for _, entry := range cfg.ManagedEngines {
			if entry.Kind == config.ManagedEngineLlamaServer {
				hasHead = true
			}
		}
		if !hasHead {
			cfg.ManagedEngines = append(cfg.ManagedEngines, installedHeadConfig(installed))
			cfg.ManagedEngineModelAllowlist = append(append([]string(nil), cfg.ManagedEngineModelAllowlist...), installed.Head.Root)
		}
	}
	return cfg
}

func (m *Manager) EnsureWorker(ctx context.Context, source string) (WorkerInstallResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(m.ctx, cancel)
	defer stop()
	return m.ensureWorker(ctx, source, workerHTTPClient())
}

func (m *Manager) ensureWorker(ctx context.Context, source string, client *http.Client) (WorkerInstallResult, error) {
	if _, err := workerRepository(source); err != nil {
		return WorkerInstallResult{}, err
	}
	m.installMu.Lock()
	defer m.installMu.Unlock()
	if err := ctx.Err(); err != nil {
		return WorkerInstallResult{}, err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return WorkerInstallResult{}, ErrClosed
	}
	for _, engine := range m.engines {
		if engine.config.Kind == config.ManagedEngineGGMLRPCServer &&
			(m.workerInstallation == nil || engine.config.ID != installedWorkerID || engine.config.BinaryPath != m.workerInstallation.Binary) {
			m.mu.Unlock()
			return WorkerInstallResult{Source: "operator", Engines: m.Statuses()}, nil
		}
	}
	if entry, exists := m.engines[installedWorkerID]; exists && entry.config.Kind != config.ManagedEngineGGMLRPCServer {
		m.mu.Unlock()
		return WorkerInstallResult{}, errors.New("automatic worker ID is already used by a different managed engine")
	}
	current := m.workerInstallation
	idle := m.workersIdleLocked()
	m.mu.Unlock()
	if !idle {
		if current != nil && current.Source == source {
			return WorkerInstallResult{
				Source: current.Source, Version: current.Tag, Engines: m.Statuses(),
				Warning: "Update deferred while managed engines are active; using the installed worker.",
			}, nil
		}
		return WorkerInstallResult{}, ErrWorkerBusy
	}
	release, asset, err := findWorkerRelease(ctx, client, source, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return WorkerInstallResult{}, err
	}
	if current != nil && current.Source == source {
		if current.Tag == release.Tag {
			if current.Digest != asset.Digest {
				return WorkerInstallResult{}, errors.New("installed worker release digest changed without a new version")
			}
			return WorkerInstallResult{Source: source, Version: current.Tag, Engines: m.Statuses()}, nil
		}
		if source == "official" && officialWorkerBuildTag.MatchString(current.Tag) && officialWorkerBuildTag.MatchString(release.Tag) {
			installedBuild, installedErr := strconv.ParseUint(strings.TrimPrefix(current.Tag, "b"), 10, 64)
			availableBuild, availableErr := strconv.ParseUint(strings.TrimPrefix(release.Tag, "b"), 10, 64)
			if installedErr != nil || availableErr != nil {
				return WorkerInstallResult{}, errors.New("invalid official worker build number")
			}
			if installedBuild > availableBuild {
				return WorkerInstallResult{Source: source, Version: current.Tag, Engines: m.Statuses(),
					Warning: "The available worker release is older; keeping the installed version."}, nil
			}
		}
	}
	if err := os.MkdirAll(m.workerRoot, 0o700); err != nil {
		return WorkerInstallResult{}, fmt.Errorf("create worker directory: %w", err)
	}
	stage, err := os.MkdirTemp(m.workerRoot, ".download-")
	if err != nil {
		return WorkerInstallResult{}, err
	}
	defer os.RemoveAll(stage)
	archive := filepath.Join(stage, "archive")
	if err := downloadWorkerArchive(ctx, client, asset, archive); err != nil {
		return WorkerInstallResult{}, err
	}
	payload := filepath.Join(stage, "payload")
	binary, err := extractWorkerArchive(archive, payload, strings.HasSuffix(asset.Name, ".zip"))
	if err != nil {
		return WorkerInstallResult{}, fmt.Errorf("extract worker release: %w", err)
	}
	// Each activation gets its own directory; the previous version remains intact.
	final, err := os.MkdirTemp(m.workerRoot, source+"-")
	if err != nil {
		return WorkerInstallResult{}, err
	}
	if err := os.Remove(final); err != nil {
		return WorkerInstallResult{}, err
	}
	if err := os.Rename(payload, final); err != nil {
		return WorkerInstallResult{}, fmt.Errorf("publish worker payload: %w", err)
	}
	activated := false
	defer func() {
		if !activated {
			_ = os.RemoveAll(final)
		}
	}()
	relative, err := filepath.Rel(payload, binary)
	if err != nil {
		return WorkerInstallResult{}, err
	}
	installed := workerInstallation{Source: source, Tag: release.Tag, Digest: asset.Digest, Binary: filepath.Join(final, relative)}
	if current != nil {
		installed.Head = current.Head
	}
	cfg := m.workerConfig
	cfg.ManagedEngines = nil
	cfg = withInstalledWorker(cfg, m.workerRoot, installed)
	configured := cfg.ManagedEngines[0]
	if err := config.ValidateManagedEngines(cfg.ManagedEngines, cfg.ManagedEngineBinaryAllowlist, cfg.ManagedEngineModelAllowlist); err != nil {
		return WorkerInstallResult{}, err
	}
	manifest, err := json.Marshal(installed)
	if err != nil {
		return WorkerInstallResult{}, err
	}
	manifestPath := filepath.Join(stage, "installation.json")
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		return WorkerInstallResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return WorkerInstallResult{}, err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return WorkerInstallResult{}, ErrClosed
	}
	if !m.workersIdleLocked() {
		m.mu.Unlock()
		return WorkerInstallResult{}, ErrWorkerBusy
	}
	m.installingWorker = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.installingWorker = false
		m.mu.Unlock()
	}()
	if err := os.Rename(manifestPath, filepath.Join(m.workerRoot, "installation.json")); err != nil {
		return WorkerInstallResult{}, fmt.Errorf("persist installed worker: %w", err)
	}
	activated = true
	status := Status{ID: installedWorkerID, Kind: string(config.ManagedEngineGGMLRPCServer), State: stateStopped,
		Endpoint: managedEndpoint(configured), RelayEndpoint: configuredRPCRelayEndpoint(m.workerConfig)}
	m.mu.Lock()
	m.engines[installedWorkerID] = &managedEngine{
		config: configured, binaryAllowlist: cfg.ManagedEngineBinaryAllowlist,
		modelAllowlist: cfg.ManagedEngineModelAllowlist, status: status,
	}
	if installed.Head != nil {
		m.engines[installedHeadID] = newInstalledHeadEngine(installed, cfg.ManagedEngineBinaryAllowlist, cfg.ManagedEngineModelAllowlist)
	}
	m.workerInstallation = &installed
	m.mu.Unlock()
	return WorkerInstallResult{Source: source, Version: release.Tag, Updated: true, Engines: m.Statuses()}, nil
}

// The manager mutex must be held; no engine is changed while it can start.
func (m *Manager) workersIdleLocked() bool {
	if m.workerControls != 0 {
		return false
	}
	for _, engine := range m.engines {
		engine.mu.Lock()
		idle := engine.command == nil && engine.status.State == stateStopped
		engine.mu.Unlock()
		if !idle {
			return false
		}
	}
	return true
}

func (m *Manager) beginWorkerControl() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if m.installingWorker {
		return ErrWorkerBusy
	}
	m.workerControls++
	return nil
}

func (m *Manager) finishWorkerControl() {
	m.mu.Lock()
	m.workerControls--
	m.mu.Unlock()
}
