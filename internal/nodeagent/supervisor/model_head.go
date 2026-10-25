package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/hardware"
)

const installedHeadID = "maistr0-auto-head"

type installedHead struct {
	Path    string `json:"path"`
	Root    string `json:"root"`
	Name    string `json:"name"`
	Context int    `json:"context,omitempty"`
}

func installedHeadConfig(installed workerInstallation) config.ManagedEngineConfig {
	contextSize := installed.Head.Context
	if contextSize == 0 {
		contextSize = 4096
	}
	return config.ManagedEngineConfig{
		ID: installedHeadID, Kind: config.ManagedEngineLlamaServer,
		BinaryPath: installedServerBinary(installed), ModelPath: installed.Head.Path,
		Options: config.ManagedEngineOptions{LlamaServer: &config.LlamaServerOptions{
			Host: "127.0.0.1", Port: 8080, ContextSize: contextSize, GPULayers: 999,
		}},
	}
}

func installedServerBinary(installed workerInstallation) string {
	name := "llama-server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(filepath.Dir(installed.Binary), name)
}

func newInstalledHeadEngine(installed workerInstallation, binaries, models []string) *managedEngine {
	cfg := installedHeadConfig(installed)
	return &managedEngine{
		config: cfg, binaryAllowlist: append([]string(nil), binaries...), modelAllowlist: append([]string(nil), models...),
		modelName: installed.Head.Name,
		status:    Status{ID: installedHeadID, Kind: string(config.ManagedEngineLlamaServer), State: stateStopped, Endpoint: managedEndpoint(cfg)},
	}
}

// ConfigureLocalHead accepts only opaque IDs rediscovered from local model inventories.
func (m *Manager) ConfigureLocalHead(ctx context.Context, modelID string, requestedContext int) (Status, error) {
	if requestedContext < 1 || requestedContext > 1<<20 {
		return Status{}, errors.New("requested context must be from 1 to 1048576")
	}
	models, err := m.LocalHeadModels(ctx, requestedContext)
	if err != nil {
		return Status{}, err
	}
	var selected *LocalHeadModel
	for i := range models.Models {
		if models.Models[i].ID == modelID {
			selected = &models.Models[i]
			break
		}
	}
	if selected == nil {
		return Status{}, fmt.Errorf("%w: local GGUF model is unavailable", ErrNotFound)
	}
	m.installMu.Lock()
	defer m.installMu.Unlock()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return Status{}, ErrClosed
	}
	if m.installingWorker || !m.workersIdleLocked() {
		m.mu.Unlock()
		return Status{}, ErrWorkerBusy
	}
	if m.workerInstallation == nil {
		m.mu.Unlock()
		return Status{}, errors.New("install the RPC runtime before configuring a model head")
	}
	for id, entry := range m.engines {
		if (entry.config.Kind == config.ManagedEngineLlamaServer && id != installedHeadID) || (id == installedHeadID && entry.modelName == "") {
			m.mu.Unlock()
			return Status{}, errors.New("an operator-configured model head already exists")
		}
	}
	installed := *m.workerInstallation
	installed.Head = &installedHead{Path: selected.path, Root: selected.root, Name: selected.Name, Context: requestedContext}
	binaries := append(append([]string(nil), m.workerConfig.ManagedEngineBinaryAllowlist...), m.workerRoot)
	modelRoots := append(append([]string(nil), m.workerConfig.ManagedEngineModelAllowlist...), selected.root)
	engine := newInstalledHeadEngine(installed, binaries, modelRoots)
	m.installingWorker = true
	m.mu.Unlock()
	defer func() { m.mu.Lock(); m.installingWorker = false; m.mu.Unlock() }()
	if err := config.ValidateManagedEngines([]config.ManagedEngineConfig{engine.config}, binaries, modelRoots); err != nil {
		return Status{}, err
	}
	data, err := json.Marshal(installed)
	if err != nil {
		return Status{}, err
	}
	tmp, err := os.CreateTemp(m.workerRoot, ".head-")
	if err != nil {
		return Status{}, err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(data)
	if err := errors.Join(writeErr, tmp.Close()); err != nil {
		return Status{}, err
	}
	if err := ctx.Err(); err != nil {
		return Status{}, err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(m.workerRoot, "installation.json")); err != nil {
		return Status{}, err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return Status{}, ErrClosed
	}
	m.workerInstallation = &installed
	m.engines[installedHeadID] = engine
	m.mu.Unlock()
	return engine.snapshot(), nil
}

// RuntimeDevices uses the same backend and libraries as the installed worker.
func (m *Manager) RuntimeDevices(ctx context.Context) ([]hardware.Device, error) {
	m.mu.Lock()
	installed := m.workerInstallation
	m.mu.Unlock()
	if installed == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, installedServerBinary(*installed), "--list-devices")
	cmd.Dir = filepath.Dir(installed.Binary)
	var output runtimeDeviceOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("enumerate installed runtime devices: %w", err)
	}
	if output.truncated {
		return nil, errors.New("installed runtime device output exceeds 256 KiB")
	}
	return hardware.ParseLlamaDevices(output.data.Bytes()), nil
}

type runtimeDeviceOutput struct {
	data      bytes.Buffer
	truncated bool
}

func (b *runtimeDeviceOutput) Write(p []byte) (int, error) {
	count := len(p)
	remaining := (256 << 10) - b.data.Len()
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, err := b.data.Write(p)
	return count, err
}
