package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/nodeagent"
	"github.com/maistr0/maistr0/internal/taskmgr"
	buildversion "github.com/maistr0/maistr0/internal/version"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

const (
	maxDiagnosticEngineProbes = 16
	maxConcurrentEngineProbes = 4
	maxDiagnosticLogFiles     = 8
	maxDiagnosticLogBytes     = 256 << 10
	maxDiagnosticErrors       = 10
)

type diagnosticConfig struct {
	Node         config.NodeConfig         `json:"node"`
	Orchestrator config.OrchestratorConfig `json:"orchestrator"`
}

type engineProbe struct {
	name   string
	engine engine.Engine
}

type engineStatus struct {
	name      string
	available bool
	skipped   bool
}

type diagnosticError struct {
	time      string
	component string
	errorType string
}

func runDiagnostics(out io.Writer, options configPrintOptions) error {
	role, cfg, err := loadDiagnosticsConfig(options)
	if err != nil {
		return err
	}
	logPath, _ := applicationLogPath()
	return writeDiagnostics(out, role, cfg, logPath)
}

func loadDiagnosticsConfig(options configPrintOptions) (string, diagnosticConfig, error) {
	role := options.role
	if role == "auto" && options.orchestratorAddr != "" {
		role = "node"
	}
	if role == "" {
		role = "auto"
	}
	if role == "node" {
		nodeCfg, err := config.LoadNodeConfig(options.nodeConfigPath)
		if err != nil {
			return "", diagnosticConfig{}, fmt.Errorf("load node config: %w", err)
		}
		if options.nodeListenAddr != "" {
			nodeCfg.ListenAddr = options.nodeListenAddr
		}
		if options.orchestratorAddr != "" {
			nodeCfg.OrchestratorAddr = options.orchestratorAddr
		}
		if options.pairingPIN != "" {
			nodeCfg.PairingPIN = options.pairingPIN
		}
		if options.orchestratorFingerprint != "" {
			nodeCfg.OrchestratorFingerprint = options.orchestratorFingerprint
		}
		if options.trayMode != "" {
			nodeCfg.TrayMode = options.trayMode
		}
		if options.memoryPath != "" {
			nodeCfg.MemoryPath = options.memoryPath
		}
		if options.noDiscovery {
			nodeCfg.DiscoveryEnabled = false
		}
		if options.noBrowser {
			nodeCfg.AutoOpenBrowser = false
		}
		if nodeCfg.NodeID == "" {
			nodeCfg.NodeID = nodeagent.DefaultNodeID()
		}
		if nodeCfg.AdvertiseAddr == "" {
			port := ""
			if index := strings.LastIndex(nodeCfg.ListenAddr, ":"); index >= 0 {
				port = nodeCfg.ListenAddr[index:]
			}
			nodeCfg.AdvertiseAddr = "https://" + discovery.OutboundIP() + port
		}
		nodeCfg.Normalize()
		if err := validateNodeConfigWithoutFilesystemChanges(nodeCfg); err != nil {
			return "", diagnosticConfig{}, fmt.Errorf("node config: %w", err)
		}
		return role, diagnosticConfig{Node: nodeCfg}, nil
	}
	if role != "auto" && role != "both" && role != "orchestrator" {
		return "", diagnosticConfig{}, fmt.Errorf("unknown --role %q (want auto, orchestrator, node, or both)", role)
	}

	orchCfg, err := config.LoadOrchestratorConfig(options.orchConfigPath)
	if err != nil {
		return "", diagnosticConfig{}, fmt.Errorf("load orchestrator config: %w", err)
	}
	if options.orchListenAddr != "" {
		orchCfg.ListenAddr = options.orchListenAddr
	}
	if options.harness != "" {
		orchCfg.Harness = options.harness
	}
	if options.memoryPath != "" {
		orchCfg.MemoryPath = options.memoryPath
	}
	if options.trayMode != "" {
		orchCfg.TrayMode = options.trayMode
	}
	if options.noBrowser {
		orchCfg.AutoOpenBrowser = false
	}
	if options.noDiscovery {
		orchCfg.DiscoveryEnabled = false
	}
	orchCfg.Normalize()
	if err := validateOrchestratorConfigWithoutFilesystemChanges(orchCfg); err != nil {
		return "", diagnosticConfig{}, fmt.Errorf("orchestrator config: %w", err)
	}

	nodeCfg, err := config.LoadNodeConfig(options.nodeConfigPath)
	if err != nil {
		return "", diagnosticConfig{}, fmt.Errorf("load node config: %w", err)
	}
	if options.nodeListenAddr != "" {
		nodeCfg.ListenAddr = options.nodeListenAddr
	}
	if options.orchestratorAddr != "" {
		nodeCfg.OrchestratorAddr = options.orchestratorAddr
	}
	if options.pairingPIN != "" {
		nodeCfg.PairingPIN = options.pairingPIN
	}
	if options.orchestratorFingerprint != "" {
		nodeCfg.OrchestratorFingerprint = options.orchestratorFingerprint
	}
	if options.trayMode != "" {
		nodeCfg.TrayMode = options.trayMode
	}
	if options.noBrowser {
		nodeCfg.AutoOpenBrowser = false
	}
	if options.noDiscovery {
		nodeCfg.DiscoveryEnabled = false
	}
	if nodeCfg.NodeID == "" {
		nodeCfg.NodeID = nodeagent.DefaultNodeID()
	}
	nodeCfg.Normalize()
	if err := validateNodeConfigWithoutFilesystemChanges(nodeCfg); err != nil {
		return "", diagnosticConfig{}, fmt.Errorf("node config: %w", err)
	}
	return role, diagnosticConfig{Node: nodeCfg, Orchestrator: orchCfg}, nil
}

func validateNodeConfigWithoutFilesystemChanges(cfg config.NodeConfig) error {
	cfg.MemoryPath = ""
	return cfg.Validate()
}

func validateOrchestratorConfigWithoutFilesystemChanges(cfg config.OrchestratorConfig) error {
	cfg.MemoryPath = ""
	return cfg.Validate()
}

func writeDiagnostics(out io.Writer, role string, cfg diagnosticConfig, logPath string) error {
	var report strings.Builder
	fmt.Fprintf(&report, "maistr0 diagnostics\nversion: %s\nrole: %s\n", buildversion.String(), role)
	switch role {
	case "node":
		fmt.Fprintf(&report, "log_level: %s\n", cfg.Node.LogLevel)
	case "auto":
		fmt.Fprintf(&report, "log_level: node=%s orchestrator=%s\n", cfg.Node.LogLevel, cfg.Orchestrator.LogLevel)
	default:
		fmt.Fprintf(&report, "log_level: %s\n", cfg.Orchestrator.LogLevel)
	}

	if role != "node" {
		fmt.Fprintf(&report, "\nOrchestrator (configured)\n  listen_addr: %s\n  mode: %s\n  harness: %s\n  discovery_enabled: %t\n",
			cfg.Orchestrator.ListenAddr, cfg.Orchestrator.Mode, cfg.Orchestrator.Harness, cfg.Orchestrator.DiscoveryEnabled)
		owner := "orchestrator-" + discovery.OutboundIP()
		memoryPath := cfg.Orchestrator.MemoryPath
		if memoryPath == "" {
			memoryPath = memory.DefaultPath(owner)
		}
		if cfg.Orchestrator.Mode == config.OrchestratorModeMinimal {
			fmt.Fprintln(&report, "  memory_database: disabled (minimal mode)")
		} else {
			fmt.Fprintf(&report, "  memory_database: %s\n", checkBoltDatabase(memoryPath))
		}
		taskPath := taskmgr.DefaultPath(owner)
		if cfg.Orchestrator.MemoryPath != "" {
			taskPath = cfg.Orchestrator.MemoryPath + ".tasks"
		}
		fmt.Fprintf(&report, "  task_history_database: %s\n", checkBoltDatabase(taskPath))
	}

	if role != "orchestrator" || cfg.Node.NodeID != "" {
		fmt.Fprintf(&report, "\nNode (configured)\n  listen_addr: %s\n  discovery_enabled: %t\n  orchestrator_configured: %t\n",
			cfg.Node.ListenAddr, cfg.Node.DiscoveryEnabled, cfg.Node.OrchestratorAddr != "")
		for _, status := range probeNodeEngines(cfg.Node) {
			switch {
			case status.skipped:
				fmt.Fprintf(&report, "  engine %s: not probed (limit %d)\n", status.name, maxDiagnosticEngineProbes)
			case status.available:
				fmt.Fprintf(&report, "  engine %s: available\n", status.name)
			default:
				fmt.Fprintf(&report, "  engine %s: unavailable\n", status.name)
			}
		}
		nodeID := cfg.Node.NodeID
		if nodeID == "" {
			nodeID = nodeagent.DefaultNodeID()
		}
		memoryPath := cfg.Node.MemoryPath
		if memoryPath == "" {
			memoryPath = memory.DefaultPath(nodeID)
		}
		taskPath := taskmgr.DefaultPath(nodeID)
		if cfg.Node.MemoryPath != "" {
			taskPath = cfg.Node.MemoryPath + ".node-tasks"
		}
		fmt.Fprintf(&report, "  memory_database: %s\n  task_history_database: %s\n",
			checkBoltDatabase(memoryPath), checkBoltDatabase(taskPath))
	}

	errors, err := recentDiagnosticErrors(logPath, time.Now())
	switch {
	case err != nil:
		fmt.Fprintln(&report, "\nRecent errors: unavailable (log read failed)")
	case len(errors) == 0:
		fmt.Fprintln(&report, "\nRecent errors: none found")
	default:
		fmt.Fprintf(&report, "\nRecent errors (latest %d):\n", len(errors))
		for _, entry := range errors {
			fmt.Fprintf(&report, "  - %s component=%s error_type=%s\n", entry.time, entry.component, entry.errorType)
		}
	}
	_, err = io.WriteString(out, report.String())
	return err
}

func probeNodeEngines(cfg config.NodeConfig) []engineStatus {
	probes := make([]engineProbe, 0, len(cfg.Engines))
	if len(cfg.Engines) != 0 {
		for _, configured := range cfg.Engines {
			probes = append(probes, engineProbe{
				name: configured.Name, engine: engine.NewConfiguredWithAPIKey(configured.Name, configured.URL, configured.APIKey),
			})
		}
	} else {
		probes = []engineProbe{
			{name: "ollama", engine: engine.NewOllama("http://127.0.0.1:11434")},
			{name: "lmstudio (1234)", engine: engine.NewLMStudio("http://127.0.0.1:1234")},
			{name: "lmstudio (1235)", engine: engine.NewLMStudio("http://127.0.0.1:1235")},
			{name: "llamacpp (8080)", engine: engine.NewLlamaCPP("http://127.0.0.1:8080")},
			{name: "llamacpp (8081)", engine: engine.NewLlamaCPP("http://127.0.0.1:8081")},
			{name: "fastflowlm", engine: engine.NewFastFlowLM("http://127.0.0.1:8000")},
		}
	}

	statuses := make([]engineStatus, len(probes))
	limit := len(probes)
	if limit > maxDiagnosticEngineProbes {
		limit = maxDiagnosticEngineProbes
	}
	semaphore := make(chan struct{}, maxConcurrentEngineProbes)
	var workers sync.WaitGroup
	for index, probe := range probes {
		statuses[index].name = probe.name
		if index >= limit || probe.engine == nil {
			statuses[index].skipped = true
			continue
		}
		workers.Add(1)
		go func(index int, probe engineProbe) {
			defer workers.Done()
			semaphore <- struct{}{}
			statuses[index].available = probe.engine.Available()
			<-semaphore
		}(index, probe)
	}
	workers.Wait()
	return statuses
}

func checkBoltDatabase(path string) string {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "not initialized"
		}
		return "unavailable (" + safeHealthError(err) + ")"
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 250 * time.Millisecond})
	if err != nil {
		return "unavailable (" + safeHealthError(err) + ")"
	}
	viewErr := db.View(func(*bolt.Tx) error { return nil })
	closeErr := db.Close()
	if viewErr != nil {
		return "unhealthy (" + safeHealthError(viewErr) + ")"
	}
	if closeErr != nil {
		return "unavailable (" + safeHealthError(closeErr) + ")"
	}
	return "healthy"
}

func safeHealthError(err error) string {
	if errors.Is(err, bolterrors.ErrTimeout) {
		return "busy"
	}
	if errors.Is(err, os.ErrPermission) {
		return "permission denied"
	}
	return fmt.Sprintf("%T", err)
}

func recentDiagnosticErrors(path string, now time.Time) ([]diagnosticError, error) {
	if path == "" {
		return nil, errors.New("log path is unavailable")
	}
	dir := filepath.Dir(path)
	currentName := filepath.Base(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	type logFile struct {
		path    string
		modTime time.Time
	}
	files := make([]logFile, 0, maxDiagnosticLogFiles)
	for _, entry := range entries {
		if entry.IsDir() || (entry.Name() != currentName &&
			(!strings.HasPrefix(entry.Name(), strings.TrimSuffix(currentName, filepath.Ext(currentName))+"-") ||
				!strings.HasSuffix(entry.Name(), filepath.Ext(currentName)))) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if now.Sub(info.ModTime()) > 7*24*time.Hour {
			continue
		}
		files = append(files, logFile{
			path: filepath.Join(dir, entry.Name()), modTime: info.ModTime(),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modTime.After(files[j].modTime) })
	if len(files) > maxDiagnosticLogFiles {
		files = files[:maxDiagnosticLogFiles]
	}

	recent := make([]diagnosticError, 0, maxDiagnosticErrors)
	for _, file := range files {
		data, err := readDiagnosticLogTail(file.path, maxDiagnosticLogBytes)
		if err != nil {
			return nil, err
		}
		lines := bytes.Split(data, []byte{'\n'})
		for index := len(lines) - 1; index >= 0 && len(recent) < maxDiagnosticErrors; index-- {
			var record struct {
				Time      string `json:"time"`
				Level     string `json:"level"`
				Component string `json:"component"`
				ErrorType string `json:"error_type"`
			}
			if err := json.Unmarshal(lines[index], &record); err != nil ||
				record.Level != "ERROR" || record.Component == "" || record.Component == "stdlib" {
				continue
			}
			errorType := record.ErrorType
			if errorType == "" {
				errorType = "unspecified"
			}
			recent = append(recent, diagnosticError{
				time: record.Time, component: record.Component, errorType: errorType,
			})
		}
		if len(recent) == maxDiagnosticErrors {
			break
		}
	}
	return recent, nil
}

func readDiagnosticLogTail(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer closeutil.Discard(file)
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	offset := info.Size() - maxBytes
	if offset < 0 {
		offset = 0
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBytes))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
			data = data[newline+1:]
		} else {
			return nil, nil
		}
	}
	return data, nil
}
