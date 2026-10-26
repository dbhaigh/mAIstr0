// Command maistr0 runs either the cluster orchestrator, a node agent, or
// both in a single process (handy for local testing on one machine). It
// also drives the optional system tray presence (Windows) and best-effort
// auto-opens the dashboard in the default browser on startup.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/nodeagent"
	"github.com/maistr0/maistr0/internal/orchestrator"
	"github.com/maistr0/maistr0/internal/selfupdate"
	"github.com/maistr0/maistr0/internal/trayapp"
	buildversion "github.com/maistr0/maistr0/internal/version"
)

const startupElectionWindow = 15 * time.Second

func main() {
	role := flag.String("role", "auto", "which component to run: auto, orchestrator, node, or both")
	nodeConfigPath := flag.String("node-config", "", "path to node JSON config file")
	orchConfigPath := flag.String("orchestrator-config", "", "path to orchestrator JSON config file")
	nodeListenAddr := flag.String("node-listen", "", "override node listen address, e.g. :7451")
	orchListenAddr := flag.String("orchestrator-listen", "", "override orchestrator listen address, e.g. :7450")
	orchestratorAddr := flag.String("orchestrator-addr", "", "override the orchestrator address the node registers with (empty = auto-discover on the LAN)")
	pairTo := flag.String("pair-to", "", "deprecated; orchestrator federation is disabled")
	pairingPIN := flag.String("pairing-pin", "", "one-time PIN displayed by the orchestrator accepting a node")
	trayMode := flag.String("tray-mode", "", `override tray behavior: "taskbar" or "hidden"`)
	noBrowser := flag.Bool("no-browser", false, "do not auto-open the dashboard in a browser on startup")
	noDiscovery := flag.Bool("no-discovery", false, "disable LAN auto-discovery (node-only; orchestrator election requires discovery)")
	harness := flag.String("harness", "", `override orchestrator agent backend: "deepseek" or "pi"`)
	memoryPath := flag.String("memory-path", "", "override the cluster memory database file (empty = per-user data dir)")
	showVersion := flag.Bool("version", false, "print version and exit")
	printConfig := flag.Bool("print-config", false, "print normalized configuration and exit")
	diagnostics := flag.Bool("diagnostics", false, "print a redacted local health summary and exit")
	noPeerUpdates := flag.Bool("no-peer-updates", false, "disable automatic idle updates from paired peers")
	applyPeerUpdate := flag.String("apply-peer-update", "", "internal staged executable replacement job")
	cleanupPeerUpdate := flag.String("cleanup-peer-update", "", "internal exited replacement-helper cleanup")
	flag.Parse()

	if *applyPeerUpdate != "" {
		if err := selfupdate.Apply(*applyPeerUpdate); err != nil {
			fmt.Fprintf(os.Stderr, "peer update replacement failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if *cleanupPeerUpdate != "" {
		if err := selfupdate.Cleanup(*cleanupPeerUpdate); err != nil {
			fmt.Fprintf(os.Stderr, "peer update cleanup failed: %v\n", err)
		}
	}
	if *showVersion {
		fmt.Printf("maistr0 %s\n", buildversion.String())
		return
	}
	if *diagnostics {
		if err := runDiagnostics(os.Stdout, configPrintOptions{
			role: *role, nodeConfigPath: *nodeConfigPath, orchConfigPath: *orchConfigPath,
			nodeListenAddr: *nodeListenAddr, orchListenAddr: *orchListenAddr,
			orchestratorAddr: *orchestratorAddr, pairingPIN: *pairingPIN,
			trayMode: *trayMode, noBrowser: *noBrowser, noDiscovery: *noDiscovery,
			harness: *harness, memoryPath: *memoryPath,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "diagnostics failed: %v\n", err)
			os.Exit(2)
		}
		return
	}
	if *printConfig {
		err := printNormalizedConfig(os.Stdout, configPrintOptions{
			role: *role, nodeConfigPath: *nodeConfigPath, orchConfigPath: *orchConfigPath,
			nodeListenAddr: *nodeListenAddr, orchListenAddr: *orchListenAddr,
			orchestratorAddr: *orchestratorAddr, pairingPIN: *pairingPIN,
			trayMode: *trayMode, noBrowser: *noBrowser, noDiscovery: *noDiscovery,
			harness: *harness, memoryPath: *memoryPath,
		})
		if err != nil {
			printConfigValidationErrors(err)
			os.Exit(2)
		}
		return
	}
	logFile, logFilePath, err := configureLogging()
	if err != nil {
		fatalf("failed to initialize application log: %v", err)
	}
	defer closeutil.Discard(logFile)
	if *pairTo != "" {
		fatal("--pair-to is no longer supported; orchestrators cannot be paired")
	}
	if *role == "auto" && *orchestratorAddr != "" {
		*role = "node"
	}
	preferActiveOrchestrator := *role == "auto"
	var electionOrchCfg config.OrchestratorConfig
	var electionNodeCfg config.NodeConfig
	if *role == "auto" || *role == "orchestrator" || *role == "both" {
		electionOrchCfg = loadOrchestratorConfig(*orchConfigPath, *orchListenAddr, *noDiscovery)
		if *harness != "" {
			electionOrchCfg.Harness = *harness
		}
		if *memoryPath != "" {
			electionOrchCfg.MemoryPath = *memoryPath
		}
		if *trayMode != "" {
			electionOrchCfg.TrayMode = *trayMode
		}
		electionOrchCfg.Normalize()
		if err := electionOrchCfg.Validate(); err != nil {
			exitConfigValidation(err)
		}
		if err := logging.SetLevel(electionOrchCfg.LogLevel); err != nil {
			fatalf("configure log level: %v", err)
		}
		electionNodeCfg = loadNodeConfig(*nodeConfigPath, *nodeListenAddr, *orchestratorAddr, *noDiscovery)
		if *pairingPIN != "" {
			electionNodeCfg.PairingPIN = *pairingPIN
		}
		if *trayMode != "" {
			electionNodeCfg.TrayMode = *trayMode
		}
		electionNodeCfg.Normalize()
		if err := electionNodeCfg.Validate(); err != nil {
			exitConfigValidation(err)
		}
		ip := discovery.OutboundIP()
		candidate := discovery.Announcement{
			Role: "candidate", ID: "orchestrator-" + ip,
			PeerAddr: "https://" + ip + portSuffix(electionOrchCfg.ListenAddr),
			Score:    cluster.SelfScore(hardware.Detect()),
		}
		probe := discovery.Listen(candidate.ID)
		if err := probe.WaitReady(2 * time.Second); err != nil {
			probe.Close()
			fatalf("cannot elect a single orchestrator without LAN discovery: %v", err)
		}
		stop := make(chan struct{})
		go discovery.Beacon(func() discovery.Announcement { return candidate }, 250*time.Millisecond, stop)
		logging.Infof("main", "main: checking for an active orchestrator and startup candidates for %s", startupElectionWindow)
		time.Sleep(startupElectionWindow)
		close(stop)
		peers := probe.Snapshot()
		probe.Close()
		winner := startupOrchestratorWinner(candidate, peers, preferActiveOrchestrator)
		if winner.ID != candidate.ID {
			if winner.PeerAddr == "" {
				fatalf("orchestrator election selected %s without an HTTPS address", winner.ID)
			}
			logging.Infof("main", "main: joining orchestrator %s at %s as a node", winner.ID, winner.PeerAddr)
			*orchestratorAddr = winner.PeerAddr
			*role = "node"
		} else {
			logging.Infof("main", "main: elected orchestrator for this cluster; starting local node worker")
			*role = "both"
		}
	}

	var dashboardURL, effectiveTrayMode string
	var autoOpen bool
	var closeOrchestrator func()
	var closeNode func()
	var updateNode *nodeagent.Agent
	var updateOrchestrator *orchestrator.Server
	var updateFailover *failoverCoordinator

	switch *role {
	case "node":
		cfg := loadNodeConfig(*nodeConfigPath, *nodeListenAddr, *orchestratorAddr, *noDiscovery)
		if *pairingPIN != "" {
			cfg.PairingPIN = *pairingPIN
		}
		if *memoryPath != "" {
			cfg.MemoryPath = *memoryPath
		}
		if *trayMode != "" {
			cfg.TrayMode = *trayMode
		}
		cfg.Normalize()
		if err := cfg.Validate(); err != nil {
			exitConfigValidation(err)
		}
		if err := logging.SetLevel(cfg.LogLevel); err != nil {
			fatalf("configure log level: %v", err)
		}
		agent, err := nodeagent.NewWithPersistentTasks(cfg)
		if err != nil {
			fatalf("failed to open node task history: %v", err)
		}
		updateNode = agent
		var nodeDiscovery *discovery.Listener
		if cfg.DiscoveryEnabled {
			nodeDiscovery = discovery.Listen(agent.ID())
			agent.SetDiscovery(nodeDiscovery)

			orchCfg := loadOrchestratorConfig(*orchConfigPath, *orchListenAddr, *noDiscovery)
			if *harness != "" {
				orchCfg.Harness = *harness
			}
			if *memoryPath != "" {
				orchCfg.MemoryPath = *memoryPath
			}
			if *trayMode != "" {
				orchCfg.TrayMode = *trayMode
			}
			orchCfg.Normalize()
			if err := orchCfg.Validate(); err != nil {
				exitConfigValidation(err)
			}
			failover := newFailoverCoordinator(agent, nodeDiscovery, orchCfg, cfg.ListenAddr)
			updateFailover = failover
			agent.SetElectionStarter(failover.StartElection)
			closeOrchestrator = func() {
				if err := failover.Close(); err != nil {
					logging.Errorf("main", "promoted orchestrator shutdown: %v", err)
				}
			}
		}
		closeNode = func() {
			if nodeDiscovery != nil {
				nodeDiscovery.Close()
			}
			if err := agent.Close(); err != nil {
				logging.Errorf("main", "node shutdown: %v", err)
			}
		}
		dashboardURL = localURL(cfg.ListenAddr)
		effectiveTrayMode, autoOpen = cfg.TrayMode, cfg.AutoOpenBrowser
		go runNode(agent)

	case "both":
		orchCfg := electionOrchCfg
		nodeCfg := electionNodeCfg

		// The local worker must register with this process's orchestrator,
		// regardless of any saved or command-line node address.
		nodeCfg.OrchestratorAddr = "https://" + discovery.OutboundIP() + portSuffix(orchCfg.ListenAddr)
		nodeCfg.Normalize()
		if err := nodeCfg.Validate(); err != nil {
			exitConfigValidation(err)
		}
		srv, err := orchestrator.NewWithConfig(orchCfg)
		if err != nil {
			fatalf("failed to configure orchestrator agent: %v", err)
		}
		updateOrchestrator = srv
		closeOrchestrator = func() {
			if err := srv.Close(); err != nil {
				logging.Errorf("main", "orchestrator shutdown: %v", err)
			}
		}
		srv.SetDiscoveryEnabled(orchCfg.DiscoveryEnabled)
		if err := srv.SetLocalNodeDashboard(localURL(nodeCfg.ListenAddr)); err != nil {
			fatalf("failed to configure local worker dashboard: %v", err)
		}
		agent, err := nodeagent.NewWithPersistentTasks(nodeCfg)
		if err != nil {
			fatalf("failed to open node task history: %v", err)
		}
		updateNode = agent
		closeNode = func() {
			if err := agent.Close(); err != nil {
				logging.Errorf("main", "node shutdown: %v", err)
			}
		}
		if err := srv.InitializeIdentity(); err != nil {
			fatalf("failed to initialize orchestrator identity: %v", err)
		}
		if err := agent.InitializeIdentity(); err != nil {
			fatalf("failed to initialize node identity: %v", err)
		}
		orchestratorCertificate, err := srv.IdentityCertificate()
		if err != nil {
			fatalf("failed to read orchestrator identity: %v", err)
		}
		nodeCertificate, err := agent.IdentityCertificate()
		if err != nil {
			fatalf("failed to read node identity: %v", err)
		}
		if err := agent.PairLocalOrchestrator(srv.SelfID(), orchestratorCertificate, nodeCfg.OrchestratorAddr); err != nil {
			fatalf("failed to pair local node with orchestrator: %v", err)
		}
		if err := srv.TrustPeer(agent.ID(), nodeCertificate); err != nil {
			fatalf("failed to pair orchestrator with local node: %v", err)
		}

		// A single process running both roles must share one UDP discovery
		// socket instead of each binding its own (which would conflict).
		// This is only used to find *other* peers on the LAN now.
		if orchCfg.DiscoveryEnabled || nodeCfg.DiscoveryEnabled {
			shared := discovery.Listen(srv.SelfID(), agent.ID())
			if err := shared.WaitReady(2 * time.Second); err != nil {
				shared.Close()
				fatalf("cannot start cluster discovery: %v", err)
			}
			srv.SetDiscovery(shared)
			agent.SetDiscovery(shared)
		}

		dashboardURL = localURL(orchCfg.ListenAddr)
		effectiveTrayMode, autoOpen = orchCfg.TrayMode, orchCfg.AutoOpenBrowser
		go runNode(agent)
		go runOrchestrator(srv, orchCfg.ListenAddr)

	default:
		fatalf("unknown --role %q (want orchestrator, node, or both)", *role)
	}

	if *trayMode != "" {
		effectiveTrayMode = *trayMode
	}
	if *noBrowser {
		autoOpen = false
	}

	if autoOpen {
		go func() {
			time.Sleep(750 * time.Millisecond)
			openBrowser(dashboardURL)
		}()
	}

	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			if closeNode != nil {
				closeNode()
			}
			if closeOrchestrator != nil {
				closeOrchestrator()
			}
		})
		os.Exit(0)
	}
	if !*noPeerUpdates {
		go selfupdate.Run(context.Background(), func() selfupdate.Source {
			source := updateNode.UpdateSource()
			if updateOrchestrator != nil {
				source.Peers = append(source.Peers, updateOrchestrator.UpdateSource().Peers...)
			}
			return source
		}, func() bool {
			return updateNode.UpdateIdle() &&
				(updateOrchestrator == nil || updateOrchestrator.UpdateIdle()) &&
				(updateFailover == nil || updateFailover.UpdateIdle())
		}, shutdown)
	}
	trayapp.Run(trayapp.Options{
		Title:        "mAIstr0 (" + *role + ")",
		Mode:         effectiveTrayMode,
		DashboardURL: dashboardURL,
		LogFilePath:  logFilePath,
		LogDirectory: filepath.Dir(logFilePath),
		OnQuit:       shutdown,
	})
}

func startupOrchestratorWinner(candidate discovery.Announcement, peers []discovery.Peer, preferActive bool) discovery.Announcement {
	if preferActive {
		if active, ok := discovery.BestActiveOrchestrator(peers); ok {
			return active.Announcement
		}
	}
	return discovery.BestOrchestratorCandidate(candidate, peers)
}

func configureLogging() (*logging.DailyFile, string, error) {
	path, err := applicationLogPath()
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, "", fmt.Errorf("create log directory: %w", err)
	}
	file, err := logging.OpenDailyFile(path, 7)
	if err != nil {
		return nil, "", fmt.Errorf("open rolling log %s: %w", path, err)
	}
	logging.ConfigureSlog(os.Stderr, file)
	logging.Infof("main", "maistr0: logging to %s", path)
	return file, path, nil
}

func applicationLogPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locate user config directory: %w", err)
	}
	return filepath.Join(configDir, "maistr0", "maistr0.log"), nil
}

func loadOrchestratorConfig(path, listenOverride string, noDiscovery bool) config.OrchestratorConfig {
	cfg, err := config.LoadOrchestratorConfig(path)
	if err != nil {
		fatalf("failed to load orchestrator config: %v", err)
	}
	if listenOverride != "" {
		cfg.ListenAddr = listenOverride
	}
	if noDiscovery {
		cfg.DiscoveryEnabled = false
	}
	return cfg
}

type configPrintOptions struct {
	role             string
	nodeConfigPath   string
	orchConfigPath   string
	nodeListenAddr   string
	orchListenAddr   string
	orchestratorAddr string
	pairingPIN       string
	trayMode         string
	noBrowser        bool
	noDiscovery      bool
	harness          string
	memoryPath       string
}

func printNormalizedConfig(out io.Writer, options configPrintOptions) error {
	role := options.role
	if role == "auto" && options.orchestratorAddr != "" {
		role = "node"
	}
	if role == "node" {
		cfg, err := config.LoadNodeConfig(options.nodeConfigPath)
		if err != nil {
			return fmt.Errorf("load node config: %w", err)
		}
		if options.nodeListenAddr != "" {
			cfg.ListenAddr = options.nodeListenAddr
		}
		if options.orchestratorAddr != "" {
			cfg.OrchestratorAddr = options.orchestratorAddr
		}
		if options.pairingPIN != "" {
			cfg.PairingPIN = options.pairingPIN
		}
		if options.trayMode != "" {
			cfg.TrayMode = options.trayMode
		}
		if options.memoryPath != "" {
			cfg.MemoryPath = options.memoryPath
		}
		if options.noDiscovery {
			cfg.DiscoveryEnabled = false
		}
		if options.noBrowser {
			cfg.AutoOpenBrowser = false
		}
		if cfg.NodeID == "" {
			cfg.NodeID = nodeagent.DefaultNodeID()
		}
		if cfg.AdvertiseAddr == "" {
			port := ""
			if index := strings.LastIndex(cfg.ListenAddr, ":"); index >= 0 {
				port = cfg.ListenAddr[index:]
			}
			cfg.AdvertiseAddr = "https://" + discovery.OutboundIP() + port
		}
		cfg.Normalize()
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("node config: %w", err)
		}
		return writeNormalizedConfig(out, struct {
			Node config.NodeConfig `json:"node"`
		}{Node: redactNodeConfig(cfg)})
	}
	if role != "auto" && role != "both" && role != "orchestrator" {
		return fmt.Errorf("unknown --role %q (want auto, orchestrator, node, or both)", role)
	}

	orchCfg, err := config.LoadOrchestratorConfig(options.orchConfigPath)
	if err != nil {
		return fmt.Errorf("load orchestrator config: %w", err)
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
	if err := orchCfg.Validate(); err != nil {
		return fmt.Errorf("orchestrator config: %w", err)
	}

	nodeCfg, err := config.LoadNodeConfig(options.nodeConfigPath)
	if err != nil {
		return fmt.Errorf("load node config: %w", err)
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
	if options.trayMode != "" {
		nodeCfg.TrayMode = options.trayMode
	}
	if options.noBrowser {
		nodeCfg.AutoOpenBrowser = false
	}
	if options.noDiscovery {
		nodeCfg.DiscoveryEnabled = false
	}
	nodeCfg.Normalize()
	if err := nodeCfg.Validate(); err != nil {
		return fmt.Errorf("node config: %w", err)
	}
	return writeNormalizedConfig(out, struct {
		Node         config.NodeConfig         `json:"node"`
		Orchestrator config.OrchestratorConfig `json:"orchestrator"`
	}{Node: redactNodeConfig(nodeCfg), Orchestrator: orchCfg})
}

func redactNodeConfig(cfg config.NodeConfig) config.NodeConfig {
	for index := range cfg.Engines {
		if cfg.Engines[index].APIKey != "" {
			cfg.Engines[index].APIKey = "[REDACTED]"
		}
	}
	return cfg
}

func writeNormalizedConfig(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func loadNodeConfig(path, listenOverride, orchestratorOverride string, noDiscovery bool) config.NodeConfig {
	cfg, err := config.LoadNodeConfig(path)
	if err != nil {
		fatalf("failed to load node config: %v", err)
	}
	if listenOverride != "" {
		cfg.ListenAddr = listenOverride
	}
	if orchestratorOverride != "" {
		cfg.OrchestratorAddr = orchestratorOverride
	}
	if noDiscovery {
		cfg.DiscoveryEnabled = false
	}
	return cfg
}

func exitConfigValidation(err error) {
	printConfigValidationErrors(err)
	os.Exit(2)
}

func printConfigValidationErrors(err error) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, violation := range joined.Unwrap() {
			printConfigValidationErrors(violation)
		}
		return
	}
	fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
}

func runOrchestrator(srv *orchestrator.Server, listenAddr string) {
	if err := srv.Run(listenAddr); err != nil {
		fatalf("orchestrator server stopped: %v", err)
	}
}

func runNode(agent *nodeagent.Agent) {
	if err := agent.Run(); err != nil {
		fatalf("node agent stopped: %v", err)
	}
}

func fatalf(format string, args ...any) {
	logging.Errorf("main", format, args...)
	os.Exit(1)
}

func fatal(args ...any) {
	logging.Errorf("main", "%s", fmt.Sprint(args...))
	os.Exit(1)
}

// localURL turns a listen address like ":7450" or "0.0.0.0:7450" into a
// browser-openable http://localhost:PORT URL.
func localURL(listenAddr string) string {
	return "http://localhost" + portSuffix(listenAddr)
}

// portSuffix extracts ":port" from a listen address like ":7450".
func portSuffix(listenAddr string) string {
	if i := strings.LastIndex(listenAddr, ":"); i >= 0 {
		return listenAddr[i:]
	}
	return listenAddr
}

// openBrowser is a best-effort, cross-platform "open the default browser"
// helper; failures (e.g. headless server with no desktop) are non-fatal.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("explorer.exe", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		logging.Warnf("main", "main: could not auto-open browser (%v); dashboard is at %s", err, url)
	}
}
