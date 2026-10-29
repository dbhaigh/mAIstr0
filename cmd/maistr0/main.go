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
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/maistr0/maistr0/internal/background"
	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/fileguard"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/nodeagent"
	"github.com/maistr0/maistr0/internal/orchestrator"
	"github.com/maistr0/maistr0/internal/selfupdate"
	"github.com/maistr0/maistr0/internal/trayapp"
	buildversion "github.com/maistr0/maistr0/internal/version"
)

const startupElectionWindow = 15 * time.Second

func main() {
	if len(os.Args) > 1 && os.Args[1] == "sign-release" {
		if err := runSignRelease(os.Args[2:], os.Stdout, os.Stderr); err != nil {
			fmt.Fprintf(os.Stderr, "sign-release failed: %v\n", err)
			os.Exit(2)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "controller" {
		if err := runControllerMode(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "maistr0 controller: %v\n", err)
			os.Exit(1)
		}
		return
	}

	role := flag.String("role", "auto", "which component to run: auto, orchestrator, node, or both")
	runtimeConfigPath := flag.String("runtime-config", config.DefaultRuntimeSettingsPath(), "path to persistent runtime settings")
	nodeConfigPath := flag.String("node-config", "", "path to node JSON config file")
	orchConfigPath := flag.String("orchestrator-config", "", "path to orchestrator JSON config file")
	nodeListenAddr := flag.String("node-listen", "", "override node listen address, e.g. :7451")
	orchListenAddr := flag.String("orchestrator-listen", "", "override orchestrator listen address, e.g. :7450")
	orchestratorAddr := flag.String("orchestrator-addr", "", "override the orchestrator address the node registers with (empty = auto-discover on the LAN)")
	pairTo := flag.String("pair-to", "", "deprecated; orchestrator federation is disabled")
	pairingPIN := flag.String("pairing-pin", "", "one-time PIN displayed by the orchestrator accepting a node")
	orchestratorFingerprint := flag.String("orchestrator-fingerprint", "", "out-of-band verified SHA-256 certificate fingerprint for first-contact pairing")
	trayMode := flag.String("tray-mode", "", `override tray behavior: "taskbar" or "hidden"`)
	noBrowser := flag.Bool("no-browser", false, "do not auto-open the dashboard in a browser on startup")
	autoOpenBrowser := flag.Bool("auto-open-browser", false, "auto-open the dashboard in a browser on startup")
	noDiscovery := flag.Bool("no-discovery", false, "disable LAN auto-discovery (node-only; orchestrator election requires discovery)")
	enableDiscovery := flag.Bool("enable-discovery", false, "enable LAN auto-discovery")
	harness := flag.String("harness", "", `override orchestrator agent backend: "deepseek" or "pi"`)
	memoryPath := flag.String("memory-path", "", "override the cluster memory database file (empty = per-user data dir)")
	showVersion := flag.Bool("version", false, "print version and exit")
	printConfig := flag.Bool("print-config", false, "print normalized configuration and exit")
	diagnostics := flag.Bool("diagnostics", false, "print a redacted local health summary and exit")
	noPeerUpdates := flag.Bool("no-peer-updates", false, "disable automatic idle updates from paired peers")
	allowPeerUpdates := flag.Bool("allow-peer-updates", false, "allow paired peers to publish executable code that runs as this account (disabled by default)")
	applyPeerUpdate := flag.String("apply-peer-update", "", "internal staged executable replacement job")
	cleanupPeerUpdate := flag.String("cleanup-peer-update", "", "internal exited replacement-helper cleanup")
	hostControlAddr := flag.String("host-control-addr", "", "internal host controller address")
	hostControllerPID := flag.Int("host-controller-pid", 0, "internal host controller process ID")
	hostControllerManaged := flag.Bool("host-controller-managed", false, "internal OS service-managed controller marker")
	hostControllerRestartArgs := flag.String("host-controller-restart-args", "", "internal controller restart arguments")
	flag.Parse()
	if *showVersion {
		fmt.Printf("maistr0 %s\n", buildversion.String())
		return
	}
	var controllerRestartArgs []string
	if *hostControllerRestartArgs != "" {
		if err := json.Unmarshal([]byte(*hostControllerRestartArgs), &controllerRestartArgs); err != nil {
			fatalf("decode controller restart arguments: %v", err)
		}
		if len(controllerRestartArgs) == 0 || controllerRestartArgs[0] != "controller" {
			fatal("invalid controller restart arguments")
		}
	}
	runtimeSettings, err := config.LoadRuntimeSettings(*runtimeConfigPath)
	if err != nil {
		fatalf("failed to load runtime settings: %v", err)
	}
	savedAutoOpenBrowser := runtimeSettings.AutoOpenBrowser
	savedDiscoveryEnabled := runtimeSettings.DiscoveryEnabled
	explicitFlags := make(map[string]bool)
	flag.Visit(func(parsed *flag.Flag) { explicitFlags[parsed.Name] = true })
	if runtimeSettings.Role != nil && !explicitFlags["role"] {
		*role = *runtimeSettings.Role
	}
	if runtimeSettings.NodeConfigPath != nil && !explicitFlags["node-config"] {
		*nodeConfigPath = *runtimeSettings.NodeConfigPath
	}
	if runtimeSettings.OrchestratorConfigPath != nil && !explicitFlags["orchestrator-config"] {
		*orchConfigPath = *runtimeSettings.OrchestratorConfigPath
	}
	if runtimeSettings.NodeListenAddr != nil && !explicitFlags["node-listen"] {
		*nodeListenAddr = *runtimeSettings.NodeListenAddr
	}
	if runtimeSettings.OrchestratorListenAddr != nil && !explicitFlags["orchestrator-listen"] {
		*orchListenAddr = *runtimeSettings.OrchestratorListenAddr
	}
	if runtimeSettings.OrchestratorAddr != nil && !explicitFlags["orchestrator-addr"] {
		*orchestratorAddr = *runtimeSettings.OrchestratorAddr
	}
	if runtimeSettings.TrayMode != nil && !explicitFlags["tray-mode"] {
		*trayMode = *runtimeSettings.TrayMode
	}
	if runtimeSettings.AutoOpenBrowser != nil && !explicitFlags["no-browser"] && !explicitFlags["auto-open-browser"] {
		*noBrowser = !*runtimeSettings.AutoOpenBrowser
	}
	if explicitFlags["auto-open-browser"] && !explicitFlags["no-browser"] {
		*noBrowser = !*autoOpenBrowser
	}
	if runtimeSettings.DiscoveryEnabled != nil && !explicitFlags["no-discovery"] && !explicitFlags["enable-discovery"] {
		*noDiscovery = !*runtimeSettings.DiscoveryEnabled
	}
	if explicitFlags["enable-discovery"] && !explicitFlags["no-discovery"] {
		*noDiscovery = !*enableDiscovery
	}
	if runtimeSettings.Harness != nil && !explicitFlags["harness"] {
		*harness = *runtimeSettings.Harness
	}
	if runtimeSettings.MemoryPath != nil && !explicitFlags["memory-path"] {
		*memoryPath = *runtimeSettings.MemoryPath
	}
	if runtimeSettings.AllowPeerUpdates != nil && !explicitFlags["allow-peer-updates"] {
		*allowPeerUpdates = *runtimeSettings.AllowPeerUpdates
	}
	if runtimeSettings.NoPeerUpdates != nil && !explicitFlags["no-peer-updates"] {
		*noPeerUpdates = *runtimeSettings.NoPeerUpdates
	}
	browserEnabledOverride := !*noBrowser && (explicitFlags["auto-open-browser"] ||
		explicitFlags["no-browser"] ||
		(savedAutoOpenBrowser != nil && *savedAutoOpenBrowser))
	discoveryEnabledOverride := !*noDiscovery && (explicitFlags["enable-discovery"] ||
		explicitFlags["no-discovery"] ||
		(savedDiscoveryEnabled != nil && *savedDiscoveryEnabled))
	runtimeSettings.Role = stringPointer(*role)
	runtimeSettings.NodeConfigPath = stringPointer(*nodeConfigPath)
	runtimeSettings.OrchestratorConfigPath = stringPointer(*orchConfigPath)
	runtimeSettings.NodeListenAddr = stringPointer(*nodeListenAddr)
	runtimeSettings.OrchestratorListenAddr = stringPointer(*orchListenAddr)
	runtimeSettings.OrchestratorAddr = stringPointer(*orchestratorAddr)
	runtimeSettings.TrayMode = stringPointer(*trayMode)
	runtimeSettings.AutoOpenBrowser = boolPointer(!*noBrowser)
	runtimeSettings.DiscoveryEnabled = boolPointer(!*noDiscovery)
	runtimeSettings.Harness = stringPointer(*harness)
	runtimeSettings.MemoryPath = stringPointer(*memoryPath)
	runtimeSettings.AllowPeerUpdates = boolPointer(*allowPeerUpdates)
	runtimeSettings.NoPeerUpdates = boolPointer(*noPeerUpdates)

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
	if *diagnostics {
		if err := runDiagnostics(os.Stdout, configPrintOptions{
			role: *role, nodeConfigPath: *nodeConfigPath, orchConfigPath: *orchConfigPath,
			nodeListenAddr: *nodeListenAddr, orchListenAddr: *orchListenAddr,
			orchestratorAddr: *orchestratorAddr, pairingPIN: *pairingPIN, orchestratorFingerprint: *orchestratorFingerprint,
			trayMode: *trayMode, noBrowser: *noBrowser, autoOpenBrowser: browserEnabledOverride,
			noDiscovery: *noDiscovery, enableDiscovery: discoveryEnabledOverride,
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
			orchestratorAddr: *orchestratorAddr, pairingPIN: *pairingPIN, orchestratorFingerprint: *orchestratorFingerprint,
			trayMode: *trayMode, noBrowser: *noBrowser, autoOpenBrowser: browserEnabledOverride,
			noDiscovery: *noDiscovery, enableDiscovery: discoveryEnabledOverride,
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
	runCtx, stopRuntime := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopRuntime()
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
		electionOrchCfg.AutoOpenBrowser = resolveAutoOpenBrowser(electionOrchCfg.AutoOpenBrowser, savedAutoOpenBrowser, explicitFlags, *noBrowser, *autoOpenBrowser)
		electionOrchCfg.DiscoveryEnabled = resolveDiscoveryEnabled(electionOrchCfg.DiscoveryEnabled, savedDiscoveryEnabled, explicitFlags, *noDiscovery, *enableDiscovery)
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
		if *orchestratorFingerprint != "" {
			electionNodeCfg.OrchestratorFingerprint = *orchestratorFingerprint
		}
		if *trayMode != "" {
			electionNodeCfg.TrayMode = *trayMode
		}
		electionNodeCfg.Normalize()
		electionNodeCfg.AutoOpenBrowser = resolveAutoOpenBrowser(electionNodeCfg.AutoOpenBrowser, savedAutoOpenBrowser, explicitFlags, *noBrowser, *autoOpenBrowser)
		electionNodeCfg.DiscoveryEnabled = resolveDiscoveryEnabled(electionNodeCfg.DiscoveryEnabled, savedDiscoveryEnabled, explicitFlags, *noDiscovery, *enableDiscovery)
		if err := electionNodeCfg.Validate(); err != nil {
			exitConfigValidation(err)
		}
		if electionOrchCfg.DiscoveryHMACSecret != electionNodeCfg.DiscoveryHMACSecret {
			fatal("node and orchestrator discovery_hmac_secret must match in auto or both roles")
		}
		ip := discovery.OutboundIP()
		candidate := discovery.Announcement{
			Role: "candidate", ID: "orchestrator-" + ip,
			PeerAddr: "https://" + ip + portSuffix(electionOrchCfg.ListenAddr),
			Score:    cluster.SelfScore(hardware.Detect()),
		}
		probe := discovery.ListenWithSecret(electionOrchCfg.DiscoveryHMACKey(), candidate.ID)
		if err := probe.WaitReady(2 * time.Second); err != nil {
			probe.Close()
			fatalf("cannot elect a single orchestrator without LAN discovery: %v", err)
		}
		stop := make(chan struct{})
		beaconDone := make(chan struct{})
		go func() {
			defer close(beaconDone)
			discovery.BeaconWithSecret(func() discovery.Announcement { return candidate }, 250*time.Millisecond, stop, electionOrchCfg.DiscoveryHMACKey())
		}()
		logging.Infof("main", "main: checking for an active orchestrator and startup candidates for %s", startupElectionWindow)
		started := background.Wait(runCtx, startupElectionWindow)
		close(stop)
		<-beaconDone
		peers := probe.Snapshot()
		probe.Close()
		if !started {
			return
		}
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
	var updatePublisherKey []byte
	sensitiveFilePaths := []string{logFilePath}

	switch *role {
	case "node":
		cfg := loadNodeConfig(*nodeConfigPath, *nodeListenAddr, *orchestratorAddr, *noDiscovery)
		if *pairingPIN != "" {
			cfg.PairingPIN = *pairingPIN
		}
		if *orchestratorFingerprint != "" {
			cfg.OrchestratorFingerprint = *orchestratorFingerprint
		}
		if *memoryPath != "" {
			cfg.MemoryPath = *memoryPath
		}
		if *trayMode != "" {
			cfg.TrayMode = *trayMode
		}
		cfg.Normalize()
		cfg.AutoOpenBrowser = resolveAutoOpenBrowser(cfg.AutoOpenBrowser, savedAutoOpenBrowser, explicitFlags, *noBrowser, *autoOpenBrowser)
		cfg.DiscoveryEnabled = resolveDiscoveryEnabled(cfg.DiscoveryEnabled, savedDiscoveryEnabled, explicitFlags, *noDiscovery, *enableDiscovery)
		cfg.ControlAddress = *hostControlAddr
		if err := cfg.Validate(); err != nil {
			exitConfigValidation(err)
		}
		runtimeSettings.AutoOpenBrowser = boolPointer(cfg.AutoOpenBrowser)
		runtimeSettings.DiscoveryEnabled = boolPointer(cfg.DiscoveryEnabled)
		runtimeSettings.TrayMode = stringPointer(cfg.TrayMode)
		runtimeSettings.Harness = stringPointer(config.DefaultOrchestratorConfig().Harness)
		updatePublisherKey = cfg.TrustedPeerUpdatePublicKey()
		if err := logging.SetLevel(cfg.LogLevel); err != nil {
			fatalf("configure log level: %v", err)
		}
		agent, err := nodeagent.NewWithPersistentTasks(cfg)
		if err != nil {
			fatalf("failed to open node task history: %v", err)
		}
		agent.SetRuntimeSettings(*runtimeConfigPath, runtimeSettings)
		agent.SetShutdownHandler(stopRuntime)
		if err := agent.InitializeIdentity(); err != nil {
			fatalf("failed to initialize node identity: %v", err)
		}
		sensitiveFilePaths = append(sensitiveFilePaths, agent.IdentityPath())
		updateNode = agent
		var nodeDiscovery *discovery.Listener
		if cfg.DiscoveryEnabled {
			nodeDiscovery = discovery.ListenWithSecret(cfg.DiscoveryHMACKey(), agent.ID())
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
			runtimeSettings.Harness = stringPointer(orchCfg.Harness)
			if err := orchCfg.Validate(); err != nil {
				exitConfigValidation(err)
			}
			if cfg.DiscoveryHMACSecret != orchCfg.DiscoveryHMACSecret {
				fatal("node and orchestrator discovery_hmac_secret must match for failover")
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
		runtimeSettings.AutoOpenBrowser = boolPointer(orchCfg.AutoOpenBrowser)
		runtimeSettings.DiscoveryEnabled = boolPointer(orchCfg.DiscoveryEnabled)
		runtimeSettings.TrayMode = stringPointer(orchCfg.TrayMode)
		runtimeSettings.Harness = stringPointer(orchCfg.Harness)
		updatePublisherKey = nodeCfg.TrustedPeerUpdatePublicKey()

		// The local worker must register with this process's orchestrator,
		// regardless of any saved or command-line node address.
		nodeCfg.OrchestratorAddr = "https://" + discovery.OutboundIP() + portSuffix(orchCfg.ListenAddr)
		nodeCfg.ControlAddress = *hostControlAddr
		nodeCfg.Normalize()
		if err := nodeCfg.Validate(); err != nil {
			exitConfigValidation(err)
		}
		srv, err := orchestrator.NewWithConfig(orchCfg)
		if err != nil {
			fatalf("failed to configure orchestrator agent: %v", err)
		}
		requirePairingVerification := runtimeSettings.RequirePairingVerification != nil &&
			*runtimeSettings.RequirePairingVerification
		srv.SetPairingVerificationRequired(requirePairingVerification)
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
		agent.SetRuntimeSettings(*runtimeConfigPath, runtimeSettings)
		srv.SetPairingPolicyUpdater(agent.SetPairingVerificationRequired)
		agent.SetShutdownHandler(stopRuntime)
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
		sensitiveFilePaths = append(sensitiveFilePaths, srv.IdentityPath(), agent.IdentityPath())
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
			shared := discovery.ListenWithSecret(orchCfg.DiscoveryHMACKey(), srv.SelfID(), agent.ID())
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

	warnSensitiveFileACLs(sensitiveFilePaths...)

	if *trayMode != "" {
		effectiveTrayMode = *trayMode
	}
	if *noBrowser {
		autoOpen = false
	}

	if autoOpen {
		go func() {
			if background.Wait(runCtx, 750*time.Millisecond) {
				openBrowser(dashboardURL)
			}
		}()
	}

	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			stopRuntime()
			if closeNode != nil {
				closeNode()
			}
			if closeOrchestrator != nil {
				closeOrchestrator()
			}
		})
		os.Exit(0)
	}
	updaterDone := make(chan struct{})
	if updateNode != nil && *allowPeerUpdates && !*noPeerUpdates {
		go func() {
			defer close(updaterDone)
			selfupdate.RunWithControllerHandoff(runCtx, func() selfupdate.Source {
				source := updateNode.UpdateSource()
				source.PublisherPublicKey = updatePublisherKey
				if updateOrchestrator != nil {
					source.Peers = append(source.Peers, updateOrchestrator.UpdateSource().Peers...)
				}
				return source
			}, func() bool {
				return updateNode.UpdateIdle() &&
					(updateOrchestrator == nil || updateOrchestrator.UpdateIdle()) &&
					(updateFailover == nil || updateFailover.UpdateIdle())
			}, shutdown, selfupdate.ControllerHandoff{
				ControllerPID:  *hostControllerPID,
				ServiceManaged: *hostControllerManaged,
				RestartArgs:    controllerRestartArgs,
			})
		}()
	} else {
		close(updaterDone)
	}
	quit := func() {
		stopRuntime()
		<-updaterDone
		shutdown()
	}
	go func() {
		<-runCtx.Done()
		quit()
	}()
	if *hostControlAddr != "" {
		<-runCtx.Done()
		return
	}
	trayapp.Run(trayapp.Options{
		Title:        "mAIstr0 (" + *role + ")",
		Mode:         effectiveTrayMode,
		DashboardURL: dashboardURL,
		LogFilePath:  logFilePath,
		LogDirectory: filepath.Dir(logFilePath),
		OnQuit:       quit,
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

func stringPointer(value string) *string { return &value }

func boolPointer(value bool) *bool { return &value }

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

func warnSensitiveFileACLs(paths ...string) {
	seen := make(map[string]struct{}, len(paths))
	files := append([]string(nil), paths...)
	for _, path := range paths {
		if filepath.Ext(path) != ".log" {
			continue
		}
		pattern := filepath.Join(filepath.Dir(path), strings.TrimSuffix(filepath.Base(path), ".log")+"-*.log")
		archives, err := filepath.Glob(pattern)
		if err != nil {
			logging.Warnf("main", "main: could not enumerate rotated log files in %s: %v", filepath.Dir(path), err)
			continue
		}
		files = append(files, archives...)
	}
	for _, path := range files {
		path = filepath.Clean(path)
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		broad, err := fileguard.HasBroadACL(path)
		if err != nil {
			logging.Warnf("main", "main: could not inspect Windows ACL for %s; review its permissions manually: %v", path, err)
			continue
		}
		if broad {
			logging.Warnf("main", "main: %s grants access to a broad Windows principal; restrict its ACL to the current user and SYSTEM", path)
		}
	}
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
	role                    string
	nodeConfigPath          string
	orchConfigPath          string
	nodeListenAddr          string
	orchListenAddr          string
	orchestratorAddr        string
	pairingPIN              string
	orchestratorFingerprint string
	trayMode                string
	noBrowser               bool
	autoOpenBrowser         bool
	noDiscovery             bool
	enableDiscovery         bool
	harness                 string
	memoryPath              string
}

func resolveAutoOpenBrowser(current bool, saved *bool, explicit map[string]bool, noBrowser, autoOpenBrowser bool) bool {
	if explicit["no-browser"] {
		return !noBrowser
	}
	if explicit["auto-open-browser"] {
		return autoOpenBrowser
	}
	if saved != nil {
		return *saved
	}
	if noBrowser {
		return false
	}
	return current
}

func resolveDiscoveryEnabled(current bool, saved *bool, explicit map[string]bool, noDiscovery, enableDiscovery bool) bool {
	if explicit["no-discovery"] {
		return !noDiscovery
	}
	if explicit["enable-discovery"] {
		return enableDiscovery
	}
	if saved != nil {
		return *saved
	}
	if noDiscovery {
		return false
	}
	return current
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
		if options.orchestratorFingerprint != "" {
			cfg.OrchestratorFingerprint = options.orchestratorFingerprint
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
		if options.enableDiscovery {
			cfg.DiscoveryEnabled = true
		}
		if options.noBrowser {
			cfg.AutoOpenBrowser = false
		}
		if options.autoOpenBrowser {
			cfg.AutoOpenBrowser = true
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
		cfg, err = redactNodeConfig(cfg)
		if err != nil {
			return err
		}
		return writeNormalizedConfig(out, struct {
			Node config.NodeConfig `json:"node"`
		}{Node: cfg})
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
	if options.autoOpenBrowser {
		orchCfg.AutoOpenBrowser = true
	}
	if options.noDiscovery {
		orchCfg.DiscoveryEnabled = false
	}
	if options.enableDiscovery {
		orchCfg.DiscoveryEnabled = true
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
	if options.orchestratorFingerprint != "" {
		nodeCfg.OrchestratorFingerprint = options.orchestratorFingerprint
	}
	if options.trayMode != "" {
		nodeCfg.TrayMode = options.trayMode
	}
	if options.noBrowser {
		nodeCfg.AutoOpenBrowser = false
	}
	if options.autoOpenBrowser {
		nodeCfg.AutoOpenBrowser = true
	}
	if options.noDiscovery {
		nodeCfg.DiscoveryEnabled = false
	}
	if options.enableDiscovery {
		nodeCfg.DiscoveryEnabled = true
	}
	nodeCfg.Normalize()
	if err := nodeCfg.Validate(); err != nil {
		return fmt.Errorf("node config: %w", err)
	}
	nodeCfg, err = redactNodeConfig(nodeCfg)
	if err != nil {
		return err
	}
	orchCfg = redactOrchestratorConfig(orchCfg)
	return writeNormalizedConfig(out, struct {
		Node         config.NodeConfig         `json:"node"`
		Orchestrator config.OrchestratorConfig `json:"orchestrator"`
	}{Node: nodeCfg, Orchestrator: orchCfg})
}

func redactNodeConfig(cfg config.NodeConfig) (config.NodeConfig, error) {
	if cfg.PairingPIN != "" {
		cfg.PairingPIN = "[REDACTED]"
	}
	if cfg.DiscoveryHMACSecret != "" {
		cfg.DiscoveryHMACSecret = "[REDACTED]"
	}
	if cfg.PeerUpdatePublisherKey != "" {
		cfg.PeerUpdatePublisherKey = "[REDACTED]"
	}
	cfg.Engines = append([]config.EngineConfig(nil), cfg.Engines...)
	urls := []*string{&cfg.OrchestratorAddr, &cfg.AdvertiseAddr}
	for index := range cfg.Engines {
		if cfg.Engines[index].APIKey != "" {
			cfg.Engines[index].APIKey = "[REDACTED]"
		}
		urls = append(urls, &cfg.Engines[index].URL, &cfg.Engines[index].RegistryURL)
	}
	for _, value := range urls {
		if *value == "" {
			continue
		}
		redacted, err := httpx.RedactedURL(*value)
		if err != nil {
			return config.NodeConfig{}, fmt.Errorf("redact configuration endpoint: %w", err)
		}
		*value = redacted
	}
	return cfg, nil
}

func redactOrchestratorConfig(cfg config.OrchestratorConfig) config.OrchestratorConfig {
	if cfg.DiscoveryHMACSecret != "" {
		cfg.DiscoveryHMACSecret = "[REDACTED]"
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
