// Command maistr0 runs either the cluster orchestrator, a node agent, or
// both in a single process (handy for local testing on one machine). It
// also drives the optional system tray presence (Windows) and best-effort
// auto-opens the dashboard in the default browser on startup.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/nodeagent"
	"github.com/maistr0/maistr0/internal/orchestrator"
	"github.com/maistr0/maistr0/internal/trayapp"
	buildversion "github.com/maistr0/maistr0/internal/version"
)

func main() {
	role := flag.String("role", "auto", "which component to run: auto, orchestrator, node, or both")
	nodeConfigPath := flag.String("node-config", "", "path to node JSON config file")
	orchConfigPath := flag.String("orchestrator-config", "", "path to orchestrator JSON config file")
	nodeListenAddr := flag.String("node-listen", "", "override node listen address, e.g. :7451")
	orchListenAddr := flag.String("orchestrator-listen", "", "override orchestrator listen address, e.g. :7450")
	orchestratorAddr := flag.String("orchestrator-addr", "", "override the orchestrator address the node registers with (empty = auto-discover on the LAN)")
	pairTo := flag.String("pair-to", "", "pair this orchestrator with another at an HTTPS address; provide its active PIN with --pairing-pin")
	pairingPIN := flag.String("pairing-pin", "", "one-time PIN displayed by the orchestrator accepting a node or orchestrator pairing")
	trayMode := flag.String("tray-mode", "", `override tray behavior: "taskbar" or "hidden"`)
	noBrowser := flag.Bool("no-browser", false, "do not auto-open the dashboard in a browser on startup")
	noDiscovery := flag.Bool("no-discovery", false, "disable LAN auto-discovery of other orchestrators/nodes")
	harness := flag.String("harness", "", `override orchestrator agent backend: "deepseek" or "pi"`)
	memoryPath := flag.String("memory-path", "", "override the cluster memory database file (empty = per-user data dir)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		log.Printf("maistr0 %s", buildversion.String())
		return
	}
	logFile, logFilePath, err := configureLogging()
	if err != nil {
		log.Fatalf("failed to initialize application log: %v", err)
	}
	defer logFile.Close()
	if *role == "auto" {
		if *pairTo != "" {
			*role = "orchestrator"
		} else if *orchestratorAddr != "" {
			*role = "node"
		} else {
			probe := discovery.Listen("startup-probe-" + discovery.OutboundIP())
			found := probe.FirstOrchestrator(4 * time.Second)
			probe.Close()
			if found != "" {
				*role = "node"
				*orchestratorAddr = found
			} else {
				*role = "both"
			}
		}
		if *pairTo != "" && *role != "orchestrator" && *role != "both" {
			log.Fatal("--pair-to can only be used with --role orchestrator or --role both")
		}
	}

	var dashboardURL, effectiveTrayMode string
	var autoOpen bool
	var closeOrchestrator func()

	switch *role {
	case "orchestrator":
		cfg := loadOrchestratorConfig(*orchConfigPath, *orchListenAddr, *noDiscovery)
		if *memoryPath != "" {
			cfg.MemoryPath = *memoryPath
		}
		if *harness != "" {
			cfg.Harness = *harness
		}
		srv, err := orchestrator.NewWithMemoryAndHarness(cfg.MemoryPath, cfg.Harness)
		if err != nil {
			log.Fatalf("failed to configure orchestrator agent: %v", err)
		}
		closeOrchestrator = func() {
			if err := srv.Close(); err != nil {
				log.Printf("orchestrator shutdown: %v", err)
			}
		}
		srv.SetDiscoveryEnabled(cfg.DiscoveryEnabled)
		if cfg.DiscoveryEnabled {
			srv.SetDiscovery(discovery.Listen(srv.SelfID()))
		}
		if *pairTo != "" {
			if *pairingPIN == "" {
				log.Fatal("--pairing-pin is required with --pair-to")
			}
			if err := srv.SetAdvertiseAddress("https://" + discovery.OutboundIP() + portSuffix(cfg.ListenAddr)); err != nil {
				log.Fatalf("invalid orchestrator advertised address: %v", err)
			}
			if err := srv.InitializeIdentity(); err != nil {
				log.Fatalf("failed to initialize orchestrator identity: %v", err)
			}
			if err := srv.PairWithOrchestrator(*pairTo, *pairingPIN); err != nil {
				log.Fatalf("failed to pair orchestrator: %v", err)
			}
		}
		dashboardURL = localURL(cfg.ListenAddr)
		effectiveTrayMode, autoOpen = cfg.TrayMode, cfg.AutoOpenBrowser
		go runOrchestrator(srv, cfg.ListenAddr)

	case "node":
		cfg := loadNodeConfig(*nodeConfigPath, *nodeListenAddr, *orchestratorAddr, *noDiscovery)
		if *pairingPIN != "" {
			cfg.PairingPIN = *pairingPIN
		}
		if *memoryPath != "" {
			cfg.MemoryPath = *memoryPath
		}
		agent := nodeagent.New(cfg)
		if cfg.DiscoveryEnabled {
			agent.SetDiscovery(discovery.Listen(agent.ID()))
		}
		dashboardURL = localURL(cfg.ListenAddr)
		effectiveTrayMode, autoOpen = cfg.TrayMode, cfg.AutoOpenBrowser
		go runNode(agent)

	case "both":
		orchCfg := loadOrchestratorConfig(*orchConfigPath, *orchListenAddr, *noDiscovery)
		nodeCfg := loadNodeConfig(*nodeConfigPath, *nodeListenAddr, *orchestratorAddr, *noDiscovery)
		if *pairingPIN != "" {
			nodeCfg.PairingPIN = *pairingPIN
		}

		// Same-process pairing doesn't need discovery, but wire the node to
		// the orchestrator's LAN address (not 127.0.0.1) so registrations
		// and logs always reference an address other machines can reach.
		if nodeCfg.OrchestratorAddr == "" {
			nodeCfg.OrchestratorAddr = "https://" + discovery.OutboundIP() + portSuffix(orchCfg.ListenAddr)
		}

		// Both roles share one process but need separate database files.
		if *memoryPath != "" {
			orchCfg.MemoryPath = *memoryPath
		}
		if *harness != "" {
			orchCfg.Harness = *harness
		}
		srv, err := orchestrator.NewWithMemoryAndHarness(orchCfg.MemoryPath, orchCfg.Harness)
		if err != nil {
			log.Fatalf("failed to configure orchestrator agent: %v", err)
		}
		closeOrchestrator = func() {
			if err := srv.Close(); err != nil {
				log.Printf("orchestrator shutdown: %v", err)
			}
		}
		srv.SetDiscoveryEnabled(orchCfg.DiscoveryEnabled)
		agent := nodeagent.New(nodeCfg)
		if err := srv.InitializeIdentity(); err != nil {
			log.Fatalf("failed to initialize orchestrator identity: %v", err)
		}
		if *pairTo != "" {
			if *pairingPIN == "" {
				log.Fatal("--pairing-pin is required with --pair-to")
			}
			if err := srv.SetAdvertiseAddress("https://" + discovery.OutboundIP() + portSuffix(orchCfg.ListenAddr)); err != nil {
				log.Fatalf("invalid orchestrator advertised address: %v", err)
			}
			if err := srv.PairWithOrchestrator(*pairTo, *pairingPIN); err != nil {
				log.Fatalf("failed to pair orchestrator: %v", err)
			}
		}
		if err := agent.InitializeIdentity(); err != nil {
			log.Fatalf("failed to initialize node identity: %v", err)
		}
		orchestratorCertificate, err := srv.IdentityCertificate()
		if err != nil {
			log.Fatalf("failed to read orchestrator identity: %v", err)
		}
		nodeCertificate, err := agent.IdentityCertificate()
		if err != nil {
			log.Fatalf("failed to read node identity: %v", err)
		}
		if err := agent.PairLocalOrchestrator(srv.SelfID(), orchestratorCertificate, nodeCfg.OrchestratorAddr); err != nil {
			log.Fatalf("failed to pair local node with orchestrator: %v", err)
		}
		if err := srv.TrustPeer(agent.ID(), nodeCertificate); err != nil {
			log.Fatalf("failed to pair orchestrator with local node: %v", err)
		}

		// A single process running both roles must share one UDP discovery
		// socket instead of each binding its own (which would conflict).
		// This is only used to find *other* peers on the LAN now.
		if orchCfg.DiscoveryEnabled || nodeCfg.DiscoveryEnabled {
			shared := discovery.Listen(srv.SelfID(), agent.ID())
			srv.SetDiscovery(shared)
			agent.SetDiscovery(shared)
		}

		dashboardURL = localURL(orchCfg.ListenAddr)
		effectiveTrayMode, autoOpen = orchCfg.TrayMode, orchCfg.AutoOpenBrowser
		go runNode(agent)
		go runOrchestrator(srv, orchCfg.ListenAddr)

	default:
		log.Fatalf("unknown --role %q (want orchestrator, node, or both)", *role)
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

	trayapp.Run(trayapp.Options{
		Title:        "mAIstr0 (" + *role + ")",
		Mode:         effectiveTrayMode,
		DashboardURL: dashboardURL,
		LogFilePath:  logFilePath,
		LogDirectory: filepath.Dir(logFilePath),
		OnQuit: func() {
			if closeOrchestrator != nil {
				closeOrchestrator()
			}
			os.Exit(0)
		},
	})
}

func configureLogging() (*logging.DailyFile, string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, "", fmt.Errorf("locate user config directory: %w", err)
	}
	logDir := filepath.Join(configDir, "maistr0")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, "", fmt.Errorf("create log directory: %w", err)
	}
	path := filepath.Join(logDir, "maistr0.log")
	file, err := logging.OpenDailyFile(path, 7)
	if err != nil {
		return nil, "", fmt.Errorf("open rolling log %s: %w", path, err)
	}
	log.SetOutput(io.MultiWriter(os.Stderr, file))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("maistr0: logging to %s", path)
	return file, path, nil
}

func loadOrchestratorConfig(path, listenOverride string, noDiscovery bool) config.OrchestratorConfig {
	cfg, err := config.LoadOrchestratorConfig(path)
	if err != nil {
		log.Fatalf("failed to load orchestrator config: %v", err)
	}
	if listenOverride != "" {
		cfg.ListenAddr = listenOverride
	}
	if noDiscovery {
		cfg.DiscoveryEnabled = false
	}
	return cfg
}

func loadNodeConfig(path, listenOverride, orchestratorOverride string, noDiscovery bool) config.NodeConfig {
	cfg, err := config.LoadNodeConfig(path)
	if err != nil {
		log.Fatalf("failed to load node config: %v", err)
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
	if cfg.NodeID == "" {
		cfg.NodeID = nodeagent.DefaultNodeID()
	}
	return cfg
}

func runOrchestrator(srv *orchestrator.Server, listenAddr string) {
	if err := srv.Run(listenAddr); err != nil {
		log.Fatalf("orchestrator server stopped: %v", err)
	}
}

func runNode(agent *nodeagent.Agent) {
	if err := agent.Run(); err != nil {
		log.Fatalf("node agent stopped: %v", err)
	}
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
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		log.Printf("main: could not auto-open browser (%v); dashboard is at %s", err, url)
	}
}
