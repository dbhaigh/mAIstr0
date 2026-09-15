// Command maistr0 runs either the cluster orchestrator, a node agent, or
// both in a single process (handy for local testing on one machine). It
// also drives the optional system tray presence (Windows) and best-effort
// auto-opens the dashboard in the default browser on startup.
package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
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
	trayMode := flag.String("tray-mode", "", `override tray behavior: "taskbar" or "hidden"`)
	noBrowser := flag.Bool("no-browser", false, "do not auto-open the dashboard in a browser on startup")
	noDiscovery := flag.Bool("no-discovery", false, "disable LAN auto-discovery of other orchestrators/nodes")
	harness := flag.String("harness", "", `override orchestrator harness: "deepseek"`)
	memoryPath := flag.String("memory-path", "", "override the cluster memory database file (empty = per-user data dir)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		log.Printf("maistr0 %s", buildversion.String())
		return
	}
	if *role == "auto" {
		if *orchestratorAddr != "" {
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
	}

	var dashboardURL, effectiveTrayMode string
	var autoOpen bool

	switch *role {
	case "orchestrator":
		cfg := loadOrchestratorConfig(*orchConfigPath, *orchListenAddr, *noDiscovery)
		if *memoryPath != "" {
			cfg.MemoryPath = *memoryPath
		}
		if *harness != "" {
			cfg.Harness = *harness
		}
		srv := orchestrator.NewWithMemoryAndHarness(cfg.MemoryPath, cfg.Harness)
		srv.SetDiscoveryEnabled(cfg.DiscoveryEnabled)
		if cfg.DiscoveryEnabled {
			srv.SetDiscovery(discovery.Listen(srv.SelfID()))
		}
		dashboardURL = localURL(cfg.ListenAddr)
		effectiveTrayMode, autoOpen = cfg.TrayMode, cfg.AutoOpenBrowser
		go runOrchestrator(srv, cfg.ListenAddr)

	case "node":
		cfg := loadNodeConfig(*nodeConfigPath, *nodeListenAddr, *orchestratorAddr, *noDiscovery)
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

		// Same-process pairing doesn't need discovery, but wire the node to
		// the orchestrator's LAN address (not 127.0.0.1) so registrations
		// and logs always reference an address other machines can reach.
		if nodeCfg.OrchestratorAddr == "" {
			nodeCfg.OrchestratorAddr = "http://" + discovery.OutboundIP() + portSuffix(orchCfg.ListenAddr)
		}

		// Both roles share one process but need separate database files.
		if *memoryPath != "" {
			orchCfg.MemoryPath = *memoryPath
		}
		if *harness != "" {
			orchCfg.Harness = *harness
		}

		srv := orchestrator.NewWithMemoryAndHarness(orchCfg.MemoryPath, orchCfg.Harness)
		srv.SetDiscoveryEnabled(orchCfg.DiscoveryEnabled)
		agent := nodeagent.New(nodeCfg)

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
		OnQuit:       func() { os.Exit(0) },
	})
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
