package nodecontrol

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/nodeagent"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/selfupdate"
)

type Options struct {
	ExecutablePath      string
	RuntimeSettingsPath string
	NodeConfigPath      string
	ListenAddr          string
	RestartArgs         []string
	ServiceManaged      bool
}

type Controller struct {
	mu       sync.RWMutex
	actionMu sync.Mutex

	executablePath   string
	runtimePath      string
	nodeConfigPath   string
	listenAddr       string
	localNodeURL     string
	controlAddress   string
	nodeID           string
	discoveryKey     []byte
	discoveryEnabled bool
	runtimeSettings  config.RuntimeSettings
	restartArgs      []string
	serviceManaged   bool
	identity         *peeridentity.Identity
	discovery        *discovery.Listener
	child            *childProcess
	desiredRunning   bool
	operation        string
	lastError        string
	wake             chan struct{}
}

type childProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

type serviceStatus struct {
	Running        bool   `json:"running"`
	DesiredRunning bool   `json:"desired_running"`
	Operation      string `json:"operation,omitempty"`
	Draining       bool   `json:"draining,omitempty"`
	ActiveTasks    int    `json:"active_tasks,omitempty"`
	ControlAddress string `json:"control_address"`
	Error          string `json:"error,omitempty"`
}

var errPeerUpdateHandoff = errors.New("controller stopping for peer-update handoff")

func Run(ctx context.Context, options Options) error {
	controller, err := newController(options)
	if err != nil {
		return err
	}
	return controller.run(ctx)
}

func newController(options Options) (*Controller, error) {
	if options.RuntimeSettingsPath == "" {
		options.RuntimeSettingsPath = config.DefaultRuntimeSettingsPath()
	}
	if options.ListenAddr == "" {
		options.ListenAddr = ":7453"
	}
	if options.ExecutablePath == "" {
		return nil, errors.New("nodecontrol: application executable path is required")
	}
	settings, err := config.LoadRuntimeSettings(options.RuntimeSettingsPath)
	if err != nil {
		return nil, fmt.Errorf("nodecontrol: load runtime settings: %w", err)
	}
	if settings.NodeConfigPath != nil && *settings.NodeConfigPath != "" {
		options.NodeConfigPath = *settings.NodeConfigPath
	}
	orchestratorConfigPath := ""
	if settings.OrchestratorConfigPath != nil {
		orchestratorConfigPath = *settings.OrchestratorConfigPath
	}
	orchestratorConfig, err := config.LoadOrchestratorConfig(orchestratorConfigPath)
	if err != nil {
		return nil, fmt.Errorf("nodecontrol: load orchestrator config: %w", err)
	}
	nodeConfig, err := config.LoadNodeConfig(options.NodeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("nodecontrol: load node config: %w", err)
	}
	if settings.OrchestratorListenAddr != nil && *settings.OrchestratorListenAddr != "" {
		orchestratorConfig.ListenAddr = *settings.OrchestratorListenAddr
	}
	if settings.Harness != nil && *settings.Harness != "" {
		orchestratorConfig.Harness = *settings.Harness
	}
	if settings.TrayMode != nil && *settings.TrayMode != "" {
		orchestratorConfig.TrayMode = *settings.TrayMode
	}
	if settings.MemoryPath != nil && *settings.MemoryPath != "" {
		orchestratorConfig.MemoryPath = *settings.MemoryPath
	}
	orchestratorConfig.Normalize()
	if settings.NodeListenAddr != nil && *settings.NodeListenAddr != "" {
		nodeConfig.ListenAddr = *settings.NodeListenAddr
	}
	if settings.MemoryPath != nil && *settings.MemoryPath != "" {
		nodeConfig.MemoryPath = *settings.MemoryPath
	}
	if settings.DiscoveryEnabled != nil {
		nodeConfig.DiscoveryEnabled = *settings.DiscoveryEnabled
	}
	if nodeConfig.NodeID == "" {
		nodeConfig.NodeID = nodeagent.DefaultNodeID()
	}
	nodeConfig.Normalize()
	if nodeConfig.AdvertiseAddr == "" {
		_, port, splitErr := net.SplitHostPort(nodeConfig.ListenAddr)
		if splitErr != nil {
			return nil, fmt.Errorf("nodecontrol: determine node advertise address: %w", splitErr)
		}
		nodeConfig.AdvertiseAddr = "https://" + discovery.OutboundIP() + ":" + port
	}
	if err := nodeConfig.Validate(); err != nil {
		return nil, fmt.Errorf("nodecontrol: validate node config: %w", err)
	}
	localURL, err := loopbackNodeURL(nodeConfig.ListenAddr)
	if err != nil {
		return nil, err
	}
	controlAddress, err := advertisedControlAddress(nodeConfig.AdvertiseAddr, options.ListenAddr)
	if err != nil {
		return nil, err
	}
	identityPath := peeridentity.DefaultPath(nodeConfig.NodeID)
	if nodeConfig.MemoryPath != "" {
		identityPath = nodeConfig.MemoryPath + ".identity"
	}
	identity, err := peeridentity.Open(identityPath, nodeConfig.NodeID)
	if err != nil {
		return nil, fmt.Errorf("nodecontrol: open shared node identity: %w", err)
	}
	settings = completeRuntimeSettings(settings, options.NodeConfigPath, nodeConfig, orchestratorConfig)
	if len(options.RestartArgs) == 0 {
		options.RestartArgs = []string{"controller", "--runtime-config", options.RuntimeSettingsPath, "--listen", options.ListenAddr}
		if options.NodeConfigPath != "" {
			options.RestartArgs = append(options.RestartArgs, "--node-config", options.NodeConfigPath)
		}
		if options.ServiceManaged {
			options.RestartArgs = append(options.RestartArgs, "--service-managed")
		}
	}
	return &Controller{
		executablePath:   options.ExecutablePath,
		runtimePath:      options.RuntimeSettingsPath,
		nodeConfigPath:   options.NodeConfigPath,
		listenAddr:       options.ListenAddr,
		localNodeURL:     localURL,
		controlAddress:   controlAddress,
		nodeID:           nodeConfig.NodeID,
		discoveryKey:     nodeConfig.DiscoveryHMACKey(),
		discoveryEnabled: *settings.DiscoveryEnabled,
		runtimeSettings:  settings,
		restartArgs:      append([]string(nil), options.RestartArgs...),
		serviceManaged:   options.ServiceManaged,
		identity:         identity,
		desiredRunning:   true,
		operation:        "starting",
		wake:             make(chan struct{}, 1),
	}, nil
}

func (c *Controller) run(ctx context.Context) error {
	listener, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		return fmt.Errorf("nodecontrol: listen on %s: %w", c.listenAddr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/service/status", c.handleStatus)
	mux.HandleFunc("POST /api/service/{action}", c.handleAction)
	mux.HandleFunc("GET /api/runtime-settings", c.handleRuntimeSettings)
	mux.HandleFunc("PATCH /api/runtime-settings", c.handleRuntimeSettings)
	handler := c.identity.RequireTrustedPeer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !c.authorizeCurrentOrchestrator(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	}))
	server := &http.Server{
		Handler:   handler,
		TLSConfig: c.identity.ServerTLSConfig(),
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(tls.NewListener(listener, server.TLSConfig)) }()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	supervisorDone := make(chan error, 1)
	go func() { supervisorDone <- c.supervise(runCtx) }()

	var serverErr error
	supervisorStopped := false
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr = err
		}
	case err := <-supervisorDone:
		supervisorStopped = true
		if err != nil {
			serverErr = err
		}
	}
	cancel()
	c.stopChild()
	if !supervisorStopped {
		if err := <-supervisorDone; err != nil {
			serverErr = errors.Join(serverErr, err)
		}
	}
	c.stopDiscovery()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		serverErr = errors.Join(serverErr, fmt.Errorf("shut down control server: %w", err))
	}
	if serverErr != nil {
		return fmt.Errorf("nodecontrol: controller stopped: %w", serverErr)
	}
	return nil
}

func (c *Controller) supervise(ctx context.Context) error {
	for {
		c.mu.RLock()
		desired, child := c.desiredRunning, c.child
		c.mu.RUnlock()
		if desired && child == nil {
			if err := c.startChild(); err != nil {
				c.setError(err)
				c.mu.Lock()
				c.desiredRunning = false
				c.mu.Unlock()
				c.startDiscovery()
				continue
			}
			continue
		}
		if !desired && child == nil {
			c.startDiscovery()
			select {
			case <-ctx.Done():
				return nil
			case <-c.wake:
			}
			continue
		}
		if child != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-child.done:
				handoff, err := selfupdate.ControllerHandoffRequested(c.executablePath, os.Getpid(), child.cmd.Process.Pid)
				if err != nil {
					return fmt.Errorf("check peer-update handoff: %w", err)
				}
				if handoff {
					c.mu.Lock()
					if c.child == child {
						c.child = nil
					}
					c.desiredRunning = false
					c.operation = "updating"
					c.mu.Unlock()
					return errPeerUpdateHandoff
				}
				c.mu.Lock()
				if c.child == child {
					c.child = nil
				}
				shouldRestart := c.desiredRunning
				if !shouldRestart && c.operation == "" {
					c.operation = ""
				}
				c.mu.Unlock()
				if shouldRestart {
					timer := time.NewTimer(time.Second)
					select {
					case <-ctx.Done():
						timer.Stop()
						return nil
					case <-timer.C:
					}
				} else {
					c.startDiscovery()
				}
			case <-c.wake:
			}
		}
	}
}

func (c *Controller) startChild() error {
	c.stopDiscovery()
	args, err := childArgs(c.runtimePath, c.nodeConfigPath, c.controlAddress, c.runtimeSettings,
		os.Getpid(), c.restartArgs, c.serviceManaged)
	if err != nil {
		return fmt.Errorf("prepare node process arguments: %w", err)
	}
	command := exec.Command(c.executablePath, args...)
	configureChildProcess(command)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start node process: %w", err)
	}
	child := &childProcess{cmd: command, done: make(chan struct{})}
	c.mu.Lock()
	c.child = child
	c.operation = ""
	c.lastError = ""
	c.mu.Unlock()
	go func() {
		child.err = command.Wait()
		close(child.done)
		c.signal()
	}()
	return nil
}

func childArgs(runtimePath, nodeConfigPath, controlAddress string, settings config.RuntimeSettings,
	controllerPID int, controllerArgs []string, serviceManaged bool) ([]string, error) {
	args := []string{"--runtime-config", runtimePath}
	addString := func(name string, value *string) {
		if value != nil {
			args = append(args, "--"+name, *value)
		}
	}
	addBool := func(name string, value *bool) {
		if value != nil {
			args = append(args, "--"+name+"="+strconv.FormatBool(*value))
		}
	}
	if settings.Role != nil {
		args = append(args, "--role", *settings.Role)
	}
	if settings.NodeConfigPath != nil && *settings.NodeConfigPath != "" {
		nodeConfigPath = *settings.NodeConfigPath
	}
	addString("node-config", stringPtr(nodeConfigPath))
	addString("orchestrator-config", settings.OrchestratorConfigPath)
	addString("node-listen", settings.NodeListenAddr)
	addString("orchestrator-listen", settings.OrchestratorListenAddr)
	addString("orchestrator-addr", settings.OrchestratorAddr)
	addString("tray-mode", settings.TrayMode)
	addString("harness", settings.Harness)
	addString("memory-path", settings.MemoryPath)
	if settings.AutoOpenBrowser == nil || !*settings.AutoOpenBrowser {
		args = append(args, "--no-browser")
	} else {
		args = append(args, "--auto-open-browser")
	}
	if settings.DiscoveryEnabled != nil && !*settings.DiscoveryEnabled {
		args = append(args, "--no-discovery")
	} else if settings.DiscoveryEnabled != nil && *settings.DiscoveryEnabled {
		args = append(args, "--enable-discovery")
	}
	addBool("allow-peer-updates", settings.AllowPeerUpdates)
	addBool("no-peer-updates", settings.NoPeerUpdates)
	args = append(args, "--host-control-addr", controlAddress)
	args = append(args,
		"--host-controller-pid", strconv.Itoa(controllerPID),
		"--host-controller-managed="+strconv.FormatBool(serviceManaged))
	controllerArgsJSON, err := json.Marshal(controllerArgs)
	if err != nil {
		return nil, fmt.Errorf("encode controller restart arguments: %w", err)
	}
	args = append(args, "--host-controller-restart-args", string(controllerArgsJSON))
	return args, nil
}

func completeRuntimeSettings(settings config.RuntimeSettings, nodeConfigPath string, nodeConfig config.NodeConfig, orchestratorConfig config.OrchestratorConfig) config.RuntimeSettings {
	if settings.Role == nil {
		settings.Role = stringPtr("auto")
	}
	if settings.NodeConfigPath == nil || *settings.NodeConfigPath == "" {
		settings.NodeConfigPath = stringPtr(nodeConfigPath)
	}
	if settings.OrchestratorConfigPath == nil {
		settings.OrchestratorConfigPath = stringPtr("")
	}
	if settings.NodeListenAddr == nil {
		settings.NodeListenAddr = stringPtr("")
	}
	if settings.OrchestratorListenAddr == nil {
		settings.OrchestratorListenAddr = stringPtr("")
	}
	if settings.OrchestratorAddr == nil {
		settings.OrchestratorAddr = stringPtr("")
	}
	if settings.TrayMode == nil {
		if *settings.Role == "node" {
			settings.TrayMode = stringPtr(nodeConfig.TrayMode)
		} else {
			settings.TrayMode = stringPtr(orchestratorConfig.TrayMode)
		}
	}
	if settings.AutoOpenBrowser == nil {
		settings.AutoOpenBrowser = boolValue(false)
	}
	if settings.DiscoveryEnabled == nil {
		settings.DiscoveryEnabled = boolValue(nodeConfig.DiscoveryEnabled || orchestratorConfig.DiscoveryEnabled)
	}
	if settings.Harness == nil {
		settings.Harness = stringPtr(orchestratorConfig.Harness)
	}
	if settings.MemoryPath == nil {
		settings.MemoryPath = stringPtr("")
	}
	if settings.AllowPeerUpdates == nil {
		settings.AllowPeerUpdates = boolValue(false)
	}
	if settings.NoPeerUpdates == nil {
		settings.NoPeerUpdates = boolValue(false)
	}
	return settings
}

func stringPtr(value string) *string { return &value }

func boolValue(value bool) *bool { return &value }

func (c *Controller) startDiscovery() {
	if !c.discoveryEnabled {
		return
	}
	c.mu.Lock()
	if c.discovery == nil {
		c.discovery = discovery.ListenWithSecret(c.discoveryKey, c.nodeID)
	}
	c.mu.Unlock()
}

func (c *Controller) stopDiscovery() {
	c.mu.Lock()
	listener := c.discovery
	c.discovery = nil
	c.mu.Unlock()
	if listener != nil {
		listener.Close()
	}
}

func (c *Controller) authorizeCurrentOrchestrator(w http.ResponseWriter, r *http.Request) bool {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "a paired orchestrator identity is required", http.StatusForbidden)
		return false
	}
	if err := c.identity.ReloadTrustedPeers(); err != nil {
		http.Error(w, "unable to refresh the paired identity roster", http.StatusServiceUnavailable)
		return false
	}
	peerID := strings.TrimSpace(r.TLS.PeerCertificates[0].Subject.CommonName)
	if !c.identity.IsTrustedOrchestratorID(peerID) {
		http.Error(w, "a paired orchestrator identity is required", http.StatusForbidden)
		return false
	}
	current, err := c.currentOrchestratorID(r.Context())
	if err != nil {
		http.Error(w, "current orchestrator authority could not be verified", http.StatusServiceUnavailable)
		return false
	}
	if current == "" || current != peerID {
		http.Error(w, "request requires the current elected orchestrator", http.StatusForbidden)
		return false
	}
	return true
}

func (c *Controller) currentOrchestratorID(ctx context.Context) (string, error) {
	c.mu.RLock()
	child := c.child
	listener := c.discovery
	c.mu.RUnlock()
	if child != nil {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.localNodeURL+"/api/lifecycle/authority", nil)
		if err != nil {
			return "", err
		}
		response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
		if err != nil {
			return "", fmt.Errorf("read active node authority: %w", err)
		}
		defer closeutil.Discard(response.Body)
		if response.StatusCode != http.StatusOK {
			return "", fmt.Errorf("active node authority returned %s", response.Status)
		}
		var result struct {
			OrchestratorID string `json:"orchestrator_id"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 4<<10)).Decode(&result); err != nil {
			return "", fmt.Errorf("decode active node authority: %w", err)
		}
		return result.OrchestratorID, nil
	}
	if listener == nil {
		return "", errors.New("discovery is not available while the node is stopped")
	}
	if current, ok := discovery.BestActiveOrchestrator(listener.Snapshot()); ok {
		return current.ID, nil
	}
	return "", errors.New("no active orchestrator was discovered")
}

func (c *Controller) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := serviceStatus{
		ControlAddress: c.controlAddress,
	}
	c.mu.RLock()
	status.Running = c.child != nil
	status.DesiredRunning = c.desiredRunning
	status.Operation = c.operation
	status.Error = c.lastError
	c.mu.RUnlock()
	if status.Running {
		request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, c.localNodeURL+"/api/lifecycle/status", nil)
		if err == nil {
			response, requestErr := (&http.Client{Timeout: 3 * time.Second}).Do(request)
			if requestErr == nil {
				defer closeutil.Discard(response.Body)
				if response.StatusCode == http.StatusOK {
					var local struct {
						Draining    bool `json:"draining"`
						ActiveTasks int  `json:"active_tasks"`
					}
					if decodeErr := json.NewDecoder(io.LimitReader(response.Body, 4<<10)).Decode(&local); decodeErr == nil {
						status.Draining = local.Draining
						status.ActiveTasks = local.ActiveTasks
					}
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, status)
}

func (c *Controller) handleAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	switch action {
	case "start", "stop", "restart", "drain":
	default:
		http.Error(w, "action must be start, stop, restart, or drain", http.StatusBadRequest)
		return
	}
	if !c.actionMu.TryLock() {
		http.Error(w, "another node lifecycle operation is in progress", http.StatusConflict)
		return
	}
	switch action {
	case "start":
		c.mu.Lock()
		if c.child != nil {
			c.mu.Unlock()
			c.actionMu.Unlock()
			writeJSON(w, http.StatusOK, map[string]string{"state": "running"})
			return
		}
		c.desiredRunning = true
		c.operation = "starting"
		c.lastError = ""
		c.mu.Unlock()
		c.signal()
		c.actionMu.Unlock()
		writeJSON(w, http.StatusAccepted, map[string]string{"state": "starting"})
	case "stop":
		c.mu.Lock()
		c.desiredRunning = false
		c.operation = "stopping"
		child := c.child
		c.mu.Unlock()
		go func() {
			defer c.actionMu.Unlock()
			if child != nil {
				c.stopProcess(child)
			}
			c.finishOperation("")
		}()
		writeJSON(w, http.StatusAccepted, map[string]string{"state": "stopping"})
	case "drain", "restart":
		c.mu.RLock()
		child := c.child
		c.mu.RUnlock()
		if child == nil {
			c.actionMu.Unlock()
			http.Error(w, "node is not running; use start", http.StatusConflict)
			return
		}
		if err := c.requestChild(http.MethodPost, "/api/lifecycle/drain"); err != nil {
			c.actionMu.Unlock()
			http.Error(w, "could not begin node drain: "+err.Error(), http.StatusBadGateway)
			return
		}
		restart := action == "restart"
		c.mu.Lock()
		c.operation = action
		if !restart {
			c.desiredRunning = false
		}
		c.mu.Unlock()
		go func() {
			defer c.actionMu.Unlock()
			if err := c.waitForDrain(child); err != nil {
				c.mu.Lock()
				if c.child == child {
					c.desiredRunning = true
				}
				c.mu.Unlock()
				c.finishOperation(err.Error())
				return
			}
			c.stopProcess(child)
			if restart {
				c.mu.Lock()
				c.desiredRunning = true
				c.operation = "starting"
				c.mu.Unlock()
				c.signal()
				return
			}
			c.finishOperation("")
		}()
		writeJSON(w, http.StatusAccepted, map[string]string{"state": action})
	}
}

func (c *Controller) waitForDrain(child *childProcess) error {
	deadline := time.NewTimer(30 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-child.done:
			return errors.New("node exited before drain completed")
		case <-deadline.C:
			return errors.New("node drain exceeded 30 minutes; the node remains draining")
		case <-ticker.C:
			request, err := http.NewRequest(http.MethodGet, c.localNodeURL+"/api/lifecycle/status", nil)
			if err != nil {
				return err
			}
			response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
			if err != nil {
				continue
			}
			var status struct {
				Idle bool `json:"idle"`
			}
			decodeErr := json.NewDecoder(io.LimitReader(response.Body, 4<<10)).Decode(&status)
			closeutil.Discard(response.Body)
			if response.StatusCode == http.StatusOK && decodeErr == nil && status.Idle {
				return nil
			}
		}
	}
}

func (c *Controller) stopProcess(child *childProcess) {
	if err := c.requestChild(http.MethodPost, "/api/lifecycle/stop"); err != nil {
		slog.Error("graceful node stop request failed", "error", err)
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-child.done:
	case <-timer.C:
		if child.cmd.Process != nil {
			if err := child.cmd.Process.Kill(); err != nil {
				slog.Error("kill child process after stop timeout", "error", err)
			}
		}
		<-child.done
	}
}

func (c *Controller) requestChild(method, path string) error {
	request, err := http.NewRequest(method, c.localNodeURL+path, nil)
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("node returned %s", response.Status)
	}
	return nil
}

func (c *Controller) handleRuntimeSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		settings, err := config.LoadRuntimeSettings(c.runtimePath)
		if err != nil {
			http.Error(w, "runtime settings are unavailable", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, settings)
		return
	}
	if r.ContentLength > 1<<20 {
		http.Error(w, "runtime settings request is too large", http.StatusRequestEntityTooLarge)
		return
	}
	var settings config.RuntimeSettings
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&settings); err != nil {
		http.Error(w, "invalid runtime settings", http.StatusBadRequest)
		return
	}
	if err := config.SaveRuntimeSettings(c.runtimePath, settings); err != nil {
		http.Error(w, "cannot save runtime settings: "+err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, settings)
}

func (c *Controller) finishOperation(message string) {
	c.mu.Lock()
	c.operation = ""
	c.lastError = message
	c.mu.Unlock()
}

func (c *Controller) setError(err error) {
	c.mu.Lock()
	c.lastError = err.Error()
	c.operation = ""
	c.mu.Unlock()
}

func (c *Controller) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Controller) stopChild() {
	c.mu.RLock()
	child := c.child
	c.mu.RUnlock()
	if child != nil {
		c.mu.Lock()
		c.desiredRunning = false
		c.mu.Unlock()
		c.stopProcess(child)
	}
}

func loopbackNodeURL(listenAddr string) (string, error) {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil || port == "" || port == "0" {
		return "", fmt.Errorf("nodecontrol: node listen address %q must have a fixed port", listenAddr)
	}
	return "http://127.0.0.1:" + port, nil
}

func advertisedControlAddress(advertiseAddr, listenAddr string) (string, error) {
	parsed, err := url.Parse(advertiseAddr)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return "", errors.New("nodecontrol: node advertise address must be an https URL")
	}
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("nodecontrol: invalid controller listen address: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", fmt.Errorf("nodecontrol: controller listen address must use a fixed port between 1 and 65535")
	}
	host := net.JoinHostPort(parsed.Hostname(), port)
	return "https://" + host, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("write controller response", "error", err)
	}
}
