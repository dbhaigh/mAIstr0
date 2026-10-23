// Package nodeagent implements the per-node HTTP service: it detects local
// LLM engines/models and hardware, reports itself to the orchestrator, and
// executes subtasks dispatched to it. Engine probes run without inventory
// locks; a generation check prevents an invalidated probe from restoring stale
// state.
package nodeagent

import (
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Agent is the public facade and composition root for node-local components.
// Component pointers are wired before Run and own their synchronized state;
// lock ownership and ordering are documented with each component.
type Agent struct {
	*engineComponent
	*telemetryComponent
	*agentState
	*identityComponent
	*orchestratorLink
	*executorComponent
	*peerComponent
	*dialogueComponent

	stageComponent      *stageComponent
	mem                 memory.Store
	generateIdempotency *generateIdempotencyCache
	statusReporter      *statusComponent
}

type peerDispatchJob struct {
	taskID     string
	assignment scheduler.Assignment
}

type pairingInvitation struct {
	OrchestratorID string    `json:"orchestrator_id"`
	Address        string    `json:"address"`
	PIN            string    `json:"pin"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type pairingInvitationRequest struct {
	NodeID         string    `json:"node_id"`
	OrchestratorID string    `json:"orchestrator_id"`
	Address        string    `json:"address"`
	PIN            string    `json:"pin"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// Dialogue represents an ongoing conversational session with an LLM on this node.
type Dialogue struct {
	ID        string               `json:"id"`
	Model     string               `json:"model"`
	System    string               `json:"system,omitempty"`
	Messages  []engine.ChatMessage `json:"messages"`
	CreatedAt time.Time            `json:"created_at"`
	UpdatedAt time.Time            `json:"updated_at"`
}

func New(cfg config.NodeConfig) (*Agent, error) {
	return newAgent(cfg, taskmgr.NewManager())
}

// NewWithPersistentTasks opens this node's durable task history before
// starting the agent. memoryPath, when configured, gets a separate file so a
// process running both node and orchestrator roles does not share databases.
func NewWithPersistentTasks(cfg config.NodeConfig) (*Agent, error) {
	cfg, err := normalizeAgentConfig(cfg)
	if err != nil {
		return nil, err
	}
	taskPath := taskmgr.DefaultPath(cfg.NodeID)
	if cfg.MemoryPath != "" {
		taskPath = cfg.MemoryPath + ".node-tasks"
	}
	tasks, err := taskmgr.OpenManagerWithOrigin(taskPath, nil, "task:"+cfg.NodeID)
	if err != nil {
		return nil, fmt.Errorf("nodeagent: open task history: %w", err)
	}
	return newAgentWithNormalizedConfig(cfg, tasks, engine.DetectAll), nil
}

func newAgent(cfg config.NodeConfig, tasks *taskmgr.Manager) (*Agent, error) {
	return newAgentWithEngineDetector(cfg, tasks, engine.DetectAll)
}

func newAgentWithEngineDetector(cfg config.NodeConfig, tasks *taskmgr.Manager, detector func() []engine.Engine) (*Agent, error) {
	cfg, err := normalizeAgentConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newAgentWithNormalizedConfig(cfg, tasks, detector), nil
}

func normalizeAgentConfig(cfg config.NodeConfig) (config.NodeConfig, error) {
	cfg.Normalize()
	if cfg.NodeID == "" {
		cfg.NodeID = DefaultNodeID()
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = "https://" + discovery.OutboundIP() + portSuffix(cfg.ListenAddr)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func newAgentWithNormalizedConfig(cfg config.NodeConfig, tasks *taskmgr.Manager, detector func() []engine.Engine) *Agent {
	tasks.SetSyncOrigin("task:" + cfg.NodeID)
	configuredEngines := make([]engine.Engine, 0, len(cfg.Engines))
	for _, configured := range cfg.Engines {
		if e := engine.NewConfiguredWithAPIKey(configured.Name, configured.URL, configured.APIKey); e != nil {
			configuredEngines = append(configuredEngines, e)
		}
	}
	hw := hardware.Detect()
	state := newAgentState(cfg)
	telemetry := newTelemetryComponent(hw)
	executor := newExecutorComponent(cfg.MaxConcurrentTasks, state)
	engines := newEngineComponent(configuredEngines, detector, telemetry, state)
	identity := newIdentityComponent()
	events := hub.New()
	link := newOrchestratorLink(state, identity, telemetry, engines, tasks, events)
	stages := newStageComponent()
	statusReporter := newStatusComponent(state, telemetry, engines, executor, stages)
	peers := newPeerComponent(
		state, telemetry, engines, executor, tasks,
		func() *http.Client { return link.peerClient },
		config.DefaultDispatchConcurrency,
	)
	a := &Agent{
		engineComponent:     engines,
		telemetryComponent:  telemetry,
		agentState:          state,
		identityComponent:   identity,
		orchestratorLink:    link,
		executorComponent:   executor,
		stageComponent:      stages,
		peerComponent:       peers,
		generateIdempotency: newGenerateIdempotencyCache(),
		statusReporter:      statusReporter,
	}
	link.statusSnapshot = a.status
	link.currentTasks = peers.currentTasks
	if store, err := memory.Open(cfg.MemoryPath, cfg.NodeID); err != nil {
		logging.Errorf("nodeagent", "nodeagent: memory unavailable, running stateless: %v", err)
	} else {
		a.mem = store
		logging.Infof("nodeagent", "nodeagent: memory store at %s", store.Path())
	}
	a.dialogueComponent = newDialogueComponent(a.cfg.NodeID, a.mem, a.engineComponent, a.executorComponent, a.remember)
	link.syncer = newSyncerComponent(
		cfg.NodeID, tasks, a.mem,
		func() *http.Client { return link.peerClient },
		func() *http.Client { return link.memorySyncClient },
		a.Peers,
		cfg.SyncMaxBytes,
		func() bool {
			if a.getOrchestratorAddr() != "" && a.getOrchestratorID() != "" {
				return false
			}
			return a.mostCapableNode().ID == cfg.NodeID
		},
	)
	return a
}

// Close flushes and releases the persistent task and memory stores.
func (a *Agent) Close() error {
	var closeErr error
	if a.orchestratorLink.tasks != nil {
		for _, task := range a.orchestratorLink.tasks.All() {
			if task.Status == taskmgr.StatusPending || task.Status == taskmgr.StatusRunning || task.Status == taskmgr.StatusSynthesis {
				if _, err := a.orchestratorLink.tasks.CancelChecked(task.ID); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
			}
		}
	}
	if a.peerComponent != nil && a.peerComponent.dispatchPool != nil {
		a.peerComponent.dispatchPool.Close()
	}
	if a.orchestratorLink.tasks != nil {
		closeErr = errors.Join(closeErr, a.orchestratorLink.tasks.Close())
	}
	if a.mem != nil {
		closeErr = errors.Join(closeErr, a.mem.Close())
	}
	return closeErr
}

// Memory exposes this node's persistent knowledge store (nil if the
// database could not be opened).
func (a *Agent) Memory() memory.Store { return a.mem }

// InitializeIdentity loads the node's persistent TLS identity before serving.
func (a *Agent) InitializeIdentity() error {
	if a.identityComponent.Initialized() {
		return nil
	}
	if err := a.identityComponent.initialize(a.cfg.NodeID, a.cfg.MemoryPath, a.cfg.AdvertiseAddr); err != nil {
		return err
	}
	transport, err := a.identityComponent.clientTransport()
	if err != nil {
		return err
	}
	a.peerClient = &http.Client{Timeout: 5 * time.Second, Transport: transport}
	a.memorySyncClient = &http.Client{
		Timeout:   time.Duration(a.cfg.SyncTimeoutSeconds) * time.Second,
		Transport: transport,
	}
	a.dispatchClient = &http.Client{Timeout: 10 * time.Minute, Transport: transport}
	a.streamClient = &http.Client{Transport: transport}
	return nil
}

func (a *Agent) IdentityCertificate() (string, error) {
	return a.identityComponent.certificate()
}

// PairLocalOrchestrator establishes mutual certificate pins for a combined
// orchestrator and worker running in one process.
func (a *Agent) PairLocalOrchestrator(orchestratorID, certificatePEM, address string) error {
	return a.orchestratorLink.pairLocal(orchestratorID, certificatePEM, address)
}

type nodePairingRequest struct {
	ID             string `json:"id"`
	Role           string `json:"role"`
	PeerAddr       string `json:"peer_addr,omitempty"`
	PIN            string `json:"pin"`
	CertificatePEM string `json:"certificate_pem"`
}

type nodePairingResponse struct {
	ID             string            `json:"id"`
	Role           string            `json:"role"`
	CertificatePEM string            `json:"certificate_pem"`
	Peers          map[string]string `json:"peers"`
}

func (a *Agent) pairOrchestrator(candidate orchestratorCandidate) error {
	return a.orchestratorLink.pairOrchestrator(candidate)
}

// PairWithOrchestrator enrolls this node at a user-selected orchestrator
// using its one-use out-of-band PIN or automatically if on the same subnet.
func (a *Agent) PairWithOrchestrator(address, pin string) error {
	return a.orchestratorLink.pairWithOrchestrator(address, pin)
}

func normalizeOrchestratorAddress(address string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(address))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("orchestrator address must be an https URL without user information")
	}
	return "https://" + parsed.Host, nil
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

func (a *Agent) pairWithAddress(address, expectedID, pin string) error {
	return a.orchestratorLink.pairWithAddress(address, expectedID, pin)
}

// remember persists one unit of local LLM work so the cluster can learn
// which models on this node actually perform well.
func (a *Agent) remember(e memory.Experience) {
	if a.mem == nil {
		return
	}
	if e.NodeID == "" {
		e.NodeID = a.cfg.NodeID
	}
	if _, err := a.mem.Record(e); err != nil {
		logging.Errorf("nodeagent", "nodeagent: memory write failed: %v", err)
	}
}

// DefaultNodeID generates a stable node ID from the hostname,
// exported for callers that need the agent's default identity.
func DefaultNodeID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node"
	}
	return hostname
}

// ID returns this agent's node ID (auto-generated if not configured).
func (a *Agent) ID() string { return a.cfg.NodeID }

type executeRequest struct {
	SubtaskID string `json:"subtask_id"`
	Model     string `json:"model"`
	Engine    string `json:"engine,omitempty"`
	Prompt    string `json:"prompt"`
	TaskType  string `json:"task_type,omitempty"`
}

type executeResponse struct {
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

type generateRequest struct {
	Model       string  `json:"model"`
	Prompt      string  `json:"prompt"`
	System      string  `json:"system,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
	MaxTokens   int     `json:"max_tokens,omitempty"`
}

type generateResponse struct {
	Model      string `json:"model"`
	Output     string `json:"output"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

type chatRequest struct {
	Model       string               `json:"model"`
	Messages    []engine.ChatMessage `json:"messages"`
	Temperature float64              `json:"temperature,omitempty"`
	MaxTokens   int                  `json:"max_tokens,omitempty"`
}

type chatResponse struct {
	Model      string             `json:"model"`
	Message    engine.ChatMessage `json:"message"`
	DurationMs int64              `json:"duration_ms"`
	Error      string             `json:"error,omitempty"`
}

type createDialogueRequest struct {
	ID     string `json:"id,omitempty"`
	Model  string `json:"model,omitempty"`
	System string `json:"system,omitempty"`
}

type dialogueMessageRequest struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content"`
	Message string `json:"message,omitempty"`
}

type dialogueMessageResponse struct {
	DialogueID string               `json:"dialogue_id"`
	Model      string               `json:"model"`
	Turn       int                  `json:"turn"`
	Reply      string               `json:"reply"`
	Messages   []engine.ChatMessage `json:"messages"`
	DurationMs int64                `json:"duration_ms"`
	Error      string               `json:"error,omitempty"`
}

type modelsPatchRequest struct {
	DisabledModels []string `json:"disabled_models"`
	DefaultModel   string   `json:"default_model"`
}

type nodeProperties struct {
	ID               string   `json:"id"`
	ListenAddr       string   `json:"listen_addr"`
	AdvertiseAddr    string   `json:"advertise_addr"`
	OrchestratorAddr string   `json:"orchestrator_addr"`
	DefaultModel     string   `json:"default_model"`
	DisabledModels   []string `json:"disabled_models"`
	EngineName       string   `json:"engine_name,omitempty"`
	EngineURL        string   `json:"engine_url,omitempty"`
	DiscoveryEnabled bool     `json:"discovery_enabled"`
	AutoOpenBrowser  bool     `json:"auto_open_browser"`
	TrayMode         string   `json:"tray_mode"`
	MemoryPath       string   `json:"memory_path"`
}

type engineInfo struct {
	Name             string    `json:"name"`
	URL              string    `json:"url"`
	CanPull          bool      `json:"can_pull"`
	Ready            bool      `json:"ready"`
	Protocols        []string  `json:"protocols,omitempty"`
	InventoryState   string    `json:"inventory_state,omitempty"`
	ModelCount       int       `json:"model_count"`
	LastCheckedAt    time.Time `json:"last_checked_at,omitempty"`
	LastSuccessfulAt time.Time `json:"last_successful_at,omitempty"`
	LastError        string    `json:"last_error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	_ = httpx.WriteJSON(w, status, v)
}

// portSuffix extracts ":port" from a listen address like ":7451" or
// "0.0.0.0:7451" so it can be appended to a discovered/outbound IP.
func portSuffix(listenAddr string) string {
	if i := strings.LastIndex(listenAddr, ":"); i >= 0 {
		return listenAddr[i:]
	}
	return listenAddr
}

type submitRequest struct {
	Description string `json:"description"`
}
