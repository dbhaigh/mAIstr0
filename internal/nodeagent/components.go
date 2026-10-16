package nodeagent

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/workpool"
)

type agentState struct {
	// mu protects config, model selection, active-task count, and peer membership.
	// It is a leaf lock: copy state under it, then release before touching other components or doing I/O.
	mu             sync.Mutex
	cfg            config.NodeConfig
	disabledModels map[string]bool
	defaultModel   string
	activeTasks    int
	peers          []cluster.NodeStatus
}

type engineComponent struct {
	engines         []engine.Engine
	detectedEngines []engine.Engine
	detectedSeenAt  map[string]time.Time
	// enginesMu protects configured/detected engines and the generation-keyed ordered snapshot cache.
	// When both scan locks are needed, engineScanMu precedes enginesMu; engine calls happen without enginesMu held.
	enginesMu sync.RWMutex
	// engineSnapshotCache is invalidated on engine scans/configuration and reused until hwGeneration changes.
	engineGeneration          uint64
	engineSnapshotCache       []engine.Engine
	engineSnapshotGeneration  uint64
	engineSnapshotHardwareGen uint64
	engineSnapshotCacheValid  bool
	// engineScanMu serializes detector calls and updates; if both scan locks are needed, it precedes enginesMu.
	engineScanMu   sync.Mutex
	engineDetector func() []engine.Engine
	lastEngineScan time.Time
	telemetry      interface {
		hardwareSnapshot() hardware.Info
		hardwareSnapshotVersioned() (hardware.Info, uint64)
	}
	state interface {
		disabledModelSnapshot() map[string]bool
		defaultModelName() string
		setModelConfig(map[string]bool, string)
	}

	// refreshMu serializes inventory refresh and invalidation; its order is refreshMu -> modelMu -> agentState.mu.
	refreshMu sync.Mutex
	// modelMu protects reported inventory; never acquire refreshMu while holding it.
	modelMu         sync.Mutex
	reportedModels  []ModelState
	reportedEngines []engine.Status
	reportedAt      time.Time
}

type telemetryComponent struct {
	// hwMu protects the latest hardware snapshot, its generation, and telemetry histories.
	// It is a leaf lock; probes and consumers run after releasing it.
	hwMu          sync.RWMutex
	hw            hardware.Info
	hwGeneration  uint64
	gpuHistory    []hardware.GPUUsageSample
	npuHistory    []hardware.NPUUsageSample
	systemHistory []hardware.SystemUsageSample
}

type identityComponent struct {
	identity *peeridentity.Identity
}

type orchestratorLink struct {
	discoveryListener *discovery.Listener
	peerClient        *http.Client
	memorySyncClient  *http.Client
	streamClient      *http.Client
	configState       interface {
		configSnapshot() config.NodeConfig
		clearPairingPIN()
	}
	trust interface {
		Initialized() bool
		CertificatePEM() string
		BootstrapTransport() *http.Transport
		TrustPeer(string, string) error
		TrustPeerDetails(string, string, string, string) error
		TrustedIDs() []string
		RemovePeer(string) error
		IsTrustedID(string) bool
	}
	telemetry interface{ hardwareSnapshot() hardware.Info }
	models    interface {
		refreshReportedModels()
		reportedModelList() []engine.Model
	}
	state interface {
		peersSnapshot() []cluster.NodeStatus
		replacePeers([]cluster.NodeStatus)
		configSnapshot() config.NodeConfig
	}
	tasks          *taskmgr.Manager
	events         *hub.Hub
	syncer         *syncerComponent
	statusSnapshot func() statusResponse
	currentTasks   func() []*taskmgr.Task
	// orchMu protects the current and co-located orchestrator identities and address; it is not nested with joinedMu or pairingInviteMu.
	orchMu              sync.RWMutex
	orchestratorAddr    string
	orchestratorID      string
	localOrchestratorID string
	// joinedMu protects registration state and is not held across registration or network calls.
	joinedMu sync.Mutex
	joined   bool
	// pairingInviteMu protects invitation state; do not hold it while calling peers or waiting for user confirmation.
	pairingInviteMu  sync.Mutex
	pairingInvite    *pairingInvitation
	pairingAccepting bool
}

type executorComponent struct {
	executionSlots chan struct{}
	state          *agentState
}

type peerComponent struct {
	dispatchClient *http.Client
	dispatchPool   *workpool.Pool[peerDispatchJob]
	peerHTTP       func() *http.Client
	state          interface {
		peersSnapshot() []cluster.NodeStatus
		configSnapshot() config.NodeConfig
	}
	telemetry interface{ hardwareSnapshot() hardware.Info }
	models    interface{ defaultModelName() string }
	executor  interface{ activeTaskCount() int }
	tasks     *taskmgr.Manager
}

type dialogueComponent struct {
	dialogues map[string]*Dialogue
	// dialoguesMu protects dialogue state; release it before inference, persistence, or cancellation callbacks.
	dialoguesMu     sync.RWMutex
	dialogueCancels map[string]context.CancelFunc
	// dialogueCancelsMu protects cancellation handles; never nest it with dialoguesMu and invoke cancels after unlock.
	dialogueCancelsMu sync.Mutex
	nodeID            string
	mem               memory.Store
	models            interface {
		defaultModelName() string
		enabledModels() []engine.Model
		engineFor(string) engine.Engine
	}
	executor interface{ beginActiveTask() func() }
	remember func(memory.Experience)
}

func newAgentState(cfg config.NodeConfig) *agentState {
	disabled := make(map[string]bool, len(cfg.DisabledModels))
	for _, model := range cfg.DisabledModels {
		disabled[model] = true
	}
	return &agentState{cfg: cfg, disabledModels: disabled, defaultModel: cfg.DefaultModel}
}

func (s *agentState) disabledModelSnapshot() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	disabled := make(map[string]bool, len(s.disabledModels))
	for name, isDisabled := range s.disabledModels {
		disabled[name] = isDisabled
	}
	return disabled
}

func (s *agentState) defaultModelName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.defaultModel
}

func (s *agentState) configSnapshot() config.NodeConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

func (s *agentState) clearPairingPIN() {
	s.mu.Lock()
	s.cfg.PairingPIN = ""
	s.mu.Unlock()
}

func (s *agentState) setModelConfig(disabled map[string]bool, defaultModel string) {
	s.mu.Lock()
	s.disabledModels = disabled
	if defaultModel != "" {
		s.defaultModel = defaultModel
	}
	s.mu.Unlock()
}

func newEngineComponent(
	engines []engine.Engine,
	detector func() []engine.Engine,
	telemetry interface {
		hardwareSnapshot() hardware.Info
		hardwareSnapshotVersioned() (hardware.Info, uint64)
	},
	state interface {
		disabledModelSnapshot() map[string]bool
		defaultModelName() string
		setModelConfig(map[string]bool, string)
	},
) *engineComponent {
	if detector == nil {
		detector = engine.DetectAll
	}
	return &engineComponent{
		engines: engines, engineDetector: detector, telemetry: telemetry, state: state,
	}
}

func newTelemetryComponent(hw hardware.Info) *telemetryComponent {
	return &telemetryComponent{hw: hw}
}

func newIdentityComponent() *identityComponent {
	return &identityComponent{}
}

func newOrchestratorLink(
	state *agentState,
	trust interface {
		Initialized() bool
		CertificatePEM() string
		BootstrapTransport() *http.Transport
		TrustPeer(string, string) error
		TrustPeerDetails(string, string, string, string) error
		TrustedIDs() []string
		RemovePeer(string) error
		IsTrustedID(string) bool
	},
	telemetry interface{ hardwareSnapshot() hardware.Info },
	models interface {
		refreshReportedModels()
		reportedModelList() []engine.Model
	},
	tasks *taskmgr.Manager,
	events *hub.Hub,
) *orchestratorLink {
	cfg := state.configSnapshot()
	return &orchestratorLink{
		peerClient:       &http.Client{Timeout: 5 * time.Second},
		memorySyncClient: &http.Client{Timeout: memorySyncTimeout},
		streamClient:     &http.Client{},
		orchestratorAddr: cfg.OrchestratorAddr,
		configState:      state,
		trust:            trust,
		telemetry:        telemetry,
		models:           models,
		state:            state,
		tasks:            tasks,
		events:           events,
	}
}

func newExecutorComponent(maxConcurrentTasks int, state *agentState) *executorComponent {
	return &executorComponent{executionSlots: make(chan struct{}, maxConcurrentTasks), state: state}
}

func newPeerComponent(
	state interface {
		peersSnapshot() []cluster.NodeStatus
		configSnapshot() config.NodeConfig
	},
	telemetry interface{ hardwareSnapshot() hardware.Info },
	models interface{ defaultModelName() string },
	executor interface{ activeTaskCount() int },
	tasks *taskmgr.Manager,
	peerHTTP func() *http.Client,
	workers int,
) *peerComponent {
	component := &peerComponent{
		dispatchClient: &http.Client{Timeout: 10 * time.Minute},
		peerHTTP:       peerHTTP,
		state:          state,
		telemetry:      telemetry,
		models:         models,
		executor:       executor,
		tasks:          tasks,
	}
	component.dispatchPool = workpool.New(workers, workers*64, func(ctx context.Context, job peerDispatchJob) {
		component.dispatchToPeer(ctx, job.taskID, job.assignment)
	})
	return component
}

func newDialogueComponent(
	nodeID string,
	mem memory.Store,
	models interface {
		defaultModelName() string
		enabledModels() []engine.Model
		engineFor(string) engine.Engine
	},
	executor interface{ beginActiveTask() func() },
	remember func(memory.Experience),
) *dialogueComponent {
	return &dialogueComponent{
		dialogues:       make(map[string]*Dialogue),
		dialogueCancels: make(map[string]context.CancelFunc),
		nodeID:          nodeID,
		mem:             mem,
		models:          models,
		executor:        executor,
		remember:        remember,
	}
}
