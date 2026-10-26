package nodeagent

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/background"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/workpool"
)

type agentState struct {
	// mu protects config and peer membership. Runtime model selection belongs to engineComponent.
	// It is a leaf lock: copy state under it, then release before touching other components or doing I/O.
	mu    sync.Mutex
	cfg   config.NodeConfig
	peers []cluster.NodeStatus
}

type engineComponent struct {
	engines         []engine.Engine
	detectedEngines []engine.Engine
	detectedSeenAt  map[string]time.Time
	// enginesMu protects configured/detected engines and the generation-keyed ordered snapshot cache.
	// Engine calls happen without this lock held.
	enginesMu sync.RWMutex
	// engineSnapshotCache is invalidated on engine scans/configuration and reused until hwGeneration changes.
	engineGeneration          uint64
	engineSnapshotCache       []engine.Engine
	engineSnapshotGeneration  uint64
	engineSnapshotHardwareGen uint64
	engineSnapshotCacheValid  bool
	operatorCapabilities      map[string]engine.Capabilities
	// engineScanMu protects engineScanDone; detector calls run after releasing it.
	engineScanMu   sync.Mutex
	engineScanDone chan struct{}
	engineDetector func() []engine.Engine
	lastEngineScan time.Time
	telemetry      interface {
		hardwareSnapshot() hardware.Info
		hardwareSnapshotVersioned() (hardware.Info, uint64)
	}
	// refreshMu protects refresh coordination and generation; it is never held during engine calls.
	refreshMu                sync.Mutex
	refreshDone              chan struct{}
	modelInventoryGeneration uint64
	// modelMu protects runtime selection and reported inventory, including derived selection flags.
	// Lock order is refreshMu -> modelMu; neither is held during engine calls.
	modelMu         sync.Mutex
	disabledModels  map[string]bool
	defaultModel    string
	reportedModels  []ModelState
	reportedEngines []engine.Status
	reportedAt      time.Time
}

type telemetryComponent struct {
	// hwMu protects the latest hardware snapshot, its generation, and telemetry histories.
	// It is a leaf lock; probes and consumers run after releasing it.
	hwMu               sync.RWMutex
	hwRefreshMu        sync.Mutex
	hw                 hardware.Info
	hwGeneration       uint64
	gpuHistory         []hardware.GPUUsageSample
	npuHistory         []hardware.NPUUsageSample
	systemHistory      []hardware.SystemUsageSample
	hardwareUsageProbe func() hardwareUsageSample
	runtimeDevices     []hardware.Device
}

type identityComponent struct {
	identity *peeridentity.Identity
}

type orchestratorLink struct {
	background        background.Group
	publisherOnce     sync.Once
	publisher         *hub.Publisher
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
		BootstrapTransportForID(string) *http.Transport
		BootstrapTransportForFingerprint(string, string) *http.Transport
		TrustPeer(string, string) error
		ValidatePeer(string, string) error
		TrustPeerDetails(string, string, string, string) error
		TrustedIDs() []string
		TrustedPeers() map[string]string
		RemovePeer(string) error
		IsTrustedID(string) bool
		IsTrustedOrchestratorID(string) bool
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
	currentTasks   func(context.Context) ([]*taskmgr.Task, error)
	heartbeat      *HeartbeatMonitor
	// orchMu protects the current and co-located orchestrator identities and address; it is not nested with joinedMu or pairingInviteMu.
	orchMu                sync.RWMutex
	orchestratorAddr      string
	orchestratorID        string
	localOrchestratorID   string
	electionCandidateAddr string
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
	// activeMu protects activeTasks, including intentionally ungated work. It is a
	// leaf lock; never hold it while acquiring a slot or calling another component.
	activeMu    sync.Mutex
	activeTasks int
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
	dialogueCancels map[string]map[*dialogueTurnHandle]struct{}
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
	return &agentState{cfg: cfg}
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

func newEngineComponent(
	engines []engine.Engine,
	detector func() []engine.Engine,
	telemetry interface {
		hardwareSnapshot() hardware.Info
		hardwareSnapshotVersioned() (hardware.Info, uint64)
	},
	disabledModels []string,
	defaultModel string,
) *engineComponent {
	if detector == nil {
		detector = engine.DetectAll
	}
	disabled := make(map[string]bool, len(disabledModels))
	for _, model := range disabledModels {
		disabled[model] = true
	}
	return &engineComponent{
		engines: engines, engineDetector: detector, telemetry: telemetry,
		disabledModels: disabled, defaultModel: defaultModel,
	}
}

func newTelemetryComponent(hw hardware.Info) *telemetryComponent {
	return &telemetryComponent{hw: hw, hardwareUsageProbe: currentHardwareUsage}
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
		BootstrapTransportForID(string) *http.Transport
		BootstrapTransportForFingerprint(string, string) *http.Transport
		TrustPeer(string, string) error
		ValidatePeer(string, string) error
		TrustPeerDetails(string, string, string, string) error
		TrustedIDs() []string
		TrustedPeers() map[string]string
		RemovePeer(string) error
		IsTrustedID(string) bool
		IsTrustedOrchestratorID(string) bool
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
	link := &orchestratorLink{
		peerClient: httpx.NewClient(httpx.ClientOptions{Timeout: httpx.ProbeTimeout}),
		memorySyncClient: httpx.NewClient(httpx.ClientOptions{
			Timeout: time.Duration(cfg.SyncTimeoutSeconds) * time.Second,
		}),
		streamClient:     httpx.NewClient(httpx.ClientOptions{}),
		orchestratorAddr: cfg.OrchestratorAddr,
		configState:      state,
		trust:            trust,
		telemetry:        telemetry,
		models:           models,
		state:            state,
		tasks:            tasks,
		events:           events,
	}
	link.heartbeat = newHeartbeatMonitor(
		link.getOrchestratorAddr,
		link.heartbeatOrchestratorID,
		func(ctx context.Context, address string) error {
			return statusProbe(link.peerClient)(ctx, address)
		},
		func(id string) {
			if id != "" && link.discoveryListener != nil {
				link.discoveryListener.IncludePeer(id)
			}
		},
	)
	return link
}

func newExecutorComponent(maxConcurrentTasks int) *executorComponent {
	return &executorComponent{executionSlots: make(chan struct{}, maxConcurrentTasks)}
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
		dispatchClient: httpx.NewClient(httpx.ClientOptions{Timeout: httpx.InferenceTimeout}),
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
		dialogueCancels: make(map[string]map[*dialogueTurnHandle]struct{}),
		nodeID:          nodeID,
		mem:             mem,
		models:          models,
		executor:        executor,
		remember:        remember,
	}
}
