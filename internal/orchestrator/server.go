// Package orchestrator implements the control-plane HTTP API and background
// bookkeeping: node registry maintenance, task decomposition/assignment, and
// dispatching subtasks out to node agents. Sync state advances only after
// complete snapshots, and component locks are not held across peer I/O.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io/fs"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/deepseekadapter"
	"github.com/maistr0/maistr0/internal/background"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/piagent"
	"github.com/maistr0/maistr0/internal/selfupdate"
	"github.com/maistr0/maistr0/internal/shard"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/web"
)

type Server struct {
	background         background.Group
	publisherOnce      sync.Once
	publisher          *hub.Publisher
	registry           *cluster.Registry
	tasks              *taskmgr.Manager
	nodeClient         *nodeClient
	dispatcher         *dispatcherComponent
	shardedModels      map[string]config.ShardedModelConfig
	pipelinePlans      *pipelinePlanStore
	pipelineLocker     *pipelinePlanLocker
	pipelineMetrics    *pipelineStageMetrics
	pipelineRequests   *pipelineRequestGate
	planner            *taskPlanner
	election           *electionComponent
	events             *hub.Hub
	routerEvents       *routerEventLog
	harness            *harnessManager
	syncer             syncerAPI
	metrics            *operationalMetrics
	routerBreakers     *routerCircuitBreakers
	routerClients      *routerClientLimiter
	mem                memory.Store
	shards             *ShardManager
	identityPath       string
	identity           *peeridentity.Identity
	orchestratorEpoch  string
	advertiseAddr      string
	localNodeDashboard string
	mode               string
	syncMaxBytes       int64
	syncTimeout        time.Duration
	routerTTFBTimeout  time.Duration
	routerIdleTimeout  time.Duration
}

type SessionStore interface {
	AgentSessions() ([][]byte, error)
	SaveAgentSession(harness, id string, payload []byte) error
	DeleteAgentSession(harness, id string) error
}

const discoveryRequestTimeout = 5 * time.Second
const nodeCapacityFreshnessWindow = 6 * time.Second

// NewWithMemory builds an orchestrator backed by a persistent memory
// database at memoryPath (empty selects the default per-user location).
func NewWithMemory(memoryPath string) (*Server, error) {
	return NewWithMemoryAndHarness(memoryPath, "pi")
}

// NewWithMemoryAndHarness builds an orchestrator using the requested agent
// backend. The returned error identifies unsupported backend names.
func NewWithMemoryAndHarness(memoryPath, harnessName string) (*Server, error) {
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = memoryPath
	cfg.Harness = harnessName
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	srv, err := newWithMemoryHarnessAndConcurrency(cfg.MemoryPath, cfg.Harness, cfg.MaxConcurrentDispatches,
		time.Duration(cfg.DispatchTimeoutSeconds)*time.Second, cfg.Mode,
		cfg.SyncMaxBytes, time.Duration(cfg.SyncTimeoutSeconds)*time.Second, cfg.ShardedModels)
	if err != nil {
		return nil, err
	}
	srv.configureRouter(cfg)
	return srv, nil
}

// NewWithConfig creates an orchestrator from its JSON configuration.
func NewWithConfig(cfg config.OrchestratorConfig) (*Server, error) {
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	srv, err := newWithMemoryHarnessAndConcurrency(cfg.MemoryPath, cfg.Harness, cfg.MaxConcurrentDispatches,
		time.Duration(cfg.DispatchTimeoutSeconds)*time.Second, cfg.Mode,
		cfg.SyncMaxBytes, time.Duration(cfg.SyncTimeoutSeconds)*time.Second, cfg.ShardedModels)
	if err != nil {
		return nil, err
	}
	srv.dispatcher.configureSpeculativeHedging(cfg.SpeculativeHedgingEnabled, cfg.MaxConcurrentHedges)
	srv.configureRouter(cfg)
	return srv, nil
}

func (s *Server) configureRouter(cfg config.OrchestratorConfig) {
	s.routerTTFBTimeout = time.Duration(cfg.RouterTimeToFirstByteTimeoutSeconds) * time.Second
	s.routerIdleTimeout = time.Duration(cfg.RouterIdleStreamTimeoutSeconds) * time.Second
	s.routerClients = newRouterClientLimiter(cfg.RouterRequestsPerMinute, cfg.RouterMaxConcurrentPerClient)
}

func newWithMemoryHarnessAndConcurrency(
	memoryPath, harnessName string,
	dispatchLimit int,
	dispatchTimeout time.Duration,
	mode string,
	syncMaxBytes int64,
	syncTimeout time.Duration,
	shardedModels []config.ShardedModelConfig,
) (*Server, error) {
	if harnessName != "deepseek" && harnessName != "pi" {
		return nil, fmt.Errorf("unknown agent backend %q (want deepseek or pi)", harnessName)
	}
	if syncMaxBytes <= 0 {
		syncMaxBytes = config.DefaultSyncMaxBytes
	}
	if syncTimeout <= 0 {
		syncTimeout = time.Duration(config.DefaultSyncTimeoutSeconds) * time.Second
	}
	reg := cluster.NewRegistry()
	if dispatchTimeout <= 0 {
		dispatchTimeout = time.Duration(config.DefaultDispatchTimeoutSeconds) * time.Second
	}
	disp := httpx.NewClient(httpx.ClientOptions{Timeout: dispatchTimeout})
	selfID := "orchestrator-" + discovery.OutboundIP()
	pipelineDataPath := memoryPath
	if pipelineDataPath == "" {
		pipelineDataPath = memory.DefaultPath(selfID)
	}
	pipelinePlans, err := openPipelinePlanStore(pipelineDataPath + ".pipelines.json")
	if err != nil {
		return nil, err
	}
	events := hub.New()
	metrics := &operationalMetrics{}
	var store memory.Store
	if mode != config.OrchestratorModeMinimal {
		openedStore, err := memory.Open(memoryPath, selfID)
		if err != nil {
			logging.Errorf("orchestrator", "orchestrator: memory unavailable, running stateless: %v", err)
		} else {
			store = openedStore
		}
	}
	taskPath := taskmgr.DefaultPath(selfID)
	if memoryPath != "" {
		taskPath = memoryPath + ".tasks"
	}
	tasks, err := taskmgr.OpenManagerWithOrigin(taskPath, nil, "task:"+selfID)
	if err != nil {
		if store != nil {
			_ = store.Close()
		}
		return nil, fmt.Errorf("orchestrator: open task history: %w", err)
	}
	nodeClient := newNodeClientWithSyncTimeout(
		httpx.NewClient(httpx.ClientOptions{Timeout: httpx.ControlTimeout}), disp,
		httpx.NewClient(httpx.ClientOptions{Timeout: discoveryRequestTimeout}), syncTimeout,
	)
	dispatcher := newDispatcherComponent(reg, tasks, store, nodeClient.dispatcher, nodeClient, dispatchLimit, dispatchTimeout)
	dispatcher.metrics = metrics
	planner := newTaskPlanner(schedulerAdapter{}, tasks, dispatcher, nodeClient, dispatcher.schedulerNodes, dispatcher.experience())
	planner.metrics = metrics
	provisioner := newAutoModelProvisioner(reg, nodeClient)
	planner.provisioner = provisioner
	election := newElectionComponent(reg, nodeClient, selfID, cluster.SelfScore(hardware.Detect()), true)
	syncer := newSyncerComponent(reg, tasks, nodeClient.sync, store, election.isElectedOrchestrator)
	syncer.maxBytes = syncMaxBytes
	srv := &Server{
		registry:          reg,
		nodeClient:        nodeClient,
		dispatcher:        dispatcher,
		shardedModels:     make(map[string]config.ShardedModelConfig, len(shardedModels)),
		pipelinePlans:     pipelinePlans,
		pipelineLocker:    newPipelinePlanLocker(),
		pipelineMetrics:   newPipelineStageMetrics(),
		pipelineRequests:  newPipelineRequestGate(),
		planner:           planner,
		election:          election,
		events:            events,
		routerEvents:      newRouterEventLog(200),
		routerBreakers:    newRouterCircuitBreakers(),
		syncer:            syncer,
		metrics:           metrics,
		tasks:             tasks,
		mem:               store,
		identityPath:      peeridentity.DefaultPath(selfID),
		orchestratorEpoch: strconv.FormatInt(time.Now().UnixNano(), 10),
		mode:              mode,
		syncMaxBytes:      syncMaxBytes,
		syncTimeout:       syncTimeout,
		routerTTFBTimeout: time.Duration(config.DefaultRouterTTFBTimeoutSeconds) * time.Second,
		routerIdleTimeout: time.Duration(config.DefaultRouterIdleStreamTimeoutSeconds) * time.Second,
	}
	srv.routerClients = newRouterClientLimiter(0, 0)
	syncer.runRecovery = srv.background.Go
	for _, model := range shardedModels {
		model.Normalize()
		srv.shardedModels[model.LogicalID] = model
	}
	if mode != config.OrchestratorModeMinimal {
		deepseekBackend := deepseekadapter.NewDeepSeek(reg, disp, store, events)
		harness := newHarnessManager(map[string]agent.Backend{"deepseek": deepseekBackend}, harnessName, store)
		srv.harness = harness
		harness.registerBackend("pi", piagent.New(&localPiProvider{generate: srv}, deepseekBackend))
		deepseekBackend.SetWorkflowExecutor(srv.executeWorkflow)
	}
	if memoryPath != "" {
		srv.identityPath = memoryPath + ".identity"
	}
	srv.tasks.SetFinalizer(srv.synthesizeTask)
	if store != nil {
		capacity, ok := dispatcher.capacityGate.(*dispatchCapacityGate)
		if !ok {
			if closeErr := srv.Close(); closeErr != nil {
				logging.Errorf("orchestrator", "orchestrator: cleanup after shard capacity initialization failure: %v", closeErr)
			}
			return nil, errors.New("orchestrator: shard groups require the node dispatch capacity gate")
		}
		srv.shards, err = NewShardManager(store, reg, nodeClient, capacity, shard.Plan)
		if err != nil {
			if closeErr := srv.Close(); closeErr != nil {
				logging.Errorf("orchestrator", "orchestrator: cleanup after shard manager initialization failure: %v", closeErr)
			}
			return nil, fmt.Errorf("orchestrator: initialize shard manager: %w", err)
		}
		nodeClient.shards = srv.shards
		dispatcher.shards = srv.shards
		dispatcher.shardModel = nodeClient
		provisioner.shards = srv.shards
	}
	if srv.harness != nil {
		if err := srv.harness.restoreAgentSessions(); err != nil {
			if closeErr := srv.Close(); closeErr != nil {
				logging.Errorf("orchestrator", "orchestrator: cleanup after session restore failure: %v", closeErr)
			}
			return nil, fmt.Errorf("orchestrator: restore agent sessions: %w", err)
		}
	}
	if store != nil {
		logging.Infof("orchestrator", "orchestrator: memory store at %s", store.Path())
	}
	return srv, nil
}

func (s *Server) Close() error {
	s.background.Stop()
	if s.election != nil {
		s.election.close()
	}
	for _, task := range s.tasks.All() {
		if task.Status == taskmgr.StatusPending || task.Status == taskmgr.StatusRunning || task.Status == taskmgr.StatusSynthesis {
			if _, err := s.tasks.CancelChecked(task.ID); err != nil {
				logging.Errorf("orchestrator", "orchestrator: cancel task %s during shutdown: %v", task.ID, err)
			}
		}
	}
	s.dispatcherPort().close()
	var agentErr error
	if s.shards != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := s.shards.Close(ctx); err != nil {
			agentErr = err
		}
		cancel()
	}
	if s.harness != nil {
		if err := s.harnessPort().close(); agentErr == nil {
			agentErr = err
		}
	}
	if s.mem != nil {
		if err := s.mem.Close(); agentErr == nil {
			agentErr = err
		}
	}
	if err := s.tasks.Close(); agentErr == nil {
		agentErr = err
	}
	return agentErr
}

// Memory exposes the orchestrator's persistent knowledge store (nil when
// unavailable or disabled by minimal mode).
func (s *Server) Memory() memory.Store { return s.mem }

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/updates/manifest", s.handlePeerUpdate)
	mux.HandleFunc("GET /api/updates/binary", s.handlePeerUpdate)
	mux.HandleFunc("POST /api/pairing/join", s.handlePairingJoin)
	mux.HandleFunc("GET /api/pairing/status", s.handlePairingStatus)
	mux.HandleFunc("GET /api/status", s.handleSystemStatus)
	mux.HandleFunc("GET /api/metrics", s.handleMetrics)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /api/cluster/health", s.handleClusterHealth)
	mux.Handle("POST /api/pairing/pin", peeridentity.LoopbackOnly(http.HandlerFunc(s.handlePairingPINRegenerate)))
	mux.HandleFunc("POST /api/pairing/connect", s.handlePairingConnect)
	mux.HandleFunc("GET /api/pairing/roster", s.handlePairingRoster)
	mux.HandleFunc("POST /api/nodes/register", s.handleRegister)
	mux.HandleFunc("POST /api/nodes/{id}/eject", s.handleEject)
	mux.HandleFunc("GET /api/nodes", s.handleListNodes)
	mux.HandleFunc("GET /api/nodes/all", s.handleListAllNodes)
	mux.HandleFunc("POST /api/nodes/models/refresh", s.handleRefreshAllNodeModels)
	mux.HandleFunc("GET /api/version", s.handleVersion)
	mux.HandleFunc("GET /api/nodes/{id}/models", s.handleNodeModelsGet)
	mux.HandleFunc("PATCH /api/nodes/{id}/models", s.handleNodeModelsPatch)
	mux.HandleFunc("POST /api/nodes/{id}/pairing/connect", s.handleNodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/properties", s.handleNodeControl)
	mux.HandleFunc("PATCH /api/nodes/{id}/properties", s.handleNodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines", s.handleNodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines/managed", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/engines/managed/install-worker", s.handleNodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines/managed/head-models", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/engines/managed/configure-head", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/engines/managed/download-model", s.handleNodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines/managed/{engine_id}/profile", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/refresh", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/pull", s.handleNodeControl)
	mux.HandleFunc("GET /api/discovery", s.handleDiscovery)
	mux.HandleFunc("POST /api/discovery/{id}/pairing/invite", s.handlePairingInvite)
	mux.HandleFunc("GET /api/events", s.serveEvents)
	mux.HandleFunc("GET /api/router/requests", s.handleRouterEvents)
	mux.HandleFunc("POST /api/pipelines/{model_id}/reshard", s.handlePipelineReshard)
	mux.HandleFunc("POST /api/shards", s.handleCreateShardGroup)
	mux.HandleFunc("GET /api/shards", s.handleListShardGroups)
	mux.HandleFunc("GET /api/shards/{id}", s.handleGetShardGroup)
	mux.HandleFunc("DELETE /api/shards/{id}", s.handleDeleteShardGroup)
	mux.HandleFunc("POST /api/shards/{id}/start", s.handleStartShardGroup)
	mux.HandleFunc("POST /api/shards/{id}/stop", s.handleStopShardGroup)
	mux.HandleFunc("POST /api/shards/{id}/restart", s.handleRestartShardGroup)
	mux.HandleFunc("POST /api/shards/{id}/members", s.handleShardMembership)
	mux.HandleFunc("POST /router/{path...}", s.handleRouterRequest)
	mux.HandleFunc("POST /api/tasks", s.handleCreateTask)
	mux.HandleFunc("GET /api/tasks", s.handleListTasks)
	mux.HandleFunc("GET /api/tasks/resumable", s.handleListResumableTasks)
	mux.HandleFunc("GET /api/tasks/{id}", s.handleGetTask)
	mux.HandleFunc("GET /api/tasks/{id}/dependencies", s.handleTaskDependencies)
	mux.HandleFunc("GET /api/tasks/{id}/assignment-explanation", s.handleTaskAssignmentExplanation)
	mux.HandleFunc("POST /api/tasks/{id}/cancel", s.handleCancelTask)
	mux.HandleFunc("POST /api/tasks/{id}/approve", s.handleApproveTask)
	mux.HandleFunc("POST /api/tasks/{id}/resume", s.handleResumeTask)
	mux.HandleFunc("GET /api/tasks/{id}/checkpoints", s.handleTaskCheckpoints)

	if s.mode != config.OrchestratorModeMinimal {
		mux.HandleFunc("GET /api/agent/sessions", s.handleAgentSessionsList)
		mux.HandleFunc("POST /api/agent/sessions", s.handleAgentSessionsCreate)
		mux.HandleFunc("GET /api/agent/sessions/{id}", s.handleAgentSessionGet)
		mux.HandleFunc("DELETE /api/agent/sessions/{id}", s.handleAgentSessionDelete)
		mux.HandleFunc("POST /api/agent/sessions/{id}/messages", s.handleAgentMessagePost)
		mux.HandleFunc("POST /api/agent/sessions/{id}/interrupt", s.handleAgentSessionInterrupt)
		mux.HandleFunc("POST /api/agent/sessions/{id}/cancel", s.handleAgentSessionInterrupt)
		mux.HandleFunc("GET /api/agent/tools", s.handleAgentToolsList)
		mux.HandleFunc("GET /api/agent/models", s.handleAgentModelsList)
		mux.HandleFunc("GET /api/agent/stats", s.handleAgentStatsGet)
		mux.HandleFunc("GET /api/agent/backend", s.handleAgentBackendGet)
		mux.HandleFunc("PATCH /api/agent/backend", s.handleAgentBackendPatch)
	}
	mux.HandleFunc("GET /api/models", s.handleListAllModels)
	mux.HandleFunc("POST /api/generate", s.handleOllamaGenerate)
	mux.HandleFunc("GET /api/tags", s.handleOllamaTags)
	mux.HandleFunc("POST /api/chat", s.handleOllamaChat)
	mux.HandleFunc("POST /api/nodes/{id}/generate", s.handleNodeGenerate)
	mux.HandleFunc("POST /api/nodes/{id}/chat", s.handleNodeChat)
	mux.HandleFunc("POST /api/nodes/{id}/dialogues", s.handleNodeDialoguesProxy)
	mux.HandleFunc("GET /api/nodes/{id}/dialogues", s.handleNodeDialoguesProxy)
	mux.HandleFunc("/api/nodes/{id}/dialogues/", s.handleNodeDialoguesProxy)
	mux.HandleFunc("POST /api/nodes/{id}/models/{model}/generate", s.handleNodeModelGenerate)
	mux.HandleFunc("POST /api/nodes/{id}/models/{model}/test", s.handleNodeModelTest)

	mux.HandleFunc("GET /v1/models", s.handleOpenAIModels)
	mux.HandleFunc("POST /v1/chat/completions", s.handleOpenAIChatCompletions)
	mux.HandleFunc("POST /v1/completions", s.handleOpenAICompletions)
	mux.HandleFunc("POST /v1/messages", s.handleAnthropicMessages)
	mux.HandleFunc("GET /api/models/catalog", s.handleModelCatalog)

	if s.mode != config.OrchestratorModeMinimal {
		mux.HandleFunc("GET /api/memory/experiences", s.handleMemoryExperiences)
		mux.HandleFunc("GET /api/memory/insights", s.handleMemoryInsights)
		mux.HandleFunc("GET /api/memory/recommendations", s.handleMemoryRecommendations)
		mux.HandleFunc("GET /api/memory/facts", s.handleMemoryFactsGet)
		mux.HandleFunc("POST /api/memory/facts", s.handleMemoryFactsPost)
		mux.HandleFunc("DELETE /api/memory/facts/{key}", s.handleMemoryFactDelete)
		mux.HandleFunc("GET /api/memory/dialogues", s.handleMemoryDialogues)
		mux.HandleFunc("GET /api/memory/dialogues/{id}", s.handleMemoryDialogueGet)
		mux.HandleFunc("POST /api/memory/sync", s.handleMemorySync)
		mux.HandleFunc("GET /api/memory/sync", s.handleMemorySyncGet)
		mux.HandleFunc("POST /api/memory/prune", s.handleMemoryPrune)
	}

	mux.HandleFunc("POST /api/nodes/{id}/peer-converse", s.handleNodePeerConverse)

	staticFS, err := fs.Sub(web.StaticFiles, "static")
	if err != nil {
		logging.Errorf("orchestrator", "embedded web assets missing: %v", err)
		panic("orchestrator: embedded web assets missing")
	}

	mux.Handle("/", web.NewHandler(staticFS))
	return mux
}

// Run starts the health-sweep loop and the HTTP server. It blocks.
func (s *Server) Run(listenAddr string) error {
	defer func() {
		s.background.Stop()
		if s.election != nil {
			s.election.close()
		}
	}()
	return s.background.Run(func(ctx context.Context) error {
		return s.run(ctx, listenAddr)
	})
}

func (s *Server) run(ctx context.Context, listenAddr string) error {
	if s.advertiseAddr == "" {
		if err := s.SetAdvertiseAddress("https://" + discovery.OutboundIP() + portSuffix(listenAddr)); err != nil {
			return fmt.Errorf("orchestrator: configure pairing address: %w", err)
		}
	}
	if err := s.InitializeIdentity(); err != nil {
		return fmt.Errorf("orchestrator: initialize cluster identity: %w", err)
	}
	if s.shards != nil {
		if err := s.shards.Recover(ctx); err != nil {
			logging.Warnf("orchestrator", "some persisted shard groups could not be restored: %v", err)
		}
	}
	s.background.Go(s.healthSweepLoop)
	s.background.Go(s.leaderLogLoop)
	s.background.Go(s.syncer.taskHistorySyncLoop)
	if err := s.electionPort().startBeacon(ctx, listenAddr); err != nil {
		return fmt.Errorf("orchestrator: start cluster discovery: %w", err)
	}
	s.background.Go(func(ctx context.Context) {
		s.electionPort().discoverySyncLoop(ctx, s.publishSnapshot)
	})
	logging.Infof("orchestrator", "orchestrator listening on %s", listenAddr)
	mux := s.fenceStateChangingRequests(s.Mux())
	secure := requireElectedOrchestrator(s.isElectedOrchestrator, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pairing/join" {
			mux.ServeHTTP(w, r)
			return
		}
		s.identity.RequireTrustedPeer(mux).ServeHTTP(w, r)
	}), nil)
	loopback := peeridentity.LoopbackOnly(requireElectedOrchestrator(
		s.isElectedOrchestrator, mux, http.HandlerFunc(s.handleYieldedDashboard),
	))
	bodyLimit, err := httpx.LimitBodyBytes(httpx.DefaultMaxRequestBodyBytes)
	if err != nil {
		return fmt.Errorf("orchestrator: configure HTTP request limit: %w", err)
	}
	withMiddleware := func(next http.Handler) http.Handler {
		return selfupdate.Admission.Wrap(httpx.Chain(next, httpx.RequestLogger(nil), httpx.RequestID, bodyLimit))
	}
	return peeridentity.ServeDualContext(ctx,
		listenAddr,
		withMiddleware(loopback),
		withMiddleware(secure),
		s.identity.ServerTLSConfig(),
	)
}

// portSuffix extracts ":port" from a listen address like ":7450".
func portSuffix(listenAddr string) string {
	for i := len(listenAddr) - 1; i >= 0; i-- {
		if listenAddr[i] == ':' {
			return listenAddr[i:]
		}
	}
	return listenAddr
}
