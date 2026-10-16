// Package orchestrator implements the control-plane HTTP API and background
// bookkeeping: node registry maintenance, task decomposition/assignment, and
// dispatching subtasks out to node agents.
package orchestrator

import (
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/piagent"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/web"
)

type Server struct {
	registry           *cluster.Registry
	tasks              *taskmgr.Manager
	nodeClient         *nodeClient
	dispatcher         *dispatcherComponent
	planner            *taskPlanner
	election           *electionComponent
	events             *hub.Hub
	routerEvents       *routerEventLog
	harness            *harnessManager
	syncer             syncerAPI
	mem                memory.Store
	identityPath       string
	identity           *peeridentity.Identity
	advertiseAddr      string
	localNodeDashboard string
	mode               string
}

type SessionStore interface {
	AgentSessions() ([][]byte, error)
	SaveAgentSession(harness, id string, payload []byte) error
	DeleteAgentSession(harness, id string) error
}

const discoveryRequestTimeout = 5 * time.Second
const nodeCapacityFreshnessWindow = 6 * time.Second

func New() (*Server, error) { return NewWithMemory("") }

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
	return newWithMemoryHarnessAndConcurrency(cfg.MemoryPath, cfg.Harness, cfg.MaxConcurrentDispatches, cfg.Mode)
}

// NewWithConfig creates an orchestrator from its JSON configuration.
func NewWithConfig(cfg config.OrchestratorConfig) (*Server, error) {
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newWithMemoryHarnessAndConcurrency(cfg.MemoryPath, cfg.Harness, cfg.MaxConcurrentDispatches, cfg.Mode)
}

func newWithMemoryHarnessAndConcurrency(memoryPath, harnessName string, dispatchLimit int, mode string) (*Server, error) {
	if harnessName != "deepseek" && harnessName != "pi" {
		return nil, fmt.Errorf("unknown agent backend %q (want deepseek or pi)", harnessName)
	}
	reg := cluster.NewRegistry()
	disp := &http.Client{Timeout: 10 * time.Minute}
	selfID := "orchestrator-" + discovery.OutboundIP()
	events := hub.New()
	var store memory.Store
	if mode != config.OrchestratorModeMinimal {
		openedStore, err := memory.Open(memoryPath, selfID)
		if err != nil {
			log.Printf("orchestrator: memory unavailable, running stateless: %v", err)
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
	nodeClient := newNodeClient(&http.Client{Timeout: 10 * time.Second}, disp, &http.Client{Timeout: discoveryRequestTimeout})
	dispatcher := newDispatcherComponent(reg, tasks, store, nodeClient.dispatcher, nodeClient, dispatchLimit)
	planner := newTaskPlanner(schedulerAdapter{}, tasks, dispatcher, nodeClient, dispatcher.schedulerNodes, dispatcher.experience())
	election := newElectionComponent(reg, nodeClient, selfID, cluster.SelfScore(hardware.Detect()), true)
	syncer := newSyncerComponent(reg, tasks, nodeClient, store)
	srv := &Server{
		registry:     reg,
		nodeClient:   nodeClient,
		dispatcher:   dispatcher,
		planner:      planner,
		election:     election,
		events:       events,
		routerEvents: newRouterEventLog(200),
		syncer:       syncer,
		tasks:        tasks,
		mem:          store,
		identityPath: peeridentity.DefaultPath(selfID),
		mode:         mode,
	}
	if mode != config.OrchestratorModeMinimal {
		deepseekBackend := agent.NewDeepSeek(reg, disp, store, events)
		harness := newHarnessManager(map[string]agent.Backend{"deepseek": deepseekBackend}, harnessName, store)
		srv.harness = harness
		harness.registerBackend("pi", piagent.New(&localPiProvider{generate: srv}, deepseekBackend))
		deepseekBackend.SetWorkflowExecutor(srv.executeWorkflow)
	}
	if memoryPath != "" {
		srv.identityPath = memoryPath + ".identity"
	}
	srv.tasks.SetFinalizer(srv.synthesizeTask)
	if srv.harness != nil {
		if err := srv.harness.restoreAgentSessions(); err != nil {
			if closeErr := srv.Close(); closeErr != nil {
				log.Printf("orchestrator: cleanup after session restore failure: %v", closeErr)
			}
			return nil, fmt.Errorf("orchestrator: restore agent sessions: %w", err)
		}
	}
	if store != nil {
		log.Printf("orchestrator: memory store at %s", store.Path())
	}
	return srv, nil
}

func (s *Server) Close() error {
	for _, task := range s.tasks.All() {
		if task.Status == taskmgr.StatusPending || task.Status == taskmgr.StatusRunning || task.Status == taskmgr.StatusSynthesis {
			if _, err := s.tasks.CancelChecked(task.ID); err != nil {
				log.Printf("orchestrator: cancel task %s during shutdown: %v", task.ID, err)
			}
		}
	}
	s.dispatcherPort().close()
	var agentErr error
	if s.harness != nil {
		agentErr = s.harnessPort().close()
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
	mux.HandleFunc("POST /api/pairing/join", s.handlePairingJoin)
	mux.HandleFunc("GET /api/pairing/status", s.handlePairingStatus)
	mux.HandleFunc("GET /api/status", s.handleSystemStatus)
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
	mux.HandleFunc("POST /api/nodes/{id}/models/refresh", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/pull", s.handleNodeControl)
	mux.HandleFunc("GET /api/discovery", s.handleDiscovery)
	mux.HandleFunc("POST /api/discovery/{id}/pairing/invite", s.handlePairingInvite)
	mux.HandleFunc("GET /api/events", s.events.ServeHTTP)
	mux.HandleFunc("GET /api/router/requests", s.handleRouterEvents)
	mux.HandleFunc("POST /router/{path...}", s.handleRouterRequest)
	mux.HandleFunc("POST /api/tasks", s.handleCreateTask)
	mux.HandleFunc("GET /api/tasks", s.handleListTasks)
	mux.HandleFunc("GET /api/tasks/resumable", s.handleListResumableTasks)
	mux.HandleFunc("GET /api/tasks/{id}", s.handleGetTask)
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
		log.Fatalf("orchestrator: embedded web assets missing: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticFS)))
	return mux
}

// Run starts the health-sweep loop and the HTTP server. It blocks.
func (s *Server) Run(listenAddr string) error {
	if s.advertiseAddr == "" {
		if err := s.SetAdvertiseAddress("https://" + discovery.OutboundIP() + portSuffix(listenAddr)); err != nil {
			return fmt.Errorf("orchestrator: configure pairing address: %w", err)
		}
	}
	if err := s.InitializeIdentity(); err != nil {
		return fmt.Errorf("orchestrator: initialize cluster identity: %w", err)
	}
	go s.healthSweepLoop()
	go s.broadcastLoop()
	go s.syncer.taskHistorySyncLoop()
	if err := s.electionPort().startBeacon(listenAddr); err != nil {
		return fmt.Errorf("orchestrator: start cluster discovery: %w", err)
	}
	go s.electionPort().discoverySyncLoop(s.publishSnapshot)
	log.Printf("orchestrator listening on %s", listenAddr)
	mux := s.Mux()
	secure := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.isElectedOrchestrator() {
			http.Error(w, "this instance yielded orchestrator leadership to a higher-ranked cluster member", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/api/pairing/join" {
			mux.ServeHTTP(w, r)
			return
		}
		s.identity.RequireTrustedPeer(mux).ServeHTTP(w, r)
	})
	loopback := peeridentity.LoopbackOnly(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.isElectedOrchestrator() {
			s.handleYieldedDashboard(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	return peeridentity.ServeDual(
		listenAddr,
		loopback,
		secure,
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
