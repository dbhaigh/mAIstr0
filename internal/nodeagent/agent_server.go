package nodeagent

import (
	"context"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io/fs"
	"net/http"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/selfupdate"
	"github.com/maistr0/maistr0/internal/version"
	"github.com/maistr0/maistr0/web"
)

// Run starts the node's HTTP server and its background registration loop
// with the orchestrator. It blocks until the server stops.
func (a *Agent) Run() error {
	defer a.orchestratorLink.background.Stop()
	return a.orchestratorLink.background.Run(a.run)
}

func (a *Agent) run(ctx context.Context) error {
	if err := a.InitializeIdentity(); err != nil {
		return fmt.Errorf("nodeagent: initialize cluster identity: %w", err)
	}
	mux := http.NewServeMux()
	distribution, err := selfupdate.CurrentDistribution()
	if err != nil {
		return fmt.Errorf("nodeagent: configure peer update distribution: %w", err)
	}
	mux.Handle("GET /api/updates/manifest", distribution)
	mux.Handle("GET /api/updates/binary", distribution)
	mux.HandleFunc("/status", a.handleStatus)
	mux.HandleFunc("GET /api/version", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": version.String()})
	})
	mux.HandleFunc("GET /api/status", a.handleSystemStatus)
	mux.HandleFunc("GET /api/pairing/status", a.handlePairingStatus)
	mux.HandleFunc("POST /api/pairing/connect", a.handlePairingConnect)
	mux.HandleFunc("POST /api/pairing/invitation", a.handlePairingInvitation)
	mux.Handle("GET /api/pairing/invitation", peeridentity.LoopbackOnly(http.HandlerFunc(a.handlePairingInvitationStatus)))
	mux.Handle("POST /api/pairing/invitation/accept", peeridentity.LoopbackOnly(http.HandlerFunc(a.handlePairingInvitationAccept)))
	mux.Handle("POST /api/pairing/invitation/reject", peeridentity.LoopbackOnly(http.HandlerFunc(a.handlePairingInvitationReject)))
	mux.HandleFunc("/execute", a.handleExecute)
	mux.HandleFunc("POST /stage/load", a.handleStageLoad)
	mux.HandleFunc("POST /stage/forward", a.handleStageForward)
	mux.HandleFunc("POST /stage/unload", a.handleStageUnload)
	mux.HandleFunc("POST /stage/open", a.handleStageOpen)
	mux.HandleFunc("POST /stage/prefill", a.handleStagePrefill)
	mux.HandleFunc("POST /stage/decode", a.handleStageDecode)
	mux.HandleFunc("POST /stage/close", a.handleStageClose)
	mux.HandleFunc("POST /generate", a.handleGenerate)
	mux.HandleFunc("POST /chat", a.handleChat)
	mux.HandleFunc("POST /proxy/{path...}", a.handleTransparentProxy)
	mux.HandleFunc("POST /dialogues", a.handleDialogueCreate)
	mux.HandleFunc("GET /dialogues", a.handleDialogueList)
	mux.HandleFunc("GET /dialogues/{id}", a.handleDialogueGet)
	mux.HandleFunc("DELETE /dialogues/{id}", a.handleDialogueDelete)
	mux.HandleFunc("POST /dialogues/{id}/messages", a.handleDialogueMessage)
	mux.HandleFunc("POST /dialogues/{id}/chat", a.handleDialogueMessage)
	mux.HandleFunc("POST /dialogues/{id}/interrupt", a.handleDialogueInterrupt)
	mux.HandleFunc("POST /dialogues/{id}/cancel", a.handleDialogueInterrupt)

	mux.HandleFunc("/models", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			a.handleModelsPatch(w, r)
			return
		}
		a.handleModelsGet(w, r)
	})
	mux.HandleFunc("/models/all", a.handleModelsAllGet)
	mux.HandleFunc("POST /models/refresh", a.handleModelsRefresh)
	mux.HandleFunc("/models/pull", a.handleModelPull)
	mux.HandleFunc("/engine", a.handleEngineInfo)
	mux.HandleFunc("/properties", a.handleProperties)
	mux.HandleFunc("GET /peers", a.handlePeersGet)
	mux.HandleFunc("POST /submit", a.handleSubmit)
	mux.HandleFunc("GET /tasks", a.handleTasksList)
	mux.HandleFunc("GET /tasks/{id}", a.handleTaskGet)
	mux.HandleFunc("POST /tasks/{id}/cancel", a.handleTaskCancel)
	mux.HandleFunc("POST /tasks/sync", a.handleTaskSync)
	mux.HandleFunc("POST /tasks/replica", a.handleTaskReplica)
	mux.HandleFunc("GET /memory/sync", a.handlePeerMemorySyncGet)
	mux.HandleFunc("POST /memory/sync", a.handlePeerMemorySyncPost)

	// Node-to-node LLM conversation: /peer/message answers an inbound
	// utterance from another node's model, /peer/converse drives a full
	// multi-round exchange between this node's LLM and a peer's LLM.
	mux.HandleFunc("POST /peer/message", a.handlePeerMessage)
	mux.HandleFunc("POST /peer/converse", a.handlePeerConverse)

	mux.HandleFunc("GET /memory/experiences", a.handleMemoryExperiences)
	mux.HandleFunc("GET /memory/insights", a.handleMemoryInsights)
	mux.HandleFunc("GET /memory/facts", a.handleMemoryFacts)
	mux.HandleFunc("POST /memory/facts", a.handleMemoryFacts)

	// Every node serves the exact same control-plane GUI as the
	// orchestrator, backed by /api/* aliases below, so any node shows the
	// same cluster information and can submit work on its members' behalf.
	mux.HandleFunc("GET /api/nodes", a.handleAPINodesGet)
	mux.HandleFunc("GET /api/nodes/all", a.handleAPINodesGet)
	mux.HandleFunc("GET /api/nodes/{id}/models", a.handleAPINodeModelsGet)
	mux.HandleFunc("PATCH /api/nodes/{id}/models", a.handleAPINodeModelsPatch)
	mux.HandleFunc("POST /api/nodes/{id}/pairing/connect", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/properties", a.handleAPINodeControl)
	mux.HandleFunc("PATCH /api/nodes/{id}/properties", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines/managed", a.handleAPINodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/engines/managed/install-worker", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines/managed/head-models", a.handleAPINodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/engines/managed/configure-head", a.handleAPINodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/engines/managed/download-model", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines/managed/{engine_id}/profile", a.handleAPINodeControl)
	registerManagedEngineRoutes(mux, a)
	mux.HandleFunc("POST /api/nodes/{id}/models/refresh", a.handleAPINodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/pull", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/discovery", a.handleAPIDiscovery)
	mux.HandleFunc("GET /api/cluster/health", a.handleAPIOrchestratorRead)
	mux.HandleFunc("GET /api/security/status", a.handleAPISecurityStatus)
	mux.HandleFunc("GET /api/shards", a.handleAPIOrchestratorRead)
	mux.HandleFunc("POST /api/shards", a.handleAPIOrchestratorProxy)
	mux.HandleFunc("DELETE /api/shards/{id}", a.handleAPIOrchestratorProxy)
	mux.HandleFunc("POST /api/shards/{id}/start", a.handleAPIOrchestratorProxy)
	mux.HandleFunc("POST /api/shards/{id}/stop", a.handleAPIOrchestratorProxy)
	mux.HandleFunc("POST /api/shards/{id}/restart", a.handleAPIOrchestratorProxy)
	mux.HandleFunc("POST /api/shards/{id}/members", a.handleAPIOrchestratorProxy)
	mux.HandleFunc("POST /api/discovery/{id}/pairing/invite", a.handleAPIDiscoveryPairingInvite)
	mux.HandleFunc("GET /api/events", a.orchestratorLink.events.ServeHTTP)
	mux.HandleFunc("POST /api/tasks", a.handleAPITasksPost)
	mux.HandleFunc("GET /api/tasks", a.handleAPITasksList)
	mux.HandleFunc("GET /api/tasks/{id}", a.handleAPITaskGet)
	mux.HandleFunc("GET /api/tasks/{id}/assignment-explanation", a.handleAPITaskAssignmentExplanation)
	mux.HandleFunc("POST /api/tasks/{id}/cancel", a.handleAPITaskCancel)
	mux.HandleFunc("POST /api/tasks/{id}/approve", a.handleAPITaskApprove)
	mux.HandleFunc("POST /api/tasks/{id}/resume", a.handleAPITaskResume)
	mux.HandleFunc("GET /api/tasks/{id}/checkpoints", a.handleAPITaskCheckpoints)
	mux.HandleFunc("POST /api/generate", a.handleGenerate)
	mux.HandleFunc("POST /api/chat", a.handleChat)
	mux.HandleFunc("POST /api/nodes/{id}/dialogues", a.handleAPINodeDialogue)
	mux.HandleFunc("GET /api/nodes/{id}/dialogues", a.handleAPINodeDialogue)
	mux.HandleFunc("/api/nodes/{id}/dialogues/", a.handleAPINodeDialogue)
	mux.HandleFunc("POST /api/dialogues", a.handleDialogueCreate)
	mux.HandleFunc("GET /api/dialogues", a.handleDialogueList)
	mux.HandleFunc("GET /api/dialogues/{id}", a.handleDialogueGet)
	mux.HandleFunc("DELETE /api/dialogues/{id}", a.handleDialogueDelete)
	mux.HandleFunc("POST /api/dialogues/{id}/messages", a.handleDialogueMessage)
	mux.HandleFunc("POST /api/dialogues/{id}/interrupt", a.handleDialogueInterrupt)
	mux.HandleFunc("POST /api/dialogues/{id}/cancel", a.handleDialogueInterrupt)
	mux.HandleFunc("POST /api/peer/converse", a.handlePeerConverse)
	mux.HandleFunc("GET /api/memory/experiences", a.handleMemoryExperiences)
	mux.HandleFunc("GET /api/memory/sync", a.handleAPIMemorySync)
	mux.HandleFunc("GET /api/memory/insights", a.handleMemoryInsights)
	mux.HandleFunc("/api/agent/", a.handleAPIAgentProxy)

	staticFS, err := fs.Sub(web.StaticFiles, "static")
	if err != nil {
		logging.Errorf("nodeagent", "embedded web assets missing: %v", err)
		panic("nodeagent: embedded web assets missing")
	}
	mux.Handle("/", web.NewHandler(staticFS))

	cfg := a.configSnapshot()
	if cfg.DiscoveryEnabled {
		if a.discoveryListener == nil {
			a.discoveryListener = discovery.ListenWithSecret(cfg.DiscoveryHMACKey(), cfg.NodeID)
			defer a.discoveryListener.Close()
		}
		// Announce continuously: before joining this is how the cluster finds
		// us, and after joining it keeps us visible in every LAN panel.
		a.orchestratorLink.background.Go(func(ctx context.Context) {
			discovery.BeaconWithSecret(func() discovery.Announcement {
				if a.orchestratorLink.localOrchestratorIDValue() == cfg.NodeID {
					return discovery.Announcement{}
				}
				orchestratorID := ""
				if a.isJoined() {
					orchestratorID = a.getOrchestratorID()
				}
				role := "node"
				peerAddr := a.configSnapshot().AdvertiseAddr
				if candidateAddr := a.orchestratorLink.electionCandidateAddrValue(); candidateAddr != "" {
					role = "candidate"
					peerAddr = candidateAddr
				}
				return discovery.Announcement{
					Role: role, ID: cfg.NodeID,
					HTTPAddr:       "http://" + discovery.OutboundIP() + portSuffix(cfg.ListenAddr),
					PeerAddr:       peerAddr,
					Score:          cluster.SelfScore(a.hardwareSnapshot()),
					OrchestratorID: orchestratorID,
				}
			}, 5*time.Second, ctx.Done(), cfg.DiscoveryHMACKey())
		})
	}
	a.orchestratorLink.background.Go(a.telemetryComponent.hardwareUsageLoopContext)
	a.orchestratorLink.background.Go(a.orchestratorLink.resolveOrchestratorAndRegister)
	a.orchestratorLink.background.Go(a.engineComponent.modelRefreshLoopContext)
	a.orchestratorLink.background.Go(a.orchestratorLink.peerPollLoop)
	a.orchestratorLink.background.Go(a.orchestratorLink.sseSubscribeLoop)
	a.orchestratorLink.background.Go(a.orchestratorLink.syncer.taskHistorySyncLoop)
	a.orchestratorLink.background.Go(a.orchestratorLink.syncer.syncPeerMemoryLoop)
	a.orchestratorLink.background.Go(a.orchestratorLink.broadcastLoop)
	a.orchestratorLink.background.Go(a.syncMemoryToOrchestratorContext)
	a.orchestratorLink.background.Go(a.orchestratorLink.heartbeat.Run)

	cfg = a.configSnapshot()
	logging.Infof("nodeagent", "node agent %q listening on %s (advertised as %s)", cfg.NodeID, cfg.ListenAddr, cfg.AdvertiseAddr)
	trustedHandler := a.identityComponent.requireTrustedPeer(a.currentCoordinatorWriteFence(mux))
	secureHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/pairing/invitation" {
			a.handlePairingInvitation(w, r)
			return
		}
		trustedHandler.ServeHTTP(w, r)
	})
	bodyLimit, err := httpx.LimitBodyBytes(httpx.DefaultMaxRequestBodyBytes)
	if err != nil {
		return fmt.Errorf("nodeagent: configure HTTP request limit: %w", err)
	}
	withMiddleware := func(next http.Handler) http.Handler {
		return selfupdate.Admission.Wrap(httpx.Chain(next, httpx.RequestLogger(nil), httpx.RequestID, bodyLimit))
	}
	return peeridentity.ServeDualContext(ctx,
		cfg.ListenAddr,
		withMiddleware(peeridentity.LoopbackOnly(mux)),
		withMiddleware(secureHandler),
		a.identityComponent.serverTLSConfig(),
	)
}

func registerManagedEngineRoutes(mux *http.ServeMux, agent *Agent) {
	mux.Handle("GET /api/engines/managed", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleManagedEngineStatus)))
	mux.Handle("POST /api/engines/managed/install-worker", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleManagedWorkerInstall)))
	mux.Handle("GET /api/engines/managed/head-models", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleLocalHeadModels)))
	mux.Handle("POST /api/engines/managed/configure-head", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleConfigureLocalHead)))
	mux.Handle("POST /api/engines/managed/download-model", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleDownloadHeadModel)))
	mux.Handle("GET /api/engines/managed/{id}/profile", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleManagedEngineModelProfile)))
	mux.Handle("POST /api/engines/managed/{id}/start", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleManagedEngineStart)))
	mux.Handle("POST /api/engines/managed/{id}/stop", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleManagedEngineStop)))
	mux.Handle("POST /api/engines/managed/{id}/restart", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleManagedEngineRestart)))
	mux.Handle("POST /api/engines/managed/{id}/proxy/{path...}", agent.requireManagedEngineOrchestrator(http.HandlerFunc(agent.handleManagedEngineProxy)))
}

func (a *Agent) currentElectedOrchestratorID() string {
	if a.orchestratorLink.discoveryListener != nil {
		if leader, ok := a.orchestratorLink.discoveryListener.BestOrchestrator(); ok {
			return leader.ID
		}
	}
	if id := a.getOrchestratorID(); id != "" {
		return id
	}
	return a.mostCapableNode().ID
}

func (a *Agent) currentCoordinatorWriteFence(next http.Handler) http.Handler {
	return currentCoordinatorWriteFence(
		next,
		a.identityComponent.IsTrustedOrchestratorID,
		a.currentElectedOrchestratorID,
	)
}

func currentCoordinatorWriteFence(next http.Handler, isOrchestrator func(string) bool, electedID func() string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			next.ServeHTTP(w, r)
			return
		}
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			next.ServeHTTP(w, r)
			return
		}
		peerID := r.TLS.PeerCertificates[0].Subject.CommonName
		if isOrchestrator(peerID) && electedID() != peerID {
			http.Error(w, "state-changing request requires the current elected orchestrator", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
