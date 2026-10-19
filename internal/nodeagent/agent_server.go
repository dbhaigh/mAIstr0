package nodeagent

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/version"
	"github.com/maistr0/maistr0/web"
)

// Run starts the node's HTTP server and its background registration loop
// with the orchestrator. It blocks until the server stops.
func (a *Agent) Run() error {
	if err := a.InitializeIdentity(); err != nil {
		return fmt.Errorf("nodeagent: initialize cluster identity: %w", err)
	}
	mux := http.NewServeMux()
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
	mux.HandleFunc("POST /api/nodes/{id}/models/refresh", a.handleAPINodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/pull", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/discovery", a.handleAPIDiscovery)
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
		log.Fatalf("nodeagent: embedded web assets missing: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	cfg := a.configSnapshot()
	if cfg.DiscoveryEnabled {
		if a.discoveryListener == nil {
			a.discoveryListener = discovery.Listen(cfg.NodeID)
		}
		// Announce continuously: before joining this is how the cluster finds
		// us, and after joining it keeps us visible in every LAN panel.
		go discovery.Beacon(func() discovery.Announcement {
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
		}, 5*time.Second, nil)
	}
	go a.telemetryComponent.hardwareUsageLoop()
	go a.orchestratorLink.resolveOrchestratorAndRegister()
	go a.engineComponent.modelRefreshLoop()
	go a.orchestratorLink.peerPollLoop()
	go a.orchestratorLink.sseSubscribeLoop()
	go a.taskHistorySyncLoop()
	go a.syncPeerMemoryLoop()
	go a.orchestratorLink.broadcastLoop()
	go a.syncMemoryToOrchestrator()
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	go a.orchestratorLink.heartbeat.Run(heartbeatCtx)

	cfg = a.configSnapshot()
	log.Printf("node agent %q listening on %s (advertised as %s)", cfg.NodeID, cfg.ListenAddr, cfg.AdvertiseAddr)
	trustedHandler := a.identityComponent.requireTrustedPeer(a.currentCoordinatorWriteFence(mux))
	secureHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/pairing/invitation" {
			a.handlePairingInvitation(w, r)
			return
		}
		trustedHandler.ServeHTTP(w, r)
	})
	return peeridentity.ServeDual(
		cfg.ListenAddr,
		peeridentity.LoopbackOnly(mux),
		secureHandler,
		a.identityComponent.serverTLSConfig(),
	)
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
