// Package orchestrator implements the control-plane HTTP API and background
// bookkeeping: node registry maintenance, task decomposition/assignment, and
// dispatching subtasks out to node agents.
package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/piagent"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
	"github.com/maistr0/maistr0/web"
)

type Server struct {
	registry            *cluster.Registry
	tasks               *taskmgr.Manager
	client              *http.Client // short timeout, for node proxy/status calls
	dispatcher          *http.Client // long timeout, subtask execution can cold-load a model
	dispatchSlots       chan struct{}
	discovery           *discovery.Listener
	selfID              string
	discoveryEnabled    bool
	events              *hub.Hub
	agentMu             sync.RWMutex
	agents              map[string]agent.Backend
	activeHarness       string
	sessionHarnesses    map[string]string
	mem                 *memory.Store
	selfScore           float64 // announced on the LAN for orchestrator election
	leaderMu            sync.Mutex
	lastLeaderID        string // last announced leader, to log transitions once
	discoveredNodes     map[string]bool
	identityPath        string
	identity            *peeridentity.Identity
	advertiseAddr       string
	peerMu              sync.RWMutex
	pairedOrchestrators map[string]string
}

func New() (*Server, error) { return NewWithMemory("") }

// NewWithMemory builds an orchestrator backed by a persistent memory
// database at memoryPath (empty selects the default per-user location).
func NewWithMemory(memoryPath string) (*Server, error) {
	return NewWithMemoryAndHarness(memoryPath, "pi")
}

// NewWithMemoryAndHarness builds an orchestrator using the requested agent
// backend. The returned error identifies unsupported backend names.
func NewWithMemoryAndHarness(memoryPath, harnessName string) (*Server, error) {
	reg := cluster.NewRegistry()
	disp := &http.Client{Timeout: 10 * time.Minute}
	selfID := "orchestrator-" + discovery.OutboundIP()
	harnessName = strings.ToLower(strings.TrimSpace(harnessName))
	if harnessName == "" {
		harnessName = "pi"
	}
	events := hub.New()
	store, err := memory.Open(memoryPath, selfID)
	if err != nil {
		log.Printf("orchestrator: memory unavailable, running stateless: %v", err)
	}
	taskPath := taskmgr.DefaultPath(selfID)
	if memoryPath != "" {
		taskPath = memoryPath + ".tasks"
	}
	tasks, err := taskmgr.OpenManager(taskPath, nil)
	if err != nil {
		if store != nil {
			_ = store.Close()
		}
		return nil, fmt.Errorf("orchestrator: open task history: %w", err)
	}
	deepseekBackend := agent.NewDeepSeek(reg, disp, store, events)
	if harnessName != "deepseek" && harnessName != "pi" {
		if store != nil {
			_ = store.Close()
		}
		_ = tasks.Close()
		return nil, fmt.Errorf("unknown agent backend %q (want deepseek or pi)", harnessName)
	}
	backends := map[string]agent.Backend{"deepseek": deepseekBackend}
	srv := &Server{
		registry:            reg,
		client:              &http.Client{Timeout: 10 * time.Second},
		dispatcher:          disp,
		dispatchSlots:       make(chan struct{}, 16),
		selfID:              selfID,
		selfScore:           cluster.SelfScore(hardware.Detect()),
		discoveryEnabled:    true,
		events:              events,
		discoveredNodes:     make(map[string]bool),
		pairedOrchestrators: make(map[string]string),
		agents:              backends,
		activeHarness:       harnessName,
		sessionHarnesses:    make(map[string]string),
		tasks:               tasks,
		mem:                 store,
		identityPath:        peeridentity.DefaultPath(selfID),
	}
	srv.agents["pi"] = piagent.New(&localPiProvider{server: srv}, deepseekBackend)
	if memoryPath != "" {
		srv.identityPath = memoryPath + ".identity"
	}
	srv.tasks.SetFinalizer(srv.synthesizeTask)
	if err := srv.restoreAgentSessions(); err != nil {
		if closeErr := srv.Close(); closeErr != nil {
			log.Printf("orchestrator: cleanup after session restore failure: %v", closeErr)
		}
		return nil, fmt.Errorf("orchestrator: restore agent sessions: %w", err)
	}
	if store != nil {
		log.Printf("orchestrator: memory store at %s", store.Path())
	}
	return srv, nil
}

// HarnessName reports the configured interactive agent backend.
func (s *Server) HarnessName() string {
	s.agentMu.RLock()
	defer s.agentMu.RUnlock()
	return s.activeHarness
}

func (s *Server) currentAgent() (string, agent.Backend) {
	s.agentMu.RLock()
	defer s.agentMu.RUnlock()
	return s.activeHarness, s.agents[s.activeHarness]
}

func (s *Server) agentForSession(id string) (string, agent.Backend, bool) {
	s.agentMu.RLock()
	defer s.agentMu.RUnlock()
	name, ok := s.sessionHarnesses[id]
	if !ok {
		return "", nil, false
	}
	backend, ok := s.agents[name]
	return name, backend, ok
}

func (s *Server) selectHarness(name string) bool {
	s.agentMu.Lock()
	defer s.agentMu.Unlock()
	if _, ok := s.agents[name]; !ok {
		return false
	}
	s.activeHarness = name
	return true
}

func (s *Server) listAgentSessions() []*agent.Session {
	s.agentMu.RLock()
	owners := make(map[string]string, len(s.sessionHarnesses))
	backends := make(map[string]agent.Backend, len(s.agents))
	for id, name := range s.sessionHarnesses {
		owners[id] = name
	}
	for name, backend := range s.agents {
		backends[name] = backend
	}
	s.agentMu.RUnlock()

	sessions := make([]*agent.Session, 0, len(owners))
	for id, name := range owners {
		session, ok := backends[name].GetSession(id)
		if !ok {
			continue
		}
		session.Harness = name
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	return sessions
}

func (s *Server) restoreAgentSessions() error {
	if s.mem == nil {
		return nil
	}
	records, err := s.mem.AgentSessions()
	if err != nil {
		return err
	}
	sessions := make([]*agent.Session, 0, len(records))
	for _, record := range records {
		var session agent.Session
		if err := json.Unmarshal(record, &session); err != nil {
			return fmt.Errorf("decode persisted session: %w", err)
		}
		if session.ID == "" || session.Harness == "" {
			return errors.New("persisted agent session is missing its ID or harness")
		}
		session.Active = false
		sessions = append(sessions, &session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].Harness != sessions[j].Harness {
			return sessions[i].Harness == "deepseek"
		}
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	for _, session := range sessions {
		backend, exists := s.agents[session.Harness]
		if !exists {
			return fmt.Errorf("persisted session %q has unsupported harness %q", session.ID, session.Harness)
		}
		restorer, ok := backend.(agent.SessionRestorer)
		if !ok {
			return fmt.Errorf("agent backend %q cannot restore sessions", session.Harness)
		}
		if err := restorer.RestoreSession(session); err != nil {
			return fmt.Errorf("restore session %q for %s: %w", session.ID, session.Harness, err)
		}
		s.sessionHarnesses[session.ID] = session.Harness
	}
	return nil
}

func (s *Server) persistAgentSession(session *agent.Session) error {
	if s.mem == nil {
		return nil
	}
	if session == nil || session.ID == "" || session.Harness == "" {
		return errors.New("session ID and harness are required")
	}
	encoded, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("encode agent session: %w", err)
	}
	return s.mem.SaveAgentSession(session.Harness, session.ID, encoded)
}

func (s *Server) agentStats() agent.AgentStats {
	_, activeBackend := s.currentAgent()
	stats := activeBackend.Stats()
	stats.TotalSessions = 0
	stats.ActiveSessions = 0
	stats.TotalMessages = 0
	stats.TotalToolCalls = 0
	for _, session := range s.listAgentSessions() {
		stats.TotalSessions++
		if session.Active {
			stats.ActiveSessions++
		}
		stats.TotalMessages += len(session.Messages)
		for _, message := range session.Messages {
			stats.TotalToolCalls += len(message.ToolCalls)
		}
	}
	return stats
}

func (s *Server) Close() error {
	s.agentMu.RLock()
	backends := make([]agent.Backend, 0, len(s.agents))
	for _, backend := range s.agents {
		backends = append(backends, backend)
	}
	s.agentMu.RUnlock()
	var agentErr error
	for _, backend := range backends {
		if err := backend.Close(); err != nil && agentErr == nil {
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
// the database could not be opened).
func (s *Server) Memory() *memory.Store { return s.mem }

// SetDiscoveryEnabled controls whether this orchestrator announces itself
// and listens for other peers on the LAN.
func (s *Server) SetDiscoveryEnabled(enabled bool) { s.discoveryEnabled = enabled }

// SetDiscovery attaches a (possibly shared) discovery listener, e.g. when
// running orchestrator+node in one process so they share a single UDP
// socket instead of each binding their own.
func (s *Server) SetDiscovery(l *discovery.Listener) { s.discovery = l }

// SelfID returns the ID this orchestrator announces itself as on the LAN.
func (s *Server) SelfID() string { return s.selfID }

func (s *Server) SetAdvertiseAddress(address string) error {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("orchestrator advertised address must be an https URL without user information")
	}
	s.advertiseAddr = "https://" + parsed.Host
	return nil
}

// InitializeIdentity loads the persistent TLS identity and creates a
// temporary pairing PIN for first-time node enrollment.
func (s *Server) InitializeIdentity() error {
	if s.identity != nil {
		return nil
	}
	identity, err := peeridentity.Open(s.identityPath, s.selfID)
	if err != nil {
		return err
	}
	pin, err := identity.NewPairingPIN()
	if err != nil {
		return err
	}
	s.identity = identity
	s.client.Transport = identity.ClientTransport()
	s.dispatcher.Transport = identity.ClientTransport()
	s.peerMu.Lock()
	for id, address := range identity.PairedOrchestratorAddresses() {
		s.pairedOrchestrators[id] = address
	}
	s.peerMu.Unlock()
	log.Printf("orchestrator: secure pairing PIN (valid for 30 minutes): %s", pin)
	return nil
}

func (s *Server) IdentityCertificate() (string, error) {
	if s.identity == nil {
		return "", errors.New("orchestrator identity is not initialized")
	}
	return s.identity.CertificatePEM(), nil
}

func (s *Server) TrustPeer(id, certificatePEM string) error {
	if s.identity == nil {
		return errors.New("orchestrator identity is not initialized")
	}
	return s.identity.TrustPeer(id, certificatePEM)
}

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pairing/join", s.handlePairingJoin)
	mux.HandleFunc("GET /api/pairing/status", s.handlePairingStatus)
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
	mux.HandleFunc("GET /api/nodes/{id}/properties", s.handleNodeControl)
	mux.HandleFunc("PATCH /api/nodes/{id}/properties", s.handleNodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/refresh", s.handleNodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/pull", s.handleNodeControl)
	mux.HandleFunc("GET /api/discovery", s.handleDiscovery)
	mux.HandleFunc("GET /api/events", s.events.ServeHTTP)
	mux.HandleFunc("POST /api/tasks", s.handleCreateTask)
	mux.HandleFunc("GET /api/tasks", s.handleListTasks)
	mux.HandleFunc("GET /api/tasks/{id}", s.handleGetTask)
	mux.HandleFunc("POST /api/tasks/{id}/cancel", s.handleCancelTask)

	// Interactive agent backend endpoints
	mux.HandleFunc("GET /api/agent/sessions", s.handleAgentSessionsList)
	mux.HandleFunc("POST /api/agent/sessions", s.handleAgentSessionsCreate)
	mux.HandleFunc("GET /api/agent/sessions/{id}", s.handleAgentSessionGet)
	mux.HandleFunc("DELETE /api/agent/sessions/{id}", s.handleAgentSessionDelete)
	mux.HandleFunc("POST /api/agent/sessions/{id}/messages", s.handleAgentMessagePost)
	mux.HandleFunc("GET /api/agent/tools", s.handleAgentToolsList)
	mux.HandleFunc("GET /api/agent/models", s.handleAgentModelsList)
	mux.HandleFunc("GET /api/agent/stats", s.handleAgentStatsGet)
	mux.HandleFunc("GET /api/agent/backend", s.handleAgentBackendGet)
	mux.HandleFunc("PATCH /api/agent/backend", s.handleAgentBackendPatch)
	// Direct cluster & node LLM model invocation endpoints
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

	// OpenAI-compatible LLM Gateway endpoints
	mux.HandleFunc("GET /v1/models", s.handleOpenAIModels)
	mux.HandleFunc("POST /v1/chat/completions", s.handleOpenAIChatCompletions)
	mux.HandleFunc("POST /v1/completions", s.handleOpenAICompletions)
	mux.HandleFunc("POST /v1/messages", s.handleAnthropicMessages)

	// Cluster memory / learning endpoints
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

	// Node-to-node LLM conversation, brokered by the orchestrator
	mux.HandleFunc("POST /api/nodes/{id}/peer-converse", s.handleNodePeerConverse)

	staticFS, err := fs.Sub(web.StaticFiles, "static")
	if err != nil {
		log.Fatalf("orchestrator: embedded web assets missing: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticFS)))
	return mux
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": version.String()})
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
	if s.discoveryEnabled {
		if s.discovery == nil {
			s.discovery = discovery.Listen(s.selfID)
		}
		selfAddr := "http://" + discovery.OutboundIP() + portSuffix(listenAddr)
		peerAddr := "https://" + discovery.OutboundIP() + portSuffix(listenAddr)
		go discovery.Beacon(func() discovery.Announcement {
			members := s.registry.Active()
			_, elected := s.registry.Leader()
			return discovery.Announcement{
				Role: "orchestrator", ID: s.selfID, HTTPAddr: selfAddr, PeerAddr: peerAddr,
				Score: s.selfScore, Members: len(members), Elected: elected,
			}
		}, 5*time.Second, nil)
	}
	go s.discoverySyncLoop()
	log.Printf("orchestrator listening on %s", listenAddr)
	mux := s.Mux()
	secure := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pairing/join" {
			mux.ServeHTTP(w, r)
			return
		}
		s.identity.RequireTrustedPeer(mux).ServeHTTP(w, r)
	})
	return peeridentity.ServeDual(
		listenAddr,
		peeridentity.LoopbackOnly(mux),
		secure,
		s.identity.ServerTLSConfig(),
	)
}

type pairingJoinRequest struct {
	ID             string            `json:"id"`
	Role           string            `json:"role"`
	PeerAddr       string            `json:"peer_addr,omitempty"`
	PIN            string            `json:"pin"`
	CertificatePEM string            `json:"certificate_pem"`
	Peers          map[string]string `json:"peers,omitempty"`
}

type pairingJoinResponse struct {
	ID             string            `json:"id"`
	Role           string            `json:"role"`
	CertificatePEM string            `json:"certificate_pem"`
	Peers          map[string]string `json:"peers"`
}

func (s *Server) handlePairingRoster(w http.ResponseWriter, _ *http.Request) {
	if s.identity == nil {
		http.Error(w, "identity is not initialized", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, s.identity.TrustedPeers())
}

func (s *Server) handlePairingStatus(w http.ResponseWriter, _ *http.Request) {
	if s.identity == nil {
		http.Error(w, "pairing is not initialized", http.StatusServiceUnavailable)
		return
	}
	status := map[string]any{"role": "orchestrator", "address": s.advertiseAddr}
	if pin, expiresAt, ok := s.identity.ActivePairingPIN(); ok {
		status["pin"] = pin
		status["expires_at"] = expiresAt
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handlePairingConnect(w http.ResponseWriter, r *http.Request) {
	if s.identity == nil {
		http.Error(w, "pairing is not initialized", http.StatusServiceUnavailable)
		return
	}
	var request struct {
		Address string `json:"address"`
		PIN     string `json:"pin"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&request); err != nil {
		http.Error(w, "invalid pairing request", http.StatusBadRequest)
		return
	}
	if err := s.PairWithOrchestrator(strings.TrimSpace(request.Address), strings.TrimSpace(request.PIN)); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"paired_orchestrator": strings.TrimSpace(request.Address),
	})
}

func (s *Server) handlePairingJoin(w http.ResponseWriter, r *http.Request) {
	if s.identity == nil {
		http.Error(w, "pairing is not initialized", http.StatusServiceUnavailable)
		return
	}
	if r.TLS == nil {
		log.Printf("orchestrator: rejected plaintext pairing request from %s", r.RemoteAddr)
		http.Error(w, "pairing requires HTTPS", http.StatusUpgradeRequired)
		return
	}
	var request pairingJoinRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request); err != nil {
		http.Error(w, "invalid pairing request", http.StatusBadRequest)
		return
	}
	if request.Role != "node" && request.Role != "orchestrator" {
		log.Printf("orchestrator: rejected pairing request from %s: invalid peer role", r.RemoteAddr)
		http.Error(w, "role must be node or orchestrator", http.StatusBadRequest)
		return
	}
	if request.ID == "" || request.ID == s.selfID {
		log.Printf("orchestrator: rejected pairing request from %s: invalid peer identity", r.RemoteAddr)
		http.Error(w, "invalid peer identity", http.StatusBadRequest)
		return
	}
	if err := s.identity.ValidatePeer(request.ID, request.CertificatePEM); err != nil {
		log.Printf("orchestrator: rejected pairing request from %s: invalid peer certificate: %v", r.RemoteAddr, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Role == "orchestrator" {
		for id, certificate := range request.Peers {
			if id == "" || id == s.selfID {
				log.Printf("orchestrator: rejected federation pairing request from %s: invalid peer roster identity", r.RemoteAddr)
				http.Error(w, "invalid peer roster identity", http.StatusBadRequest)
				return
			}
			if err := s.identity.ValidatePeer(id, certificate); err != nil {
				log.Printf("orchestrator: rejected federation pairing request from %s: invalid roster certificate: %v", r.RemoteAddr, err)
				http.Error(w, "invalid peer roster certificate: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
	}
	if !s.identity.ConsumePairingPIN(request.PIN) {
		log.Printf("orchestrator: rejected pairing request from %s: PIN invalid, expired, or already used", r.RemoteAddr)
		http.Error(w, "pairing PIN is invalid or expired", http.StatusUnauthorized)
		return
	}
	if err := s.identity.TrustPeerDetails(request.ID, request.CertificatePEM, request.Role, request.PeerAddr); err != nil {
		log.Printf("orchestrator: failed to save paired peer from %s: %v", r.RemoteAddr, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Role == "orchestrator" && request.PeerAddr != "" {
		s.peerMu.Lock()
		s.pairedOrchestrators[request.ID] = request.PeerAddr
		s.peerMu.Unlock()
	}
	if request.Role == "orchestrator" {
		for id, certificate := range request.Peers {
			if id == request.ID {
				continue
			}
			if err := s.identity.TrustPeer(id, certificate); err != nil {
				http.Error(w, "orchestrator paired but could not trust peer "+id+": "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	nextPIN, err := s.identity.NewPairingPIN()
	if err != nil {
		http.Error(w, "peer paired but could not create the next pairing PIN", http.StatusInternalServerError)
		return
	}
	log.Printf("orchestrator: secure pairing PIN (valid for 30 minutes): %s", nextPIN)
	log.Printf("orchestrator: successfully paired peer role=%s from %s", request.Role, r.RemoteAddr)
	writeJSON(w, http.StatusOK, pairingJoinResponse{
		ID: s.selfID, Role: "orchestrator", CertificatePEM: s.identity.CertificatePEM(),
		Peers: s.identity.TrustedPeers(),
	})
}

// PairWithOrchestrator enrolls this orchestrator with another using its
// one-use out-of-band PIN, then pins the returned identity and trusted roster.
func (s *Server) PairWithOrchestrator(address, pin string) (resultErr error) {
	if s.identity == nil {
		return errors.New("orchestrator identity is not initialized")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("pairing address must be an https URL without user information")
	}
	if pin == "" {
		return errors.New("pairing PIN is required")
	}
	log.Printf("orchestrator: starting federation pairing with %s", "https://"+parsed.Host)
	defer func() {
		if resultErr != nil {
			log.Printf("orchestrator: federation pairing with %s failed: %v", "https://"+parsed.Host, resultErr)
		}
	}()
	body, err := json.Marshal(pairingJoinRequest{
		ID: s.selfID, Role: "orchestrator", PeerAddr: s.advertiseAddr,
		PIN: pin, CertificatePEM: s.identity.CertificatePEM(), Peers: s.identity.TrustedPeers(),
	})
	if err != nil {
		return fmt.Errorf("encode orchestrator pairing request: %w", err)
	}
	address = "https://" + parsed.Host
	client := &http.Client{Timeout: 10 * time.Second, Transport: s.identity.BootstrapTransport()}
	resp, err := client.Post(address+"/api/pairing/join", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("pair with orchestrator at %s: %w", address, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("orchestrator pairing rejected: %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	var result pairingJoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode orchestrator pairing response: %w", err)
	}
	if result.Role != "orchestrator" || result.ID == "" || result.ID == s.selfID {
		return fmt.Errorf("pairing endpoint returned invalid orchestrator identity %q with role %q", result.ID, result.Role)
	}
	if err := s.identity.ValidatePeer(result.ID, result.CertificatePEM); err != nil {
		return err
	}
	if s.discovery != nil {
		for _, peer := range s.discovery.Snapshot() {
			if peer.Role == "orchestrator" && peer.PeerAddr == address && peer.ID != result.ID {
				return fmt.Errorf("pairing endpoint identity %q does not match discovered identity %q", result.ID, peer.ID)
			}
		}
	}
	if err := s.identity.TrustPeerDetails(result.ID, result.CertificatePEM, "orchestrator", address); err != nil {
		return err
	}
	for id, certificate := range result.Peers {
		if id == s.selfID {
			continue
		}
		if err := s.identity.TrustPeer(id, certificate); err != nil {
			return fmt.Errorf("trust paired roster member %q: %w", id, err)
		}
	}
	s.peerMu.Lock()
	s.pairedOrchestrators[result.ID] = address
	s.peerMu.Unlock()
	log.Printf("orchestrator: paired with orchestrator %s at %s", result.ID, address)
	return nil
}

func (s *Server) discoverySyncLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.syncDiscoveredPeers()
	}
}

func (s *Server) syncDiscoveredPeers() {
	visibleNodes := make(map[string]bool)
	type discoveryResult struct {
		peer   discovery.Peer
		status []cluster.NodeStatus
	}
	results := make(chan discoveryResult)
	var probes sync.WaitGroup
	peers := make(map[string]discovery.Peer)
	if s.discovery != nil {
		for _, peer := range s.discovery.Snapshot() {
			peers[peer.ID] = peer
		}
	}
	s.peerMu.RLock()
	for id, address := range s.pairedOrchestrators {
		peers[id] = discovery.Peer{Announcement: discovery.Announcement{
			Role: "orchestrator", ID: id, PeerAddr: address,
		}}
	}
	s.peerMu.RUnlock()
	for _, peer := range peers {
		if peer.PeerAddr == "" || !s.identity.IsTrustedID(peer.ID) {
			continue
		}
		if peer.Role != "node" && peer.Role != "orchestrator" {
			continue
		}
		probes.Add(1)
		go func(peer discovery.Peer) {
			defer probes.Done()
			if peer.Role == "node" {
				status, err := s.discoveredNodeStatus(peer.PeerAddr)
				if err != nil {
					log.Printf("orchestrator: secure status probe for paired node %s failed: %v", peer.ID, err)
					return
				}
				results <- discoveryResult{peer: peer, status: []cluster.NodeStatus{status}}
				return
			}
			nodes, err := s.fetchDiscoveredClusterNodes(peer.PeerAddr)
			if err != nil {
				log.Printf("orchestrator: secure cluster sync with paired orchestrator %s failed: %v", peer.ID, err)
				return
			}
			results <- discoveryResult{peer: peer, status: nodes}
		}(peer)
	}
	go func() {
		probes.Wait()
		close(results)
	}()
	for result := range results {
		if result.peer.Role == "node" {
			visibleNodes[result.peer.ID] = true
			s.discoveredNodes[result.peer.ID] = true
		}
		for _, status := range result.status {
			s.registry.Upsert(status)
		}
	}
	for id := range s.discoveredNodes {
		if visibleNodes[id] {
			continue
		}
		if _, ok := s.registry.Get(id); ok {
			s.registry.MarkUnhealthy(id)
			log.Printf("orchestrator: node %s is offline after its LAN announcement expired", id)
		}
		delete(s.discoveredNodes, id)
	}
	s.publishSnapshot()
}

func (s *Server) fetchDiscoveredClusterNodes(addr string) ([]cluster.NodeStatus, error) {
	client := &http.Client{Timeout: 1500 * time.Millisecond, Transport: s.identity.ClientTransport()}
	resp, err := client.Get(addr + "/api/nodes")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("cluster status endpoint returned %s", resp.Status)
	}
	var nodes []cluster.NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, fmt.Errorf("decode cluster status: %w", err)
	}
	return nodes, nil
}

func (s *Server) discoveredNodeStatus(addr string) (cluster.NodeStatus, error) {
	client := &http.Client{Timeout: 1500 * time.Millisecond, Transport: s.identity.ClientTransport()}
	resp, err := client.Get(addr + "/status")
	if err != nil {
		return cluster.NodeStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return cluster.NodeStatus{}, errors.New("node status returned " + resp.Status)
	}
	var status cluster.NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return cluster.NodeStatus{}, err
	}
	return status, nil
}

// handleDiscovery returns other mAIstr0 instances (orchestrators or nodes)
// found on the local network via UDP broadcast, for the GUI's network panel.
func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if s.discovery == nil {
		writeJSON(w, http.StatusOK, []discovery.Peer{})
		return
	}
	writeJSON(w, http.StatusOK, s.discovery.Snapshot())
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

func (s *Server) healthSweepLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, n := range s.registry.All() {
			if n.Healthy && time.Since(n.LastSeen) > 30*time.Second {
				s.registry.MarkUnhealthy(n.ID)
				log.Printf("orchestrator: node %s is offline after missing heartbeats", n.ID)
			}
		}
		s.publishSnapshot()
	}
}

// clusterSnapshot is the payload pushed over /api/events; the GUI applies
// it directly instead of polling the individual REST endpoints.
type clusterSnapshot struct {
	Nodes        []cluster.NodeStatus `json:"nodes"`
	Tasks        []*taskmgr.Task      `json:"tasks"`
	Discovery    []discovery.Peer     `json:"discovery"`
	BuildVersion string               `json:"build_version"`
	LeaderID     string               `json:"leader_id,omitempty"`
	MemberCount  int                  `json:"member_count"`
	AgentStats   *agent.AgentStats    `json:"agent_stats,omitempty"`
	Memory       *memory.Insights     `json:"memory,omitempty"`
}

func (s *Server) snapshot() clusterSnapshot {
	var disc []discovery.Peer
	if s.discovery != nil {
		disc = s.discovery.Snapshot()
	}
	nodes := s.registry.All()
	activeNodes := 0
	leaderID := ""
	for _, n := range nodes {
		if n.Healthy {
			activeNodes++
		}
		if n.Leader {
			leaderID = n.ID
			break
		}
	}
	s.leaderMu.Lock()
	if leaderID != s.lastLeaderID {
		if leaderID != "" {
			log.Printf("orchestrator: cluster leadership passed to %s (most capable node)", leaderID)
		}
		s.lastLeaderID = leaderID
	}
	s.leaderMu.Unlock()
	stats := s.agentStats()
	snap := clusterSnapshot{
		Nodes:        nodes,
		Tasks:        s.tasks.Recent(50),
		Discovery:    disc,
		BuildVersion: version.String(),
		LeaderID:     leaderID,
		MemberCount:  activeNodes,
		AgentStats:   &stats,
	}
	if s.mem != nil {
		ins := s.mem.Insights()
		snap.Memory = &ins
	}
	return snap
}

func (s *Server) publishSnapshot() {
	data, err := json.Marshal(s.snapshot())
	if err != nil {
		return
	}
	s.events.Broadcast(data)
}

// broadcastLoop pushes cluster state to every connected dashboard on a
// short interval so nodes coming online, load changes, and task progress
// all show up in real time without the browser needing to poll.
func (s *Server) broadcastLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.publishSnapshot()
	}
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var status cluster.NodeStatus
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if status.ID == "" || status.Address == "" {
		http.Error(w, "id and address are required", http.StatusBadRequest)
		return
	}
	if status.Version != "" {
		for _, existing := range s.registry.All() {
			if existing.Version != "" && version.Compare(status.Version, existing.Version) < 0 {
				message := "node " + status.ID + " version " + status.Version + " is older than node " + existing.ID + " version " + existing.Version
				log.Printf("orchestrator: %s", message)
				s.registry.Upsert(status)
				http.Error(w, message, http.StatusConflict)
				return
			}
		}
	}
	s.registry.Upsert(status)
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleEject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}
	if s.identity != nil {
		if err := s.identity.RemovePeer(id); err != nil {
			http.Error(w, "could not revoke node certificate: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	s.registry.Remove(id)
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) notifyOrchestratorsOfEjection(nodeID string) {
	addresses := make(map[string]string)
	if s.discovery != nil {
		for _, peer := range s.discovery.Snapshot() {
			if peer.Role == "orchestrator" && peer.ID != s.selfID && peer.PeerAddr != "" {
				addresses[peer.ID] = peer.PeerAddr
			}
		}
	}
	s.peerMu.RLock()
	for id, address := range s.pairedOrchestrators {
		addresses[id] = address
	}
	s.peerMu.RUnlock()
	for id, address := range addresses {
		if s.identity == nil || !s.identity.IsTrustedID(id) {
			continue
		}
		go func(addr string) {
			req, err := http.NewRequest(http.MethodPost, addr+"/api/nodes/"+url.PathEscape(nodeID)+"/eject", nil)
			if err != nil {
				return
			}
			resp, err := s.client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}(address)
	}
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.Active())
}

func (s *Server) handleListAllNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.All())
}

func (s *Server) handleRefreshAllNodeModels(w http.ResponseWriter, r *http.Request) {
	results := make(map[string]string)
	for _, node := range s.registry.Active() {
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, node.Address+"/models/refresh", nil)
		if err != nil {
			results[node.ID] = err.Error()
			continue
		}
		response, err := s.dispatcher.Do(request)
		if err != nil {
			results[node.ID] = "unreachable: " + err.Error()
			continue
		}
		response.Body.Close()
		if response.StatusCode >= 300 {
			results[node.ID] = response.Status
			continue
		}
		results[node.ID] = "refreshed"
	}
	writeJSON(w, http.StatusOK, results)
}

// handleNodeModelsGet proxies to a node agent's /models/all so the GUI can
// show every discovered model (enabled or not) without the browser needing
// direct network access to each node.
func (s *Server) handleNodeModelsGet(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	resp, err := s.client.Get(n.Address + "/models/all")
	if err != nil {
		http.Error(w, "node unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// handleNodeModelsPatch proxies a disabled-models update to the node agent.
func (s *Server) handleNodeModelsPatch(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := http.NewRequest(http.MethodPatch, n.Address+"/models", bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, "node unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleNodeControl(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	path := "/properties"
	if strings.HasSuffix(r.URL.Path, "/engines") {
		path = "/engine"
	}
	if strings.HasSuffix(r.URL.Path, "/pull") {
		path = "/models/pull"
	}
	if strings.HasSuffix(r.URL.Path, "/refresh") {
		path = "/models/refresh"
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := http.NewRequest(r.Method, n.Address+path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, values := range r.Header {
		for _, value := range values {
			req.Header.Add(k, value)
		}
	}
	resp, err := s.dispatcher.Do(req)
	if err != nil {
		http.Error(w, "node communication error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(k, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// handleNodeChat proxies a multi-turn chat request directly to a node's /chat endpoint.
func (s *Server) handleNodeChat(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		http.Error(w, "node not found or offline", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := http.NewRequest(http.MethodPost, n.Address+"/chat", bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.dispatcher.Do(req)
	if err != nil {
		http.Error(w, "node execution failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// handleNodeDialoguesProxy proxies dialogue interactions directly to a node's dialogue endpoints.
func (s *Server) handleNodeDialoguesProxy(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		http.Error(w, "node not found or offline", http.StatusNotFound)
		return
	}
	// Extract subpath under /api/nodes/{id}/dialogues
	pathPrefix := "/api/nodes/" + url.PathEscape(nodeID)
	subPath := strings.TrimPrefix(r.URL.Path, pathPrefix)
	if !strings.HasPrefix(subPath, "/") {
		subPath = "/" + subPath
	}
	destURL := n.Address + subPath
	if r.URL.RawQuery != "" {
		destURL += "?" + r.URL.RawQuery
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := http.NewRequest(r.Method, destURL, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, v := range r.Header {
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}
	resp, err := s.dispatcher.Do(req)
	if err != nil {
		http.Error(w, "node communication error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		for _, val := range v {
			w.Header().Add(k, val)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

type createTaskRequest struct {
	Description string              `json:"description"`
	Subtasks    []scheduler.Subtask `json:"subtasks,omitempty"`
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Description) == "" {
		http.Error(w, "description is required", http.StatusBadRequest)
		return
	}

	subtasks := req.Subtasks
	if len(subtasks) == 0 {
		subtasks = scheduler.Decompose(req.Description)
	}
	subtasks, err := scheduler.ValidatePlan(subtasks)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	assignments, err := scheduler.AssignWithExperience(subtasks, s.registry.Active(), s.experience())
	if err != nil {
		if errors.Is(err, scheduler.ErrNoHealthyNodes) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		var unassignable *scheduler.UnassignableSubtasksError
		if errors.As(err, &unassignable) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(assignments) != len(subtasks) {
		http.Error(w, "scheduler returned an incomplete assignment", http.StatusServiceUnavailable)
		return
	}

	task, err := s.tasks.CreateChecked(req.Description, assignments)
	if err != nil {
		http.Error(w, "could not persist task: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for _, a := range assignments {
		go s.dispatch(task.ID, a)
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) dispatch(taskID string, a scheduler.Assignment) {
	ctx, ok := s.tasks.Context(taskID)
	if !ok {
		return
	}
	if !s.tasks.WaitForDependencies(ctx, taskID, a.Subtask.ID) {
		if ctx.Err() == nil {
			s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", errors.New("a required subtask did not complete"))
		}
		return
	}
	select {
	case s.dispatchSlots <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-s.dispatchSlots }()
	if err := s.tasks.StartSubtaskChecked(taskID, a.Subtask.ID); err != nil {
		log.Printf("orchestrator: could not persist start of subtask %s: %v", a.Subtask.ID, err)
		s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", err)
		return
	}

	prompt, ok := s.tasks.InputForSubtask(taskID, a.Subtask.ID)
	if !ok {
		s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", errors.New("subtask prerequisites are not available"))
		return
	}
	current := a
	alreadyTried := map[string]bool{a.NodeID: true}
	for attempt := 0; attempt < 2; attempt++ {
		node, healthy := s.registry.Get(current.NodeID)
		var output string
		var attemptErr error
		retryable := true
		if !healthy || !node.Healthy || node.Address != current.Address {
			attemptErr = errors.New("node was ejected before dispatch")
		} else {
			s.registry.IncrementLoad(current.NodeID, 1)
			started := time.Now()
			output, attemptErr, retryable = s.executeSubtask(ctx, current, prompt)
			s.registry.IncrementLoad(current.NodeID, -1)
			s.recordDispatch(current, output, attemptErr, time.Since(started))
		}
		if attemptErr == nil {
			s.tasks.CompleteSubtask(taskID, a.Subtask.ID, output, nil)
			return
		}
		if !retryable || attempt == 1 || ctx.Err() != nil {
			s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", attemptErr)
			return
		}

		alreadyTried[current.NodeID] = true
		retry, err := s.retryAssignment(a.Subtask, alreadyTried)
		if err != nil {
			log.Printf("orchestrator: subtask %s retry unavailable after %v: %v", a.Subtask.ID, attemptErr, err)
			s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", attemptErr)
			return
		}
		log.Printf("orchestrator: retrying subtask %s on node %s after node %s failed: %v",
			a.Subtask.ID, retry.NodeID, current.NodeID, attemptErr)
		if err := s.tasks.ReassignSubtaskChecked(taskID, a.Subtask.ID, retry); err != nil {
			log.Printf("orchestrator: could not persist retry assignment for subtask %s: %v", a.Subtask.ID, err)
			s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", err)
			return
		}
		current = retry
	}
}

func (s *Server) executeSubtask(ctx context.Context, assignment scheduler.Assignment, prompt string) (string, error, bool) {
	body, err := json.Marshal(map[string]string{
		"subtask_id": assignment.Subtask.ID,
		"model":      assignment.Model,
		"prompt":     prompt,
		"task_type":  assignment.Subtask.TaskType,
	})
	if err != nil {
		return "", err, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, assignment.Address+"/execute", bytes.NewReader(body))
	if err != nil {
		return "", err, false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.dispatcher.Do(req)
	if err != nil {
		return "", err, true
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		if readErr != nil {
			return "", fmt.Errorf("read node error response: %w", readErr), true
		}
		return "", errors.New("node returned " + resp.Status + ": " + string(errBody)), resp.StatusCode >= 500
	}
	var out struct {
		Output string `json:"output"`
		Error  string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err, true
	}
	if out.Error != "" {
		return "", errors.New(out.Error), true
	}
	return out.Output, nil, false
}

func (s *Server) retryAssignment(subtask scheduler.Subtask, excluded map[string]bool) (scheduler.Assignment, error) {
	nodes := s.registry.Active()
	eligible := make([]cluster.NodeStatus, 0, len(nodes))
	for _, node := range nodes {
		if !excluded[node.ID] {
			eligible = append(eligible, node)
		}
	}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{subtask}, eligible, s.experience())
	if err != nil {
		return scheduler.Assignment{}, err
	}
	if len(assignments) != 1 {
		return scheduler.Assignment{}, errors.New("scheduler returned no retry assignment")
	}
	return assignments[0], nil
}

// experience exposes the memory store to the scheduler, or nil when memory
// is unavailable so routing falls back to purely static scoring.
func (s *Server) experience() scheduler.Experience {
	if s.mem == nil {
		return nil
	}
	return s.mem
}

// recordDispatch writes the outcome of one dispatched subtask to memory,
// which is what teaches the scheduler which pairings to prefer next time.
func (s *Server) recordDispatch(a scheduler.Assignment, output string, err error, elapsed time.Duration) {
	if s.mem == nil {
		return
	}
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	_, _ = s.mem.Record(memory.Experience{
		Kind:        "subtask",
		TaskType:    a.Subtask.TaskType,
		Description: a.Subtask.Description,
		Prompt:      a.Subtask.Description,
		Output:      output,
		NodeID:      a.NodeID,
		Model:       a.Model,
		Success:     err == nil,
		Error:       errMsg,
		DurationMs:  elapsed.Milliseconds(),
		Tags:        append([]string{"dispatch"}, a.Subtask.Tags...),
	})
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.tasks.All())
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, ok := s.tasks.Get(id)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, ok := s.tasks.Get(id)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	cancelledOK, err := s.tasks.CancelChecked(id)
	if err != nil {
		http.Error(w, "could not persist cancellation: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !cancelledOK {
		http.Error(w, "task is already complete", http.StatusConflict)
		return
	}
	cancelled, _ := s.tasks.Get(id)
	writeJSON(w, http.StatusOK, cancelled)
}

// --- Interactive Agent Backend HTTP Handlers ---

func (s *Server) handleAgentSessionsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.listAgentSessions())
}

type createSessionRequest struct {
	Title            string  `json:"title"`
	CoordinatorModel string  `json:"coordinator_model"`
	MaxSteps         int     `json:"max_steps"`
	Temperature      float64 `json:"temperature"`
	SystemPrompt     string  `json:"system_prompt"`
}

func (s *Server) handleAgentSessionsCreate(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	harness, backend := s.currentAgent()
	session, err := backend.CreateSession(req.Title, agent.SessionConfig{
		CoordinatorModel: req.CoordinatorModel,
		MaxSteps:         req.MaxSteps,
		Temperature:      req.Temperature,
		SystemPrompt:     req.SystemPrompt,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	session.Harness = harness
	if err := s.persistAgentSession(session); err != nil {
		backend.DeleteSession(session.ID)
		http.Error(w, "could not persist agent session: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.agentMu.Lock()
	s.sessionHarnesses[session.ID] = harness
	s.agentMu.Unlock()
	s.publishSnapshot()
	writeJSON(w, http.StatusCreated, session)
}

func (s *Server) handleAgentSessionGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	harness, backend, exists := s.agentForSession(id)
	if !exists {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	session, ok := backend.GetSession(id)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	session.Harness = harness
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) handleAgentSessionDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	harness, backend, exists := s.agentForSession(id)
	if !exists {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if s.mem != nil {
		if err := s.mem.DeleteAgentSession(harness, id); err != nil {
			http.Error(w, "could not delete persisted agent session: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if !backend.DeleteSession(id) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.agentMu.Lock()
	if s.sessionHarnesses[id] == harness {
		delete(s.sessionHarnesses, id)
	}
	s.agentMu.Unlock()
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

type agentMessageRequest struct {
	Content string `json:"content"`
	Message string `json:"message"`
}

func (s *Server) handleAgentMessagePost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	harness, backend, exists := s.agentForSession(id)
	if !exists {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	var req agentMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	content := strings.TrimSpace(req.Content)
	if content == "" {
		content = strings.TrimSpace(req.Message)
	}
	if content == "" {
		http.Error(w, "content is required", http.StatusBadRequest)
		return
	}

	isStream := r.URL.Query().Get("stream") == "true" || strings.Contains(r.Header.Get("Accept"), "text/event-stream")

	if isStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		streamChan := make(chan agent.StreamEvent, 50)
		go func() {
			_, sendErr := backend.SendMessage(r.Context(), id, content, streamChan)
			if sendErr != nil {
				streamChan <- agent.StreamEvent{Type: agent.EventError, SessionID: id, Error: sendErr.Error()}
			}
			close(streamChan)
		}()

		for ev := range streamChan {
			data, err := json.Marshal(ev)
			if err == nil {
				_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
		session, ok := backend.GetSession(id)
		if !ok {
			err := errors.New("agent session disappeared while processing the message")
			log.Printf("orchestrator: persist streamed agent session %s: %v", id, err)
			writeAgentStreamError(w, flusher, id, err)
		} else {
			session.Harness = harness
			if err := s.persistAgentSession(session); err != nil {
				log.Printf("orchestrator: persist streamed agent session %s: %v", id, err)
				writeAgentStreamError(w, flusher, id, err)
			}
		}
		s.publishSnapshot()
		return
	}

	msg, sendErr := backend.SendMessage(r.Context(), id, content, nil)
	session, ok := backend.GetSession(id)
	if !ok {
		http.Error(w, "agent session disappeared while processing the message", http.StatusInternalServerError)
		return
	}
	session.Harness = harness
	if err := s.persistAgentSession(session); err != nil {
		if sendErr != nil {
			log.Printf("orchestrator: persist failed agent session %s: %v", id, err)
		}
		http.Error(w, "could not persist agent session: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.publishSnapshot()
	if sendErr != nil {
		http.Error(w, sendErr.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, msg)
}

func writeAgentStreamError(w http.ResponseWriter, flusher http.Flusher, sessionID string, err error) {
	event := agent.StreamEvent{Type: agent.EventError, SessionID: sessionID, Error: "could not persist agent session: " + err.Error()}
	data, marshalErr := json.Marshal(event)
	if marshalErr != nil {
		log.Printf("orchestrator: encode agent stream error: %v", marshalErr)
		return
	}
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()
}

func (s *Server) handleAgentToolsList(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	writeJSON(w, http.StatusOK, backend.ListTools())
}

func (s *Server) handleAgentModelsList(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	writeJSON(w, http.StatusOK, backend.ListClusterModels())
}

func (s *Server) handleAgentStatsGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agentStats())
}

func (s *Server) handleAgentBackendGet(w http.ResponseWriter, r *http.Request) {
	harness, _ := s.currentAgent()
	writeJSON(w, http.StatusOK, map[string]any{
		"name": harness, "harnesses": []string{"pi", "deepseek"},
		"supports_coordinator_model": true,
	})
}

func (s *Server) handleAgentBackendPatch(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Harness string `json:"harness"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request.Harness = strings.ToLower(strings.TrimSpace(request.Harness))
	if !s.selectHarness(request.Harness) {
		http.Error(w, `unsupported harness (want "pi" or "deepseek")`, http.StatusBadRequest)
		return
	}
	s.handleAgentBackendGet(w, r)
}

// --- Direct Node & Cluster LLM Model Invocation Handlers ---

func (s *Server) handleListAllModels(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	writeJSON(w, http.StatusOK, backend.ListClusterModels())
}

type generateAPIRequest struct {
	NodeID      string  `json:"node_id,omitempty"`
	Model       string  `json:"model,omitempty"`
	Prompt      string  `json:"prompt"`
	System      string  `json:"system,omitempty"`
	TaskType    string  `json:"task_type,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
	MaxTokens   int     `json:"max_tokens,omitempty"`
}

type generateAPIResponse struct {
	NodeID      string `json:"node_id"`
	NodeAddress string `json:"node_address"`
	Model       string `json:"model"`
	Output      string `json:"output"`
	DurationMs  int64  `json:"duration_ms"`
	Error       string `json:"error,omitempty"`
}

func (s *Server) handleGenerate(w http.ResponseWriter, r *http.Request) {
	var req generateAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, generateAPIResponse{Error: err.Error()})
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		writeJSON(w, http.StatusBadRequest, generateAPIResponse{Error: "prompt is required"})
		return
	}

	nodes := s.registry.Active()
	if len(nodes) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, generateAPIResponse{Error: "no healthy nodes in cluster"})
		return
	}

	candidates := nodes
	if req.NodeID != "" {
		n, ok := s.registry.Get(req.NodeID)
		if !ok || !n.Healthy {
			writeJSON(w, http.StatusNotFound, generateAPIResponse{Error: "node not found or offline: " + req.NodeID})
			return
		}
		candidates = []cluster.NodeStatus{n}
	}
	if req.Model != "" {
		hasModel := false
		for i := range candidates {
			var compatible []engine.Model
			for _, model := range candidates[i].Models {
				if strings.EqualFold(model.Name, req.Model) {
					compatible = append(compatible, model)
					hasModel = true
				}
			}
			candidates[i].Models = compatible
		}
		if !hasModel {
			writeJSON(w, http.StatusNotFound, generateAPIResponse{Error: "model not found on any cluster node: " + req.Model})
			return
		}
	}

	taskType := strings.TrimSpace(req.TaskType)
	if taskType == "" {
		taskType = "general"
	}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{{
		ID: "generate", Description: req.Prompt, TaskType: taskType,
	}}, candidates, s.experience())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, generateAPIResponse{Error: "could not assign request to a healthy node"})
		return
	}
	if len(assignments) != 1 {
		writeJSON(w, http.StatusServiceUnavailable, generateAPIResponse{Error: "scheduler returned no assignment"})
		return
	}
	assignment := assignments[0]
	targetNode, ok := s.registry.Get(assignment.NodeID)
	if !ok || !targetNode.Healthy || targetNode.Address != assignment.Address {
		writeJSON(w, http.StatusServiceUnavailable, generateAPIResponse{Error: "selected node is no longer healthy"})
		return
	}
	targetModel := assignment.Model

	fullPrompt := req.Prompt
	if req.System != "" {
		fullPrompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", req.System, req.Prompt)
	}

	s.registry.IncrementLoad(targetNode.ID, 1)
	defer s.registry.IncrementLoad(targetNode.ID, -1)
	if strings.EqualFold(r.URL.Query().Get("stream"), "true") {
		s.streamNodeGenerate(w, r, targetNode.Address, targetModel, req, fullPrompt)
		return
	}

	start := time.Now()
	output, err := s.callNodeGenerateWithOptions(targetNode.Address, targetModel, fullPrompt, req.Temperature, req.MaxTokens)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, generateAPIResponse{
			NodeID:      targetNode.ID,
			NodeAddress: targetNode.Address,
			Model:       targetModel,
			DurationMs:  duration,
			Error:       err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, generateAPIResponse{
		NodeID:      targetNode.ID,
		NodeAddress: targetNode.Address,
		Model:       targetModel,
		Output:      output,
		DurationMs:  duration,
	})
}

func (s *Server) streamNodeGenerate(w http.ResponseWriter, r *http.Request, address, model string, genReq generateAPIRequest, prompt string) {
	body, err := marshalGeneratePayload(model, prompt, genReq.Temperature, genReq.MaxTokens)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, generateAPIResponse{Error: err.Error()})
		return
	}
	nodeReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, address+"/generate?stream=true", bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, generateAPIResponse{Error: err.Error()})
		return
	}
	nodeReq.Header.Set("Content-Type", "application/json")
	resp, err := s.dispatcher.Do(nodeReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, generateAPIResponse{Error: err.Error()})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		writeJSON(w, resp.StatusCode, generateAPIResponse{Error: strings.TrimSpace(string(errBody))})
		return
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		var out generateAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			writeJSON(w, http.StatusBadGateway, generateAPIResponse{Error: "invalid response from node: " + err.Error()})
			return
		}
		data, err := json.Marshal(map[string]string{"output": out.Output})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, generateAPIResponse{Error: err.Error()})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		if out.Error != "" {
			errData, _ := json.Marshal(map[string]string{"error": out.Error})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", errData)
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		if _, err := fmt.Fprintln(w, scanner.Text()); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("orchestrator: stream from node %s failed: %v", address, err)
	}
}

func (s *Server) handleNodeGenerate(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	var req generateAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, generateAPIResponse{Error: err.Error()})
		return
	}
	req.NodeID = nodeID
	body, _ := json.Marshal(req)
	r.Body = io.NopCloser(bytes.NewReader(body))
	s.handleGenerate(w, r)
}

func (s *Server) handleNodeModelGenerate(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	model := r.PathValue("model")
	var req generateAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, generateAPIResponse{Error: err.Error()})
		return
	}
	req.NodeID = nodeID
	req.Model = model
	body, _ := json.Marshal(req)
	r.Body = io.NopCloser(bytes.NewReader(body))
	s.handleGenerate(w, r)
}

func (s *Server) handleNodeModelTest(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	model := r.PathValue("model")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		writeJSON(w, http.StatusNotFound, map[string]any{"healthy": false, "error": "node offline"})
		return
	}

	testPrompt := "Respond with OK in 3 words."
	start := time.Now()
	s.registry.IncrementLoad(n.ID, 1)
	out, err := s.callNodeGenerate(n.Address, model, testPrompt)
	s.registry.IncrementLoad(n.ID, -1)
	elapsed := time.Since(start).Milliseconds()

	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"node_id":     n.ID,
			"model":       model,
			"healthy":     false,
			"error":       err.Error(),
			"duration_ms": elapsed,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":     n.ID,
		"model":       model,
		"healthy":     true,
		"output":      out,
		"duration_ms": elapsed,
	})
}

func (s *Server) callNodeGenerate(nodeAddr, model, prompt string) (string, error) {
	return s.callNodeGenerateWithOptions(nodeAddr, model, prompt, 0, 0)
}

func (s *Server) callNodeGenerateWithOptions(nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	return s.callNodeGenerateWithOptionsContext(context.Background(), nodeAddr, model, prompt, temperature, maxTokens)
}

func (s *Server) callNodeGenerateWithOptionsContext(ctx context.Context, nodeAddr, model, prompt string, temperature float64, maxTokens int) (string, error) {
	// Try /generate first, fallback to /execute
	genPayload, err := marshalGeneratePayload(model, prompt, temperature, maxTokens)
	if err != nil {
		return "", err
	}

	genReq, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeAddr+"/generate", bytes.NewReader(genPayload))
	if err != nil {
		return "", err
	}
	genReq.Header.Set("Content-Type", "application/json")
	resp, err := s.dispatcher.Do(genReq)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		var res struct {
			Output string `json:"output"`
			Error  string `json:"error,omitempty"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&res); decodeErr == nil {
			if res.Error != "" {
				return "", errors.New(res.Error)
			}
			return res.Output, nil
		}
	}
	if resp != nil {
		resp.Body.Close()
	}

	// Fallback to /execute
	execPayload, _ := json.Marshal(map[string]string{
		"subtask_id": "direct-gen",
		"model":      model,
		"prompt":     prompt,
	})
	execReq, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeAddr+"/execute", bytes.NewReader(execPayload))
	if err != nil {
		return "", err
	}
	execReq.Header.Set("Content-Type", "application/json")
	resp, err = s.dispatcher.Do(execReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("node returned %s: %s", resp.Status, string(errBody))
	}
	var out struct {
		Output string `json:"output"`
		Error  string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Error != "" {
		return "", errors.New(out.Error)
	}
	return out.Output, nil
}

func marshalGeneratePayload(model, prompt string, temperature float64, maxTokens int) ([]byte, error) {
	payload := map[string]any{"model": model, "prompt": prompt}
	if temperature != 0 {
		payload["temperature"] = temperature
	}
	if maxTokens > 0 {
		payload["max_tokens"] = maxTokens
	}
	return json.Marshal(payload)
}

// --- OpenAI-Compatible Gateway Handlers ---

type openAIModelObj struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

func (s *Server) handleOpenAIModels(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	models := backend.ListClusterModels()
	list := make([]openAIModelObj, 0, len(models))
	seen := make(map[string]bool)
	now := time.Now().Unix()

	for _, m := range models {
		if !seen[m.Name] {
			seen[m.Name] = true
			list = append(list, openAIModelObj{
				ID:      m.Name,
				Object:  "model",
				Created: now,
				OwnedBy: m.NodeID,
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   list,
	})
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatRequest struct {
	Model       string              `json:"model"`
	Messages    []openAIChatMessage `json:"messages"`
	Stream      bool                `json:"stream"`
	Temperature float64             `json:"temperature"`
	MaxTokens   int                 `json:"max_tokens,omitempty"`
}

func (s *Server) handleOpenAIChatCompletions(w http.ResponseWriter, r *http.Request) {
	var req openAIChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, "messages array is required", http.StatusBadRequest)
		return
	}

	// Preserve every message role supported by the engine prompt format.
	var sb strings.Builder
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "user", "assistant", "tool":
			sb.WriteString("<|im_start|>" + m.Role + "\n" + m.Content + "<|im_end|>\n")
		}
	}
	sb.WriteString("<|im_start|>assistant\n")

	genReq := generateAPIRequest{
		Model:       req.Model,
		Prompt:      sb.String(),
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}

	genBody, _ := json.Marshal(genReq)
	generateURL := "/api/generate"
	if req.Stream {
		generateURL += "?stream=true"
	}
	fakeReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, generateURL, bytes.NewReader(genBody))
	if req.Stream {
		streamWriter := newOpenAIStreamWriter(w, req.Model)
		s.handleGenerate(streamWriter, fakeReq)
		streamWriter.finish()
		return
	}
	rec := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	s.handleGenerate(rec, fakeReq)

	if rec.status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
		return
	}

	var genResp generateAPIResponse
	_ = json.Unmarshal(rec.body.Bytes(), &genResp)

	writeJSON(w, http.StatusOK, map[string]any{
		"id":                 "chatcmpl-" + genResp.NodeID,
		"object":             "chat.completion",
		"created":            time.Now().Unix(),
		"model":              genResp.Model,
		"system_fingerprint": "maistr0-" + genResp.NodeID,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]string{
					"role":    "assistant",
					"content": genResp.Output,
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]int{
			"prompt_tokens":     len(genReq.Prompt) / 4,
			"completion_tokens": len(genResp.Output) / 4,
			"total_tokens":      (len(genReq.Prompt) + len(genResp.Output)) / 4,
		},
	})
}

type openAICompletionRequest struct {
	Model       string  `json:"model"`
	Prompt      string  `json:"prompt"`
	Temperature float64 `json:"temperature"`
	MaxTokens   int     `json:"max_tokens,omitempty"`
}

func (s *Server) handleOpenAICompletions(w http.ResponseWriter, r *http.Request) {
	var req openAICompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	genReq := generateAPIRequest{
		Model:       req.Model,
		Prompt:      req.Prompt,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}

	genBody, _ := json.Marshal(genReq)
	fakeReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "/api/generate", bytes.NewReader(genBody))
	rec := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	s.handleGenerate(rec, fakeReq)

	if rec.status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
		return
	}

	var genResp generateAPIResponse
	_ = json.Unmarshal(rec.body.Bytes(), &genResp)

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "cmpl-" + genResp.NodeID,
		"object":  "text_completion",
		"created": time.Now().Unix(),
		"model":   genResp.Model,
		"choices": []map[string]any{
			{
				"text":          genResp.Output,
				"index":         0,
				"finish_reason": "stop",
			},
		},
	})
}

type responseCapture struct {
	header http.Header
	body   *bytes.Buffer
	status int
}

func (r *responseCapture) Header() http.Header { return r.header }
func (r *responseCapture) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(b)
}
func (r *responseCapture) WriteHeader(status int) { r.status = status }

type openAIStreamWriter struct {
	target  http.ResponseWriter
	header  http.Header
	model   string
	status  int
	buffer  strings.Builder
	flusher http.Flusher
}

func newOpenAIStreamWriter(target http.ResponseWriter, model string) *openAIStreamWriter {
	flusher, _ := target.(http.Flusher)
	return &openAIStreamWriter{target: target, header: make(http.Header), model: model, flusher: flusher}
}

func (w *openAIStreamWriter) Header() http.Header { return w.header }

func (w *openAIStreamWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	for key, values := range w.header {
		for _, value := range values {
			w.target.Header().Add(key, value)
		}
	}
	if status < 400 {
		w.target.Header().Set("Content-Type", "text/event-stream")
		w.target.Header().Set("Cache-Control", "no-cache")
		w.target.Header().Set("Connection", "keep-alive")
	}
	w.target.WriteHeader(status)
}

func (w *openAIStreamWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= 400 {
		return w.target.Write(data)
	}
	w.buffer.Write(data)
	for {
		buffer := w.buffer.String()
		newline := strings.IndexByte(buffer, '\n')
		if newline < 0 {
			break
		}
		line := buffer[:newline]
		w.buffer.Reset()
		w.buffer.WriteString(buffer[newline+1:])
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var event struct {
			Output string `json:"output"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return len(data), err
		}
		if event.Error != "" {
			if err := w.writeChunk(map[string]any{"error": map[string]string{"message": event.Error, "type": "server_error"}}); err != nil {
				return len(data), err
			}
		}
		if event.Output != "" {
			chunk := map[string]any{
				"id": "chatcmpl-maistr0", "object": "chat.completion.chunk",
				"created": time.Now().Unix(), "model": w.model,
				"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": event.Output}, "finish_reason": nil}},
			}
			if err := w.writeChunk(chunk); err != nil {
				return len(data), err
			}
		}
	}
	return len(data), nil
}

func (w *openAIStreamWriter) writeChunk(chunk any) error {
	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w.target, "data: %s\n\n", data); err != nil {
		return err
	}
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return nil
}

func (w *openAIStreamWriter) finish() {
	if w.status >= 400 {
		return
	}
	_ = w.writeChunk(map[string]any{
		"id": "chatcmpl-maistr0", "object": "chat.completion.chunk",
		"created": time.Now().Unix(), "model": w.model,
		"choices": []map[string]any{{"index": 0, "delta": map[string]string{}, "finish_reason": "stop"}},
	})
	_, _ = fmt.Fprint(w.target, "data: [DONE]\n\n")
	if w.flusher != nil {
		w.flusher.Flush()
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
