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
	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/piagent"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
	"github.com/maistr0/maistr0/web"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type Server struct {
	registry           *cluster.Registry
	tasks              *taskmgr.Manager
	client             *http.Client // short timeout, for node proxy/status calls
	dispatcher         *http.Client // long timeout, subtask execution can cold-load a model
	dispatchSlots      chan struct{}
	discovery          *discovery.Listener
	selfID             string
	discoveryEnabled   bool
	events             *hub.Hub
	agentMu            sync.RWMutex
	agents             map[string]agent.Backend
	activeHarness      string
	sessionHarnesses   map[string]string
	sessionCancels     map[string]context.CancelFunc
	sessionCancelsMu   sync.Mutex
	mem                *memory.Store
	selfScore          float64 // announced on the LAN for orchestrator election
	leaderMu           sync.Mutex
	lastLeaderID       string // last announced leader, to log transitions once
	discoveredNodes    map[string]bool
	identityPath       string
	identity           *peeridentity.Identity
	advertiseAddr      string
	localNodeDashboard string
}

const discoveryRequestTimeout = 5 * time.Second

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
		registry:         reg,
		client:           &http.Client{Timeout: 10 * time.Second},
		dispatcher:       disp,
		dispatchSlots:    make(chan struct{}, 16),
		selfID:           selfID,
		selfScore:        cluster.SelfScore(hardware.Detect()),
		discoveryEnabled: true,
		events:           events,
		discoveredNodes:  make(map[string]bool),
		agents:           backends,
		activeHarness:    harnessName,
		sessionHarnesses: make(map[string]string),
		sessionCancels:   make(map[string]context.CancelFunc),
		tasks:            tasks,
		mem:              store,
		identityPath:     peeridentity.DefaultPath(selfID),
	}
	srv.agents["pi"] = piagent.New(&localPiProvider{server: srv}, deepseekBackend)
	deepseekBackend.SetWorkflowExecutor(srv.executeWorkflow)
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

// SetLocalNodeDashboard configures the local pairing UI used after yielding.
func (s *Server) SetLocalNodeDashboard(address string) error {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("local node dashboard must be a loopback HTTP URL")
	}
	ip := net.ParseIP(parsed.Hostname())
	if parsed.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("local node dashboard must be a loopback HTTP URL")
	}
	s.localNodeDashboard = "http://" + parsed.Host
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
	if err := identity.RemovePairedOrchestrators(); err != nil {
		return fmt.Errorf("remove unsupported orchestrator pairing: %w", err)
	}
	s.identity = identity
	s.client.Transport = identity.ClientTransport()
	s.dispatcher.Transport = identity.ClientTransport()
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
	mux.HandleFunc("GET /api/status", s.handleSystemStatus)
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
	mux.HandleFunc("POST /api/tasks", s.handleCreateTask)
	mux.HandleFunc("GET /api/tasks", s.handleListTasks)
	mux.HandleFunc("GET /api/tasks/{id}", s.handleGetTask)
	mux.HandleFunc("POST /api/tasks/{id}/cancel", s.handleCancelTask)
	mux.HandleFunc("POST /api/tasks/{id}/approve", s.handleApproveTask)

	// Interactive agent backend endpoints
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
	mux.HandleFunc("GET /api/models/catalog", s.handleModelCatalog)

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
		if err := s.discovery.WaitReady(2 * time.Second); err != nil {
			return fmt.Errorf("orchestrator: start cluster discovery: %w", err)
		}
		selfAddr := "http://" + discovery.OutboundIP() + portSuffix(listenAddr)
		peerAddr := "https://" + discovery.OutboundIP() + portSuffix(listenAddr)
		go discovery.Beacon(func() discovery.Announcement {
			members := s.registry.Active()
			_, elected := s.registry.Leader()
			role := "candidate"
			if s.isElectedOrchestrator() {
				role = "orchestrator"
			}
			return discovery.Announcement{
				Role: role, ID: s.selfID, HTTPAddr: selfAddr, PeerAddr: peerAddr,
				Score: s.selfScore, Members: len(members), Elected: elected,
			}
		}, 5*time.Second, nil)
	}
	go s.discoverySyncLoop()
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

func (s *Server) isElectedOrchestrator() bool {
	if !s.discoveryEnabled || s.discovery == nil {
		return true
	}
	self := discovery.Announcement{Role: "orchestrator", ID: s.selfID, Score: s.selfScore}
	return discovery.IsElectedOrchestrator(self, s.discovery.Snapshot())
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

func (s *Server) discoverySyncLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.syncDiscoveredPeers()
	}
}

func (s *Server) syncDiscoveredPeers() {
	if !s.isElectedOrchestrator() {
		return
	}
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
	for _, peer := range peers {
		if peer.PeerAddr == "" || !s.identity.IsTrustedID(peer.ID) {
			continue
		}
		if peer.Role != "node" {
			continue
		}
		probes.Add(1)
		go func(peer discovery.Peer) {
			defer probes.Done()
			status, err := s.discoveredNodeStatus(peer.PeerAddr)
			if err != nil {
				log.Printf("orchestrator: secure status probe for paired node %s failed: %v", peer.ID, err)
				return
			}
			results <- discoveryResult{peer: peer, status: []cluster.NodeStatus{status}}
		}(peer)
	}
	go func() {
		probes.Wait()
		close(results)
	}()
	for result := range results {
		visibleNodes[result.peer.ID] = true
		s.discoveredNodes[result.peer.ID] = true
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

func (s *Server) discoveredNodeStatus(addr string) (cluster.NodeStatus, error) {
	client := &http.Client{Timeout: discoveryRequestTimeout, Transport: s.identity.ClientTransport()}
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

func (s *Server) schedulerNodes() []cluster.NodeStatus {
	nodes := s.registry.Active()
	byID := make(map[string]*cluster.NodeStatus, len(nodes))
	for i := range nodes {
		byID[nodes[i].ID] = &nodes[i]
	}
	for nodeID, pending := range s.tasks.PendingAssignmentsByNode() {
		if node, ok := byID[nodeID]; ok {
			node.QueuedTasks = pending
		}
	}
	return nodes
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
			attemptPrompt := prompt
			for validationAttempt := 0; ; validationAttempt++ {
				started := time.Now()
				output, attemptErr, retryable = s.executeSubtask(ctx, current, attemptPrompt)
				if attemptErr == nil {
					attemptErr = validateTaskOutput(current.Subtask, output)
					if attemptErr != nil {
						retryable = false
					}
				}
				s.recordDispatch(current, output, attemptErr, time.Since(started))
				if attemptErr == nil || retryable || validationAttempt >= current.Subtask.MaxRetries {
					break
				}
				attemptPrompt = validationRetryPrompt(prompt, attemptErr, output)
			}
			s.registry.IncrementLoad(current.NodeID, -1)
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

func validationRetryPrompt(prompt string, validationErr error, output string) string {
	const maxPriorOutputBytes = 8 * 1024
	if len(output) > maxPriorOutputBytes {
		output = output[:maxPriorOutputBytes]
	}
	return fmt.Sprintf("%s\n\nYour previous result did not pass validation: %s\nRevise it and return a corrected result. Previous result:\n%s",
		prompt, validationErr, output)
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
	nodes := s.schedulerNodes()
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
	if _, err := s.mem.Record(memory.Experience{
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
		Tags:        append(append([]string{"dispatch", "role:" + a.Subtask.Role}, a.Subtask.Tags...), "stage:"+a.Subtask.TaskType),
	}); err != nil {
		log.Printf("orchestrator: record subtask experience failed: %v", err)
	}
}

// --- Interactive Agent Backend HTTP Handlers ---

type createSessionRequest struct {
	Title            string  `json:"title"`
	CoordinatorModel string  `json:"coordinator_model"`
	MaxSteps         int     `json:"max_steps"`
	Temperature      float64 `json:"temperature"`
	SystemPrompt     string  `json:"system_prompt"`
}

type agentMessageRequest struct {
	Content string `json:"content"`
	Message string `json:"message"`
}

// InterruptSession cancels an in-flight message turn for the given session ID.
func (s *Server) InterruptSession(id string) bool {
	s.sessionCancelsMu.Lock()
	cancel, hasCancel := s.sessionCancels[id]
	s.sessionCancelsMu.Unlock()
	if hasCancel && cancel != nil {
		cancel()
	}

	_, backend, exists := s.agentForSession(id)
	backendInterrupted := false
	if exists && backend != nil {
		backendInterrupted = backend.InterruptSession(id)
	}

	return hasCancel || backendInterrupted
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

// --- Direct Node & Cluster LLM Model Invocation Handlers ---

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

type openAICompletionRequest struct {
	Model       string  `json:"model"`
	Prompt      string  `json:"prompt"`
	Temperature float64 `json:"temperature"`
	MaxTokens   int     `json:"max_tokens,omitempty"`
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
