// Package orchestrator implements the control-plane HTTP API and background
// bookkeeping: node registry maintenance, task decomposition/assignment, and
// dispatching subtasks out to node agents.
package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hermes"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
	"github.com/maistr0/maistr0/web"
)

type Server struct {
	registry         *cluster.Registry
	tasks            *taskmgr.Manager
	client           *http.Client // short timeout, for node proxy/status calls
	dispatcher       *http.Client // long timeout, subtask execution can cold-load a model
	discovery        *discovery.Listener
	selfID           string
	discoveryEnabled bool
	events           *hub.Hub
	agent            *hermes.Harness
	mem              *memory.Store
	harness          string
	selfScore        float64 // announced on the LAN for orchestrator election
	leaderMu         sync.Mutex
	lastLeaderID     string // last announced leader, to log transitions once
	discoveredNodes  map[string]bool
}

func New() *Server { return NewWithMemory("") }

// NewWithMemory builds an orchestrator backed by a persistent memory
// database at memoryPath (empty selects the default per-user location).
func NewWithMemory(memoryPath string) *Server {
	return NewWithMemoryAndHarness(memoryPath, "deepseek")
}

// NewWithMemoryAndHarness builds an orchestrator using the requested agent
// harness. Unknown names fall back to DeepSeek, the default harness.
func NewWithMemoryAndHarness(memoryPath, harnessName string) *Server {
	reg := cluster.NewRegistry()
	disp := &http.Client{Timeout: 10 * time.Minute}
	selfID := "orchestrator-" + discovery.OutboundIP()
	harnessName = strings.ToLower(strings.TrimSpace(harnessName))
	if harnessName == "" {
		harnessName = "deepseek"
	}
	var agent *hermes.Harness
	switch harnessName {
	case "deepseek":
		agent = hermes.NewDeepSeek(reg, disp)
	default:
		harnessName = "deepseek"
		agent = hermes.NewDeepSeek(reg, disp)
	}
	srv := &Server{
		registry:         reg,
		tasks:            taskmgr.NewManager(),
		client:           &http.Client{Timeout: 10 * time.Second},
		dispatcher:       disp,
		selfID:           selfID,
		selfScore:        cluster.SelfScore(hardware.Detect()),
		discoveryEnabled: true,
		events:           hub.New(),
		discoveredNodes:  make(map[string]bool),
		agent:            agent,
		harness:          harnessName,
	}
	if store, err := memory.Open(memoryPath, selfID); err != nil {
		log.Printf("orchestrator: memory unavailable, running stateless: %v", err)
	} else {
		srv.mem = store
		srv.agent.SetMemory(store)
		log.Printf("orchestrator: memory store at %s", store.Path())
	}
	srv.agent.SetEventsHub(srv.events)
	return srv
}

// HarnessName reports the configured interactive agent harness.
func (s *Server) HarnessName() string { return s.harness }

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

func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/nodes/register", s.handleRegister)
	mux.HandleFunc("POST /api/nodes/{id}/eject", s.handleEject)
	mux.HandleFunc("GET /api/nodes", s.handleListNodes)
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

	// Interactive Hermes Agent harness endpoints
	mux.HandleFunc("GET /api/agent/sessions", s.handleAgentSessionsList)
	mux.HandleFunc("POST /api/agent/sessions", s.handleAgentSessionsCreate)
	mux.HandleFunc("GET /api/agent/sessions/{id}", s.handleAgentSessionGet)
	mux.HandleFunc("DELETE /api/agent/sessions/{id}", s.handleAgentSessionDelete)
	mux.HandleFunc("POST /api/agent/sessions/{id}/messages", s.handleAgentMessagePost)
	mux.HandleFunc("GET /api/agent/tools", s.handleAgentToolsList)
	mux.HandleFunc("GET /api/agent/models", s.handleAgentModelsList)
	mux.HandleFunc("GET /api/agent/stats", s.handleAgentStatsGet)

	// Direct cluster & node LLM model invocation endpoints
	mux.HandleFunc("GET /api/models", s.handleListAllModels)
	mux.HandleFunc("POST /api/generate", s.handleGenerate)
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
	go s.healthSweepLoop()
	go s.broadcastLoop()
	if s.discoveryEnabled {
		if s.discovery == nil {
			s.discovery = discovery.Listen(s.selfID)
		}
		selfAddr := "http://" + discovery.OutboundIP() + portSuffix(listenAddr)
		go discovery.Beacon(func() discovery.Announcement {
			members := s.registry.Active()
			_, elected := s.registry.Leader()
			return discovery.Announcement{
				Role: "orchestrator", ID: s.selfID, HTTPAddr: selfAddr,
				Score: s.selfScore, Members: len(members), Elected: elected,
			}
		}, 5*time.Second, nil)
	}
	go s.discoverySyncLoop()
	log.Printf("orchestrator listening on %s", listenAddr)
	return http.ListenAndServe(listenAddr, s.Mux())
}

func (s *Server) discoverySyncLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if s.discovery == nil {
			continue
		}
		visibleNodes := make(map[string]bool)
		for _, peer := range s.discovery.Snapshot() {
			if peer.HTTPAddr == "" {
				continue
			}
			if peer.Role == "node" {
				visibleNodes[peer.ID] = true
				s.discoveredNodes[peer.ID] = true
				status, err := s.discoveredNodeStatus(peer.HTTPAddr)
				if err == nil {
					s.registry.Upsert(status)
				}
				continue
			}
			if peer.Role == "orchestrator" {
				for _, status := range s.discoveredClusterNodes(peer.HTTPAddr) {
					s.registry.Upsert(status)
				}
			}
		}
		for id := range s.discoveredNodes {
			if visibleNodes[id] {
				continue
			}
			if _, ok := s.registry.Get(id); ok {
				s.registry.Remove(id)
				s.notifyOrchestratorsOfEjection(id)
				log.Printf("orchestrator: ejected node %s after its LAN announcement expired", id)
			}
			delete(s.discoveredNodes, id)
		}
		s.publishSnapshot()
	}
}

func (s *Server) discoveredClusterNodes(addr string) []cluster.NodeStatus {
	client := &http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Get(addr + "/api/nodes")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil
	}
	var nodes []cluster.NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil
	}
	return nodes
}

func (s *Server) discoveredNodeStatus(addr string) (cluster.NodeStatus, error) {
	client := &http.Client{Timeout: 1500 * time.Millisecond}
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
			if time.Since(n.LastSeen) > 30*time.Second {
				s.registry.Remove(n.ID)
				s.notifyOrchestratorsOfEjection(n.ID)
				log.Printf("orchestrator: ejected unavailable node %s from the cluster", n.ID)
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
	AgentStats   *hermes.AgentStats   `json:"agent_stats,omitempty"`
	Memory       *memory.Insights     `json:"memory,omitempty"`
}

func (s *Server) snapshot() clusterSnapshot {
	var disc []discovery.Peer
	if s.discovery != nil {
		disc = s.discovery.Snapshot()
	}
	nodes := s.registry.Active()
	leaderID := ""
	for _, n := range nodes {
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
	stats := s.agent.Stats()
	snap := clusterSnapshot{
		Nodes:        nodes,
		Tasks:        s.tasks.All(),
		Discovery:    disc,
		BuildVersion: version.String(),
		LeaderID:     leaderID,
		MemberCount:  len(nodes),
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
	s.registry.Remove(id)
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) notifyOrchestratorsOfEjection(nodeID string) {
	if s.discovery == nil {
		return
	}
	for _, peer := range s.discovery.Snapshot() {
		if peer.Role != "orchestrator" || peer.ID == s.selfID || peer.HTTPAddr == "" {
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
		}(peer.HTTPAddr)
	}
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.Active())
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
	Description string `json:"description"`
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	subtasks := scheduler.Decompose(req.Description)
	if len(subtasks) == 0 {
		http.Error(w, "description produced no subtasks", http.StatusBadRequest)
		return
	}

	assignments, err := scheduler.AssignWithExperience(subtasks, s.registry.Active(), s.experience())
	if err != nil {
		if errors.Is(err, scheduler.ErrNoHealthyNodes) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	task := s.tasks.Create(req.Description, assignments)
	for _, a := range assignments {
		s.registry.IncrementLoad(a.NodeID, 1)
		go s.dispatch(task.ID, a)
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) dispatch(taskID string, a scheduler.Assignment) {
	defer s.registry.IncrementLoad(a.NodeID, -1)
	s.tasks.StartSubtask(taskID, a.Subtask.ID)
	if node, ok := s.registry.Get(a.NodeID); !ok || !node.Healthy || node.Address != a.Address {
		s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", errors.New("node was ejected before dispatch"))
		return
	}

	body, _ := json.Marshal(map[string]string{
		"subtask_id": a.Subtask.ID,
		"model":      a.Model,
		"prompt":     a.Subtask.Description,
		"task_type":  a.Subtask.TaskType,
	})

	start := time.Now()
	resp, err := s.dispatcher.Post(a.Address+"/execute", "application/json", bytes.NewReader(body))
	if err != nil {
		s.recordDispatch(a, "", err, time.Since(start))
		s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		failure := errors.New("node returned " + resp.Status + ": " + string(errBody))
		s.recordDispatch(a, "", failure, time.Since(start))
		s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", failure)
		return
	}

	var out struct {
		Output string `json:"output"`
		Error  string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		s.recordDispatch(a, "", err, time.Since(start))
		s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", err)
		return
	}
	if out.Error != "" {
		failure := errors.New(out.Error)
		s.recordDispatch(a, "", failure, time.Since(start))
		s.tasks.CompleteSubtask(taskID, a.Subtask.ID, "", failure)
		return
	}
	s.recordDispatch(a, out.Output, nil, time.Since(start))
	s.tasks.CompleteSubtask(taskID, a.Subtask.ID, out.Output, nil)
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

// --- Interactive Hermes Agent HTTP Handlers ---

func (s *Server) handleAgentSessionsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agent.ListSessions())
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
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	session := s.agent.CreateSession(req.Title, hermes.SessionConfig{
		CoordinatorModel: req.CoordinatorModel,
		MaxSteps:         req.MaxSteps,
		Temperature:      req.Temperature,
		SystemPrompt:     req.SystemPrompt,
	})
	s.publishSnapshot()
	writeJSON(w, http.StatusCreated, session)
}

func (s *Server) handleAgentSessionGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	session, ok := s.agent.GetSession(id)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) handleAgentSessionDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.agent.DeleteSession(id) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

type agentMessageRequest struct {
	Content string `json:"content"`
	Message string `json:"message"`
}

func (s *Server) handleAgentMessagePost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
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

		streamChan := make(chan hermes.StreamEvent, 50)
		go func() {
			_, _ = s.agent.SendMessage(r.Context(), id, content, streamChan)
			close(streamChan)
		}()

		for ev := range streamChan {
			data, err := json.Marshal(ev)
			if err == nil {
				_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
		s.publishSnapshot()
		return
	}

	msg, err := s.agent.SendMessage(r.Context(), id, content, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.publishSnapshot()
	writeJSON(w, http.StatusOK, msg)
}

func (s *Server) handleAgentToolsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agent.ListTools())
}

func (s *Server) handleAgentModelsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agent.ListClusterModels())
}

func (s *Server) handleAgentStatsGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agent.Stats())
}

// --- Direct Node & Cluster LLM Model Invocation Handlers ---

func (s *Server) handleListAllModels(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agent.ListClusterModels())
}

type generateAPIRequest struct {
	NodeID      string  `json:"node_id,omitempty"`
	Model       string  `json:"model,omitempty"`
	Prompt      string  `json:"prompt"`
	System      string  `json:"system,omitempty"`
	TaskType    string  `json:"task_type,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
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

	var targetNode *cluster.NodeStatus
	var targetModel string

	if req.NodeID != "" {
		n, ok := s.registry.Get(req.NodeID)
		if !ok || !n.Healthy {
			writeJSON(w, http.StatusNotFound, generateAPIResponse{Error: "node not found or offline: " + req.NodeID})
			return
		}
		targetNode = &n
		if req.Model != "" {
			targetModel = req.Model
		} else if n.DefaultModel != "" {
			targetModel = n.DefaultModel
		} else if len(n.Models) > 0 {
			targetModel = n.Models[0].Name
		}
	} else if req.Model != "" {
		// Find healthy node hosting this model, pick least loaded
		bestLoad := 999999
		for i := range nodes {
			n := &nodes[i]
			for _, m := range n.Models {
				if strings.EqualFold(m.Name, req.Model) {
					if n.ActiveTasks < bestLoad {
						bestLoad = n.ActiveTasks
						targetNode = n
						targetModel = m.Name
					}
					break
				}
			}
		}
		if targetNode == nil {
			writeJSON(w, http.StatusNotFound, generateAPIResponse{Error: "model not found on any cluster node: " + req.Model})
			return
		}
	} else {
		// Auto select best node based on task type / load
		taskType := req.TaskType
		if taskType == "" {
			taskType = "general"
		}
		bestLoad := 999999
		for i := range nodes {
			n := &nodes[i]
			if len(n.Models) > 0 && n.ActiveTasks < bestLoad {
				bestLoad = n.ActiveTasks
				targetNode = n
				if n.DefaultModel != "" {
					targetModel = n.DefaultModel
				} else {
					targetModel = n.Models[0].Name
				}
			}
		}
	}

	if targetNode == nil || targetModel == "" {
		writeJSON(w, http.StatusServiceUnavailable, generateAPIResponse{Error: "could not assign request to a healthy node"})
		return
	}

	fullPrompt := req.Prompt
	if req.System != "" {
		fullPrompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", req.System, req.Prompt)
	}

	s.registry.IncrementLoad(targetNode.ID, 1)
	defer s.registry.IncrementLoad(targetNode.ID, -1)

	start := time.Now()
	output, err := s.callNodeGenerate(targetNode.Address, targetModel, fullPrompt)
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
	// Try /generate first, fallback to /execute
	genPayload, _ := json.Marshal(map[string]string{
		"model":  model,
		"prompt": prompt,
	})

	resp, err := s.dispatcher.Post(nodeAddr+"/generate", "application/json", bytes.NewReader(genPayload))
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
	resp, err = s.dispatcher.Post(nodeAddr+"/execute", "application/json", bytes.NewReader(execPayload))
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

// --- OpenAI-Compatible Gateway Handlers ---

type openAIModelObj struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

func (s *Server) handleOpenAIModels(w http.ResponseWriter, r *http.Request) {
	models := s.agent.ListClusterModels()
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

	// Reconstruct prompt from messages
	var sb strings.Builder
	for _, m := range req.Messages {
		if m.Role == "system" {
			sb.WriteString("<|im_start|>system\n" + m.Content + "<|im_end|>\n")
		} else if m.Role == "user" {
			sb.WriteString("<|im_start|>user\n" + m.Content + "<|im_end|>\n")
		} else if m.Role == "assistant" {
			sb.WriteString("<|im_start|>assistant\n" + m.Content + "<|im_end|>\n")
		}
	}
	sb.WriteString("<|im_start|>assistant\n")

	genReq := generateAPIRequest{
		Model:       req.Model,
		Prompt:      sb.String(),
		Temperature: req.Temperature,
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

	if req.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, _ := w.(http.Flusher)

		chunk := map[string]any{
			"id":      "chatcmpl-" + genResp.NodeID,
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   genResp.Model,
			"choices": []map[string]any{
				{
					"index":         0,
					"delta":         map[string]string{"content": genResp.Output},
					"finish_reason": "stop",
				},
			},
		}
		data, _ := json.Marshal(chunk)
		_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", data)
		if flusher != nil {
			flusher.Flush()
		}
		return
	}

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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
