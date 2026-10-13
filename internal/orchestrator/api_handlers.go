package orchestrator

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
)

func (s *Server) handleSystemStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"role":        "orchestrator",
		"persistence": memory.AvailabilityFor(s.mem != nil),
	})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": version.String()})
}

func (s *Server) handleYieldedDashboard(w http.ResponseWriter, r *http.Request) {
	if s.localNodeDashboard != "" && r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/":
			http.Redirect(w, r, s.localNodeDashboard+"/", http.StatusTemporaryRedirect)
			return
		case "/api/pairing/status":
			writeJSON(w, http.StatusOK, map[string]string{
				"role": "node", "dashboard_url": s.localNodeDashboard,
			})
			return
		}
	}
	http.Error(w, "this instance yielded orchestrator leadership to a higher-ranked cluster member; use the local node dashboard to join", http.StatusServiceUnavailable)
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
	status := map[string]any{"role": "orchestrator", "address": s.advertiseAddr, "orchestrator_id": s.selfID}
	if pin, expiresAt, ok := s.identity.ActivePairingPIN(); ok {
		status["pin"] = pin
		status["expires_at"] = expiresAt
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handlePairingPINRegenerate(w http.ResponseWriter, _ *http.Request) {
	if s.identity == nil {
		http.Error(w, "pairing is not initialized", http.StatusServiceUnavailable)
		return
	}
	pin, err := s.identity.NewPairingPIN()
	if err != nil {
		http.Error(w, "could not generate a pairing PIN: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_, expiresAt, active := s.identity.ActivePairingPIN()
	if !active {
		http.Error(w, "generated pairing PIN is not active", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pin":        pin,
		"expires_at": expiresAt,
	})
}

func (s *Server) handlePairingConnect(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "orchestrator-to-orchestrator pairing is disabled; join the cluster as a node", http.StatusForbidden)
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
	if request.Role != "node" {
		log.Printf("orchestrator: rejected pairing request from %s: invalid peer role", r.RemoteAddr)
		http.Error(w, "only nodes can join an orchestrator", http.StatusBadRequest)
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
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	remoteIP := net.ParseIP(host)
	sameSubnet := discovery.IsSameSubnet(remoteIP)

	pinValid := s.identity.ConsumePairingPIN(request.PIN)
	if !sameSubnet && !pinValid {
		log.Printf("orchestrator: rejected pairing request from %s: PIN invalid, expired, or already used", r.RemoteAddr)
		http.Error(w, "pairing PIN is invalid or expired", http.StatusUnauthorized)
		return
	}
	peerAddr := request.PeerAddr
	if peerAddr == "" && s.discovery != nil {
		for _, peer := range s.discovery.Snapshot() {
			if peer.ID == request.ID && peer.PeerAddr != "" {
				peerAddr = peer.PeerAddr
				break
			}
		}
	}
	if err := s.identity.TrustPeerDetails(request.ID, request.CertificatePEM, request.Role, peerAddr); err != nil {
		log.Printf("orchestrator: failed to save paired peer from %s: %v", r.RemoteAddr, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if pinValid {
		nextPIN, err := s.identity.NewPairingPIN()
		if err != nil {
			http.Error(w, "peer paired but could not create the next pairing PIN", http.StatusInternalServerError)
			return
		}
		log.Printf("orchestrator: secure pairing PIN (valid for 30 minutes): %s", nextPIN)
		log.Printf("orchestrator: successfully paired peer role=%s from %s using PIN", request.Role, r.RemoteAddr)
	} else {
		log.Printf("orchestrator: automatically paired node %s from same subnet (%s)", request.ID, r.RemoteAddr)
	}
	writeJSON(w, http.StatusOK, pairingJoinResponse{
		ID: s.selfID, Role: "orchestrator", CertificatePEM: s.identity.CertificatePEM(),
		Peers: s.identity.TrustedPeers(),
	})
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

func (s *Server) handlePairingInvite(w http.ResponseWriter, r *http.Request) {
	if s.identity == nil {
		http.Error(w, "pairing is not initialized", http.StatusServiceUnavailable)
		return
	}
	nodeID := r.PathValue("id")
	if nodeID == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}
	if _, paired := s.registry.Get(nodeID); paired {
		http.Error(w, "node is already paired; use the node re-pair action", http.StatusConflict)
		return
	}
	var request struct {
		PIN string `json:"pin"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&request); err != nil {
		http.Error(w, "invalid pairing invitation request", http.StatusBadRequest)
		return
	}
	activePIN, expiresAt, active := s.identity.ActivePairingPIN()
	if !active || len(request.PIN) != len(activePIN) ||
		subtle.ConstantTimeCompare([]byte(request.PIN), []byte(activePIN)) != 1 {
		http.Error(w, "pairing PIN is invalid or expired", http.StatusUnauthorized)
		return
	}
	if s.advertiseAddr == "" {
		http.Error(w, "orchestrator HTTPS address is unavailable", http.StatusServiceUnavailable)
		return
	}
	orchestratorURL, err := url.Parse(s.advertiseAddr)
	if err != nil || orchestratorURL.Scheme != "https" || orchestratorURL.Host == "" ||
		orchestratorURL.User != nil || orchestratorURL.Path != "" && orchestratorURL.Path != "/" ||
		orchestratorURL.RawQuery != "" || orchestratorURL.Fragment != "" {
		http.Error(w, "orchestrator HTTPS address is invalid", http.StatusServiceUnavailable)
		return
	}
	if s.discovery == nil {
		http.Error(w, "LAN discovery is unavailable", http.StatusServiceUnavailable)
		return
	}
	var nodePeer *discovery.Peer
	for _, peer := range s.discovery.Snapshot() {
		if peer.ID == nodeID && peer.Role == "node" {
			copy := peer
			nodePeer = &copy
			break
		}
	}
	if nodePeer == nil {
		http.Error(w, "node is no longer visible on the local network", http.StatusNotFound)
		return
	}
	targetURL, err := url.Parse(nodePeer.PeerAddr)
	if err != nil || targetURL.Scheme != "https" || targetURL.Host == "" ||
		targetURL.User != nil || targetURL.Path != "" && targetURL.Path != "/" ||
		targetURL.RawQuery != "" || targetURL.Fragment != "" {
		http.Error(w, "discovered node does not advertise a valid HTTPS address", http.StatusBadRequest)
		return
	}
	payload, err := json.Marshal(map[string]any{
		"node_id":         nodeID,
		"orchestrator_id": s.selfID,
		"address":         s.advertiseAddr,
		"pin":             request.PIN,
		"expires_at":      expiresAt,
	})
	if err != nil {
		http.Error(w, "could not encode pairing invitation", http.StatusInternalServerError)
		return
	}
	outbound, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		strings.TrimRight(nodePeer.PeerAddr, "/")+"/api/pairing/invitation", bytes.NewReader(payload))
	if err != nil {
		http.Error(w, "could not prepare pairing invitation: "+err.Error(), http.StatusInternalServerError)
		return
	}
	outbound.Header.Set("Content-Type", "application/json")
	client := &http.Client{
		Timeout:   discoveryRequestTimeout,
		Transport: s.identity.BootstrapTransportForID(nodeID),
	}
	response, err := client.Do(outbound)
	if err != nil {
		http.Error(w, "could not reach discovered node: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		http.Error(w, "node rejected pairing invitation: "+strings.TrimSpace(string(message)), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"node_id":    nodeID,
		"address":    nodePeer.PeerAddr,
		"expires_at": expiresAt,
	})
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

func (s *Server) handleModelCatalog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, engine.ModelCatalog(r.URL.Query().Get("engine"), r.URL.Query().Get("q")))
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
	if strings.HasSuffix(r.URL.Path, "/pairing/connect") {
		path = "/api/pairing/connect"
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

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.recoverTaskHistory()
	task, err := s.createTask(r.Context(), req)
	if err != nil {
		var createErr *taskCreateError
		if errors.As(err, &createErr) {
			http.Error(w, createErr.Error(), createErr.status)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	s.recoverTaskHistory()
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

func (s *Server) handleApproveTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	taskBeforeApproval, ok := s.tasks.Get(id)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	if taskBeforeApproval.Status != taskmgr.StatusAwaitingApproval {
		http.Error(w, "task is not awaiting approval", http.StatusConflict)
		return
	}
	if err := s.tasks.ApproveChecked(id); err != nil {
		if errors.Is(err, taskmgr.ErrNotAwaitingApproval) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, "could not approve task: "+err.Error(), http.StatusInternalServerError)
		return
	}
	task, _ := s.tasks.Get(id)
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleListResumableTasks(w http.ResponseWriter, r *http.Request) {
	resumable := s.tasks.ResumableTasks()
	if resumable == nil {
		resumable = []*taskmgr.Task{}
	}
	writeJSON(w, http.StatusOK, resumable)
}

func (s *Server) handleResumeTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	task, ok := s.tasks.Get(id)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	if !task.Resumable {
		http.Error(w, "task is not resumable", http.StatusConflict)
		return
	}
	resumed, err := s.tasks.ResumeTaskChecked(id)
	if err != nil {
		http.Error(w, "could not resume task: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for _, subtask := range resumed.Subtasks {
		if subtask.Status == taskmgr.StatusPending {
			go s.dispatch(resumed.ID, subtask.Assignment)
		}
	}
	writeJSON(w, http.StatusOK, resumed)
}

func (s *Server) handleTaskCheckpoints(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.tasks.Get(id); !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	cps, err := s.tasks.GetCheckpoints(id)
	if err != nil {
		http.Error(w, "could not load checkpoints: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if cps == nil {
		cps = []taskmgr.Checkpoint{}
	}
	writeJSON(w, http.StatusOK, cps)
}

func (s *Server) handleAgentSessionsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.listAgentSessions())
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

	turnCtx, turnCancel := context.WithCancel(r.Context())
	s.sessionCancelsMu.Lock()
	s.sessionCancels[id] = turnCancel
	s.sessionCancelsMu.Unlock()
	defer func() {
		s.sessionCancelsMu.Lock()
		delete(s.sessionCancels, id)
		s.sessionCancelsMu.Unlock()
		turnCancel()
	}()

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
			_, sendErr := backend.SendMessage(turnCtx, id, content, streamChan)
			if sendErr != nil {
				if !errors.Is(sendErr, context.Canceled) && turnCtx.Err() == nil {
					streamChan <- agent.StreamEvent{Type: agent.EventError, SessionID: id, Error: sendErr.Error()}
				}
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

	msg, sendErr := backend.SendMessage(turnCtx, id, content, nil)
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
		if errors.Is(sendErr, context.Canceled) || turnCtx.Err() != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"session_id": id,
				"status":     "interrupted",
				"message":    msg,
			})
			return
		}
		http.Error(w, sendErr.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, msg)
}

func (s *Server) handleAgentSessionInterrupt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "session ID is required", http.StatusBadRequest)
		return
	}
	_, _, exists := s.agentForSession(id)
	if !exists {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	interrupted := s.InterruptSession(id)
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id":  id,
		"interrupted": interrupted,
	})
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

func (s *Server) handleListAllModels(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	writeJSON(w, http.StatusOK, backend.ListClusterModels())
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

	nodes := s.schedulerNodes()
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
		for _, candidate := range nodes {
			if candidate.ID == req.NodeID {
				candidates = []cluster.NodeStatus{candidate}
				break
			}
		}
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
