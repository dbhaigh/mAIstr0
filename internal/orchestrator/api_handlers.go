package orchestrator

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/version"
)

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

func (s *Server) handleSystemStatus(w http.ResponseWriter, _ *http.Request) {
	persistence := memory.AvailabilityFor(s.mem != nil)
	status := map[string]any{
		"role":        "orchestrator",
		"persistence": persistence,
	}
	if s.mode == config.OrchestratorModeMinimal {
		persistence = memory.Availability{}
		status["persistence"] = persistence
		status["mode"] = config.OrchestratorModeMinimal
	}
	writeJSON(w, http.StatusOK, status)
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
	writeError(w, apperr.UnavailableError(errors.New("this instance yielded orchestrator leadership to a higher-ranked cluster member; use the local node dashboard to join")))
}

func (s *Server) handlePairingRoster(w http.ResponseWriter, _ *http.Request) {
	if s.identity == nil {
		writeError(w, apperr.UnavailableError(errors.New("identity is not initialized")))
		return
	}
	writeJSON(w, http.StatusOK, s.identity.TrustedPeers())
}

func (s *Server) handlePairingStatus(w http.ResponseWriter, _ *http.Request) {
	if s.identity == nil {
		writeError(w, apperr.UnavailableError(errors.New("pairing is not initialized")))
		return
	}
	status := map[string]any{"role": "orchestrator", "address": s.advertiseAddr, "orchestrator_id": s.SelfID()}
	if pin, expiresAt, ok := s.identity.ActivePairingPIN(); ok {
		status["pin"] = pin
		status["expires_at"] = expiresAt
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handlePairingPINRegenerate(w http.ResponseWriter, _ *http.Request) {
	if s.identity == nil {
		writeError(w, apperr.UnavailableError(errors.New("pairing is not initialized")))
		return
	}
	pin, err := s.identity.NewPairingPIN()
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not generate a pairing PIN: %w", err)))
		return
	}
	_, expiresAt, active := s.identity.ActivePairingPIN()
	if !active {
		writeError(w, apperr.InternalError(errors.New("generated pairing PIN is not active")))
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
		writeError(w, apperr.UnavailableError(errors.New("pairing is not initialized")))
		return
	}
	if r.TLS == nil {
		logging.Errorf("orchestrator", "orchestrator: rejected plaintext pairing request from %s", r.RemoteAddr)
		http.Error(w, "pairing requires HTTPS", http.StatusUpgradeRequired)
		return
	}
	request, err := httpx.DecodeJSON[pairingJoinRequest](w, r, 1<<20)
	if err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing request")))
		return
	}
	if request.Role != "node" {
		logging.Errorf("orchestrator", "orchestrator: rejected pairing request from %s: invalid peer role", r.RemoteAddr)
		writeError(w, apperr.Invalid(errors.New("only nodes can join an orchestrator")))
		return
	}
	if request.ID == "" || request.ID == s.SelfID() {
		logging.Errorf("orchestrator", "orchestrator: rejected pairing request from %s: invalid peer identity", r.RemoteAddr)
		writeError(w, apperr.Invalid(errors.New("invalid peer identity")))
		return
	}
	if err := s.identity.ValidatePeer(request.ID, request.CertificatePEM); err != nil {
		logging.Errorf("orchestrator", "orchestrator: rejected pairing request from %s: invalid peer certificate: %v", r.RemoteAddr, err)
		writeError(w, apperr.Invalid(err))
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
		logging.Errorf("orchestrator", "orchestrator: rejected pairing request from %s: PIN invalid, expired, or already used", r.RemoteAddr)
		http.Error(w, "pairing PIN is invalid or expired", http.StatusUnauthorized)
		return
	}
	peerAddr := request.PeerAddr
	if peerAddr == "" {
		for _, peer := range s.electionPort().discoverySnapshot() {
			if peer.ID == request.ID && peer.PeerAddr != "" {
				peerAddr = peer.PeerAddr
				break
			}
		}
	}
	if err := s.identity.TrustPeerDetails(request.ID, request.CertificatePEM, request.Role, peerAddr); err != nil {
		logging.Errorf("orchestrator", "orchestrator: failed to save paired peer from %s: %v", r.RemoteAddr, err)
		writeError(w, apperr.Invalid(err))
		return
	}
	if pinValid {
		if _, err := s.identity.NewPairingPIN(); err != nil {
			writeError(w, apperr.InternalError(errors.New("peer paired but could not create the next pairing PIN")))
			return
		}
		logging.Infof("orchestrator", "orchestrator: successfully paired peer role=%s from %s using PIN", request.Role, r.RemoteAddr)
	} else {
		logging.Infof("orchestrator", "orchestrator: automatically paired node %s from same subnet (%s)", request.ID, r.RemoteAddr)
	}
	s.metrics.recordPairingEvent()
	writeJSON(w, http.StatusOK, pairingJoinResponse{
		ID: s.SelfID(), Role: "orchestrator", CertificatePEM: s.identity.CertificatePEM(),
		Peers: s.identity.TrustedPeers(),
	})
}

// handleDiscovery returns other mAIstr0 instances (orchestrators or nodes)
// found on the local network via UDP broadcast, for the GUI's network panel.
func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if !s.electionPort().hasDiscovery() {
		writeJSON(w, http.StatusOK, []discovery.Peer{})
		return
	}
	writeJSON(w, http.StatusOK, s.electionPort().discoverySnapshot())
}

func (s *Server) handlePairingInvite(w http.ResponseWriter, r *http.Request) {
	if s.identity == nil {
		writeError(w, apperr.UnavailableError(errors.New("pairing is not initialized")))
		return
	}
	nodeID := r.PathValue("id")
	if nodeID == "" {
		writeError(w, apperr.Invalid(errors.New("node id is required")))
		return
	}
	if _, paired := s.registry.Get(nodeID); paired {
		writeError(w, apperr.ConflictError(errors.New("node is already paired; use the node re-pair action")))
		return
	}
	request, err := httpx.DecodeJSON[struct {
		PIN string `json:"pin"`
	}](w, r, 4<<10)
	if err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing invitation request")))
		return
	}
	activePIN, expiresAt, active := s.identity.ActivePairingPIN()
	if !active || len(request.PIN) != len(activePIN) ||
		subtle.ConstantTimeCompare([]byte(request.PIN), []byte(activePIN)) != 1 {
		http.Error(w, "pairing PIN is invalid or expired", http.StatusUnauthorized)
		return
	}
	if s.advertiseAddr == "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator HTTPS address is unavailable")))
		return
	}
	orchestratorURL, err := url.Parse(s.advertiseAddr)
	if err != nil || orchestratorURL.Scheme != "https" || orchestratorURL.Host == "" ||
		orchestratorURL.User != nil || orchestratorURL.Path != "" && orchestratorURL.Path != "/" ||
		orchestratorURL.RawQuery != "" || orchestratorURL.Fragment != "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator HTTPS address is invalid")))
		return
	}
	if !s.electionPort().hasDiscovery() {
		writeError(w, apperr.UnavailableError(errors.New("LAN discovery is unavailable")))
		return
	}
	var nodePeer *discovery.Peer
	for _, peer := range s.electionPort().discoverySnapshot() {
		if peer.ID == nodeID && peer.Role == "node" {
			copy := peer
			nodePeer = &copy
			break
		}
	}
	if nodePeer == nil {
		writeError(w, apperr.NotFoundError(errors.New("node is no longer visible on the local network")))
		return
	}
	targetURL, err := url.Parse(nodePeer.PeerAddr)
	if err != nil || targetURL.Scheme != "https" || targetURL.Host == "" ||
		targetURL.User != nil || targetURL.Path != "" && targetURL.Path != "/" ||
		targetURL.RawQuery != "" || targetURL.Fragment != "" {
		writeError(w, apperr.Invalid(errors.New("discovered node does not advertise a valid HTTPS address")))
		return
	}
	payload, err := json.Marshal(map[string]any{
		"node_id":         nodeID,
		"orchestrator_id": s.SelfID(),
		"address":         s.advertiseAddr,
		"pin":             request.PIN,
		"expires_at":      expiresAt,
	})
	if err != nil {
		writeError(w, apperr.InternalError(errors.New("could not encode pairing invitation")))
		return
	}
	outbound, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		strings.TrimRight(nodePeer.PeerAddr, "/")+"/api/pairing/invitation", bytes.NewReader(payload))
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not prepare pairing invitation: %w", err)))
		return
	}
	outbound.Header.Set("Content-Type", "application/json")
	if requestID := httpx.RequestIDFromContext(r.Context()); requestID != "" {
		outbound.Header.Set(httpx.RequestIDHeader, requestID)
		outbound.Header.Set(inferroute.HeaderRequestID, requestID)
	}
	client := httpx.NewClient(httpx.ClientOptions{
		Timeout:   discoveryRequestTimeout,
		Transport: s.identity.BootstrapTransportForID(nodeID),
	})
	response, err := client.Do(outbound)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("could not reach discovered node: %w", err)))
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		writeError(w, apperr.UpstreamError(fmt.Errorf("node rejected pairing invitation: %s", strings.TrimSpace(string(message)))))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"node_id":    nodeID,
		"address":    nodePeer.PeerAddr,
		"expires_at": expiresAt,
	})
}
