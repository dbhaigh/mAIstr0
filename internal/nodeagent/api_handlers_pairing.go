package nodeagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/discovery"
	"net/http"
	"time"
)

func (a *Agent) handlePairingStatus(w http.ResponseWriter, _ *http.Request) {
	cfg := a.configSnapshot()
	orchestratorID := a.getOrchestratorID()
	trusted := orchestratorID != "" && a.identityComponent.IsTrustedID(orchestratorID)
	joined := a.isJoined()
	orchestratorAddr := a.getOrchestratorAddr()
	pairingRequired := false
	autoJoinAvailable := false
	if !joined {
		candidates := a.orchestratorCandidates()
		if len(candidates) > 0 {
			orchestratorAddr = candidates[0].address
			orchestratorID = candidates[0].id
			trusted = a.identityComponent.IsTrustedID(orchestratorID)
		}
		for _, candidate := range candidates {
			if discovery.IsSameSubnetAddress(candidate.address) {
				autoJoinAvailable = true
			}
			if !a.identityComponent.IsTrustedID(candidate.id) {
				pairingRequired = true
				break
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"role":                "node",
		"node_id":             cfg.NodeID,
		"node_addr":           cfg.AdvertiseAddr,
		"orchestrator_addr":   orchestratorAddr,
		"orchestrator_id":     orchestratorID,
		"trusted":             trusted,
		"joined":              joined,
		"pairing_required":    pairingRequired,
		"auto_join_available": autoJoinAvailable,
	})
}

func (a *Agent) handlePairingConnect(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Address string `json:"address"`
		PIN     string `json:"pin"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&request); err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing request")))
		return
	}
	if err := a.PairWithOrchestrator(request.Address, request.PIN); err != nil {
		writeError(w, apperr.UpstreamError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"orchestrator_addr": a.getOrchestratorAddr(),
		"orchestrator_id":   a.getOrchestratorID(),
	})
}

func (a *Agent) handlePairingInvitation(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil {
		http.Error(w, "pairing invitations require HTTPS", http.StatusUpgradeRequired)
		return
	}
	if len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "pairing invitation must include an orchestrator identity", http.StatusForbidden)
		return
	}
	var request pairingInvitationRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&request); err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing invitation")))
		return
	}
	address, err := normalizeOrchestratorAddress(request.Address)
	if err != nil || request.NodeID != a.cfg.NodeID || request.OrchestratorID == "" ||
		r.TLS.PeerCertificates[0].Subject.CommonName != request.OrchestratorID || !validPairingPIN(request.PIN) {
		writeError(w, apperr.Invalid(errors.New("pairing invitation identity or details are invalid")))
		return
	}
	now := time.Now()
	expiresAt := request.ExpiresAt
	if expiresAt.IsZero() || expiresAt.After(now.Add(10*time.Minute)) {
		expiresAt = now.Add(10 * time.Minute)
	}
	if !expiresAt.After(now) {
		http.Error(w, "pairing invitation has expired", http.StatusGone)
		return
	}

	invitation := &pairingInvitation{
		OrchestratorID: request.OrchestratorID,
		Address:        address,
		PIN:            request.PIN,
		ExpiresAt:      expiresAt,
	}
	stored, err := a.orchestratorLink.storePairingInvitation(invitation, now)
	if err != nil {
		writeError(w, apperr.ConflictError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "expires_at": stored.ExpiresAt})
}

func (a *Agent) handlePairingInvitationStatus(w http.ResponseWriter, _ *http.Request) {
	invitation := a.orchestratorLink.pairingInvitationStatus(time.Now())
	if invitation == nil {
		writeJSON(w, http.StatusOK, map[string]any{"pending": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pending":         true,
		"orchestrator_id": invitation.OrchestratorID,
		"address":         invitation.Address,
		"expires_at":      invitation.ExpiresAt,
	})
}

func (a *Agent) handlePairingInvitationAccept(w http.ResponseWriter, _ *http.Request) {
	invitation, err := a.orchestratorLink.beginPairingInvitationAcceptance(time.Now())
	if errors.Is(err, errPairingInvitationAlreadyAccepting) {
		writeError(w, apperr.ConflictError(err))
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusGone)
		return
	}

	err = a.pairWithAddress(invitation.Address, invitation.OrchestratorID, invitation.PIN)

	a.orchestratorLink.finishPairingInvitationAcceptance(invitation, err)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("could not join cluster: %w", err)))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"orchestrator_id":   a.getOrchestratorID(),
		"orchestrator_addr": a.getOrchestratorAddr(),
	})
}

func (a *Agent) handlePairingInvitationReject(w http.ResponseWriter, _ *http.Request) {
	if err := a.orchestratorLink.rejectPairingInvitation(); err != nil {
		writeError(w, apperr.ConflictError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
