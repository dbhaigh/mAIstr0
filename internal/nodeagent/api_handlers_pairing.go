package nodeagent

import (
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"net/http"
	"strings"
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
			if !a.identityComponent.IsTrustedID(candidate.id) {
				pairingRequired = true
				break
			}
		}
	}
	fingerprint := ""
	if a.identityComponent.Initialized() {
		var err error
		fingerprint, err = peeridentity.CertificateFingerprint(a.identityComponent.CertificatePEM())
		if err != nil {
			writeError(w, apperr.InternalError(err))
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"fingerprint":         fingerprint,
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
	request, err := httpx.DecodeJSON[struct {
		Address        string `json:"address"`
		PIN            string `json:"pin"`
		OrchestratorID string `json:"orchestrator_id,omitempty"`
		Fingerprint    string `json:"fingerprint,omitempty"`
	}](w, r, 4<<10)
	if err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing request")))
		return
	}
	if request.Fingerprint != "" {
		if err := peeridentity.ValidateFingerprint(request.Fingerprint); err != nil {
			writeError(w, apperr.Invalid(err))
			return
		}
	}
	if request.OrchestratorID != "" {
		err = a.orchestratorLink.pairWithDiscoveredOrchestrator(request.OrchestratorID, request.Address, request.PIN, request.Fingerprint)
	} else {
		address, normalizeErr := normalizeOrchestratorAddress(request.Address)
		if normalizeErr != nil {
			writeError(w, apperr.Invalid(normalizeErr))
			return
		}
		err = a.orchestratorLink.pairWithAddressContext(r.Context(), address, "", request.PIN, request.Fingerprint)
	}
	if errors.Is(err, errSelectedOrchestratorNotDiscovered) {
		writeError(w, apperr.ConflictError(err))
		return
	}
	if err != nil {
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
	request, err := httpx.DecodeJSON[pairingInvitationRequest](w, r, 4<<10)
	if err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing invitation")))
		return
	}
	address, err := normalizeOrchestratorAddress(request.Address)
	if err != nil || request.NodeID != a.cfg.NodeID || request.OrchestratorID == "" ||
		r.TLS.PeerCertificates[0].Subject.CommonName != request.OrchestratorID || !config.ValidPairingPIN(request.PIN) {
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
	fingerprint, err := peeridentity.ConnectionFingerprint(r.TLS)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	invitation.Fingerprint = fingerprint
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
		"fingerprint":     invitation.Fingerprint,
	})
}

func (a *Agent) handlePairingInvitationAccept(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSON[struct {
		Fingerprint string `json:"fingerprint"`
	}](w, r, 4<<10)
	if err != nil || peeridentity.ValidateFingerprint(request.Fingerprint) != nil {
		writeError(w, apperr.Invalid(errors.New("the out-of-band verified orchestrator fingerprint is required")))
		return
	}
	invitation, err := a.orchestratorLink.beginPairingInvitationAcceptance(time.Now())
	if errors.Is(err, errPairingInvitationAlreadyAccepting) {
		writeError(w, apperr.ConflictError(err))
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusGone)
		return
	}
	if !strings.EqualFold(request.Fingerprint, invitation.Fingerprint) {
		err := errors.New("verified fingerprint does not match the invitation")
		a.orchestratorLink.finishPairingInvitationAcceptance(invitation, err)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	err = a.orchestratorLink.pairWithAddress(invitation.Address, invitation.OrchestratorID, invitation.PIN, invitation.Fingerprint)

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
