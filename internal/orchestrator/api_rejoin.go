package orchestrator

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

const rejoinTicketTTL = 5 * time.Minute

type rejoinTicketResponse struct {
	NodeID         string    `json:"node_id"`
	OrchestratorID string    `json:"orchestrator_id"`
	Ticket         string    `json:"ticket"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type rejoinRequest struct {
	ID             string `json:"id"`
	PeerAddr       string `json:"peer_addr,omitempty"`
	CertificatePEM string `json:"certificate_pem"`
	Ticket         string `json:"ticket"`
}

func (s *Server) handleRejoinTicket(w http.ResponseWriter, r *http.Request) {
	if s.identity == nil {
		writeError(w, apperr.UnavailableError(errors.New("identity is not initialized")))
		return
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		logRejoinAudit("rejected", "", "missing_client_certificate")
		http.Error(w, "rejoin ticket issuance requires a paired mTLS node", http.StatusForbidden)
		return
	}
	peerID := strings.TrimSpace(r.TLS.PeerCertificates[0].Subject.CommonName)
	if peerID == "" || !s.identity.IsTrustedNodeID(peerID) ||
		!s.identity.IsTrustedCertificate(r.TLS.PeerCertificates[0]) {
		logRejoinAudit("rejected", peerID, "node_not_previously_trusted")
		http.Error(w, "rejoin tickets are available only to previously trusted nodes", http.StatusForbidden)
		return
	}
	if s.PairingVerificationRequired() {
		logRejoinAudit("rejected", peerID, "strict_pairing_verification")
		http.Error(w, "strict pairing verification requires fingerprint and PIN pairing", http.StatusForbidden)
		return
	}
	if !requestOnSameSubnet(r) {
		logRejoinAudit("rejected", peerID, "outside_local_subnet")
		http.Error(w, "rejoin tickets are restricted to the previously trusted local subnet", http.StatusForbidden)
		return
	}
	ticket, expiresAt, err := s.identity.IssueRejoinTicket(peerID, time.Now(), rejoinTicketTTL)
	if err != nil {
		logRejoinAudit("rejected", peerID, "ticket_persistence_failed")
		writeError(w, apperr.InternalError(fmt.Errorf("issue rejoin ticket: %w", err)))
		return
	}
	logRejoinAudit("issued", peerID, "success")
	writeJSON(w, http.StatusOK, rejoinTicketResponse{
		NodeID: peerID, OrchestratorID: s.SelfID(), Ticket: ticket, ExpiresAt: expiresAt,
	})
}

func (s *Server) handleRejoin(w http.ResponseWriter, r *http.Request) {
	if s.identity == nil {
		writeError(w, apperr.UnavailableError(errors.New("identity is not initialized")))
		return
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		logRejoinAudit("rejected", "", "missing_client_certificate")
		http.Error(w, "rejoin requires HTTPS client certificate proof", http.StatusUpgradeRequired)
		return
	}
	request, err := httpx.DecodeJSON[rejoinRequest](w, r, 1<<20)
	if err != nil {
		logRejoinAudit("rejected", "", "invalid_request")
		writeError(w, apperr.Invalid(errors.New("invalid rejoin request")))
		return
	}
	request.ID = strings.TrimSpace(request.ID)
	if request.ID == "" || request.ID == s.SelfID() {
		logRejoinAudit("rejected", request.ID, "invalid_node_identity")
		writeError(w, apperr.Invalid(errors.New("invalid node identity")))
		return
	}
	if err := s.identity.ValidatePeer(request.ID, request.CertificatePEM); err != nil {
		logRejoinAudit("rejected", request.ID, "invalid_certificate")
		writeError(w, apperr.Invalid(err))
		return
	}
	if err := peeridentity.ValidatePeerConnection(request.ID, request.CertificatePEM, r.TLS); err != nil {
		logRejoinAudit("rejected", request.ID, "certificate_tls_mismatch")
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if !s.identity.IsTrustedNodeID(request.ID) {
		logRejoinAudit("rejected", request.ID, "node_not_previously_trusted")
		http.Error(w, "rejoin is available only to previously trusted nodes", http.StatusForbidden)
		return
	}
	if s.PairingVerificationRequired() {
		logRejoinAudit("rejected", request.ID, "strict_pairing_verification")
		http.Error(w, "strict pairing verification requires fingerprint and PIN pairing", http.StatusForbidden)
		return
	}
	if !requestOnSameSubnet(r) {
		logRejoinAudit("rejected", request.ID, "outside_local_subnet")
		http.Error(w, "rejoin is restricted to the previously trusted local subnet", http.StatusForbidden)
		return
	}
	if err := s.identity.ConsumeRejoinTicketAndTrust(
		request.ID, request.Ticket, request.CertificatePEM, request.PeerAddr, time.Now(),
	); err != nil {
		reason := "ticket_invalid"
		switch {
		case errors.Is(err, peeridentity.ErrRejoinTicketExpired):
			reason = "ticket_expired"
		case errors.Is(err, peeridentity.ErrRejoinTicketWrongNode):
			reason = "ticket_wrong_node"
		case errors.Is(err, peeridentity.ErrRejoinPeerAddress):
			logRejoinAudit("rejected", request.ID, "invalid_peer_address")
			writeError(w, apperr.Invalid(err))
			return
		case errors.Is(err, peeridentity.ErrRejoinTicketInvalid), errors.Is(err, peeridentity.ErrRejoinNodeNotTrusted):
			logRejoinAudit("rejected", request.ID, reason)
			http.Error(w, "rejoin ticket was rejected", http.StatusForbidden)
			return
		default:
			logRejoinAudit("failed", request.ID, "trust_update_failed")
			writeError(w, apperr.InternalError(fmt.Errorf("restore node trust: %w", err)))
			return
		}
		logRejoinAudit("rejected", request.ID, reason)
		http.Error(w, "rejoin ticket was rejected", http.StatusForbidden)
		return
	}
	logRejoinAudit("consumed", request.ID, "success")
	writeJSON(w, http.StatusOK, pairingJoinResponse{
		ID: s.SelfID(), Role: "orchestrator", CertificatePEM: s.identity.CertificatePEM(),
		Peers: s.identity.TrustedPeers(),
	})
}

func logRejoinAudit(event, nodeID, reason string) {
	logging.Infof("orchestrator", "security audit event=rejoin.ticket.%s node=%q reason=%q", event, nodeID, reason)
}
