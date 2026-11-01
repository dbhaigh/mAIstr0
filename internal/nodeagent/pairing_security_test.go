package nodeagent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func pairingSecurityAgent(t *testing.T, nodeID string) *Agent {
	t.Helper()
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: nodeID, MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	return agent
}

func pairingSecurityIdentity(t *testing.T, id string) *peeridentity.Identity {
	t.Helper()
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), id+".json"), id)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func pairingSecurityCertificate(t *testing.T, identity *peeridentity.Identity) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(identity.CertificatePEM()))
	if block == nil {
		t.Fatal("identity certificate is not PEM encoded")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func TestPairingInvitationUsesTLSFingerprintAndKeepsPINTransient(t *testing.T) {
	agent := pairingSecurityAgent(t, "invitation-security-node")
	orchestrator := pairingSecurityIdentity(t, "invitation-security-orchestrator")
	pin, err := orchestrator.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	expectedFingerprint := securityFingerprint(t, orchestrator)
	payload, err := json.Marshal(pairingInvitationRequest{
		NodeID: agent.cfg.NodeID, OrchestratorID: orchestrator.ID(),
		Address: "https://127.0.0.1:7450", PIN: pin, ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/invitation", bytes.NewReader(payload))
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{pairingSecurityCertificate(t, orchestrator)},
	}
	response := httptest.NewRecorder()
	agent.handlePairingInvitation(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("invitation returned %d: %s", response.Code, response.Body.String())
	}

	invitation := agent.orchestratorLink.pairingInvitationStatus(time.Now())
	if invitation == nil || invitation.Fingerprint != expectedFingerprint {
		t.Fatalf("stored invitation fingerprint = %v, want the TLS peer fingerprint", invitation)
	}
	if agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatal("receiving an invitation established trust before local acceptance")
	}
	encodedInvitation, err := json.Marshal(invitation)
	if err != nil {
		t.Fatal(err)
	}
	var invitationFields map[string]json.RawMessage
	if err := json.Unmarshal(encodedInvitation, &invitationFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := invitationFields["pin"]; ok {
		t.Fatal("serialized invitation contains a PIN field")
	}

	status := httptest.NewRecorder()
	agent.handlePairingInvitationStatus(status, httptest.NewRequest(http.MethodGet, "/api/pairing/invitation", nil))
	var statusFields map[string]json.RawMessage
	if err := json.Unmarshal(status.Body.Bytes(), &statusFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := statusFields["pin"]; ok {
		t.Fatal("invitation status contains a PIN field")
	}

	identityFile, err := os.ReadFile(agent.identity.Path())
	if err != nil {
		t.Fatal(err)
	}
	var persistedFields map[string]json.RawMessage
	if err := json.Unmarshal(identityFile, &persistedFields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"pin", "pairing_pin", "pairingPIN"} {
		if _, ok := persistedFields[field]; ok {
			t.Fatalf("identity persistence contains a %q field", field)
		}
	}
}

func TestPairingInvitationRejectsAnotherValidCertificateFingerprint(t *testing.T) {
	agent := pairingSecurityAgent(t, "invitation-mismatch-node")
	orchestrator := pairingSecurityIdentity(t, "invitation-mismatch-orchestrator")
	other := pairingSecurityIdentity(t, "other-invitation-orchestrator")
	pin, err := orchestrator.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	expectedFingerprint := securityFingerprint(t, orchestrator)
	otherFingerprint := securityFingerprint(t, other)
	if expectedFingerprint == otherFingerprint {
		t.Fatal("test certificates unexpectedly have the same fingerprint")
	}
	now := time.Now()
	_, err = agent.orchestratorLink.storePairingInvitation(&pairingInvitation{
		Fingerprint: expectedFingerprint, OrchestratorID: orchestrator.ID(),
		Address: "https://127.0.0.1:7450", PIN: pin, ExpiresAt: now.Add(time.Minute),
	}, now)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(map[string]string{"fingerprint": otherFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	agent.handlePairingInvitationAccept(response, httptest.NewRequest(
		http.MethodPost, "/api/pairing/invitation/accept", bytes.NewReader(payload),
	))
	if response.Code != http.StatusForbidden {
		t.Fatalf("mismatched certificate fingerprint returned %d: %s", response.Code, response.Body.String())
	}
	pending := agent.orchestratorLink.pairingInvitationStatus(time.Now())
	if pending == nil || pending.Fingerprint != expectedFingerprint {
		t.Fatal("rejected fingerprint mismatch discarded or changed the invitation")
	}
	if agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatal("mismatched fingerprint established trust")
	}
}

func TestExpiredPairingInvitationIsRejectedAtReceiptAndAcceptance(t *testing.T) {
	agent := pairingSecurityAgent(t, "expired-invitation-node")
	orchestrator := pairingSecurityIdentity(t, "expired-invitation-orchestrator")
	pin, err := orchestrator.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	certificate := pairingSecurityCertificate(t, orchestrator)
	expiredAt := time.Now().Add(-time.Minute)
	payload, err := json.Marshal(pairingInvitationRequest{
		NodeID: agent.cfg.NodeID, OrchestratorID: orchestrator.ID(),
		Address: "https://127.0.0.1:7450", PIN: pin, ExpiresAt: expiredAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/invitation", bytes.NewReader(payload))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
	response := httptest.NewRecorder()
	agent.handlePairingInvitation(response, request)
	if response.Code != http.StatusGone || agent.orchestratorLink.pairingInvitationStatus(time.Now()) != nil {
		t.Fatalf("expired incoming invitation returned %d or remained pending", response.Code)
	}

	fingerprint := securityFingerprint(t, orchestrator)
	agent.orchestratorLink.pairingInvite = &pairingInvitation{
		Fingerprint: fingerprint, OrchestratorID: orchestrator.ID(),
		Address: "https://127.0.0.1:7450", PIN: pin, ExpiresAt: time.Now().Add(-time.Second),
	}
	acceptPayload, err := json.Marshal(map[string]string{"fingerprint": fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	accepted := httptest.NewRecorder()
	agent.handlePairingInvitationAccept(accepted, httptest.NewRequest(
		http.MethodPost, "/api/pairing/invitation/accept", bytes.NewReader(acceptPayload),
	))
	if accepted.Code != http.StatusGone || agent.orchestratorLink.pairingInvitationStatus(time.Now()) != nil {
		t.Fatalf("expired pending invitation returned %d or remained pending", accepted.Code)
	}
	if agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatal("expired invitation established trust")
	}
}

func TestStrictPairingRequiresFingerprintAndOneTimePINOnFirstContact(t *testing.T) {
	agent := pairingSecurityAgent(t, "strict-pairing-node")
	orchestrator := pairingSecurityIdentity(t, "strict-pairing-orchestrator")
	pin, err := orchestrator.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	receivedPINs := make(chan string, 2)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request nodePairingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid pairing request", http.StatusBadRequest)
			return
		}
		receivedPINs <- request.PIN
		if !orchestrator.ConsumePairingPIN(request.PIN) {
			http.Error(w, "pairing PIN is invalid or expired", http.StatusUnauthorized)
			return
		}
		writeJSON(w, http.StatusOK, nodePairingResponse{
			ID: orchestrator.ID(), Role: "orchestrator", CertificatePEM: orchestrator.CertificatePEM(),
		})
	}))
	server.TLS = orchestrator.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	required := true
	agent.SetRuntimeSettings("", config.RuntimeSettings{RequirePairingVerification: &required})
	if !agent.PairingVerificationRequired() {
		t.Fatal("strict pairing policy was not applied")
	}

	err = agent.orchestratorLink.pairWithAddressContextAndSubnetPolicy(
		context.Background(), server.URL, orchestrator.ID(), pin, true,
	)
	if err == nil {
		t.Fatal("strict first-contact pairing succeeded without a verified fingerprint")
	}
	select {
	case <-receivedPINs:
		t.Fatal("strict first-contact pairing reached the server without a fingerprint")
	default:
	}
	if !orchestrator.VerifyPairingPIN(pin) {
		t.Fatal("fingerprint rejection consumed the one-time PIN")
	}

	fingerprint := securityFingerprint(t, orchestrator)
	err = agent.orchestratorLink.pairWithAddressContextAndSubnetPolicy(
		context.Background(), server.URL, orchestrator.ID(), "", true, fingerprint,
	)
	if err == nil {
		t.Fatal("strict pairing succeeded without the one-time PIN")
	}
	select {
	case received := <-receivedPINs:
		if received != "" {
			t.Fatal("strict no-PIN attempt unexpectedly sent a PIN")
		}
	default:
		t.Fatal("verified fingerprint attempt did not reach the pairing server")
	}
	if !orchestrator.VerifyPairingPIN(pin) {
		t.Fatal("missing-PIN rejection consumed the one-time PIN")
	}

	err = agent.orchestratorLink.pairWithAddressContextAndSubnetPolicy(
		context.Background(), server.URL, orchestrator.ID(), pin, true, fingerprint,
	)
	if err != nil {
		t.Fatalf("strict pairing with both checks failed: %v", err)
	}
	select {
	case received := <-receivedPINs:
		if received != pin {
			t.Fatal("strict pairing did not send the active one-time PIN")
		}
	default:
		t.Fatal("strict pairing did not reach the pairing server")
	}
	if !agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatal("strict pairing with both checks did not establish trust")
	}
}

func TestPairingStatusDisablesAutoJoinWithStrictVerification(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "strict-pairing-status-node", OrchestratorAddr: "https://localhost:7450",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	required := true
	agent.SetRuntimeSettings("", config.RuntimeSettings{RequirePairingVerification: &required})

	response := httptest.NewRecorder()
	agent.handlePairingStatus(response, httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil))
	var status struct {
		PairingRequired            bool `json:"pairing_required"`
		AutoJoinAvailable          bool `json:"auto_join_available"`
		RequirePairingVerification bool `json:"require_pairing_verification"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || !status.PairingRequired ||
		status.AutoJoinAvailable || !status.RequirePairingVerification {
		t.Fatalf("strict pairing status = %d, %+v", response.Code, status)
	}
}
