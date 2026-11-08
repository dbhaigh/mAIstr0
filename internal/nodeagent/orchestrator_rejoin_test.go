package nodeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestUpgradeRejoinTicketSurvivesRestartAndRestoresTrust(t *testing.T) {
	nodePath := filepath.Join(t.TempDir(), "node.json")
	nodeIdentity, err := peeridentity.Open(nodePath, "node-rejoin-test")
	if err != nil {
		t.Fatal(err)
	}
	orchestratorIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-rejoin-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := orchestratorIdentity.TrustPeerDetails(
		nodeIdentity.ID(), nodeIdentity.CertificatePEM(), "node", "",
	); err != nil {
		t.Fatal(err)
	}
	const ticket = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pairing/rejoin-ticket", func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 ||
			r.TLS.PeerCertificates[0].Subject.CommonName != nodeIdentity.ID() {
			http.Error(w, "unexpected ticket requester", http.StatusForbidden)
			return
		}
		if err := peeridentity.ValidatePeerConnection(
			nodeIdentity.ID(), nodeIdentity.CertificatePEM(), r.TLS,
		); err != nil {
			http.Error(w, "ticket requester certificate mismatch", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(rejoinTicketResponse{
			NodeID: nodeIdentity.ID(), OrchestratorID: orchestratorIdentity.ID(),
			Ticket: ticket, ExpiresAt: time.Now().Add(5 * time.Minute),
		})
	})
	mux.HandleFunc("POST /api/pairing/rejoin", func(w http.ResponseWriter, r *http.Request) {
		var request nodeRejoinRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid rejoin request", http.StatusBadRequest)
			return
		}
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 ||
			request.Ticket != ticket || request.ID != nodeIdentity.ID() {
			http.Error(w, "rejoin credentials do not match", http.StatusForbidden)
			return
		}
		if err := peeridentity.ValidatePeerConnection(request.ID, request.CertificatePEM, r.TLS); err != nil {
			http.Error(w, "rejoin certificate mismatch", http.StatusForbidden)
			return
		}
		if err := orchestratorIdentity.TrustPeerDetails(
			request.ID, request.CertificatePEM, "node", request.PeerAddr,
		); err != nil {
			http.Error(w, "could not update node trust", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(nodePairingResponse{
			ID: orchestratorIdentity.ID(), Role: "orchestrator",
			CertificatePEM: orchestratorIdentity.CertificatePEM(),
			Peers:          orchestratorIdentity.TrustedPeers(),
		})
	})
	httpsServer := httptest.NewUnstartedServer(mux)
	httpsServer.TLS = orchestratorIdentity.ServerTLSConfig()
	httpsServer.StartTLS()
	t.Cleanup(httpsServer.Close)
	if err := nodeIdentity.TrustPeerDetails(
		orchestratorIdentity.ID(), orchestratorIdentity.CertificatePEM(), "orchestrator", httpsServer.URL,
	); err != nil {
		t.Fatal(err)
	}

	newLink := func(identity *peeridentity.Identity) *orchestratorLink {
		state := &agentState{cfg: config.NodeConfig{
			NodeID: identity.ID(), AdvertiseAddr: "https://127.0.0.1:7451",
		}}
		return &orchestratorLink{
			trust: &identityComponent{identity: identity}, configState: state,
			orchestratorID: orchestratorIdentity.ID(), orchestratorAddr: httpsServer.URL,
		}
	}
	link := newLink(nodeIdentity)
	if err := link.prepareRejoinTicket(context.Background()); err != nil {
		t.Fatalf("prepare rejoin ticket before upgrade: %v", err)
	}
	if link.tryPendingRejoin(context.Background(), orchestratorCandidate{
		id: orchestratorIdentity.ID(), address: httpsServer.URL, role: "orchestrator",
	}) {
		t.Fatal("running process consumed its rejoin ticket before restart")
	}
	cached, exists, err := nodeIdentity.PendingRejoinTicket(orchestratorIdentity.ID(), time.Now())
	if err != nil || !exists || cached.Token != ticket {
		t.Fatalf("cached ticket = %+v, exists %t, error %v", cached, exists, err)
	}

	restartedIdentity, err := peeridentity.Open(nodePath, nodeIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	if restartedIdentity.CertificatePEM() == nodeIdentity.CertificatePEM() {
		t.Fatal("restart reused the previous node certificate")
	}
	restartedLink := newLink(restartedIdentity)
	var rejoinStatus cluster.UpgradeStatus
	restartedLink.reportUpgradeStatus = func(status cluster.UpgradeStatus) {
		rejoinStatus = status
	}
	if !restartedLink.tryPendingRejoin(context.Background(), orchestratorCandidate{
		id: orchestratorIdentity.ID(), address: httpsServer.URL, role: "orchestrator",
	}) {
		t.Fatal("restarted node did not rejoin with its cached ticket")
	}
	if got := orchestratorIdentity.TrustedPeers()[restartedIdentity.ID()]; got != restartedIdentity.CertificatePEM() {
		t.Fatal("orchestrator did not replace the node certificate during rejoin")
	}
	if restartedLink.getOrchestratorID() != orchestratorIdentity.ID() ||
		restartedLink.getOrchestratorAddr() != httpsServer.URL {
		t.Fatal("successful rejoin did not restore the orchestrator link")
	}
	if rejoinStatus.State != "completed" || rejoinStatus.TicketStatus != "consumed" || rejoinStatus.Progress != 100 {
		t.Fatalf("rejoin status = %+v, want completed ticket consumption", rejoinStatus)
	}
	if _, exists, err := restartedIdentity.PendingRejoinTicket(orchestratorIdentity.ID(), time.Now()); err != nil || exists {
		t.Fatalf("consumed ticket remains cached: exists %t, error %v", exists, err)
	}
}

func TestPrepareRejoinTicketRejectsNonHTTPSOrchestratorAddress(t *testing.T) {
	nodeIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "node-http-rejoin")
	if err != nil {
		t.Fatal(err)
	}
	orchestratorIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-http-rejoin")
	if err != nil {
		t.Fatal(err)
	}
	if err := nodeIdentity.TrustPeerDetails(
		orchestratorIdentity.ID(), orchestratorIdentity.CertificatePEM(), "orchestrator", "https://127.0.0.1:7450",
	); err != nil {
		t.Fatal(err)
	}
	link := &orchestratorLink{
		trust:            &identityComponent{identity: nodeIdentity},
		configState:      &agentState{cfg: config.NodeConfig{NodeID: nodeIdentity.ID()}},
		orchestratorID:   orchestratorIdentity.ID(),
		orchestratorAddr: "http://127.0.0.1:7450",
	}
	err = link.prepareRejoinTicket(context.Background())
	if err == nil || err.Error() != "upgrade rejoin requires the current orchestrator HTTPS address" {
		t.Fatalf("prepareRejoinTicket error = %v, want HTTPS enforcement", err)
	}
}

func TestTryPendingRejoinReportsExpiredCachedTicket(t *testing.T) {
	nodeIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "node-rejoin-expired")
	if err != nil {
		t.Fatal(err)
	}
	expiresAt := time.Now().Add(20 * time.Millisecond)
	if err := nodeIdentity.StorePendingRejoinTicket(peeridentity.PendingRejoinTicket{
		NodeID: nodeIdentity.ID(), OrchestratorID: "orchestrator-expired",
		Token: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", ExpiresAt: expiresAt,
	}); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(expiresAt))
	defer timer.Stop()
	<-timer.C

	link := &orchestratorLink{trust: &identityComponent{identity: nodeIdentity}}
	var statuses []cluster.UpgradeStatus
	link.reportUpgradeStatus = func(status cluster.UpgradeStatus) {
		statuses = append(statuses, status)
	}
	if link.tryPendingRejoin(context.Background(), orchestratorCandidate{
		id: "orchestrator-expired", address: "https://127.0.0.1:7450", role: "orchestrator",
	}) {
		t.Fatal("expired rejoin ticket was accepted")
	}
	if len(statuses) != 1 {
		t.Fatalf("expired ticket status updates = %+v, want one failure", statuses)
	}
	status := statuses[0]
	if status.State != "failed" || status.Phase != "rejoin_ticket_expired" ||
		status.Progress != 95 || status.TicketStatus != "expired" {
		t.Fatalf("expired ticket status = %+v", status)
	}
	if _, exists, err := nodeIdentity.PendingRejoinTicket("orchestrator-expired", time.Now()); err != nil || exists {
		t.Fatalf("expired ticket remains cached: exists %t, error %v", exists, err)
	}
}

func TestTryPendingRejoinClearsTicketRejectedByOrchestrator(t *testing.T) {
	nodeIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.json"), "node-rejoin-rejected")
	if err != nil {
		t.Fatal(err)
	}
	orchestratorIdentity, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-rejoin-rejected")
	if err != nil {
		t.Fatal(err)
	}
	if err := orchestratorIdentity.TrustPeerDetails(
		nodeIdentity.ID(), nodeIdentity.CertificatePEM(), "node", "",
	); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pairing/rejoin", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	httpsServer := httptest.NewUnstartedServer(mux)
	httpsServer.TLS = orchestratorIdentity.ServerTLSConfig()
	httpsServer.StartTLS()
	t.Cleanup(httpsServer.Close)
	if err := nodeIdentity.TrustPeerDetails(
		orchestratorIdentity.ID(), orchestratorIdentity.CertificatePEM(), "orchestrator", httpsServer.URL,
	); err != nil {
		t.Fatal(err)
	}

	const ticket = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if err := nodeIdentity.StorePendingRejoinTicket(peeridentity.PendingRejoinTicket{
		NodeID: nodeIdentity.ID(), OrchestratorID: orchestratorIdentity.ID(),
		Token: ticket, ExpiresAt: time.Now().Add(5 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	link := &orchestratorLink{
		trust:       &identityComponent{identity: nodeIdentity},
		configState: &agentState{cfg: config.NodeConfig{NodeID: nodeIdentity.ID()}},
	}
	var statuses []cluster.UpgradeStatus
	link.reportUpgradeStatus = func(status cluster.UpgradeStatus) {
		statuses = append(statuses, status)
	}
	if link.tryPendingRejoin(context.Background(), orchestratorCandidate{
		id: orchestratorIdentity.ID(), address: httpsServer.URL, role: "orchestrator",
	}) {
		t.Fatal("rejected rejoin ticket was accepted")
	}
	if len(statuses) != 2 {
		t.Fatalf("rejoin status updates = %+v, want in-progress and rejected", statuses)
	}
	last := statuses[len(statuses)-1]
	if last.State != "failed" || last.Phase != "rejoin_rejected" ||
		last.TicketStatus != "rejected" || last.FailureReason != "the orchestrator rejected the rejoin ticket (HTTP 403)" {
		t.Fatalf("rejected rejoin status = %+v", last)
	}
	if _, exists, err := nodeIdentity.PendingRejoinTicket(orchestratorIdentity.ID(), time.Now()); err != nil || exists {
		t.Fatalf("rejected ticket remains cached: exists %t, error %v", exists, err)
	}
	if got := nodeIdentity.TrustedPeers()[orchestratorIdentity.ID()]; got != orchestratorIdentity.CertificatePEM() {
		t.Fatal("rejected rejoin changed the orchestrator's existing trust")
	}
}
