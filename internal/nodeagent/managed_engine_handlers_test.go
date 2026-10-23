package nodeagent

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestManagedEngineRoutesRequirePinnedCurrentOrchestrator(t *testing.T) {
	cfg := config.DefaultNodeConfig()
	cfg.NodeID = "managed-engine-node"
	cfg.MemoryPath = filepath.Join(t.TempDir(), "node.db")
	agent := mustNewTestAgent(t, cfg)
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close node agent: %v", err)
		}
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}

	oldOrchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "old-orchestrator.json"), "old-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	currentOrchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "current-orchestrator.json"), "current-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	pairedNode, err := peeridentity.Open(filepath.Join(t.TempDir(), "paired-node.json"), "paired-node")
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range []struct {
		identity *peeridentity.Identity
		role     string
	}{
		{oldOrchestrator, "orchestrator"},
		{currentOrchestrator, "orchestrator"},
		{pairedNode, "node"},
	} {
		if err := agent.identityComponent.TrustPeerDetails(peer.identity.ID(), peer.identity.CertificatePEM(), peer.role, "https://127.0.0.1:7450"); err != nil {
			t.Fatal(err)
		}
	}
	agent.setOrchestratorID(currentOrchestrator.ID())

	mux := http.NewServeMux()
	registerManagedEngineRoutes(mux, agent)
	tests := []struct {
		name        string
		certificate *x509.Certificate
		wantStatus  int
	}{
		{name: "plaintext request", wantStatus: http.StatusForbidden},
		{name: "paired worker", certificate: certificateFromPEM(t, pairedNode.CertificatePEM()), wantStatus: http.StatusForbidden},
		{name: "pinned previous orchestrator", certificate: certificateFromPEM(t, oldOrchestrator.CertificatePEM()), wantStatus: http.StatusForbidden},
		{name: "unpaired certificate", certificate: certificateFromPEM(t, mustIdentity(t, "unpaired").CertificatePEM()), wantStatus: http.StatusForbidden},
		{name: "current paired orchestrator", certificate: certificateFromPEM(t, currentOrchestrator.CertificatePEM()), wantStatus: http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/engines/managed", nil)
			if test.certificate != nil {
				request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{test.certificate}}
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("managed engine status returned %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
		})
	}

	request := httptest.NewRequest(http.MethodPost, "/api/engines/managed/missing/start", strings.NewReader(`{}`))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificateFromPEM(t, currentOrchestrator.CertificatePEM())}}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown managed engine start returned %d, want 404: %s", response.Code, response.Body.String())
	}
}

func mustIdentity(t *testing.T, id string) *peeridentity.Identity {
	t.Helper()
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), id+".json"), id)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func certificateFromPEM(t *testing.T, certificatePEM string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(certificatePEM))
	if block == nil {
		t.Fatal("identity certificate is not PEM encoded")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}
