package peeridentity

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestPairingPINIsSingleUse(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "identity.json"), "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	if !identity.ConsumePairingPIN(pin) {
		t.Fatal("valid pairing PIN was rejected")
	}
	if identity.ConsumePairingPIN(pin) {
		t.Fatal("pairing PIN was accepted more than once")
	}
}

func TestPairingPINLimitsFailedAttempts(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "identity.json"), "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 20; attempt++ {
		if identity.ConsumePairingPIN("000000") {
			t.Fatal("incorrect pairing PIN was accepted")
		}
	}
	if identity.ConsumePairingPIN(pin) {
		t.Fatal("pairing PIN remained active after too many failed attempts")
	}
}

func TestPairingPersistsAndChecksCertificatePins(t *testing.T) {
	one, err := Open(filepath.Join(t.TempDir(), "one.json"), "node-one")
	if err != nil {
		t.Fatal(err)
	}
	two, err := Open(filepath.Join(t.TempDir(), "two.json"), "node-two")
	if err != nil {
		t.Fatal(err)
	}
	if err := one.TrustPeer(two.ID(), two.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if !one.IsTrustedID(two.ID()) {
		t.Fatal("paired peer was not added to the trust roster")
	}

	reloaded, err := Open(one.path, "node-one")
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.IsTrustedID(two.ID()) {
		t.Fatal("paired peer trust did not persist")
	}
	if err := reloaded.TrustPeer(two.ID(), one.CertificatePEM()); err == nil {
		t.Fatal("certificate with a mismatched identity was accepted")
	}
}

func TestRemovePairedOrchestratorsKeepsNodeTrust(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "identity.json"), "cluster-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := Open(filepath.Join(t.TempDir(), "paired-orchestrator.json"), "paired-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	node, err := Open(filepath.Join(t.TempDir(), "node.json"), "cluster-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.TrustPeerDetails(orchestrator.ID(), orchestrator.CertificatePEM(), "orchestrator", "https://192.0.2.10:7450"); err != nil {
		t.Fatal(err)
	}
	if err := identity.TrustPeerDetails(node.ID(), node.CertificatePEM(), "node", ""); err != nil {
		t.Fatal(err)
	}

	if err := identity.RemovePairedOrchestrators(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(identity.path, identity.ID())
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.IsTrustedID(orchestrator.ID()) {
		t.Fatal("orchestrator federation pin survived cleanup")
	}
	if !reloaded.IsTrustedID(node.ID()) {
		t.Fatal("ordinary node pin was removed")
	}
}

func TestTrustedHandlerRequiresPairedTLSClient(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "identity.json"), "node")
	if err != nil {
		t.Fatal(err)
	}
	handler := identity.RequireTrustedPeer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("unpaired plaintext request returned %d, want forbidden", recorder.Code)
	}
}

func TestBootstrapTransportForIDChecksDiscoveredPeerIdentity(t *testing.T) {
	serverIdentity, err := Open(filepath.Join(t.TempDir(), "server.json"), "discovered-node")
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity, err := Open(filepath.Join(t.TempDir(), "client.json"), "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverIdentity.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	client := &http.Client{Transport: clientIdentity.BootstrapTransportForID(serverIdentity.ID())}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("request to the expected discovered peer failed: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("expected peer returned %d", response.StatusCode)
	}

	client = &http.Client{Transport: clientIdentity.BootstrapTransportForID("different-node")}
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("bootstrap request succeeded against a peer with a different identity")
	}
}
