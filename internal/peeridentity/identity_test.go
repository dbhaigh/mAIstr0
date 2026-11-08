package peeridentity

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/fileguard"
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

func TestPairingPINExpires(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "identity.json"), "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	identity.mu.Lock()
	identity.pairingExpires = time.Now().Add(-time.Second)
	identity.mu.Unlock()

	if identity.VerifyPairingPIN(pin) || identity.ConsumePairingPIN(pin) {
		t.Fatal("expired pairing PIN was accepted")
	}
	if _, _, ok := identity.ActivePairingPIN(); ok {
		t.Fatal("expired pairing PIN remained active")
	}
}

func TestReloadTrustedPeersReadsSharedControllerRoster(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-identity.json")
	controllerIdentity, err := Open(path, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	nodeIdentity, err := Open(path, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := nodeIdentity.TrustPeerDetails(orchestrator.ID(), orchestrator.CertificatePEM(), "orchestrator", "https://orchestrator.example:7450"); err != nil {
		t.Fatal(err)
	}
	if controllerIdentity.IsTrustedOrchestratorID(orchestrator.ID()) {
		t.Fatal("controller identity unexpectedly observed the other process's in-memory roster")
	}
	if err := controllerIdentity.ReloadTrustedPeers(); err != nil {
		t.Fatal(err)
	}
	if !controllerIdentity.IsTrustedOrchestratorID(orchestrator.ID()) {
		t.Fatal("controller identity did not reload the shared orchestrator trust roster")
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

func TestExpiredPeerCertificateIsRejected(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "identity.json"), "local")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := Open(filepath.Join(t.TempDir(), "peer.json"), "expired-peer")
	if err != nil {
		t.Fatal(err)
	}
	expiredPEM := expiredCertificatePEM(t, peer)
	expiredCert, err := parseCertificate(expiredPEM)
	if err != nil {
		t.Fatal(err)
	}
	identity.mu.Lock()
	identity.trusted[peer.ID()] = trustedPeer{Fingerprint: certificateFingerprint(expiredCert)}
	identity.mu.Unlock()

	if identity.IsTrustedCertificate(expiredCert) {
		t.Fatal("expired pinned peer certificate was trusted")
	}
	if err := identity.TrustPeer(peer.ID(), expiredPEM); err == nil {
		t.Fatal("expired peer certificate was accepted for pairing")
	}
}

func TestOpenRegeneratesCertificateAndPreservesTrust(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	identity, err := Open(path, "rotating-node")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := Open(filepath.Join(t.TempDir(), "peer.json"), "paired-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.TrustPeer(peer.ID(), peer.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	expiredPEM := expiredCertificatePEM(t, identity)
	expiredCert, err := parseCertificate(expiredPEM)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(identityFile{
		ID: identity.ID(), Certificate: expiredPEM, Trusted: identity.trusted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, identity.ID())
	if err != nil {
		t.Fatal(err)
	}
	newCert := reopened.certificate.Leaf
	if newCert == nil || !certificateCurrentlyValid(newCert, time.Now()) {
		t.Fatal("regenerated identity certificate is not currently valid")
	}
	if certificateFingerprint(newCert) == certificateFingerprint(expiredCert) {
		t.Fatal("expired certificate fingerprint was retained after startup")
	}
	if !reopened.IsTrustedID(peer.ID()) {
		t.Fatal("certificate rotation discarded the trusted peer roster")
	}

	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved identityFile
	if err := json.Unmarshal(persisted, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Certificate != reopened.CertificatePEM() {
		t.Fatal("regenerated certificate was not persisted")
	}
}

func TestOpenDoesNotPersistPrivateKeyAndMigratesLegacyIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	identity, err := Open(path, "ephemeral-node")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := Open(filepath.Join(t.TempDir(), "peer.json"), "paired-node")
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.TrustPeer(peer.ID(), peer.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	oldCertificate := identity.CertificatePEM()
	oldPrivateKey := identity.privateKeyPEM

	legacy, err := json.Marshal(struct {
		ID          string                 `json:"id"`
		Certificate string                 `json:"certificate"`
		PrivateKey  string                 `json:"private_key"`
		Trusted     map[string]trustedPeer `json:"trusted"`
	}{
		ID: identity.ID(), Certificate: oldCertificate,
		PrivateKey: oldPrivateKey, Trusted: identity.trusted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, identity.ID())
	if err != nil {
		t.Fatal(err)
	}
	if reopened.ID() != identity.ID() {
		t.Fatalf("reopened identity ID = %q, want %q", reopened.ID(), identity.ID())
	}
	if reopened.CertificatePEM() == oldCertificate {
		t.Fatal("reopening reused the previous process's certificate")
	}
	if reopened.privateKeyPEM == oldPrivateKey {
		t.Fatal("reopening reused the previous process's private key")
	}
	if !reopened.IsTrustedID(peer.ID()) {
		t.Fatal("reopening discarded the trusted peer roster")
	}

	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(persisted, []byte(`"private_key"`)) ||
		bytes.Contains(persisted, []byte("PRIVATE KEY")) {
		t.Fatal("private-key data remained in the identity file after migration")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("identity file permissions = %o, want 600", info.Mode().Perm())
	}

	nextProcess, err := Open(path, identity.ID())
	if err != nil {
		t.Fatal(err)
	}
	if nextProcess.CertificatePEM() == reopened.CertificatePEM() {
		t.Fatal("a new process reused the previous process's certificate")
	}
	if !nextProcess.IsTrustedID(peer.ID()) {
		t.Fatal("a subsequent reopen discarded the trusted peer roster")
	}
}

func TestRuntimeCertificateRotationRequiresPeerRepair(t *testing.T) {
	serverIdentity, err := Open(filepath.Join(t.TempDir(), "server.json"), "rotating-server")
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity, err := Open(filepath.Join(t.TempDir(), "client.json"), "paired-client")
	if err != nil {
		t.Fatal(err)
	}
	if err := serverIdentity.TrustPeer(clientIdentity.ID(), clientIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := clientIdentity.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	oldCertificate := serverIdentity.CertificatePEM()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverIdentity.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	clientTransport := clientIdentity.ClientTransport()
	client := &http.Client{Timeout: 3 * time.Second, Transport: clientTransport}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()

	serverIdentity.mu.Lock()
	expiredLeaf := *serverIdentity.certificate.Leaf
	expiredLeaf.NotAfter = time.Now().Add(-time.Second)
	serverIdentity.certificate.Leaf = &expiredLeaf
	serverIdentity.mu.Unlock()
	if _, err := serverIdentity.currentIdentityCertificate(); err != nil {
		t.Fatal(err)
	}
	if serverIdentity.CertificatePEM() == oldCertificate {
		t.Fatal("runtime expiration did not rotate the identity certificate")
	}
	clientTransport.CloseIdleConnections()
	if response, err := client.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("peer accepted the rotated certificate without updating its pin")
	}

	if err := clientIdentity.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	response, err = client.Get(server.URL)
	if err != nil {
		t.Fatalf("request failed after the peer pin was repaired: %v", err)
	}
	_ = response.Body.Close()
}

func TestIdentityTLSRequiresTLS13(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "identity.json"), "tls13-node")
	if err != nil {
		t.Fatal(err)
	}
	if got := identity.ServerTLSConfig().MinVersion; got != tls.VersionTLS13 {
		t.Fatalf("server minimum TLS version = %d, want TLS 1.3", got)
	}
	transport := identity.ClientTransport()
	t.Cleanup(transport.CloseIdleConnections)
	if got := transport.TLSClientConfig.MinVersion; got != tls.VersionTLS13 {
		t.Fatalf("client minimum TLS version = %d, want TLS 1.3", got)
	}
	if got := identity.BootstrapTransport().TLSClientConfig.MinVersion; got != tls.VersionTLS13 {
		t.Fatalf("bootstrap minimum TLS version = %d, want TLS 1.3", got)
	}
}

func TestClientTransportPinChangeClosesPooledConnections(t *testing.T) {
	serverIdentity, err := Open(filepath.Join(t.TempDir(), "server.json"), "pinned-server")
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity, err := Open(filepath.Join(t.TempDir(), "client.json"), "pinned-client")
	if err != nil {
		t.Fatal(err)
	}
	if err := serverIdentity.TrustPeer(clientIdentity.ID(), clientIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := clientIdentity.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverIdentity.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	transport := clientIdentity.ClientTransport()
	client := &http.Client{Timeout: 3 * time.Second, Transport: transport}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("initial pinned request returned %d", response.StatusCode)
	}
	clientIdentity.mu.RLock()
	pooledConnections := len(clientIdentity.activeConns)
	clientIdentity.mu.RUnlock()
	if pooledConnections == 0 {
		t.Fatal("initial request did not create a tracked connection")
	}

	replacement, err := Open(filepath.Join(t.TempDir(), "replacement.json"), serverIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := clientIdentity.TrustPeer(serverIdentity.ID(), replacement.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	clientIdentity.mu.RLock()
	pooledConnections = len(clientIdentity.activeConns)
	clientIdentity.mu.RUnlock()
	if pooledConnections != 0 {
		t.Fatalf("pin change left %d tracked connections open", pooledConnections)
	}
	if response, err := client.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("request succeeded against server certificate rejected by the replacement pin")
	}
}

func TestRejoinTicketIsNodeBoundDurableAndSingleUse(t *testing.T) {
	dir := t.TempDir()
	orchestratorPath := filepath.Join(dir, "orchestrator.json")
	nodePath := filepath.Join(dir, "node.json")
	orchestrator, err := Open(orchestratorPath, "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	node, err := Open(nodePath, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	otherNode, err := Open(filepath.Join(dir, "other-node.json"), "node-b")
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range []*Identity{node, otherNode} {
		if err := orchestrator.TrustPeerDetails(peer.ID(), peer.CertificatePEM(), "node", ""); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()
	token, expiresAt, err := orchestrator.IssueRejoinTicket(node.ID(), now, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !expiresAt.After(now) || expiresAt.After(now.Add(5*time.Minute)) {
		t.Fatalf("ticket expiry = %s, want within five minutes after %s", expiresAt, now)
	}
	persisted, err := os.ReadFile(orchestratorPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(persisted, []byte(token)) {
		t.Fatal("orchestrator persisted the raw rejoin bearer token")
	}
	var saved identityFile
	if err := json.Unmarshal(persisted, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.RejoinTickets) != 1 {
		t.Fatalf("persisted ticket count = %d, want one", len(saved.RejoinTickets))
	}

	reopenedOrchestrator, err := Open(orchestratorPath, orchestrator.ID())
	if err != nil {
		t.Fatal(err)
	}
	restartedNode, err := Open(nodePath, node.ID())
	if err != nil {
		t.Fatal(err)
	}
	if restartedNode.CertificatePEM() == node.CertificatePEM() {
		t.Fatal("restarted node reused its previous certificate")
	}
	if err := reopenedOrchestrator.ConsumeRejoinTicketAndTrust(
		otherNode.ID(), token, otherNode.CertificatePEM(), "", time.Now(),
	); !errors.Is(err, ErrRejoinTicketWrongNode) {
		t.Fatalf("wrong-node ticket result = %v, want ErrRejoinTicketWrongNode", err)
	}

	if err := reopenedOrchestrator.ConsumeRejoinTicketAndTrust(
		node.ID(), token, restartedNode.CertificatePEM(), "https://node-a.example:7451", time.Now(),
	); err != nil {
		t.Fatalf("consume valid ticket after restart: %v", err)
	}
	newCertificate, err := parseCertificate(restartedNode.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	if !reopenedOrchestrator.IsTrustedCertificate(newCertificate) {
		t.Fatal("orchestrator did not trust the restarted node certificate")
	}
	oldCertificate, err := parseCertificate(node.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	if reopenedOrchestrator.IsTrustedCertificate(oldCertificate) {
		t.Fatal("orchestrator retained the node's previous certificate pin")
	}
	if err := reopenedOrchestrator.ConsumeRejoinTicketAndTrust(
		node.ID(), token, restartedNode.CertificatePEM(), "", time.Now(),
	); !errors.Is(err, ErrRejoinTicketInvalid) {
		t.Fatalf("reused ticket result = %v, want ErrRejoinTicketInvalid", err)
	}
}

func TestRejoinTicketExpiresAndCannotBeReused(t *testing.T) {
	orchestrator, err := Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	node, err := Open(filepath.Join(t.TempDir(), "node.json"), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := orchestrator.TrustPeerDetails(node.ID(), node.CertificatePEM(), "node", ""); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	token, _, err := orchestrator.IssueRejoinTicket(node.ID(), now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := orchestrator.ConsumeRejoinTicketAndTrust(
		node.ID(), token, node.CertificatePEM(), "", now.Add(2*time.Second),
	); !errors.Is(err, ErrRejoinTicketExpired) {
		t.Fatalf("expired ticket result = %v, want ErrRejoinTicketExpired", err)
	}
	if err := orchestrator.ConsumeRejoinTicketAndTrust(
		node.ID(), token, node.CertificatePEM(), "", now.Add(2*time.Second),
	); !errors.Is(err, ErrRejoinTicketInvalid) {
		t.Fatalf("reused expired ticket result = %v, want ErrRejoinTicketInvalid", err)
	}
}

func TestPendingRejoinTicketSurvivesRestartInProtectedSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.json")
	node, err := Open(path, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	token := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	ticket := PendingRejoinTicket{
		NodeID: node.ID(), OrchestratorID: "orchestrator",
		Token: token, ExpiresAt: time.Now().Add(5 * time.Minute),
	}
	if err := node.StorePendingRejoinTicket(ticket); err != nil {
		t.Fatal(err)
	}
	cachePath, err := node.pendingRejoinPath()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		broad, err := fileguard.HasBroadACL(cachePath)
		if err != nil {
			t.Fatal(err)
		}
		if broad {
			t.Fatal("pending ticket sidecar grants access to a broad Windows principal")
		}
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("pending ticket sidecar permissions = %o, want 600", info.Mode().Perm())
	}

	restarted, err := Open(path, node.ID())
	if err != nil {
		t.Fatal(err)
	}
	got, exists, err := restarted.PendingRejoinTicket("orchestrator", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !exists || got.NodeID != ticket.NodeID || got.OrchestratorID != ticket.OrchestratorID ||
		got.Token != ticket.Token || !got.ExpiresAt.Equal(ticket.ExpiresAt) {
		t.Fatalf("reopened pending ticket = %+v, exists %t; want %+v", got, exists, ticket)
	}
	if _, exists, err := restarted.PendingRejoinTicket("different-orchestrator", time.Now()); err != nil || exists {
		t.Fatalf("ticket for another orchestrator = exists %t, error %v; want false, nil", exists, err)
	}
	if err := restarted.ClearPendingRejoinTicket("orchestrator", "wrong-token"); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := restarted.PendingRejoinTicket("orchestrator", time.Now()); err != nil || !exists {
		t.Fatalf("mismatched clear removed pending ticket: exists %t, error %v", exists, err)
	}
	if err := restarted.ClearPendingRejoinTicket("orchestrator", token); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := restarted.PendingRejoinTicket("orchestrator", time.Now()); err != nil || exists {
		t.Fatalf("cleared pending ticket = exists %t, error %v; want false, nil", exists, err)
	}
}

func TestPendingRejoinTicketExpiryIsReportedAndRemoved(t *testing.T) {
	identity, err := Open(filepath.Join(t.TempDir(), "node.json"), "node-expired-ticket")
	if err != nil {
		t.Fatal(err)
	}
	ticket := PendingRejoinTicket{
		NodeID: identity.ID(), OrchestratorID: "orchestrator",
		Token:     "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		ExpiresAt: time.Now().Add(time.Minute),
	}
	if err := identity.StorePendingRejoinTicket(ticket); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := identity.PendingRejoinTicket("orchestrator", time.Now().Add(2*time.Minute)); exists ||
		!errors.Is(err, ErrPendingRejoinTicketExpired) {
		t.Fatalf("expired ticket result = exists %t, error %v; want false and ErrPendingRejoinTicketExpired", exists, err)
	}
}

func expiredCertificatePEM(t *testing.T, identity *Identity) string {
	t.Helper()
	key, ok := identity.certificate.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("identity private key has type %T, want *ecdsa.PrivateKey", identity.certificate.PrivateKey)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: identity.ID()},
		NotBefore:             now.Add(-2 * time.Hour),
		NotAfter:              now.Add(-time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
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

func TestTrustPeerRefreshPreservesDetails(t *testing.T) {
	methods := []struct {
		name string
		pin  func(*Identity, string, string) error
	}{
		{"TrustPeer", (*Identity).TrustPeer},
		{"TrustPeerDetails", func(identity *Identity, id, certificate string) error {
			return identity.TrustPeerDetails(id, certificate, "", "")
		}},
	}
	for _, method := range methods {
		for _, failWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failWrite=%t", method.name, failWrite), func(t *testing.T) {
				identity := tcOpen(t, "owner")
				peer := tcOpen(t, "peer")
				replacement := tcOpen(t, peer.ID())
				const address = "https://192.0.2.10:7450"
				if err := identity.TrustPeerDetails(peer.ID(), peer.CertificatePEM(), "orchestrator", address); err != nil {
					t.Fatal(err)
				}
				previous := identity.trusted[peer.ID()]
				if err := method.pin(identity, peer.ID(), identity.CertificatePEM()); err == nil {
					t.Fatal("accepted certificate with a mismatched identity")
				}
				if identity.trusted[peer.ID()] != previous {
					t.Fatal("invalid refresh changed the trusted peer")
				}
				path := identity.path
				if failWrite {
					identity.path = t.TempDir()
				}
				err := method.pin(identity, peer.ID(), replacement.CertificatePEM())
				if (err != nil) != failWrite {
					t.Fatalf("refresh error = %v, want failure = %t", err, failWrite)
				}
				want := previous
				if !failWrite {
					cert, err := parseCertificate(replacement.CertificatePEM())
					if err != nil {
						t.Fatal(err)
					}
					want.Fingerprint = certificateFingerprint(cert)
					want.Certificate = replacement.CertificatePEM()
				}
				if identity.trusted[peer.ID()] != want {
					t.Fatal("refresh did not preserve metadata and the expected certificate")
				}
				reloaded, err := Open(path, identity.ID())
				if err != nil {
					t.Fatal(err)
				}
				if reloaded.trusted[peer.ID()] != want {
					t.Fatal("live and persisted trust differ after refresh")
				}
			})
		}
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

	fingerprint, err := CertificateFingerprint(serverIdentity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: clientIdentity.BootstrapTransportForFingerprint(serverIdentity.ID(), fingerprint)}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("request to the expected discovered peer failed: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("expected peer returned %d", response.StatusCode)
	}

	client = &http.Client{Transport: clientIdentity.BootstrapTransportForFingerprint("different-node", fingerprint)}
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("bootstrap request succeeded against a peer with a different identity")
	}
}

func TestBootstrapTransportForLANIDAllowsExpectedUntrustedIdentity(t *testing.T) {
	serverIdentity, err := Open(filepath.Join(t.TempDir(), "server.json"), "same-subnet-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	clientIdentity, err := Open(filepath.Join(t.TempDir(), "client.json"), "same-subnet-node")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverIdentity.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	client := &http.Client{Transport: clientIdentity.BootstrapTransportForLANID(serverIdentity.ID())}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("request to the expected discovered peer failed: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("expected peer returned %d", response.StatusCode)
	}

	client = &http.Client{Transport: clientIdentity.BootstrapTransportForLANID("different-node")}
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("LAN bootstrap accepted a certificate with the wrong identity")
	}
}

func TestClientTransportUsesPooledConnectionsAndHTTP2(t *testing.T) {
	transport := (&Identity{}).ClientTransport()
	t.Cleanup(transport.CloseIdleConnections)
	if transport.MaxIdleConns != 256 || transport.MaxIdleConnsPerHost != 64 {
		t.Fatalf("idle connection limits = %d/%d, want 256/64", transport.MaxIdleConns, transport.MaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != 90*time.Second {
		t.Fatalf("idle connection timeout = %s, want %s", transport.IdleConnTimeout, 90*time.Second)
	}
	if transport.DisableKeepAlives || !transport.ForceAttemptHTTP2 {
		t.Fatalf("keep-alives/HTTP2 = %t/%t, want enabled", !transport.DisableKeepAlives, transport.ForceAttemptHTTP2)
	}
}
