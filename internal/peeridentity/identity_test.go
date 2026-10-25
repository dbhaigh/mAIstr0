package peeridentity

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestOpenRotatesExpiredCertificateAndPreservesTrust(t *testing.T) {
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
		ID: identity.ID(), Certificate: expiredPEM,
		PrivateKey: identity.privateKeyPEM, Trusted: identity.trusted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	rotated, err := Open(path, identity.ID())
	if err != nil {
		t.Fatal(err)
	}
	newCert := rotated.certificate.Leaf
	if newCert == nil || !certificateCurrentlyValid(newCert, time.Now()) {
		t.Fatal("rotated identity certificate is not currently valid")
	}
	if certificateFingerprint(newCert) == certificateFingerprint(expiredCert) {
		t.Fatal("expired certificate fingerprint was retained after rotation")
	}
	if !rotated.IsTrustedID(peer.ID()) {
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
	if saved.Certificate != rotated.CertificatePEM() {
		t.Fatal("rotated certificate was not persisted")
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
	response.Body.Close()

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
		response.Body.Close()
		t.Fatal("peer accepted the rotated certificate without updating its pin")
	}

	if err := clientIdentity.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	response, err = client.Get(server.URL)
	if err != nil {
		t.Fatalf("request failed after the peer pin was repaired: %v", err)
	}
	response.Body.Close()
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
	response.Body.Close()
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
		response.Body.Close()
		t.Fatal("request succeeded against server certificate rejected by the replacement pin")
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
