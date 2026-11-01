package peeridentity

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPairingPINSharedExpirationBoundary(t *testing.T) {
	expires := time.Unix(100, 0)
	for _, test := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"before expiration", expires.Add(-time.Nanosecond), true},
		{"exact expiration", expires, false},
		{"after expiration", expires.Add(time.Nanosecond), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity := &Identity{pairingPIN: "123456", pairingExpires: expires}
			identity.mu.RLock()
			active := identity.pairingPINActiveLocked(test.at)
			identity.mu.RUnlock()
			if active != test.want {
				t.Fatalf("PIN activity at %v = %t, want %t", test.at, active, test.want)
			}
		})
	}
}

func TestBootstrapRequiresVerifiedFingerprintOrExistingPin(t *testing.T) {
	serverIdentity := tcOpen(t, "bootstrap-server")
	fingerprint, err := CertificateFingerprint(serverIdentity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	server, reached := tcStartTLSServer(t, serverIdentity.ServerTLSConfig(), http.HandlerFunc(tcNoContent))
	for _, tt := range []struct {
		name, fingerprint string
		pinned, allowed   bool
	}{
		{name: "missing trust anchor"},
		{name: "wrong fingerprint", fingerprint: strings.Repeat("0", 64)},
		{name: "malformed fingerprint", fingerprint: "not-a-fingerprint"},
		{name: "verified fingerprint", fingerprint: fingerprint, allowed: true},
		{name: "persisted pin", pinned: true, allowed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clientIdentity := tcOpen(t, "bootstrap-client")
			if tt.pinned {
				if err := clientIdentity.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
					t.Fatal(err)
				}
			}
			transport := clientIdentity.BootstrapTransportForFingerprint(serverIdentity.ID(), tt.fingerprint)
			t.Cleanup(transport.CloseIdleConnections)
			before := reached.Load()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			response, err := client.Get(server.URL)
			if err == nil {
				_ = response.Body.Close()
			}
			if (err == nil) != tt.allowed {
				t.Fatalf("bootstrap allowed=%v error=%v, want %v", err == nil, err, tt.allowed)
			}
			if !tt.allowed && reached.Load() != before {
				t.Fatal("unauthenticated bootstrap reached the HTTP handler")
			}
		})
	}
}

func TestValidateFingerprintRequiresExactly64HexCharacters(t *testing.T) {
	identity := tcOpen(t, "fingerprint-format")
	fingerprint, err := CertificateFingerprint(identity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	if len(fingerprint) != 64 {
		t.Fatalf("SHA-256 certificate fingerprint length = %d, want 64", len(fingerprint))
	}
	if err := ValidateFingerprint(fingerprint); err != nil {
		t.Fatalf("valid certificate fingerprint rejected: %v", err)
	}
	if err := ValidateFingerprint(strings.ToUpper(fingerprint)); err != nil {
		t.Fatalf("uppercase hexadecimal fingerprint rejected: %v", err)
	}

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "short", value: fingerprint[:63]},
		{name: "long", value: fingerprint + "0"},
		{name: "non-hexadecimal", value: strings.Repeat("g", 64)},
		{name: "leading whitespace", value: " " + fingerprint},
		{name: "trailing whitespace", value: fingerprint + " "},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateFingerprint(test.value); err == nil {
				t.Fatal("invalid fingerprint was accepted")
			}
		})
	}
}

func TestBootstrapFingerprintRejectsDifferentCertificateWithSameID(t *testing.T) {
	serverIdentity := tcOpen(t, "same-bootstrap-id")
	otherCertificate := tcOpen(t, "same-bootstrap-id")
	expectedFingerprint, err := CertificateFingerprint(serverIdentity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	wrongFingerprint, err := CertificateFingerprint(otherCertificate.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	if expectedFingerprint == wrongFingerprint {
		t.Fatal("test certificates unexpectedly have the same fingerprint")
	}

	server, reached := tcStartTLSServer(t, serverIdentity.ServerTLSConfig(), http.HandlerFunc(tcNoContent))
	defer server.Close()
	clientIdentity := tcOpen(t, "bootstrap-fingerprint-client")
	transport := clientIdentity.BootstrapTransportForFingerprint(serverIdentity.ID(), wrongFingerprint)
	t.Cleanup(transport.CloseIdleConnections)
	before := reached.Load()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	response, err := client.Get(server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("a different certificate with the expected identity was accepted")
	}
	if reached.Load() != before {
		t.Fatal("certificate fingerprint mismatch reached the HTTP handler")
	}
}

func TestCertificateFingerprintGenerationIsIdempotent(t *testing.T) {
	identity := tcOpen(t, "idempotent-fingerprint")
	expected, err := CertificateFingerprint(identity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	for _, certificatePEM := range []string{
		identity.CertificatePEM(),
		"\n" + identity.CertificatePEM() + "\n",
	} {
		actual, err := CertificateFingerprint(certificatePEM)
		if err != nil {
			t.Fatal(err)
		}
		if actual != expected {
			t.Fatal("the same DER certificate produced different fingerprints")
		}
	}
}

func TestTrustedCertificateFingerprintComparisonIsCaseInsensitive(t *testing.T) {
	identity := tcOpen(t, "case-insensitive-owner")
	peer := tcOpen(t, "case-insensitive-peer")
	if err := identity.TrustPeer(peer.ID(), peer.CertificatePEM()); err != nil {
		t.Fatal(err)
	}

	identity.mu.Lock()
	trusted := identity.trusted[peer.ID()]
	trusted.Fingerprint = strings.ToUpper(trusted.Fingerprint)
	identity.trusted[peer.ID()] = trusted
	identity.mu.Unlock()

	if !identity.IsTrustedCertificate(peer.certificate.Leaf) {
		t.Fatal("uppercase stored fingerprint did not match the same certificate")
	}
	local, remote := net.Pipe()
	defer remote.Close()
	tracked, err := identity.TrackPinnedPeerConnection(peer.certificate.Leaf, local)
	if err != nil {
		t.Fatalf("uppercase stored fingerprint rejected a pinned connection: %v", err)
	}
	trackedIdentityConn := tracked.(*identityConn)
	if err := identity.TrustPeerDetails(peer.ID(), peer.CertificatePEM(), "", ""); err != nil {
		t.Fatal(err)
	}
	identity.mu.RLock()
	_, stillTracked := identity.activeConns[trackedIdentityConn]
	identity.mu.RUnlock()
	if !stillTracked {
		t.Fatal("case-only fingerprint difference invalidated the active peer connection")
	}
	if err := tracked.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPairingRecordMustMatchTLSKeyProof(t *testing.T) {
	identity := tcOpen(t, "peer")
	other := tcOpen(t, "peer")
	certificate, err := parseCertificate(identity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	state := &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
	if err := ValidatePeerConnection(identity.ID(), identity.CertificatePEM(), state); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePeerConnection(other.ID(), other.CertificatePEM(), state); err == nil {
		t.Fatal("a different certificate with the same name bypassed TLS key proof")
	}
	if err := ValidatePeerConnection(identity.ID(), identity.CertificatePEM(), &tls.ConnectionState{}); err == nil {
		t.Fatal("enrollment without a client certificate was accepted")
	}
}

func TestIdentityReopenRestrictsExistingPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits do not enforce Windows ACLs")
	}
	identity := tcOpen(t, "permissions")
	if err := os.Chmod(identity.path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(identity.path, identity.ID()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(identity.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity permissions = %o, want 600", info.Mode().Perm())
	}
}
