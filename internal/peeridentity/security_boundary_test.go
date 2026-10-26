package peeridentity

import (
	"crypto/tls"
	"crypto/x509"
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
