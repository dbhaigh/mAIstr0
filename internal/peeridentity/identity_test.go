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
