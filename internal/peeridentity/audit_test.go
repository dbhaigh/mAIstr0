package peeridentity

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestTrustBoundaryAuditLogsOmitSecrets(t *testing.T) {
	previous := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	identity := tcOpen(t, "audit-owner")
	peer := tcOpen(t, "audit-peer")
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	if !identity.VerifyPairingPIN(pin) {
		t.Fatal("generated one-time PIN did not verify")
	}
	if err := identity.TrustPeer(peer.ID(), peer.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if !identity.ConsumePairingPIN(pin) {
		t.Fatal("generated one-time PIN was not consumed")
	}
	if err := identity.RemovePeer(peer.ID()); err != nil {
		t.Fatal(err)
	}

	content := logs.String()
	for _, event := range []string{
		"identity.created",
		"pairing.pin_generation",
		"pairing.pin_verification",
		"peer.trust_update",
		"pairing.pin_consumption",
		"peer.trust_revoke",
	} {
		if !strings.Contains(content, `"event":"`+event+`"`) {
			t.Errorf("audit event %q missing from logs", event)
		}
	}
	if strings.Contains(content, `"pin":`) || strings.Contains(content, `"private_key":`) ||
		strings.Contains(content, identity.privateKeyPEM) {
		t.Fatal("audit log exposed a pairing PIN or private key")
	}
}
