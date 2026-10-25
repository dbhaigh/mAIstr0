package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestUseIdentityPreservesPromotedNodeRoster(t *testing.T) {
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	cfg.Normalize()
	server, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})

	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.identity"), "promoted-node")
	if err != nil {
		t.Fatal(err)
	}
	member, err := peeridentity.Open(filepath.Join(t.TempDir(), "member.identity"), "paired-member")
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.TrustPeerDetails(member.ID(), member.CertificatePEM(), "node", ""); err != nil {
		t.Fatal(err)
	}
	if err := server.UseIdentity(identity); err != nil {
		t.Fatal(err)
	}

	if server.SelfID() != identity.ID() {
		t.Fatalf("server identity = %q, want promoted node ID %q", server.SelfID(), identity.ID())
	}
	if !server.identity.IsTrustedID(member.ID()) {
		t.Fatal("promoted orchestrator lost the existing member trust roster")
	}
	if _, _, active := server.identity.ActivePairingPIN(); !active {
		t.Fatal("promoted orchestrator did not create a pairing PIN for new members")
	}
}
