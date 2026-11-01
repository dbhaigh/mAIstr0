package discovery

import (
	"testing"
	"time"
)

func TestAuthenticatedAnnouncementsRejectForgedAndReplayedPackets(t *testing.T) {
	now := time.Now()
	secret := []byte("test-only-cluster-secret-32-bytes")
	wrongSecret := []byte("different-test-cluster-secret-32")
	listener := &Listener{
		authKey:  secret,
		peers:    make(map[string]Peer),
		excluded: make(map[string]bool),
		lastSeen: make(map[string]int64),
	}
	announcement := Announcement{Role: "node", ID: "authenticated-peer", PeerAddr: "https://node.invalid"}

	forged := announcement
	if listener.acceptAnnouncement(forged, now) {
		t.Fatal("unsigned announcement was accepted with authentication enabled")
	}
	wrongKey, err := signAnnouncement(announcement, wrongSecret, now)
	if err != nil {
		t.Fatal(err)
	}
	if listener.acceptAnnouncement(wrongKey, now) {
		t.Fatal("announcement signed with another cluster key was accepted")
	}

	signed, err := signAnnouncement(announcement, secret, now)
	if err != nil {
		t.Fatal(err)
	}
	if !listener.acceptAnnouncement(signed, now) {
		t.Fatal("valid authenticated announcement was rejected")
	}
	if listener.acceptAnnouncement(signed, now.Add(time.Second)) {
		t.Fatal("replayed announcement was accepted")
	}

	tampered := signed
	tampered.PeerAddr = "https://forged.invalid"
	if listener.acceptAnnouncement(tampered, now.Add(2*time.Second)) {
		t.Fatal("tampered announcement was accepted")
	}

	stale, err := signAnnouncement(Announcement{Role: "node", ID: "stale-peer"}, secret, now.Add(-announcementMaxClockSkew-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if listener.acceptAnnouncement(stale, now) {
		t.Fatal("stale replay was accepted")
	}
}

func TestAuthenticatedAnnouncementsRejectOlderFreshReplay(t *testing.T) {
	now := time.Now()
	secret := []byte("test-only-cluster-secret-32-bytes")
	listener := &Listener{
		authKey:  secret,
		peers:    make(map[string]Peer),
		excluded: make(map[string]bool),
		lastSeen: make(map[string]int64),
	}

	current, err := signAnnouncement(Announcement{
		Role: "node", ID: "replayed-peer", PeerAddr: "https://current.invalid",
	}, secret, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !listener.acceptAnnouncement(current, now.Add(time.Second)) {
		t.Fatal("current authenticated announcement was rejected")
	}

	older, err := signAnnouncement(Announcement{
		Role: "node", ID: "replayed-peer", PeerAddr: "https://replayed.invalid",
	}, secret, now.Add(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if listener.acceptAnnouncement(older, now.Add(time.Second)) {
		t.Fatal("older but still fresh authenticated announcement was accepted")
	}
	peers := listener.Snapshot()
	if len(peers) != 1 || peers[0].PeerAddr != "https://current.invalid" {
		t.Fatalf("replayed packet replaced the current peer announcement: %#v", peers)
	}
}

func TestUnauthenticatedAnnouncementCompatibilityRemainsEnabledByDefault(t *testing.T) {
	now := time.Now()
	listener := &Listener{
		peers:    make(map[string]Peer),
		excluded: make(map[string]bool),
	}
	announcement := Announcement{Role: "node", ID: "legacy-peer"}
	if !listener.acceptAnnouncement(announcement, now) {
		t.Fatal("legacy announcement was rejected without a configured key")
	}
	if peers := listener.Snapshot(); len(peers) != 1 || peers[0].ID != announcement.ID {
		t.Fatalf("legacy announcement not visible: %#v", peers)
	}
}
