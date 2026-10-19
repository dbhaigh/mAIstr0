package main

import (
	"testing"

	"github.com/maistr0/maistr0/internal/discovery"
)

func TestFailoverElectionIgnoresFailedAndUntrustedPeers(t *testing.T) {
	candidate := discovery.Announcement{Role: "candidate", ID: "node-local", PeerAddr: "https://local:7450", Score: 10}
	peers := []discovery.Peer{
		{Announcement: discovery.Announcement{Role: "orchestrator", ID: "failed-leader", PeerAddr: "https://failed:7450", Score: 100}},
		{Announcement: discovery.Announcement{Role: "candidate", ID: "untrusted-candidate", PeerAddr: "https://untrusted:7450", Score: 90}},
	}
	winner := failoverElectionWinner(candidate, peers, "failed-leader", func(id string) bool {
		return id != "untrusted-candidate"
	})
	if winner.ID != candidate.ID {
		t.Fatalf("winner = %q, want local candidate %q", winner.ID, candidate.ID)
	}
}

func TestFailoverElectionPrefersTrustedActiveCoordinator(t *testing.T) {
	candidate := discovery.Announcement{Role: "candidate", ID: "node-local", PeerAddr: "https://local:7450", Score: 100}
	active := discovery.Peer{Announcement: discovery.Announcement{
		Role: "orchestrator", ID: "trusted-leader", PeerAddr: "https://leader:7450", Score: 1,
	}}
	winner := failoverElectionWinner(candidate, []discovery.Peer{active}, "failed-leader", func(string) bool {
		return true
	})
	if winner.ID != "trusted-leader" {
		t.Fatalf("winner = %q, want existing trusted orchestrator", winner.ID)
	}
}
