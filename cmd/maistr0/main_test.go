package main

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/discovery"
)

func TestStartupElectionWindowCoversDiscoveryLease(t *testing.T) {
	if startupElectionWindow < 15*time.Second {
		t.Fatalf("startup election window = %s, want at least the 15-second discovery lease", startupElectionWindow)
	}
}

func TestStartupOrchestratorWinnerPrefersActiveCoordinatorInAutoMode(t *testing.T) {
	candidate := discovery.Announcement{
		Role: "candidate", ID: "new-node", PeerAddr: "https://new-node:7450", Score: 100,
	}
	peers := []discovery.Peer{
		{Announcement: discovery.Announcement{
			Role: "candidate", ID: "another-starting-node", PeerAddr: "https://other:7450", Score: 200,
		}},
		{Announcement: discovery.Announcement{
			Role: "orchestrator", ID: "existing-coordinator", PeerAddr: "https://leader:7450", Score: 10,
		}},
	}

	winner := startupOrchestratorWinner(candidate, peers, true)
	if winner.ID != "existing-coordinator" || winner.PeerAddr != "https://leader:7450" {
		t.Fatalf("automatic startup winner = %#v, want existing coordinator", winner)
	}
}

func TestStartupOrchestratorWinnerElectsAmongCandidatesWhenNoCoordinatorIsActive(t *testing.T) {
	candidate := discovery.Announcement{
		Role: "candidate", ID: "new-node", PeerAddr: "https://new-node:7450", Score: 100,
	}
	peers := []discovery.Peer{{
		Announcement: discovery.Announcement{
			Role: "candidate", ID: "another-starting-node", PeerAddr: "https://other:7450", Score: 200,
		},
	}}

	winner := startupOrchestratorWinner(candidate, peers, true)
	if winner.ID != "another-starting-node" {
		t.Fatalf("automatic startup winner = %#v, want highest-ranked starting candidate", winner)
	}
}

func TestExplicitOrchestratorStartupCanStillChallengeActiveCoordinator(t *testing.T) {
	candidate := discovery.Announcement{
		Role: "candidate", ID: "new-node", PeerAddr: "https://new-node:7450", Score: 100,
	}
	peers := []discovery.Peer{{
		Announcement: discovery.Announcement{
			Role: "orchestrator", ID: "existing-coordinator", PeerAddr: "https://leader:7450", Score: 10,
		},
	}}

	winner := startupOrchestratorWinner(candidate, peers, false)
	if winner.ID != "new-node" {
		t.Fatalf("explicit orchestrator startup winner = %#v, want new-node", winner)
	}
}
