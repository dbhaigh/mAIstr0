package discovery

import (
	"testing"
	"time"
)

func TestBestOrchestratorCandidatePrefersCapabilityThenStableID(t *testing.T) {
	self := Announcement{Role: "candidate", ID: "orchestrator-c", Score: 10}
	peers := []Peer{
		{Announcement: Announcement{Role: "candidate", ID: "orchestrator-b", Score: 12}},
		{Announcement: Announcement{Role: "orchestrator", ID: "orchestrator-a", Score: 12}},
		{Announcement: Announcement{Role: "node", ID: "high-scoring-node", Score: 100}},
	}

	winner := BestOrchestratorCandidate(self, peers)
	if winner.ID != "orchestrator-a" || winner.Role != "orchestrator" {
		t.Fatalf("winner = %#v, want orchestrator-a on the capability tie", winner)
	}
	if IsBestOrchestratorCandidate(self, peers) {
		t.Fatal("lower-ranked candidate was elected")
	}
}

func TestBestOrchestratorCandidateElectsSelfWhenAlone(t *testing.T) {
	self := Announcement{Role: "candidate", ID: "orchestrator-a", Score: 1}
	if !IsBestOrchestratorCandidate(self, nil) {
		t.Fatal("the only candidate should win")
	}
}

func TestElectedOrchestratorDoesNotPreemptActiveLeader(t *testing.T) {
	self := Announcement{Role: "orchestrator", ID: "orchestrator-old", Score: 10}
	challenger := Peer{Announcement: Announcement{Role: "candidate", ID: "orchestrator-new", Score: 20}}
	if !IsElectedOrchestrator(self, []Peer{challenger}) {
		t.Fatal("current startup winner yielded while a challenger was still preparing")
	}
	challenger.Role = "orchestrator"
	if IsElectedOrchestrator(self, []Peer{challenger}) {
		t.Fatal("lower-capability active orchestrator remained elected")
	}
	if !IsElectedOrchestrator(challenger.Announcement, []Peer{{Announcement: self}}) {
		t.Fatal("higher-capability active orchestrator did not win")
	}
}

func TestListenerWaitReady(t *testing.T) {
	listener := Listen("discovery-readiness-test")
	defer listener.Close()
	if err := listener.WaitReady(3 * time.Second); err != nil {
		t.Fatal(err)
	}
}
