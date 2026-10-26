package main

import (
	"errors"
	"net/http"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
)

func failoverPeer(role, id string, score float64) discovery.Peer {
	return discovery.Peer{Announcement: discovery.Announcement{
		Role: role, ID: id, PeerAddr: "https://" + id + ":7450", Score: score,
	}}
}

func permuteFailoverPeers(peers []discovery.Peer) [][]discovery.Peer {
	if len(peers) <= 1 {
		return [][]discovery.Peer{append([]discovery.Peer(nil), peers...)}
	}
	var out [][]discovery.Peer
	for i := range peers {
		rest := append(append([]discovery.Peer(nil), peers[:i]...), peers[i+1:]...)
		for _, tail := range permuteFailoverPeers(rest) {
			out = append(out, append([]discovery.Peer{peers[i]}, tail...))
		}
	}
	return out
}

func TestFailoverReElectionCharacterization(t *testing.T) {
	self := discovery.Announcement{Role: "candidate", ID: "node-m", PeerAddr: "https://node-m:7450", Score: 50}
	tests := []struct {
		name      string
		peers     []discovery.Peer
		untrusted []string
		want      string
	}{
		{
			name: "next-best trusted candidate wins after the failed leader is excluded",
			peers: []discovery.Peer{
				failoverPeer("orchestrator", "failed", 1000),
				failoverPeer("candidate", "untrusted", 900),
				failoverPeer("candidate", "node-x", 80),
				failoverPeer("candidate", "node-y", 70),
			},
			untrusted: []string{"untrusted"},
			want:      "node-x",
		},
		{
			name:  "failed identity is excluded even when it re-announces as a candidate",
			peers: []discovery.Peer{failoverPeer("candidate", "failed", 1000), failoverPeer("candidate", "node-y", 10)},
			want:  "node-m",
		},
		{
			name:  "equal scores elect the lowest trusted ID",
			peers: []discovery.Peer{failoverPeer("candidate", "node-z", 50), failoverPeer("candidate", "node-b", 50), failoverPeer("candidate", "node-c", 50)},
			want:  "node-b",
		},
		{
			name: "untrusted active orchestrator does not suppress the election",
			peers: []discovery.Peer{
				failoverPeer("orchestrator", "rogue", 1),
				failoverPeer("candidate", "node-a", 50),
			},
			untrusted: []string{"rogue"},
			want:      "node-a",
		},
		{
			name: "trusted active orchestrator is adopted over stronger candidates",
			peers: []discovery.Peer{
				failoverPeer("candidate", "node-a", 900),
				failoverPeer("orchestrator", "node-q", 1),
				failoverPeer("orchestrator", "node-p", 1),
			},
			want: "node-p",
		},
		{
			name: "addressless and worker announcements cannot win",
			peers: []discovery.Peer{
				{Announcement: discovery.Announcement{Role: "candidate", ID: "node-a", Score: 900}},
				{Announcement: discovery.Announcement{Role: "orchestrator", ID: "node-b", Score: 900}},
				failoverPeer("node", "node-c", 900),
			},
			want: "node-m",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isTrusted := func(id string) bool {
				for _, untrusted := range tt.untrusted {
					if id == untrusted {
						return false
					}
				}
				return true
			}
			for _, order := range permuteFailoverPeers(tt.peers) {
				if got := failoverElectionWinner(self, order, "failed", isTrusted).ID; got != tt.want {
					t.Fatalf("winner = %q, want %q (order %v)", got, tt.want, order)
				}
			}
		})
	}
}

// TestFailoverPartitionsPromoteIndependently documents current behavior:
// failover is partition-local. Each side excludes the unreachable leader and
// elects its own best trusted member; nothing fences the other side.
func TestFailoverPartitionsPromoteIndependently(t *testing.T) {
	trusted := func(string) bool { return true }
	a1 := discovery.Announcement{Role: "candidate", ID: "a1", PeerAddr: "https://a1:7450", Score: 60}
	a2 := failoverPeer("candidate", "a2", 40)
	b1 := discovery.Announcement{Role: "candidate", ID: "b1", PeerAddr: "https://b1:7450", Score: 30}
	b2 := failoverPeer("candidate", "b2", 20)
	leader := failoverPeer("orchestrator", "leader", 100)

	if got := failoverElectionWinner(a1, []discovery.Peer{a2, leader}, "leader", trusted).ID; got != "a1" {
		t.Fatalf("side A winner = %q, want a1", got)
	}
	if got := failoverElectionWinner(b1, []discovery.Peer{b2, leader}, "leader", trusted).ID; got != "b1" {
		t.Fatalf("side B winner = %q, want b1", got)
	}

	// After healing, the stronger promoted orchestrator is the step-down
	// target for the weaker one and never the other way round.
	promotedA := failoverPeer("orchestrator", "a1", 60)
	promotedB := discovery.Announcement{Role: "orchestrator", ID: "b1", Score: 30}
	if got := stepDownCandidates(promotedB, "leader", []discovery.Peer{promotedA}, trusted); len(got) != 1 || got[0].ID != "a1" {
		t.Fatalf("weaker promoted leader step-down targets = %v, want [a1]", got)
	}
	if got := stepDownCandidates(promotedA.Announcement, "leader", []discovery.Peer{failoverPeer("orchestrator", "b1", 30)}, trusted); len(got) != 0 {
		t.Fatalf("stronger promoted leader step-down targets = %v, want none", got)
	}
}

type closedFailoverIdentity struct{}

func (closedFailoverIdentity) ID() string                                            { return "self" }
func (closedFailoverIdentity) CertificatePEM() string                                { return "" }
func (closedFailoverIdentity) TrustPeerDetails(string, string, string, string) error { return nil }
func (closedFailoverIdentity) ClientTransport() *http.Transport                      { return &http.Transport{} }
func (closedFailoverIdentity) IsTrustedID(string) bool                               { return true }

func TestFailoverCoordinatorCloseFencesLaterPromotion(t *testing.T) {
	newClosed := func(t *testing.T) *failoverCoordinator {
		t.Helper()
		f := newFailoverCoordinator(nil, nil, config.OrchestratorConfig{ListenAddr: "127.0.0.1:0", DiscoveryEnabled: true}, "")
		if err := f.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}
		return f
	}
	tests := []struct {
		name  string
		check func(t *testing.T, f *failoverCoordinator)
	}{
		{
			name: "Close is idempotent",
			check: func(t *testing.T, f *failoverCoordinator) {
				if err := f.Close(); err != nil {
					t.Fatalf("second Close() = %v", err)
				}
			},
		},
		{
			name: "StartElection after Close is a no-op",
			check: func(t *testing.T, f *failoverCoordinator) {
				// A nil agent would panic if the election ran past the guard.
				f.StartElection("failed")
				if !f.UpdateIdle() {
					t.Fatal("closed coordinator reports an election or promotion in progress")
				}
			},
		},
		{
			name: "election window ends immediately once closed",
			check: func(t *testing.T, f *failoverCoordinator) {
				if f.waitElectionWindow(startupElectionWindow) {
					t.Fatal("closed coordinator completed its election window")
				}
			},
		},
		{
			name: "promote after Close is refused",
			check: func(t *testing.T, f *failoverCoordinator) {
				if err := f.promote("https://self:7450", closedFailoverIdentity{}, "failed"); !errors.Is(err, errFailoverClosed) {
					t.Fatalf("promote() = %v, want errFailoverClosed", err)
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.promoted != nil || f.retired != nil {
					t.Fatal("closed coordinator retained a promoted orchestrator")
				}
			},
		},
		{
			name: "step-down watch is not started after Close",
			check: func(t *testing.T, f *failoverCoordinator) {
				f.startWatch(nil, closedFailoverIdentity{}, "failed")
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.stopWatch != nil {
					t.Fatal("closed coordinator started a step-down watch")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, newClosed(t))
		})
	}
}

func TestFailoverElectionWindowElapsesWhileOpen(t *testing.T) {
	f := newFailoverCoordinator(nil, nil, config.OrchestratorConfig{}, "")
	if !f.waitElectionWindow(0) {
		t.Fatal("open coordinator did not complete its election window")
	}
	zero := &failoverCoordinator{}
	if !zero.waitElectionWindow(0) {
		t.Fatal("zero-value coordinator did not complete its election window")
	}
}
