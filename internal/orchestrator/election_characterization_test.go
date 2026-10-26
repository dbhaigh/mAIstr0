package orchestrator

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/discovery"
)

type electionViewFake struct{ peers []discovery.Peer }

func (f electionViewFake) Snapshot() []discovery.Peer  { return f.peers }
func (electionViewFake) WaitReady(time.Duration) error { return nil }

// TestElectionComponentWriteAuthorityFollowsLocalView documents that write
// authority is derived only from the local discovery view: disjoint
// partitions each keep an elected orchestrator until they see each other.
func TestElectionComponentWriteAuthorityFollowsLocalView(t *testing.T) {
	active := func(id string, score float64) discovery.Peer {
		return discovery.Peer{Announcement: discovery.Announcement{Role: "orchestrator", ID: id, Score: score}, LastSeen: time.Now()}
	}
	candidate := func(id string, score float64) discovery.Peer {
		return discovery.Peer{Announcement: discovery.Announcement{Role: "candidate", ID: id, Score: score}, LastSeen: time.Now()}
	}
	tests := []struct {
		name        string
		selfID      string
		score       float64
		enabled     bool
		noListener  bool
		retired     bool
		peers       []discovery.Peer
		wantElected bool
	}{
		{name: "isolated partition keeps authority", selfID: "b1", score: 30, enabled: true, wantElected: true},
		{name: "stronger candidate cannot preempt a running orchestrator", selfID: "b1", score: 30, enabled: true, peers: []discovery.Peer{candidate("a1", 90)}, wantElected: true},
		{name: "healed view yields to a stronger active orchestrator", selfID: "b1", score: 30, enabled: true, peers: []discovery.Peer{active("a1", 90)}},
		{name: "healed view keeps authority over a weaker active orchestrator", selfID: "a1", score: 90, enabled: true, peers: []discovery.Peer{active("b1", 30)}, wantElected: true},
		{name: "equal score yields to the lower ID", selfID: "node-b", score: 50, enabled: true, peers: []discovery.Peer{active("node-a", 50)}},
		{name: "equal score keeps authority with the lower ID", selfID: "node-a", score: 50, enabled: true, peers: []discovery.Peer{active("node-b", 50)}, wantElected: true},
		{name: "retired server never claims authority", selfID: "a1", score: 90, enabled: true, retired: true},
		{name: "discovery disabled always claims authority", selfID: "b1", score: 30, peers: []discovery.Peer{active("a1", 90)}, wantElected: true},
		{name: "missing listener always claims authority", selfID: "b1", score: 30, enabled: true, noListener: true, wantElected: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newElectionComponent(nil, nil, tt.selfID, tt.score, tt.enabled)
			if !tt.noListener {
				e.discovery = electionViewFake{peers: tt.peers}
			}
			e.setRetired(tt.retired)
			if got := e.isElectedOrchestrator(); got != tt.wantElected {
				t.Fatalf("isElectedOrchestrator() = %t, want %t", got, tt.wantElected)
			}
		})
	}
}
