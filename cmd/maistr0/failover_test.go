package main

import (
	"context"
	"errors"
	"reflect"
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

func TestStepDownCandidates(t *testing.T) {
	self := discovery.Announcement{Role: "orchestrator", ID: "promoted", Score: 148, Members: 1}
	orchestrator := func(id string, score float64, members int) discovery.Peer {
		return discovery.Peer{Announcement: discovery.Announcement{
			Role: "orchestrator", ID: id, PeerAddr: "https://" + id + ":7450", Score: score, Members: members,
		}}
	}
	tests := []struct {
		name       string
		replacedID string
		peers      []discovery.Peer
		untrusted  string
		want       []string
	}{
		{
			name:       "returned original with more members wins despite lower score",
			replacedID: "original",
			peers:      []discovery.Peer{orchestrator("original", 145.9, 3)},
			want:       []string{"original"},
		},
		{
			name:       "returned original with equal members wins",
			replacedID: "original",
			peers:      []discovery.Peer{orchestrator("original", 10, 1)},
			want:       []string{"original"},
		},
		{
			name:       "returned original with fewer members does not win",
			replacedID: "original",
			peers:      []discovery.Peer{orchestrator("original", 10, 0)},
		},
		{
			name:  "higher-ranked orchestrator wins",
			peers: []discovery.Peer{orchestrator("stronger", 200, 0)},
			want:  []string{"stronger"},
		},
		{
			name:  "equal score breaks ties by lower ID",
			peers: []discovery.Peer{orchestrator("aaa", 148, 0), orchestrator("zzz", 148, 0)},
			want:  []string{"aaa"},
		},
		{
			name:  "larger partition wins and candidates are ordered by members",
			peers: []discovery.Peer{orchestrator("bigger", 1, 2), orchestrator("biggest", 1, 5)},
			want:  []string{"biggest", "bigger"},
		},
		{
			name: "ignores nodes candidates self untrusted and addressless peers",
			peers: []discovery.Peer{
				{Announcement: discovery.Announcement{Role: "node", ID: "node", PeerAddr: "https://node:7451", Score: 500, Members: 9}},
				{Announcement: discovery.Announcement{Role: "candidate", ID: "candidate", PeerAddr: "https://c:7450", Score: 500, Members: 9}},
				{Announcement: discovery.Announcement{Role: "orchestrator", ID: "promoted", PeerAddr: "https://self:7450", Score: 500, Members: 9}},
				{Announcement: discovery.Announcement{Role: "orchestrator", ID: "noaddr", Score: 500, Members: 9}},
				orchestrator("untrusted", 500, 9),
			},
			untrusted: "untrusted",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stepDownCandidates(self, tt.replacedID, tt.peers, func(id string) bool { return id != tt.untrusted })
			ids := []string{}
			for _, peer := range got {
				ids = append(ids, peer.ID)
			}
			want := tt.want
			if want == nil {
				want = []string{}
			}
			if !reflect.DeepEqual(ids, want) {
				t.Fatalf("stepDownCandidates() = %v, want %v", ids, want)
			}
		})
	}
}

func TestFirstHealthyChallengerSkipsYieldedOrUnreachable(t *testing.T) {
	peer := func(id string) discovery.Peer {
		return discovery.Peer{Announcement: discovery.Announcement{ID: id, PeerAddr: "https://" + id}}
	}
	tests := []struct {
		name    string
		healthy map[string]bool
		want    string
	}{
		{name: "first healthy candidate", healthy: map[string]bool{"https://a": true, "https://b": true}, want: "a"},
		{name: "yielded first candidate is skipped", healthy: map[string]bool{"https://b": true}, want: "b"},
		{name: "no healthy candidate", healthy: map[string]bool{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := firstHealthyChallenger(context.Background(), []discovery.Peer{peer("a"), peer("b")}, func(_ context.Context, address string) error {
				if tt.healthy[address] {
					return nil
				}
				return errors.New("503 Service Unavailable")
			})
			if ok != (tt.want != "") || got.ID != tt.want {
				t.Fatalf("firstHealthyChallenger() = (%q, %v), want %q", got.ID, ok, tt.want)
			}
		})
	}
}
