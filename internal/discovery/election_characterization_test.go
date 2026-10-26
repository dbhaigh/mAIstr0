package discovery

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"
)

// These tests characterize the ranking-and-TTL election described in
// docs/lan-election.md. They drive time through explicit LastSeen values and
// feed announcements through serve with a scripted socket; nothing sleeps.

func permutePeers(peers []Peer) [][]Peer {
	if len(peers) <= 1 {
		return [][]Peer{append([]Peer(nil), peers...)}
	}
	var out [][]Peer
	for i := range peers {
		rest := make([]Peer, 0, len(peers)-1)
		rest = append(rest, peers[:i]...)
		rest = append(rest, peers[i+1:]...)
		for _, tail := range permutePeers(rest) {
			out = append(out, append([]Peer{peers[i]}, tail...))
		}
	}
	return out
}

func TestElectionTieBreakIsStableAcrossInputPermutations(t *testing.T) {
	ann := func(role, id string, score float64) Peer {
		return Peer{Announcement: Announcement{Role: role, ID: id, Score: score}}
	}
	tests := []struct {
		name       string
		self       Announcement
		peers      []Peer
		wantBest   string
		wantActive string
	}{
		{
			name:       "equal scores elect the lowest ID",
			self:       Announcement{Role: "candidate", ID: "node-m", Score: 50},
			peers:      []Peer{ann("candidate", "node-z", 50), ann("orchestrator", "node-c", 50), ann("candidate", "node-b", 50), ann("orchestrator", "node-x", 50)},
			wantBest:   "node-b",
			wantActive: "node-c",
		},
		{
			name:       "self holds the lowest ID on a tie",
			self:       Announcement{Role: "candidate", ID: "node-a", Score: 50},
			peers:      []Peer{ann("candidate", "node-b", 50), ann("orchestrator", "node-c", 50), ann("candidate", "node-d", 50)},
			wantBest:   "node-a",
			wantActive: "node-c",
		},
		{
			name:       "higher score beats a lower ID",
			self:       Announcement{Role: "candidate", ID: "node-a", Score: 10},
			peers:      []Peer{ann("candidate", "node-z", 11), ann("orchestrator", "node-b", 10), ann("orchestrator", "node-y", 11)},
			wantBest:   "node-y",
			wantActive: "node-y",
		},
		{
			name:       "ID comparison is byte-wise, not numeric",
			self:       Announcement{Role: "candidate", ID: "node-9", Score: 1},
			peers:      []Peer{ann("candidate", "node-10", 1), ann("orchestrator", "node-2", 1), ann("orchestrator", "node-11", 1)},
			wantBest:   "node-10",
			wantActive: "node-11",
		},
		{
			name:       "nodes and unknown roles never compete",
			self:       Announcement{Role: "candidate", ID: "node-m", Score: 1},
			peers:      []Peer{ann("node", "node-a", 1000), ann("", "node-b", 1000), ann("observer", "node-c", 1000), ann("orchestrator", "node-n", 1)},
			wantBest:   "node-m",
			wantActive: "node-n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, order := range permutePeers(tt.peers) {
				if got := BestOrchestratorCandidate(tt.self, order).ID; got != tt.wantBest {
					t.Fatalf("BestOrchestratorCandidate(%v) = %q, want %q", peerIDs(order), got, tt.wantBest)
				}
				active, ok := BestActiveOrchestrator(order)
				if !ok || active.ID != tt.wantActive {
					t.Fatalf("BestActiveOrchestrator(%v) = %q/%t, want %q", peerIDs(order), active.ID, ok, tt.wantActive)
				}
			}
		})
	}
}

func peerIDs(peers []Peer) []string {
	ids := make([]string, 0, len(peers))
	for _, peer := range peers {
		ids = append(ids, peer.ID)
	}
	return ids
}

func TestSnapshotDropsOnlyCandidatesOutsideTheTTL(t *testing.T) {
	now := time.Now()
	listener := &Listener{
		excluded: map[string]bool{},
		peers: map[string]Peer{
			"stale-strong": {Announcement: Announcement{Role: "orchestrator", ID: "stale-strong", Score: 500}, LastSeen: now.Add(-peerTTL - time.Millisecond)},
			"stale-far":    {Announcement: Announcement{Role: "candidate", ID: "stale-far", Score: 400}, LastSeen: now.Add(-time.Hour)},
			"fresh-weak":   {Announcement: Announcement{Role: "candidate", ID: "fresh-weak", Score: 5}, LastSeen: now.Add(-peerTTL + time.Second)},
			"fresh-now":    {Announcement: Announcement{Role: "node", ID: "fresh-now", Score: 900}, LastSeen: now},
		},
	}

	visible := listener.Snapshot()
	got := peerIDs(visible)
	sort.Strings(got)
	if want := []string{"fresh-now", "fresh-weak"}; !equalStrings(got, want) {
		t.Fatalf("Snapshot() = %v, want %v", got, want)
	}
	if _, ok := BestActiveOrchestrator(visible); ok {
		t.Fatal("an expired orchestrator is still reported as the active leader")
	}
	self := Announcement{Role: "candidate", ID: "self", Score: 1}
	if winner := BestOrchestratorCandidate(self, visible).ID; winner != "fresh-weak" {
		t.Fatalf("winner = %q, want the strongest unexpired candidate fresh-weak", winner)
	}
	if !IsElectedOrchestrator(Announcement{Role: "orchestrator", ID: "self", Score: 1}, visible) {
		t.Fatal("a running orchestrator did not stay elected once the only active leader expired")
	}
	if _, ok := listener.peers["stale-strong"]; ok {
		t.Fatal("expired peer was not purged from listener state")
	}
}

// scriptedListener builds a listener whose serve loop consumes announcements
// synchronously and returns once the script is exhausted.
func scriptedListener(selfIDs ...string) *Listener {
	self := make(map[string]bool, len(selfIDs))
	for _, id := range selfIDs {
		self[id] = true
	}
	return &Listener{
		selfIDs:  self,
		peers:    map[string]Peer{},
		excluded: map[string]bool{},
		stop:     make(chan struct{}),
		ready:    make(chan struct{}),
	}
}

var errScriptDone = errors.New("script done")

func deliver(t *testing.T, listener *Listener, payloads ...[]byte) {
	t.Helper()
	reads := make([]fakeRead, 0, len(payloads)+1)
	for _, payload := range payloads {
		reads = append(reads, fakeRead{payload: payload})
	}
	reads = append(reads, fakeRead{err: errScriptDone})
	if err := listener.serve(&fakePacketConn{reads: reads}); !errors.Is(err, errScriptDone) {
		t.Fatalf("serve() = %v, want scripted end", err)
	}
}

func encode(t *testing.T, ann Announcement) []byte {
	t.Helper()
	payload, err := json.Marshal(ann)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestServeAppliesAnnouncementsToTheElectionView(t *testing.T) {
	listener := scriptedListener("self")
	listener.ExcludePeer("failed-leader")
	deliver(t, listener,
		encode(t, Announcement{Role: "orchestrator", ID: "self", Score: 1000}),
		encode(t, Announcement{Role: "orchestrator", ID: "", Score: 1000}),
		[]byte("{not json"),
		encode(t, Announcement{Role: "orchestrator", ID: "failed-leader", Score: 1000}),
		encode(t, Announcement{Role: "candidate", ID: "peer-b", Score: 20}),
		encode(t, Announcement{Role: "orchestrator", ID: "peer-b", Score: 20}),
		encode(t, Announcement{Role: "candidate", ID: "peer-c", Score: 10}),
	)

	view := map[string]string{}
	for _, peer := range listener.Snapshot() {
		view[peer.ID] = peer.Role
	}
	want := map[string]string{"peer-b": "orchestrator", "peer-c": "candidate"}
	if len(view) != len(want) || view["peer-b"] != want["peer-b"] || view["peer-c"] != want["peer-c"] {
		t.Fatalf("view = %v, want %v (self, empty IDs, malformed and excluded announcements ignored; latest role wins)", view, want)
	}

	listener.IncludePeer("failed-leader")
	deliver(t, listener, encode(t, Announcement{Role: "orchestrator", ID: "failed-leader", Score: 1000}))
	if best, ok := BestActiveOrchestrator(listener.Snapshot()); !ok || best.ID != "failed-leader" {
		t.Fatalf("recovered leader not re-elected after IncludePeer: %q/%t", best.ID, ok)
	}
}

func TestExpiredLeaderIsRestoredByAFreshAnnouncement(t *testing.T) {
	listener := scriptedListener("self")
	listener.peers["leader"] = Peer{
		Announcement: Announcement{Role: "orchestrator", ID: "leader", Score: 100},
		LastSeen:     time.Now().Add(-2 * peerTTL),
	}
	self := Announcement{Role: "candidate", ID: "self", Score: 1}

	if !IsElectedOrchestrator(self, listener.Snapshot()) {
		t.Fatal("candidate did not win while the leader's announcement was expired")
	}
	deliver(t, listener, encode(t, Announcement{Role: "orchestrator", ID: "leader", Score: 100}))
	visible := listener.Snapshot()
	if len(visible) != 1 || time.Since(visible[0].LastSeen) > peerTTL {
		t.Fatalf("fresh announcement did not renew the leader lease: %+v", visible)
	}
	if IsElectedOrchestrator(self, visible) {
		t.Fatal("candidate stayed elected after the leader's announcements returned")
	}
}

// view is one node's discovery snapshot of the other members.
func view(self string, members map[string]Announcement, visible ...string) []Peer {
	peers := make([]Peer, 0, len(visible))
	for _, id := range visible {
		if id != self {
			peers = append(peers, Peer{Announcement: members[id], LastSeen: time.Now()})
		}
	}
	return peers
}

func electedSet(members map[string]Announcement, visibility map[string][]string) []string {
	var elected []string
	for id, ann := range members {
		if IsElectedOrchestrator(ann, view(id, members, visibility[id]...)) {
			elected = append(elected, id)
		}
	}
	sort.Strings(elected)
	return elected
}

// TestPartitionElectsOneLeaderPerSideAndHealsToOne documents current
// behavior: the election is partition-local, not consensus. Each disjoint
// visible set elects its own leader, and both stay elected until their
// announcements become mutually visible again.
func TestPartitionElectsOneLeaderPerSideAndHealsToOne(t *testing.T) {
	members := map[string]Announcement{
		"a1": {Role: "orchestrator", ID: "a1", Score: 90},
		"a2": {Role: "candidate", ID: "a2", Score: 80},
		"b1": {Role: "candidate", ID: "b1", Score: 70},
		"b2": {Role: "candidate", ID: "b2", Score: 70},
	}
	sideA := []string{"a1", "a2"}
	sideB := []string{"b1", "b2"}
	all := []string{"a1", "a2", "b1", "b2"}

	tests := []struct {
		name       string
		visibility map[string][]string
		promote    []string // candidates that became running orchestrators
		want       []string
	}{
		{
			name:       "connected cluster has one leader",
			visibility: map[string][]string{"a1": all, "a2": all, "b1": all, "b2": all},
			want:       []string{"a1"},
		},
		{
			name:       "partition elects a local winner on each side",
			visibility: map[string][]string{"a1": sideA, "a2": sideA, "b1": sideB, "b2": sideB},
			promote:    []string{"b1"},
			want:       []string{"a1", "b1"},
		},
		{
			name:       "healed partition converges on the global best",
			visibility: map[string][]string{"a1": all, "a2": all, "b1": all, "b2": all},
			promote:    []string{"b1"},
			want:       []string{"a1"},
		},
		{
			name:       "one-way visibility toward the stronger leader converges",
			visibility: map[string][]string{"a1": sideA, "a2": sideA, "b1": all, "b2": all},
			promote:    []string{"b1"},
			want:       []string{"a1"},
		},
		{
			name:       "one-way visibility toward the weaker leader stays split",
			visibility: map[string][]string{"a1": all, "a2": all, "b1": sideB, "b2": sideB},
			promote:    []string{"b1"},
			want:       []string{"a1", "b1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := make(map[string]Announcement, len(members))
			for id, ann := range members {
				state[id] = ann
			}
			for _, id := range tt.promote {
				ann := state[id]
				ann.Role = "orchestrator"
				state[id] = ann
			}
			if got := electedSet(state, tt.visibility); !equalStrings(got, tt.want) {
				t.Fatalf("elected = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPartitionWinnersAgreeOnceAllSidesShareOneView(t *testing.T) {
	sideA := []Peer{
		{Announcement: Announcement{Role: "candidate", ID: "n-03", Score: 40}},
		{Announcement: Announcement{Role: "candidate", ID: "n-01", Score: 40}},
	}
	sideB := []Peer{
		{Announcement: Announcement{Role: "candidate", ID: "n-02", Score: 40}},
		{Announcement: Announcement{Role: "candidate", ID: "n-04", Score: 30}},
	}
	winner := func(self Announcement, visible []Peer) string {
		return BestOrchestratorCandidate(self, visible).ID
	}
	if a, b := winner(sideA[0].Announcement, sideA[1:]), winner(sideB[1].Announcement, sideB[:1]); a != "n-01" || b != "n-02" {
		t.Fatalf("partition winners = %q/%q, want n-01/n-02", a, b)
	}
	merged := append(append([]Peer(nil), sideA...), sideB...)
	for i, member := range merged {
		others := append(append([]Peer(nil), merged[:i]...), merged[i+1:]...)
		if got := winner(member.Announcement, others); got != "n-01" {
			t.Fatalf("after healing %s elects %q, want n-01", member.ID, got)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
