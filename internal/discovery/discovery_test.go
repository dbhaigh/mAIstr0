package discovery

import (
	"net"
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

func TestBestOrchestratorCandidateWithTwoVisibleCandidates(t *testing.T) {
	candidateA := Announcement{Role: "candidate", ID: "orchestrator-a", Score: 1}
	candidateB := Announcement{Role: "candidate", ID: "orchestrator-b", Score: 2}

	if IsBestOrchestratorCandidate(candidateA, []Peer{{Announcement: candidateB}}) {
		t.Fatal("lower-capability candidate won while the higher-capability candidate was visible")
	}
	if !IsBestOrchestratorCandidate(candidateB, []Peer{{Announcement: candidateA}}) {
		t.Fatal("higher-capability candidate lost while both candidates were visible")
	}
}

func TestBestActiveOrchestratorIgnoresCandidates(t *testing.T) {
	peers := []Peer{
		{Announcement: Announcement{Role: "candidate", ID: "candidate-high", Score: 100}},
		{Announcement: Announcement{Role: "orchestrator", ID: "active-low", Score: 10}},
		{Announcement: Announcement{Role: "orchestrator", ID: "active-high", Score: 20}},
		{Announcement: Announcement{Role: "node", ID: "node-high", Score: 200}},
	}

	best, ok := BestActiveOrchestrator(peers)
	if !ok || best.ID != "active-high" {
		t.Fatalf("BestActiveOrchestrator() = %#v, %v; want active-high", best, ok)
	}
}

func TestPartitionedCandidatesCanElectIndependentLocalWinners(t *testing.T) {
	candidateA := Announcement{Role: "candidate", ID: "orchestrator-a", Score: 1}
	candidateB := Announcement{Role: "candidate", ID: "orchestrator-b", Score: 2}

	if !IsBestOrchestratorCandidate(candidateA, nil) || !IsBestOrchestratorCandidate(candidateB, nil) {
		t.Fatal("each isolated partition should elect its only visible candidate")
	}
}

func TestSnapshotExpiresCandidatesAndAllowsPartitionLocalWinner(t *testing.T) {
	listener := &Listener{peers: map[string]Peer{
		"orchestrator-visible": {
			Announcement: Announcement{Role: "candidate", ID: "orchestrator-visible", Score: 100},
			LastSeen:     time.Now().Add(-peerTTL - time.Second),
		},
	}}

	visible := listener.Snapshot()
	if len(visible) != 0 {
		t.Fatalf("Snapshot returned %d expired peers, want none", len(visible))
	}
	local := Announcement{Role: "candidate", ID: "orchestrator-local", Score: 1}
	if !IsBestOrchestratorCandidate(local, visible) {
		t.Fatal("local candidate did not win after the remote candidate's visibility lease expired")
	}
	if len(listener.peers) != 0 {
		t.Fatalf("expired peers were not removed from listener state: %#v", listener.peers)
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

func TestIsSameSubnet(t *testing.T) {
	if !IsSameSubnet(net.ParseIP("127.0.0.1")) {
		t.Fatal("expected 127.0.0.1 to be on same subnet (loopback)")
	}
	if !IsSameSubnet(net.ParseIP("::1")) {
		t.Fatal("expected ::1 to be on same subnet (loopback)")
	}
	if IsSameSubnet(nil) {
		t.Fatal("expected nil IP to return false")
	}

	// Mock interfaces to test specific subnet matching
	orig := interfaceAddrs
	defer func() { interfaceAddrs = orig }()

	_, testSubnet, err := net.ParseCIDR("192.168.10.0/24")
	if err != nil {
		t.Fatal(err)
	}
	interfaceAddrs = func() ([]net.Addr, error) {
		return []net.Addr{testSubnet}, nil
	}

	if !IsSameSubnet(net.ParseIP("192.168.10.50")) {
		t.Fatal("expected 192.168.10.50 to be in 192.168.10.0/24 subnet")
	}
	if IsSameSubnet(net.ParseIP("192.168.11.50")) {
		t.Fatal("expected 192.168.11.50 not to be in 192.168.10.0/24 subnet")
	}
	if IsSameSubnet(net.ParseIP("10.0.0.1")) {
		t.Fatal("expected 10.0.0.1 not to be in 192.168.10.0/24 subnet")
	}
}

func TestIsSameSubnetAddress(t *testing.T) {
	if !IsSameSubnetAddress("localhost") {
		t.Fatal("expected localhost to be on same subnet")
	}
	if !IsSameSubnetAddress("localhost:7450") {
		t.Fatal("expected localhost:7450 to be on same subnet")
	}
	if !IsSameSubnetAddress("http://localhost:7450") {
		t.Fatal("expected http://localhost:7450 to be on same subnet")
	}
	if !IsSameSubnetAddress("https://127.0.0.1:7450") {
		t.Fatal("expected https://127.0.0.1:7450 to be on same subnet")
	}
	if IsSameSubnetAddress("") {
		t.Fatal("expected empty string to return false")
	}
	if IsSameSubnetAddress("not-an-ip-address-12345.invalid") {
		t.Fatal("expected unresolvable invalid address to return false")
	}
}
