package cluster

import (
	"testing"

	"github.com/maistr0/maistr0/internal/hardware"
)

func node(id string, score, fastScore float64) NodeStatus {
	return NodeStatus{
		ID:        id,
		Address:   "http://" + id,
		Hardware:  hardware.Info{Score: score},
		FastScore: fastScore,
	}
}

func TestLeaderIsMostCapableNode(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("small", 10, 20))
	r.Upsert(node("big", 50, 120))
	r.Upsert(node("medium", 30, 60))

	leader, ok := r.Leader()
	if !ok {
		t.Fatal("expected a leader to be elected")
	}
	if leader.ID != "big" {
		t.Fatalf("expected leader %q, got %q", "big", leader.ID)
	}

	marked := 0
	for _, n := range r.All() {
		if n.Leader {
			marked++
			if n.ID != "big" {
				t.Fatalf("leader flag on wrong node %q", n.ID)
			}
		}
	}
	if marked != 1 {
		t.Fatalf("expected exactly one leader flag, got %d", marked)
	}
}

func TestSingleMemberCannotElectLeader(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("only", 50, 120))

	if _, ok := r.Leader(); ok {
		t.Fatal("expected no leader until a second healthy member joins")
	}
	for _, member := range r.All() {
		if member.Leader {
			t.Fatalf("single member %q was marked leader", member.ID)
		}
	}
}

func TestLeadershipPassesWhenLeaderUnhealthy(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("big", 50, 120))
	r.Upsert(node("medium", 30, 60))
	r.Upsert(node("small", 20, 30))

	r.MarkUnhealthy("big")

	leader, ok := r.Leader()
	if !ok {
		t.Fatal("expected leadership to pass while two healthy members remain")
	}
	if leader.ID != "medium" {
		t.Fatalf("expected leader %q, got %q", "medium", leader.ID)
	}
}

func TestNoLeaderWithoutHealthyNodes(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("only", 10, 10))
	r.MarkUnhealthy("only")

	if _, ok := r.Leader(); ok {
		t.Fatal("expected no leader when every node is unhealthy")
	}
}

func TestRemoveEjectsNodeFromCluster(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("one", 10, 10))
	r.Upsert(node("two", 20, 20))
	r.Remove("two")

	if _, ok := r.Get("two"); ok {
		t.Fatal("expected removed node to leave the registry")
	}
	if members := r.All(); len(members) != 1 {
		t.Fatalf("expected one remaining member, got %d", len(members))
	}
	if _, ok := r.Leader(); ok {
		t.Fatal("expected no leader after removal leaves one member")
	}
}

func TestOlderNodeGetsVersionError(t *testing.T) {
	r := NewRegistry()
	newer := node("newer", 20, 20)
	newer.Version = "0.0002"
	r.Upsert(newer)

	older := node("older", 10, 10)
	older.Version = "0.0001"
	r.Upsert(older)

	got, ok := r.Get("older")
	if !ok {
		t.Fatal("expected older node to remain visible for diagnosis")
	}
	if got.VersionError == "" {
		t.Fatal("expected a version mismatch error for the older node")
	}
	if got.VersionError != "node version 0.0001 is older than cluster version 0.0002" {
		t.Fatalf("unexpected version error: %q", got.VersionError)
	}
}
