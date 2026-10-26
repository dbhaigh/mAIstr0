package orchestrator

import (
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
)

func TestSnapshotCountsEveryHealthyMemberRegardlessOfLeaderPosition(t *testing.T) {
	for _, test := range []struct {
		name       string
		strongest  string
		offline    []string
		wantCount  int
		wantLeader string
	}{
		{name: "leader sorts first", strongest: "a", wantCount: 3, wantLeader: "a"},
		{name: "leader sorts in the middle", strongest: "b", wantCount: 3, wantLeader: "b"},
		{name: "leader sorts last", strongest: "c", wantCount: 3, wantLeader: "c"},
		{name: "offline members excluded", strongest: "a", offline: []string{"c"}, wantCount: 2, wantLeader: "a"},
		{name: "single healthy member has no leader", strongest: "a", offline: []string{"b", "c"}, wantCount: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newTestServer(t)
			for _, id := range []string{"a", "b", "c"} {
				node := cluster.NodeStatus{ID: id, Address: "https://" + id}
				if id == test.strongest {
					node.Hardware.CPUCores = 128
				}
				s.registry.Upsert(node)
			}
			for _, id := range test.offline {
				s.registry.MarkUnhealthy(id)
			}
			snapshot := s.snapshot()
			if snapshot.MemberCount != test.wantCount || snapshot.LeaderID != test.wantLeader {
				t.Fatalf("member_count = %d leader = %q, want %d and %q", snapshot.MemberCount, snapshot.LeaderID, test.wantCount, test.wantLeader)
			}
		})
	}
}
