package cluster

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestAppendEffectiveCapabilitiesPreservesPrefixAndDeduplicates(t *testing.T) {
	status := NodeStatus{
		Capabilities: []string{" GPU ", "custom", "custom"},
		Models: []engine.Model{{
			Name: "Qwen2.5-Coder", SizeGB: 8,
			Tags: []string{"Code", "math", "reasoning", "FAST"},
		}},
		Hardware: hardware.Info{HasGPU: true, HasNPU: true, TotalRAMMB: 16 * 1024},
	}

	got := status.AppendEffectiveCapabilities([]string{"prefix"})
	want := []string{
		"prefix", "analyst", "code", "coder", "custom", "fast", "gpu",
		"high-quality", "long-context", "math", "npu", "reasoning",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AppendEffectiveCapabilities() = %v, want %v", got, want)
	}

	if got := (NodeStatus{}).EffectiveCapabilities(); !reflect.DeepEqual(got, []string{"general"}) {
		t.Fatalf("empty node capabilities = %v, want [general]", got)
	}
}

func TestUpdateDevicesPreservesHeartbeatAndCopiesMemory(t *testing.T) {
	registry := NewRegistry()
	registry.Upsert(NodeStatus{ID: "node", Address: "https://node", Healthy: true, ActiveTasks: 2})
	before, _ := registry.Get("node")
	_ = registry.All()
	devices := []hardware.Device{{Kind: "gpu", FreeMemoryMB: 4096, Backends: []string{"vulkan"}}}
	if !registry.UpdateDevices("node", "https://node", devices) {
		t.Fatal("device refresh rejected")
	}
	devices[0].FreeMemoryMB = 0
	devices[0].Backends[0] = "changed"
	got, _ := registry.Get("node")
	if got.Hardware.Devices[0].FreeMemoryMB != 4096 || got.Hardware.Devices[0].Backends[0] != "vulkan" ||
		got.ActiveTasks != 2 || !got.Healthy || !got.LastSeen.Equal(before.LastSeen) {
		t.Fatalf("device refresh replaced unrelated state: %+v", got)
	}
	if registry.UpdateDevices("node", "https://old-address", nil) || registry.UpdateDevices("missing", "", nil) {
		t.Fatal("accepted stale registration")
	}
	if len(registry.All()[0].Hardware.Devices) != 1 {
		t.Fatal("cached snapshot was not invalidated")
	}
}

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

func TestEngineReadinessUsesReportedStatusAndKeepsLegacyRecordsCompatible(t *testing.T) {
	if !node("legacy", 1, 1).EngineReady("ollama") {
		t.Fatal("legacy node without engine status should remain compatible")
	}
	status := NodeStatus{Engines: []engine.Status{
		{Name: "ollama", Ready: false},
		{Name: "vllm", Ready: true},
	}}
	if status.EngineReady("OLLAMA") {
		t.Fatal("unready engine was reported ready")
	}
	if !status.EngineReady("vllm") {
		t.Fatal("ready engine was reported unready")
	}
	if status.EngineReady("missing") {
		t.Fatal("engine absent from a populated status list should be unavailable")
	}
}

func TestElectionOnlyRewardsNPUWithReadyRunningModel(t *testing.T) {
	base := NodeStatus{
		Hardware:  hardware.Info{Score: 230, HasNPU: true, CPUCores: 2},
		FastScore: 4,
	}
	tests := []struct {
		name string
		node NodeStatus
		want float64
	}{
		{name: "unverified NPU presence", node: base, want: 203},
		{
			name: "legacy reported NPU speed without a running model",
			node: func() NodeStatus {
				node := base
				node.FastScore = 104
				return node
			}(),
			want: 203,
		},
		{
			name: "ready engine running on NPU",
			node: func() NodeStatus {
				node := base
				node.Models = []engine.Model{{Name: "npu-model", Engine: "fastflowlm", Device: "npu"}}
				node.Engines = []engine.Status{{Name: "fastflowlm", Ready: true, ModelCount: 1}}
				return node
			}(),
			want: 308,
		},
		{
			name: "legacy reported NPU speed with running model",
			node: func() NodeStatus {
				node := base
				node.FastScore = 104
				node.Models = []engine.Model{{Name: "npu-model", Engine: "fastflowlm", Device: "npu"}}
				node.Engines = []engine.Status{{Name: "fastflowlm", Ready: true, ModelCount: 1}}
				return node
			}(),
			want: 308,
		},
		{
			name: "engine has no ready model",
			node: func() NodeStatus {
				node := base
				node.Models = []engine.Model{{Name: "npu-model", Engine: "fastflowlm", Device: "npu"}}
				node.Engines = []engine.Status{{Name: "fastflowlm", Ready: true}}
				return node
			}(),
			want: 203,
		},
		{
			name: "model device is unknown",
			node: func() NodeStatus {
				node := base
				node.Models = []engine.Model{{Name: "npu-model", Engine: "fastflowlm"}}
				node.Engines = []engine.Status{{Name: "fastflowlm", Ready: true, ModelCount: 1}}
				return node
			}(),
			want: 203,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CapabilityScore(test.node); got != test.want {
				t.Fatalf("CapabilityScore() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestFastScoreIgnoresUnverifiedNPU(t *testing.T) {
	hw := hardware.Info{CPUCores: 4, HasNPU: true}
	if got, want := FastScore(hw), float64(8); got != want {
		t.Fatalf("FastScore() = %v, want %v", got, want)
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

func TestAllUsesCachedSnapshotAndInvalidatesOnWrites(t *testing.T) {
	r := NewRegistry()
	status := node("cached", 10, 10)
	status.Models = []engine.Model{{Name: "model", Tags: []string{"original"}}}
	r.Upsert(status)

	first := r.All()
	if len(first) != 1 || !r.allCacheValid {
		t.Fatalf("first snapshot = %#v, cache valid = %v", first, r.allCacheValid)
	}

	first[0].Models[0].Tags[0] = "caller-mutation"
	if got := r.All()[0].Models[0].Tags[0]; got != "original" {
		t.Fatalf("mutating a returned snapshot changed cached data: %q", got)
	}

	r.IncrementLoad("cached", 1)
	if r.allCacheValid {
		t.Fatal("load update did not invalidate the snapshot")
	}
	if got := r.All()[0].ActiveTasks; got != 1 {
		t.Fatalf("active tasks after invalidation = %d, want 1", got)
	}
	r.MarkUnhealthy("cached")
	if r.allCacheValid {
		t.Fatal("health update did not invalidate the snapshot")
	}
	if got := r.All()[0].Healthy; got {
		t.Fatal("cached snapshot retained stale health")
	}
}

func TestAllModelSlicesRemainIndependent(t *testing.T) {
	r := NewRegistry()
	status := node("models", 10, 10)
	status.Models = []engine.Model{
		{Name: "first", Tags: []string{"first"}, Protocols: []string{"first-protocol"}},
		{Name: "second", Tags: []string{"second"}, Protocols: []string{"second-protocol"}},
	}
	r.Upsert(status)

	first := r.All()
	first[0].Models[0].Tags = append(first[0].Models[0].Tags, "appended")
	first[0].Models[1].Tags[0] = "caller-mutation"
	first[0].Models[0].Protocols[0] = "caller-mutation"

	got := r.All()[0].Models
	if got[0].Tags[0] != "first" || len(got[0].Tags) != 1 ||
		got[1].Tags[0] != "second" || got[0].Protocols[0] != "first-protocol" {
		t.Fatalf("mutating model slices in a returned snapshot changed cached data: %+v", got)
	}
}

func TestAllPreservesNilAndEmptyModelSlices(t *testing.T) {
	r := NewRegistry()
	nilModels := node("nil-models", 10, 10)
	nilModels.Models = nil
	emptyModels := node("empty-models", 10, 10)
	emptyModels.Models = []engine.Model{}
	r.Upsert(nilModels)
	r.Upsert(emptyModels)

	statuses := r.All()
	if len(statuses) != 2 {
		t.Fatalf("registry returned %d nodes, want 2", len(statuses))
	}
	for _, status := range statuses {
		switch status.ID {
		case "nil-models":
			if status.Models != nil {
				t.Fatalf("nil model slice became non-nil: %#v", status.Models)
			}
		case "empty-models":
			if status.Models == nil {
				t.Fatal("empty model slice became nil")
			}
		}
	}
}

func TestSnapshotSinceCoalescesChangesAndReportsRemovals(t *testing.T) {
	r := NewRegistry()
	for _, id := range []string{"alpha", "bravo", "charlie", "delta", "echo"} {
		status := node(id, 10, 10)
		status.Models = []engine.Model{{Name: id, Tags: []string{"original"}}}
		r.Upsert(status)
	}
	base := r.SnapshotSince(0)
	if !base.Full || len(base.Nodes) != 5 {
		t.Fatalf("initial snapshot = %+v, want full snapshot with five nodes", base)
	}

	r.IncrementLoad("bravo", 1)
	r.Remove("delta")
	r.IncrementLoad("bravo", 1)

	got := r.SnapshotSince(base.Revision)
	if got.Full {
		t.Fatal("small change set unexpectedly coalesced to a full snapshot")
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != "bravo" || got.Nodes[0].ActiveTasks != 2 {
		t.Fatalf("delta nodes = %+v, want only bravo with two active tasks", got.Nodes)
	}
	if !reflect.DeepEqual(got.Removed, []string{"delta"}) {
		t.Fatalf("removed IDs = %v, want [delta]", got.Removed)
	}

	got.Nodes[0].Models[0].Tags[0] = "caller-mutation"
	again := r.SnapshotSince(base.Revision)
	if again.Nodes[0].Models[0].Tags[0] != "original" {
		t.Fatal("mutating a delta changed registry state")
	}
}

func TestSnapshotSinceIncludesLeadershipChanges(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("high", 50, 50))
	r.Upsert(node("medium", 30, 30))
	r.Upsert(node("low", 10, 10))
	base := r.SnapshotSince(0)

	r.MarkUnhealthy("high")
	got := r.SnapshotSince(base.Revision)
	if got.Full {
		t.Fatal("leadership change unexpectedly coalesced to a full snapshot")
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("delta contains %d nodes, want the unhealthy former leader and new leader", len(got.Nodes))
	}
	statuses := make(map[string]NodeStatus, len(got.Nodes))
	for _, status := range got.Nodes {
		statuses[status.ID] = status
	}
	if statuses["high"].Healthy || statuses["high"].Leader {
		t.Fatalf("former leader status = %+v, want unhealthy and not leader", statuses["high"])
	}
	if !statuses["medium"].Leader {
		t.Fatalf("new leader status = %+v, want leader", statuses["medium"])
	}
}

func TestSnapshotSinceCoalescesLargeDeltaIntoFullSnapshot(t *testing.T) {
	r := NewRegistry()
	for _, id := range []string{"one", "two", "three"} {
		r.Upsert(node(id, 10, 10))
	}
	base := r.SnapshotSince(0)

	for _, id := range []string{"one", "two", "three"} {
		r.IncrementLoad(id, 1)
	}

	got := r.SnapshotSince(base.Revision)
	if !got.Full || len(got.Nodes) != 3 || len(got.Removed) != 0 {
		t.Fatalf("coalesced snapshot = %+v, want a full three-node snapshot", got)
	}
}

func TestSnapshotSinceFallsBackWhenHistoryExpires(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("one", 10, 10))
	r.Upsert(node("two", 20, 20))
	base := r.SnapshotSince(0)

	for i := 0; i <= snapshotChangeHistoryLimit; i++ {
		r.IncrementLoad("one", 1)
	}

	got := r.SnapshotSince(base.Revision)
	if !got.Full || len(got.Nodes) != 2 {
		t.Fatalf("expired-revision snapshot = %+v, want full snapshot", got)
	}
}

func TestSnapshotSinceReaddedNodeIsAnUpdate(t *testing.T) {
	r := NewRegistry()
	for _, id := range []string{"one", "two", "three"} {
		r.Upsert(node(id, 10, 10))
	}
	base := r.SnapshotSince(0)

	r.Remove("two")
	r.Upsert(node("two", 5, 5))

	got := r.SnapshotSince(base.Revision)
	if got.Full {
		t.Fatal("re-added node unexpectedly coalesced to a full snapshot")
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != "two" || len(got.Removed) != 0 {
		t.Fatalf("re-added node delta = %+v, want one update and no removals", got)
	}
}

func TestSnapshotSinceReturnsEmptyDeltaAtCurrentRevision(t *testing.T) {
	r := NewRegistry()
	r.Upsert(node("one", 10, 10))
	base := r.SnapshotSince(0)

	got := r.SnapshotSince(base.Revision)
	if got.Full || got.Revision != base.Revision || len(got.Nodes) != 0 || len(got.Removed) != 0 {
		t.Fatalf("current-revision snapshot = %+v, want an empty delta", got)
	}
}

func TestSnapshotSinceReportsVersionErrorUpdates(t *testing.T) {
	r := NewRegistry()
	old := node("old", 10, 10)
	old.Version = "0.0001"
	r.Upsert(old)
	r.Upsert(node("unversioned-one", 20, 20))
	r.Upsert(node("unversioned-two", 30, 30))
	base := r.SnapshotSince(0)

	newer := node("new", 5, 5)
	newer.Version = "0.0002"
	r.Upsert(newer)

	got := r.SnapshotSince(base.Revision)
	if got.Full || len(got.Nodes) != 2 {
		t.Fatalf("version update delta = %+v, want two changed nodes", got)
	}
	statuses := make(map[string]NodeStatus, len(got.Nodes))
	for _, status := range got.Nodes {
		statuses[status.ID] = status
	}
	if statuses["old"].VersionError == "" {
		t.Fatalf("older node was not included with its version error: %+v", statuses["old"])
	}
	if statuses["new"].VersionError != "" {
		t.Fatalf("newer node has unexpected version error: %q", statuses["new"].VersionError)
	}
}

func TestRegistryClonesHardwareInventory(t *testing.T) {
	r := NewRegistry()
	status := node("inventory", 10, 10)
	status.Hardware.Devices = []hardware.Device{{
		Kind: "gpu", Vendor: "nvidia", Name: "test GPU", Index: 0,
		Backends: []string{"cuda", "vulkan"},
	}}
	status.Hardware.NUMANodes = []int{0, 1}
	status.Hardware.NetworkLinks = []hardware.NetworkLink{{Name: "ib0", SpeedMbps: 100000, RDMA: true}}
	status.Engines = []engine.Status{{
		Name: "test", Capabilities: engine.Capabilities{
			Devices: []string{"gpu"}, Backends: []string{"cuda"},
		},
	}}
	r.Upsert(status)

	snapshot, ok := r.Get("inventory")
	if !ok {
		t.Fatal("inventory node was not registered")
	}
	snapshot.Hardware.Devices[0].Backends[0] = "mutated"
	snapshot.Hardware.NUMANodes[0] = 9
	snapshot.Hardware.NetworkLinks[0].Name = "mutated"
	snapshot.Engines[0].Capabilities.Devices[0] = "mutated"

	got, ok := r.Get("inventory")
	if !ok {
		t.Fatal("inventory node disappeared")
	}
	if got.Hardware.Devices[0].Backends[0] != "cuda" ||
		got.Hardware.NUMANodes[0] != 0 ||
		got.Hardware.NetworkLinks[0].Name != "ib0" ||
		got.Engines[0].Capabilities.Devices[0] != "gpu" {
		t.Fatalf("mutating returned device inventory or engine capabilities changed stored data: %+v", got)
	}
}

func BenchmarkRegistrySnapshot100Nodes(b *testing.B) {
	newRegistry := func() *Registry {
		r := NewRegistry()
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("node-%03d", i)
			status := node(id, float64(i+1), float64(i+1))
			status.Models = []engine.Model{{
				Name:      "model-" + id,
				Tags:      []string{"text", "fast"},
				Protocols: []string{"openai"},
			}}
			status.Hardware.Devices = []hardware.Device{{
				Kind: "gpu", Backends: []string{"cuda", "vulkan"},
			}}
			r.Upsert(status)
		}
		_ = r.All()
		return r
	}

	b.Run("All", func(b *testing.B) {
		r := newRegistry()
		var snapshot []NodeStatus
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			snapshot = r.All()
		}
		b.StopTimer()
		if len(snapshot) != 100 {
			b.Fatalf("snapshot contains %d nodes, want 100", len(snapshot))
		}
	})

	b.Run("OneNodeDelta", func(b *testing.B) {
		r := newRegistry()
		base := r.SnapshotSince(0)
		r.IncrementLoad("node-050", 1)
		var snapshot RegistrySnapshot
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			snapshot = r.SnapshotSince(base.Revision)
		}
		b.StopTimer()
		if snapshot.Full || len(snapshot.Nodes) != 1 {
			b.Fatalf("delta contains full=%v, nodes=%d; want one changed node", snapshot.Full, len(snapshot.Nodes))
		}
	})
}
