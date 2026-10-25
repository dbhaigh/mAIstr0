package cluster

import (
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
