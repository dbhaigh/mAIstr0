package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/shard"
	"net/http"
	"net/http/httptest"
	"strings"
)

func TestShardMembershipOmittedNodePersistsRetryableFailure(t *testing.T) {
	m, _, _, _, store := newTestShardManager(t, "")
	group, err := m.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	m.planner = func(model shard.ModelProfile, candidates []shard.CandidateNode, policy shard.Policy) (shard.ShardPlan, error) {
		plan, err := testShardPlanner(model, candidates, policy)
		plan.Nodes = plan.Nodes[:1]
		return plan, err
	}
	current, err := m.Reconfigure(context.Background(), group.ID, ShardMembershipRequest{NodeIDs: []string{"node-1", "node-2"}})
	if !errors.Is(err, errShardGroupUnavailable) || current.ID != group.ID || current.State != ShardGroupFailed {
		t.Fatalf("omitted node did not fail actual group: %+v %v", current, err)
	}
	var persisted ShardGroup
	if err := json.Unmarshal(store.groups[group.ID], &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.State != ShardGroupFailed || !strings.Contains(persisted.Error, "node-2") {
		t.Fatalf("failure was not persisted: %+v", persisted)
	}
	if len(store.groups) != 1 {
		t.Fatalf("unexpected persisted groups: %v", store.groups)
	}
	m.planner = testShardPlanner
	retried, err := m.Start(context.Background(), group.ID)
	if err != nil || retried.State != ShardGroupReady {
		t.Fatalf("failed reload cannot retry: %+v %v", retried, err)
	}
}

func TestShardMembershipUsesFreshFreedDeviceMemory(t *testing.T) {
	m, registry, engines, _, _ := newTestShardManager(t, "")
	for id, node := range registry.nodes {
		node.Hardware.Devices = []hardware.Device{{Kind: "gpu", Vendor: "nvidia", MemoryMB: 16 << 10, FreeMemoryMB: 16 << 10}}
		node.TelemetryAt = time.Now()
		registry.nodes[id] = node
	}
	m.planner = shard.Plan
	request := testShardRequest()
	request.Model.FileSizeBytes = 1 << 20
	group, err := m.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	engines.nodeStatuses = make(map[string]cluster.NodeStatus)
	for id, node := range registry.nodes {
		engines.nodeStatuses[node.Address] = node
		node.Hardware.Devices = []hardware.Device{{Kind: "gpu", Vendor: "nvidia", MemoryMB: 16 << 10, FreeMemoryMB: 1}}
		registry.nodes[id] = node
	}
	current, err := m.Reconfigure(context.Background(), group.ID, ShardMembershipRequest{NodeIDs: []string{group.Members[len(group.Members)-1].NodeID}})
	if err != nil || current.State != ShardGroupReady || len(current.Plan.Nodes) != 1 {
		t.Fatalf("failed to use released memory: %+v %v", current, err)
	}
}

func TestShardMembershipHTTPValidation(t *testing.T) {
	m, _, _, _, _ := newTestShardManager(t, "")
	group, err := m.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{shards: m}
	for _, body := range []string{`{}`, `{"node_ids":[]}`, `{"node_ids":["node-1"],"unexpected":1}`, `{"node_ids":["node-2"]}`} {
		request := httptest.NewRequest(http.MethodPost, "/api/shards/"+group.ID+"/members", strings.NewReader(body))
		request.SetPathValue("id", group.ID)
		response := httptest.NewRecorder()
		server.handleShardMembership(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d: %s", body, response.Code, response.Body.String())
		}
	}
}

func TestShardMembershipRemoveAndAddRetainsIdentityAndModel(t *testing.T) {
	m, _, engines, capacity, _ := newTestShardManager(t, "")
	m.planner = func(model shard.ModelProfile, candidates []shard.CandidateNode, policy shard.Policy) (shard.ShardPlan, error) {
		plan, _ := testShardPlanner(model, candidates, policy)
		selected := make(map[string]bool)
		for _, candidate := range candidates {
			selected[candidate.ID] = true
		}
		nodes := plan.Nodes[:0]
		for _, node := range plan.Nodes {
			if selected[node.NodeID] {
				nodes = append(nodes, node)
			}
		}
		plan.Nodes = nodes
		if len(nodes) == 1 {
			plan.Nodes[0].StartLayer = 0
			plan.Nodes[0].EndLayer = model.LayerCount
			plan.TensorSplit = "2"
		}
		return plan, nil
	}
	group, err := m.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]string{{"node-1"}, {"node-1", "node-2"}} {
		before := len(engines.events)
		reloaded, err := m.Reconfigure(context.Background(), group.ID, ShardMembershipRequest{NodeIDs: ids})
		if err != nil {
			t.Fatal(err)
		}
		if reloaded.ID != group.ID || !reflect.DeepEqual(reloaded.Model, group.Model) || !reloaded.CreatedAt.Equal(group.CreatedAt) ||
			!reflect.DeepEqual(reloaded.PinnedNodes, ids) || len(reloaded.Members) != len(ids)+1 || reloaded.State != ShardGroupReady {
			t.Fatalf("unexpected reconfigured group: %+v", reloaded)
		}
		events := engines.events[before:]
		if events[0] != "stop:llama-head" || events[len(events)-1] != "start:llama-head" {
			t.Fatalf("unsafe reload ordering: %v", events)
		}
	}
	if capacity.reservations != 3 || capacity.releases != 2 {
		t.Fatalf("capacity reservations=%d releases=%d", capacity.reservations, capacity.releases)
	}
}

func TestShardMembershipInvalidInputDoesNotStopRunningGroup(t *testing.T) {
	for _, ids := range [][]string{nil, {"node-2"}, {"node-1", "node-1"}, {"node-1", "missing"}, {" "}} {
		m, _, engines, _, _ := newTestShardManager(t, "")
		group, err := m.Create(context.Background(), testShardRequest())
		if err != nil {
			t.Fatal(err)
		}
		before := len(engines.events)
		if _, err := m.Reconfigure(context.Background(), group.ID, ShardMembershipRequest{NodeIDs: ids}); err == nil {
			t.Fatalf("accepted %v", ids)
		}
		current, _ := m.Get(group.ID)
		if current.State != ShardGroupReady || len(engines.events) != before {
			t.Fatalf("invalid edit stopped group: %v", ids)
		}
	}
}

func TestShardMembershipWaitCancellationPreservesOldPlacement(t *testing.T) {
	m, _, engines, _, _ := newTestShardManager(t, "")
	group, err := m.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	release, err := m.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	before := len(engines.events)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	current, err := m.Reconfigure(ctx, group.ID, ShardMembershipRequest{NodeIDs: []string{"node-1"}})
	if !errors.Is(err, context.DeadlineExceeded) || current.State != ShardGroupReady || len(engines.events) != before {
		t.Fatalf("canceled edit changed placement: %+v %v", current, err)
	}
}
