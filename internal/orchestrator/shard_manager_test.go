package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
	"github.com/maistr0/maistr0/internal/workpool"
)

func TestShardManagerStartsWorkersBeforeHeadAndDeletesInReverse(t *testing.T) {
	manager, _, engines, capacity, store := newTestShardManager(t, "")

	group, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	if group.State != ShardGroupReady {
		t.Fatalf("group state = %q, want ready", group.State)
	}
	if got, want := engines.events, []string{
		"start:rpc-1", "start:rpc-2", "start:llama-head",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("start order = %v, want %v", got, want)
	}
	if len(capacity.targets) != 2 {
		t.Fatalf("reserved targets = %v, want both member nodes", capacity.targets)
	}
	if len(group.Members) != 3 || group.Members[2].Role != "head" {
		t.Fatalf("group members = %+v, want two workers followed by head", group.Members)
	}

	if err := manager.Delete(context.Background(), group.ID); err != nil {
		t.Fatal(err)
	}
	wantEvents := []string{
		"start:rpc-1", "start:rpc-2", "start:llama-head",
		"stop:llama-head", "stop:rpc-2", "stop:rpc-1",
	}
	if !reflect.DeepEqual(engines.events, wantEvents) {
		t.Fatalf("lifecycle order = %v, want %v", engines.events, wantEvents)
	}
	if capacity.releases != 1 {
		t.Fatalf("reservation releases = %d, want 1", capacity.releases)
	}
	if _, ok := manager.Get(group.ID); ok {
		t.Fatal("deleted group remains in manager")
	}
	if len(store.groups) != 0 {
		t.Fatalf("persisted groups = %v, want none", store.groups)
	}
}

func TestShardManagerCleansUpPartialStartFailure(t *testing.T) {
	manager, _, engines, capacity, _ := newTestShardManager(t, "llama-head")

	group, err := manager.Create(context.Background(), testShardRequest())
	if err == nil {
		t.Fatal("Create returned no error for a failed head start")
	}
	if group.State != ShardGroupFailed {
		t.Fatalf("group state = %q, want failed", group.State)
	}
	wantEvents := []string{
		"start:rpc-1", "start:rpc-2", "start:llama-head",
		"stop:rpc-2", "stop:rpc-1",
	}
	if !reflect.DeepEqual(engines.events, wantEvents) {
		t.Fatalf("lifecycle order = %v, want %v", engines.events, wantEvents)
	}
	if capacity.releases != 1 {
		t.Fatalf("reservation releases = %d, want 1", capacity.releases)
	}
}

func TestShardManagerMarksLostMemberDegradedThenFailed(t *testing.T) {
	manager, registry, engines, capacity, _ := newTestShardManager(t, "")
	group, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	registry.setHealthy("node-2", false)

	if err := manager.CheckHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	degraded, ok := manager.Get(group.ID)
	if !ok || degraded.State != ShardGroupDegraded {
		t.Fatalf("first health result = %+v, want degraded", degraded)
	}
	if err := manager.CheckHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed, ok := manager.Get(group.ID)
	if !ok || failed.State != ShardGroupFailed {
		t.Fatalf("second health result = %+v, want failed", failed)
	}
	if capacity.releases != 1 {
		t.Fatalf("reservation releases = %d, want 1", capacity.releases)
	}
	if got, want := engines.events[len(engines.events)-3:], []string{
		"stop:llama-head", "stop:rpc-2", "stop:rpc-1",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("member-loss stop order = %v, want %v", got, want)
	}
}

func TestShardManagerDetectsManagedProcessLossOnHealthyNode(t *testing.T) {
	manager, _, engines, _, _ := newTestShardManager(t, "")
	group, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	engines.setState("https://node-2:7451", "rpc-2", "failed")

	if err := manager.CheckHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	degraded, _ := manager.Get(group.ID)
	if degraded.State != ShardGroupDegraded {
		t.Fatalf("first health state = %q, want degraded", degraded.State)
	}
	if err := manager.CheckHealth(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed, _ := manager.Get(group.ID)
	if failed.State != ShardGroupFailed {
		t.Fatalf("second health state = %q, want failed", failed.State)
	}
}

func TestShardManagerSerializesRequestsAndWaitsBeforeDelete(t *testing.T) {
	manager, _, engines, _, _ := newTestShardManager(t, "")
	group, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	releaseRequest, err := manager.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelWait()
	if _, err := manager.AcquireRequest(waitCtx, group.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second request error = %v, want deadline while the group slot is occupied", err)
	}

	deleteCtx, cancelDelete := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelDelete()
	if err := manager.Delete(deleteCtx, group.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delete error = %v, want deadline while inference is in flight", err)
	}
	if group, _ := manager.Get(group.ID); group.State != ShardGroupStopping {
		t.Fatalf("group state after interrupted delete = %q, want stopping", group.State)
	}
	if len(engines.events) != 3 {
		t.Fatalf("engines stopped before request completed: events = %v", engines.events)
	}

	releaseRequest()
	if err := manager.Delete(context.Background(), group.ID); err != nil {
		t.Fatal(err)
	}
	if len(engines.events) != 6 {
		t.Fatalf("teardown did not complete after request release: events = %v", engines.events)
	}
}

func TestGroupForModelPrefersActiveReplacementOverFailedGroup(t *testing.T) {
	manager, _, _, _, _ := newTestShardManager(t, "")
	previous, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.mutate(previous.ID, func(group *ShardGroup) {
		group.State = ShardGroupFailed
	}); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	selected, ok := manager.GroupForModel(testShardRequest().Model.Name)
	if !ok || selected.ID != replacement.ID || selected.State != ShardGroupReady {
		t.Fatalf("selected group = %+v, want ready replacement %s (old failed %s)", selected, replacement.ID, previous.ID)
	}
}

func TestShardManagerRecoveryRechecksManagedEngineReadiness(t *testing.T) {
	t.Run("ready engines restore group", func(t *testing.T) {
		manager, _, _, _, store := newTestShardManager(t, "")
		group, err := manager.Create(context.Background(), testShardRequest())
		if err != nil {
			t.Fatal(err)
		}
		recoveredEngines := newTestManagedEngines()
		for address, statuses := range recoveredEngines.statuses {
			for id, status := range statuses {
				status.State = "ready"
				statuses[id] = status
			}
			recoveredEngines.statuses[address] = statuses
		}
		recoveredCapacity := &testShardCapacity{}
		recovered, err := NewShardManager(store, newTestShardRegistry(), recoveredEngines, recoveredCapacity, testShardPlanner)
		if err != nil {
			t.Fatal(err)
		}
		loaded, _ := recovered.Get(group.ID)
		if loaded.State != ShardGroupDegraded {
			t.Fatalf("startup state = %q, want degraded pending verification", loaded.State)
		}
		if err := recovered.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		loaded, _ = recovered.Get(group.ID)
		if loaded.State != ShardGroupReady || recoveredCapacity.reservations != 1 {
			t.Fatalf("recovered group = %+v, reservations = %d", loaded, recoveredCapacity.reservations)
		}
	})

	t.Run("stopped engines fail recovery", func(t *testing.T) {
		manager, _, _, _, store := newTestShardManager(t, "")
		group, err := manager.Create(context.Background(), testShardRequest())
		if err != nil {
			t.Fatal(err)
		}
		stoppedEngines := newTestManagedEngines()
		for address, statuses := range stoppedEngines.statuses {
			for id, status := range statuses {
				status.State = "stopped"
				statuses[id] = status
			}
			stoppedEngines.statuses[address] = statuses
		}
		recovered, err := NewShardManager(store, newTestShardRegistry(), stoppedEngines, &testShardCapacity{}, testShardPlanner)
		if err != nil {
			t.Fatal(err)
		}
		if err := recovered.Recover(context.Background()); err == nil {
			t.Fatal("Recover succeeded although persisted member engines were stopped")
		}
		loaded, ok := recovered.Get(group.ID)
		if !ok || loaded.State != ShardGroupFailed {
			t.Fatalf("recovered group = %+v, want failed", loaded)
		}
		if len(stoppedEngines.events) == 0 {
			t.Fatal("recovery did not stop engines that failed readiness verification")
		}
	})
}

func TestShardGroupHandlersExposeCreateListGetAndDelete(t *testing.T) {
	manager, _, _, _, _ := newTestShardManager(t, "")
	server := &Server{shards: manager}
	body, err := json.Marshal(testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	createResponse := httptest.NewRecorder()
	server.handleCreateShardGroup(createResponse, httptest.NewRequest(http.MethodPost, "/api/shards", bytes.NewReader(body)))
	if createResponse.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", createResponse.Code, createResponse.Body.String())
	}
	var created ShardGroup
	if err := json.Unmarshal(createResponse.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	listResponse := httptest.NewRecorder()
	server.handleListShardGroups(listResponse, httptest.NewRequest(http.MethodGet, "/api/shards", nil))
	var listed struct {
		Groups []ShardGroup `json:"groups"`
	}
	if err := json.Unmarshal(listResponse.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if listResponse.Code != http.StatusOK || len(listed.Groups) != 1 || listed.Groups[0].ID != created.ID {
		t.Fatalf("list status/body = %d/%+v", listResponse.Code, listed)
	}

	getResponse := httptest.NewRecorder()
	getRequest := httptest.NewRequest(http.MethodGet, "/api/shards/"+created.ID, nil)
	getRequest.SetPathValue("id", created.ID)
	server.handleGetShardGroup(getResponse, getRequest)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getResponse.Code, getResponse.Body.String())
	}

	deleteResponse := httptest.NewRecorder()
	deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/shards/"+created.ID, nil)
	deleteRequest.SetPathValue("id", created.ID)
	server.handleDeleteShardGroup(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body = %s", deleteResponse.Code, deleteResponse.Body.String())
	}
}

func newTestShardManager(t *testing.T, failStart string) (*ShardManager, *testShardRegistry, *testManagedEngines, *testShardCapacity, *testShardStore) {
	t.Helper()
	registry := newTestShardRegistry()
	engines := newTestManagedEngines()
	engines.failStartID = failStart
	capacity := &testShardCapacity{}
	store := &testShardStore{groups: make(map[string][]byte)}
	manager, err := NewShardManager(store, registry, engines, capacity, testShardPlanner)
	if err != nil {
		t.Fatal(err)
	}
	return manager, registry, engines, capacity, store
}

func testShardRequest() CreateShardGroupRequest {
	return CreateShardGroupRequest{
		Model: shard.ModelProfile{
			Name: "test-model", Parameters: 1000, LayerCount: 2, HiddenSize: 4,
			KVHeads: 1, HeadDim: 4, Quantization: "q4_0", RequestedContext: 128,
		},
		Mode: shard.ModeLlamaCPPRPC,
	}
}

func testShardPlanner(model shard.ModelProfile, _ []shard.CandidateNode, policy shard.Policy) (shard.ShardPlan, error) {
	return shard.ShardPlan{
		Mode: policy.Mode, Model: model, RPC: "node-1:50052,node-2:50052", TensorSplit: "1,1",
		Nodes: []shard.NodePlan{
			{NodeID: "node-1", StartLayer: 0, EndLayer: 1, Devices: []shard.DeviceAllocation{{Kind: "gpu"}}},
			{NodeID: "node-2", StartLayer: 1, EndLayer: 2, Devices: []shard.DeviceAllocation{{Kind: "gpu"}}},
		},
		Rationale: []string{"test planner selected both nodes"},
	}, nil
}

type testShardStore struct {
	groups map[string][]byte
}

func (store *testShardStore) SaveShardGroup(id string, payload []byte) error {
	store.groups[id] = append([]byte(nil), payload...)
	return nil
}

func (store *testShardStore) ShardGroups() ([][]byte, error) {
	groups := make([][]byte, 0, len(store.groups))
	for _, payload := range store.groups {
		groups = append(groups, append([]byte(nil), payload...))
	}
	return groups, nil
}

func (store *testShardStore) DeleteShardGroup(id string) error {
	delete(store.groups, id)
	return nil
}

type testShardRegistry struct {
	mu    sync.Mutex
	nodes map[string]cluster.NodeStatus
}

func newTestShardRegistry() *testShardRegistry {
	return &testShardRegistry{nodes: map[string]cluster.NodeStatus{
		"node-1": {ID: "node-1", Address: "https://node-1:7451", Healthy: true, Hardware: hardware.Info{OS: "linux"}},
		"node-2": {ID: "node-2", Address: "https://node-2:7451", Healthy: true, Hardware: hardware.Info{OS: "linux"}},
	}}
}

func (registry *testShardRegistry) All() []cluster.NodeStatus {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	nodes := make([]cluster.NodeStatus, 0, len(registry.nodes))
	for _, node := range registry.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (registry *testShardRegistry) Get(id string) (cluster.NodeStatus, bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	node, ok := registry.nodes[id]
	return node, ok
}

func (registry *testShardRegistry) setHealthy(id string, healthy bool) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	node := registry.nodes[id]
	node.Healthy = healthy
	registry.nodes[id] = node
}

type testManagedEngines struct {
	mu          sync.Mutex
	statuses    map[string]map[string]supervisor.Status
	events      []string
	failStartID string
}

func newTestManagedEngines() *testManagedEngines {
	return &testManagedEngines{statuses: map[string]map[string]supervisor.Status{
		"https://node-1:7451": {
			"rpc-1":      {ID: "rpc-1", Kind: "ggml-rpc-server", State: "stopped", Endpoint: "tcp://127.0.0.1:50052"},
			"llama-head": {ID: "llama-head", Kind: "llama-server", State: "stopped", Endpoint: "http://127.0.0.1:8080"},
		},
		"https://node-2:7451": {
			"rpc-2": {ID: "rpc-2", Kind: "ggml-rpc-server", State: "stopped", Endpoint: "tcp://127.0.0.1:50052"},
		},
	}}
}

func (engines *testManagedEngines) List(_ context.Context, address string) ([]supervisor.Status, error) {
	engines.mu.Lock()
	defer engines.mu.Unlock()
	statuses := make([]supervisor.Status, 0, len(engines.statuses[address]))
	for _, status := range engines.statuses[address] {
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func (engines *testManagedEngines) Start(_ context.Context, address, id string, _ *supervisor.ShardPlan) (supervisor.Status, error) {
	engines.mu.Lock()
	defer engines.mu.Unlock()
	engines.events = append(engines.events, "start:"+id)
	if id == engines.failStartID {
		return supervisor.Status{}, fmt.Errorf("simulated start failure: %s", id)
	}
	status := engines.statuses[address][id]
	status.State = "ready"
	engines.statuses[address][id] = status
	return status, nil
}

func (engines *testManagedEngines) Stop(_ context.Context, address, id string) (supervisor.Status, error) {
	engines.mu.Lock()
	defer engines.mu.Unlock()
	engines.events = append(engines.events, "stop:"+id)
	status := engines.statuses[address][id]
	status.State = "stopped"
	engines.statuses[address][id] = status
	return status, nil
}

func (engines *testManagedEngines) setState(address, id, state string) {
	engines.mu.Lock()
	defer engines.mu.Unlock()
	status := engines.statuses[address][id]
	status.State = state
	engines.statuses[address][id] = status
}

type testShardCapacity struct {
	reservations int
	releases     int
	targets      []workpool.CapacityTarget
}

func (capacity *testShardCapacity) ReserveShardNodes(_ context.Context, targets []workpool.CapacityTarget) (ShardCapacityReservation, error) {
	capacity.reservations++
	capacity.targets = append([]workpool.CapacityTarget(nil), targets...)
	return &testShardReservation{capacity: capacity}, nil
}

type testShardReservation struct {
	capacity *testShardCapacity
	once     sync.Once
}

func (reservation *testShardReservation) Release() {
	reservation.once.Do(func() { reservation.capacity.releases++ })
}
