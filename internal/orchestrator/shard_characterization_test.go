package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
	"github.com/maistr0/maistr0/internal/workpool"
)

// shardCharBlockingEngines blocks Start for one engine until released, so tests
// can observe a group while its create/start operation is still in progress.
type shardCharBlockingEngines struct {
	*testManagedEngines
	blockID string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newShardCharBlockingEngines(blockID string) *shardCharBlockingEngines {
	return &shardCharBlockingEngines{
		testManagedEngines: newTestManagedEngines(),
		blockID:            blockID,
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
	}
}

func (e *shardCharBlockingEngines) Start(ctx context.Context, address, id string, plan *supervisor.ShardPlan) (supervisor.Status, error) {
	if id == e.blockID {
		e.once.Do(func() { close(e.entered) })
		select {
		case <-e.release:
		case <-ctx.Done():
			return supervisor.Status{}, ctx.Err()
		}
	}
	return e.testManagedEngines.Start(ctx, address, id, plan)
}

// shardCharNotReadyEngines reports a successful start call whose status is not ready.
type shardCharNotReadyEngines struct {
	*testManagedEngines
	notReadyID string
}

func (e *shardCharNotReadyEngines) Start(ctx context.Context, address, id string, plan *supervisor.ShardPlan) (supervisor.Status, error) {
	status, err := e.testManagedEngines.Start(ctx, address, id, plan)
	if id == e.notReadyID {
		status.State = "starting"
	}
	return status, err
}

type shardCharFailingCapacity struct {
	calls int
	err   error
}

func (c *shardCharFailingCapacity) ReserveShardNodes(context.Context, []workpool.CapacityTarget) (ShardCapacityReservation, error) {
	c.calls++
	return nil, c.err
}

func shardCharPersisted(t *testing.T, store *testShardStore, id string) ShardGroup {
	t.Helper()
	payload, ok := store.groups[id]
	if !ok {
		t.Fatalf("shard group %s is not persisted", id)
	}
	var group ShardGroup
	if err := json.Unmarshal(payload, &group); err != nil {
		t.Fatal(err)
	}
	return group
}

func shardCharAssertFailedAndClean(t *testing.T, manager *ShardManager, store *testShardStore, group ShardGroup) {
	t.Helper()
	if group.State != ShardGroupFailed || group.Error == "" {
		t.Fatalf("returned group = %+v, want failed with an error", group)
	}
	persisted := shardCharPersisted(t, store, group.ID)
	if persisted.State != ShardGroupFailed || persisted.Error != group.Error {
		t.Fatalf("persisted group state/error = %q/%q, want failed/%q", persisted.State, persisted.Error, group.Error)
	}
	for _, member := range persisted.Members {
		if member.Started {
			t.Fatalf("persisted member still marked started: %+v", member)
		}
	}
	manager.mu.RLock()
	_, reserved := manager.reservations[group.ID]
	busy := manager.operations[group.ID]
	verified := manager.verified[group.ID]
	manager.mu.RUnlock()
	if reserved || busy || verified {
		t.Fatalf("failed group retains reservation=%t operation=%t verified=%t", reserved, busy, verified)
	}
	if _, err := manager.AcquireRequest(context.Background(), group.ID); !errors.Is(err, errShardGroupUnavailable) {
		t.Fatalf("failed group admitted a request: %v", err)
	}
}

func TestShardCreateInsufficientMemoryFailsWithoutReservations(t *testing.T) {
	manager, registry, engines, capacity, store := newTestShardManager(t, "")
	registry.mu.Lock()
	for id, node := range registry.nodes {
		node.Hardware.Devices = []hardware.Device{{Kind: "gpu", Vendor: "nvidia", MemoryMB: 4096, FreeMemoryMB: 3072}}
		node.TelemetryAt = time.Now()
		registry.nodes[id] = node
	}
	registry.mu.Unlock()
	manager.planner = shard.Plan
	request := testShardRequest()
	request.Model.FileSizeBytes = 64 << 30

	group, err := manager.Create(context.Background(), request)
	var refusal *shard.Refusal
	if !errors.As(err, &refusal) || refusal.Code != shard.RefusalInsufficientMemory {
		t.Fatalf("Create() error = %v, want insufficient-memory refusal", err)
	}
	shardCharAssertFailedAndClean(t, manager, store, group)
	if capacity.reservations != 0 || capacity.releases != 0 || len(engines.events) != 0 {
		t.Fatalf("refused plan reserved=%d released=%d events=%v", capacity.reservations, capacity.releases, engines.events)
	}
	if len(group.Members) != 0 {
		t.Fatalf("refused plan recorded members: %+v", group.Members)
	}
	if !strings.Contains(strings.Join(group.Rationale, "\n"), "group startup failed") {
		t.Fatalf("refusal rationale was not recorded: %v", group.Rationale)
	}
}

func TestShardCreateMemberStartFailureStopsStartedMembersInReverse(t *testing.T) {
	for _, test := range []struct {
		name       string
		failStart  string
		notReady   string
		wantEvents []string
		wantError  string
	}{
		{
			name:       "first worker fails",
			failStart:  "rpc-1",
			wantEvents: []string{"start:rpc-1"},
			wantError:  "start worker engine rpc-1 on node node-1",
		},
		{
			name:       "second worker fails after first started",
			failStart:  "rpc-2",
			wantEvents: []string{"start:rpc-1", "start:rpc-2", "stop:rpc-1"},
			wantError:  "start worker engine rpc-2 on node node-2",
		},
		{
			name:       "head fails after all workers started",
			failStart:  "llama-head",
			wantEvents: []string{"start:rpc-1", "start:rpc-2", "start:llama-head", "stop:rpc-2", "stop:rpc-1"},
			wantError:  "start head engine llama-head on node node-1",
		},
		{
			// Regression: a start call that succeeds but reports a non-ready status
			// (for example, a crash followed by supervisor auto-restart) must still
			// stop that member, newest first, before earlier members.
			name:       "worker reports non-ready status",
			notReady:   "rpc-2",
			wantEvents: []string{"start:rpc-1", "start:rpc-2", "stop:rpc-2", "stop:rpc-1"},
			wantError:  "managed engine rpc-2 on node node-2 did not report ready",
		},
		{
			name:       "head reports non-ready status",
			notReady:   "llama-head",
			wantEvents: []string{"start:rpc-1", "start:rpc-2", "start:llama-head", "stop:llama-head", "stop:rpc-2", "stop:rpc-1"},
			wantError:  "managed engine llama-head on node node-1 did not report ready",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			engines := newTestManagedEngines()
			engines.failStartID = test.failStart
			capacity := &testShardCapacity{}
			store := &testShardStore{groups: make(map[string][]byte)}
			manager, err := NewShardManager(store, newTestShardRegistry(),
				&shardCharNotReadyEngines{testManagedEngines: engines, notReadyID: test.notReady}, capacity, testShardPlanner)
			if err != nil {
				t.Fatal(err)
			}

			group, err := manager.Create(context.Background(), testShardRequest())
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Create() error = %v, want %q", err, test.wantError)
			}
			if !reflect.DeepEqual(engines.events, test.wantEvents) {
				t.Fatalf("lifecycle events = %v, want %v", engines.events, test.wantEvents)
			}
			if capacity.reservations != 1 || capacity.releases != 1 {
				t.Fatalf("reservations=%d releases=%d, want exactly one of each", capacity.reservations, capacity.releases)
			}
			shardCharAssertFailedAndClean(t, manager, store, group)
			if !strings.Contains(group.Error, test.wantError) {
				t.Fatalf("group error = %q, want %q", group.Error, test.wantError)
			}
		})
	}
}

func TestShardCreateUnavailableMemberFailsBeforeStartingEngines(t *testing.T) {
	for _, test := range []struct {
		name            string
		pinned          []string
		unhealthy       string
		capacityErr     error
		wantUnavailable bool
		wantReserves    int
	}{
		{
			name:            "pinned member is unhealthy",
			pinned:          []string{"node-1", "node-2"},
			unhealthy:       "node-2",
			wantUnavailable: true,
		},
		{
			// The test planner still selects node-2; member construction rejects it
			// with an untyped error after planning.
			name:      "planned member is unhealthy",
			unhealthy: "node-2",
		},
		{
			name:            "member capacity cannot be reserved",
			capacityErr:     errors.New("a shard member is no longer available"),
			wantUnavailable: true,
			wantReserves:    1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := newTestShardRegistry()
			if test.unhealthy != "" {
				registry.setHealthy(test.unhealthy, false)
			}
			engines := newTestManagedEngines()
			store := &testShardStore{groups: make(map[string][]byte)}
			var capacity ShardCapacityReserver = &testShardCapacity{}
			failing := &shardCharFailingCapacity{err: test.capacityErr}
			if test.capacityErr != nil {
				capacity = failing
			}
			manager, err := NewShardManager(store, registry, engines, capacity, testShardPlanner)
			if err != nil {
				t.Fatal(err)
			}
			request := testShardRequest()
			request.PinnedNodes = test.pinned

			group, err := manager.Create(context.Background(), request)
			if err == nil {
				t.Fatal("Create() succeeded with an unavailable member")
			}
			if got := errors.Is(err, errShardGroupUnavailable); got != test.wantUnavailable {
				t.Fatalf("Create() error = %v, errors.Is(unavailable) = %t, want %t", err, got, test.wantUnavailable)
			}
			shardCharAssertFailedAndClean(t, manager, store, group)
			if len(engines.events) != 0 {
				t.Fatalf("unavailable member started engines: %v", engines.events)
			}
			reserves := failing.calls
			if fake, ok := capacity.(*testShardCapacity); ok {
				reserves = fake.reservations
			}
			if reserves != test.wantReserves {
				t.Fatalf("reservation attempts = %d, want %d", reserves, test.wantReserves)
			}
		})
	}
}

func TestShardRecoveryFromPersistedStateReverifiesMembers(t *testing.T) {
	allStopEvents := []string{"stop:llama-head", "stop:rpc-2", "stop:rpc-1"}
	for _, test := range []struct {
		name          string
		persist       func(t *testing.T, manager *ShardManager, id string)
		unhealthy     string
		capacityErr   error
		wantState     ShardGroupState
		wantErr       bool
		wantErrorText string
		wantEvents    []string
		wantReserved  bool
	}{
		{
			name:         "ready group with ready members is restored",
			wantState:    ShardGroupReady,
			wantReserved: true,
		},
		{
			name:          "unavailable member fails recovery and force-stops every member",
			unhealthy:     "node-2",
			wantState:     ShardGroupFailed,
			wantErr:       true,
			wantErrorText: "one or more managed engines are not ready",
			wantEvents:    allStopEvents,
		},
		{
			name:          "capacity reservation failure fails recovery",
			capacityErr:   errors.New("node capacity is unavailable"),
			wantState:     ShardGroupFailed,
			wantErr:       true,
			wantErrorText: "reserve recovered member capacity",
			wantEvents:    allStopEvents,
		},
		{
			name: "group persisted mid-start is torn down",
			persist: func(t *testing.T, manager *ShardManager, id string) {
				if err := manager.mutate(id, func(group *ShardGroup) {
					group.State = ShardGroupStartingHead
					group.Members[len(group.Members)-1].Started = false
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := manager.engines.Stop(context.Background(), "https://node-1:7451", "llama-head"); err != nil {
					t.Fatal(err)
				}
			},
			wantState:     ShardGroupFailed,
			wantErr:       true,
			wantErrorText: "one or more managed engines are not ready",
			wantEvents:    allStopEvents,
		},
		{
			name: "failed group stays failed without engine calls",
			persist: func(t *testing.T, manager *ShardManager, id string) {
				if err := manager.mutate(id, func(group *ShardGroup) {
					group.State = ShardGroupFailed
					group.Error = "previous failure"
				}); err != nil {
					t.Fatal(err)
				}
			},
			wantState:     ShardGroupFailed,
			wantErrorText: "previous failure",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager, _, engines, _, store := newTestShardManager(t, "")
			group, err := manager.Create(context.Background(), testShardRequest())
			if err != nil {
				t.Fatal(err)
			}
			if test.persist != nil {
				test.persist(t, manager, group.ID)
			}
			engines.mu.Lock()
			engines.events = nil
			engines.mu.Unlock()

			registry := newTestShardRegistry()
			if test.unhealthy != "" {
				registry.setHealthy(test.unhealthy, false)
			}
			var capacity ShardCapacityReserver = &testShardCapacity{}
			if test.capacityErr != nil {
				capacity = &shardCharFailingCapacity{err: test.capacityErr}
			}
			recovered, err := NewShardManager(store, registry, engines, capacity, testShardPlanner)
			if err != nil {
				t.Fatal(err)
			}
			if loaded, _ := recovered.Get(group.ID); test.persist == nil && loaded.State != ShardGroupDegraded {
				t.Fatalf("loaded state = %q, want degraded pending verification", loaded.State)
			}
			err = recovered.Recover(context.Background())
			if (err != nil) != test.wantErr {
				t.Fatalf("Recover() error = %v, wantErr %t", err, test.wantErr)
			}
			current, _ := recovered.Get(group.ID)
			if current.State != test.wantState || !strings.Contains(current.Error, test.wantErrorText) {
				t.Fatalf("recovered state/error = %q/%q, want %q containing %q", current.State, current.Error, test.wantState, test.wantErrorText)
			}
			if persisted := shardCharPersisted(t, store, group.ID); persisted.State != test.wantState {
				t.Fatalf("persisted state = %q, want %q", persisted.State, test.wantState)
			}
			if got := shardCharEvents(engines); !reflect.DeepEqual(got, test.wantEvents) && (len(got) != 0 || len(test.wantEvents) != 0) {
				t.Fatalf("recovery engine events = %v, want %v", got, test.wantEvents)
			}
			recovered.mu.RLock()
			_, reserved := recovered.reservations[group.ID]
			verified := recovered.verified[group.ID]
			_, pending := recovered.recovery[group.ID]
			recovered.mu.RUnlock()
			if reserved != test.wantReserved || !verified || pending {
				t.Fatalf("reserved=%t verified=%t pending=%t, want reserved=%t verified=true pending=false",
					reserved, verified, pending, test.wantReserved)
			}
			if test.wantState == ShardGroupFailed {
				if _, err := recovered.AcquireRequest(context.Background(), group.ID); !errors.Is(err, errShardGroupUnavailable) {
					t.Fatalf("failed recovered group admitted a request: %v", err)
				}
			}
		})
	}
}

func shardCharEvents(engines *testManagedEngines) []string {
	engines.mu.Lock()
	defer engines.mu.Unlock()
	return append([]string(nil), engines.events...)
}

func TestShardStopAndDeleteIdempotency(t *testing.T) {
	t.Run("delete twice reports not found without engine calls", func(t *testing.T) {
		manager, _, engines, capacity, store := newTestShardManager(t, "")
		group, err := manager.Create(context.Background(), testShardRequest())
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Delete(context.Background(), group.ID); err != nil {
			t.Fatal(err)
		}
		events := len(engines.events)
		if err := manager.Delete(context.Background(), group.ID); !errors.Is(err, errShardGroupNotFound) {
			t.Fatalf("second Delete() error = %v, want not found", err)
		}
		for _, action := range []func(context.Context, string) (ShardGroup, error){manager.Start, manager.Stop, manager.Restart} {
			if _, err := action(context.Background(), group.ID); !errors.Is(err, errShardGroupNotFound) {
				t.Fatalf("action on deleted group error = %v, want not found", err)
			}
		}
		if _, err := manager.AcquireRequest(context.Background(), group.ID); !errors.Is(err, errShardGroupNotFound) {
			t.Fatalf("AcquireRequest() on deleted group error = %v, want not found", err)
		}
		if len(engines.events) != events || capacity.releases != 1 || len(store.groups) != 0 {
			t.Fatalf("events=%v releases=%d persisted=%d", engines.events, capacity.releases, len(store.groups))
		}
	})

	t.Run("delete stopped group stops nothing and releases nothing again", func(t *testing.T) {
		manager, _, engines, capacity, store := newTestShardManager(t, "")
		group, err := manager.Create(context.Background(), testShardRequest())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Stop(context.Background(), group.ID); err != nil {
			t.Fatal(err)
		}
		events := len(engines.events)
		if err := manager.Delete(context.Background(), group.ID); err != nil {
			t.Fatal(err)
		}
		if len(engines.events) != events || capacity.releases != 1 || len(store.groups) != 0 {
			t.Fatalf("events=%v releases=%d persisted=%d", engines.events[events:], capacity.releases, len(store.groups))
		}
	})

	t.Run("stop failed group without started members marks it stopped", func(t *testing.T) {
		manager, _, engines, capacity, store := newTestShardManager(t, "rpc-1")
		group, err := manager.Create(context.Background(), testShardRequest())
		if err == nil {
			t.Fatal("expected start failure")
		}
		events := len(engines.events)
		stopped, err := manager.Stop(context.Background(), group.ID)
		if err != nil || stopped.State != ShardGroupStopped || stopped.Error != "" {
			t.Fatalf("Stop() = %+v, %v; want stopped without error", stopped, err)
		}
		if len(engines.events) != events || capacity.releases != 1 {
			t.Fatalf("stopping a failed group called engines %v or released again (%d)", engines.events[events:], capacity.releases)
		}
		if persisted := shardCharPersisted(t, store, group.ID); persisted.State != ShardGroupStopped {
			t.Fatalf("persisted state = %q, want stopped", persisted.State)
		}
	})
}

func TestShardOperationsConflictWhileStartInProgress(t *testing.T) {
	for _, test := range []struct {
		name           string
		startAfterStop bool
	}{
		{name: "create"},
		{name: "start after stop", startAfterStop: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engines := newShardCharBlockingEngines("")
			capacity := &testShardCapacity{}
			store := &testShardStore{groups: make(map[string][]byte)}
			manager, err := NewShardManager(store, newTestShardRegistry(), engines, capacity, testShardPlanner)
			if err != nil {
				t.Fatal(err)
			}
			var id string
			if test.startAfterStop {
				group, err := manager.Create(context.Background(), testShardRequest())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := manager.Stop(context.Background(), group.ID); err != nil {
					t.Fatal(err)
				}
				id = group.ID
			}
			engines.blockID = "llama-head"

			type result struct {
				group ShardGroup
				err   error
			}
			done := make(chan result, 1)
			go func() {
				var group ShardGroup
				var err error
				if id == "" {
					group, err = manager.Create(context.Background(), testShardRequest())
				} else {
					group, err = manager.Start(context.Background(), id)
				}
				done <- result{group, err}
			}()
			select {
			case <-engines.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("start did not reach the head engine")
			}
			if id == "" {
				groups := manager.List()
				if len(groups) != 1 {
					t.Fatalf("groups during create = %+v", groups)
				}
				id = groups[0].ID
			}
			if current, _ := manager.Get(id); current.State != ShardGroupStartingHead {
				t.Fatalf("state while head starts = %q, want starting_head", current.State)
			}
			if err := manager.Delete(context.Background(), id); !errors.Is(err, errShardGroupOperation) {
				t.Fatalf("Delete() during start error = %v, want operation conflict", err)
			}
			for name, action := range map[string]func(context.Context, string) (ShardGroup, error){
				"start": manager.Start, "stop": manager.Stop, "restart": manager.Restart,
			} {
				if _, err := action(context.Background(), id); !errors.Is(err, errShardGroupOperation) {
					t.Fatalf("%s during start error = %v, want operation conflict", name, err)
				}
			}
			if _, err := manager.AcquireRequest(context.Background(), id); !errors.Is(err, errShardGroupUnavailable) {
				t.Fatalf("AcquireRequest() during start error = %v, want unavailable", err)
			}
			request := testShardRequest()
			if _, err := manager.Create(context.Background(), request); !errors.Is(err, errShardGroupConflict) {
				t.Fatalf("duplicate Create() during start error = %v, want conflict", err)
			}
			if current, _ := manager.Get(id); current.State != ShardGroupStartingHead {
				t.Fatalf("rejected operations changed state to %q", current.State)
			}

			close(engines.release)
			got := <-done
			if got.err != nil || got.group.State != ShardGroupReady {
				t.Fatalf("in-progress start result = %+v, %v", got.group, got.err)
			}
			if len(manager.List()) != 1 {
				t.Fatalf("rejected duplicate create left groups: %+v", manager.List())
			}
			if err := manager.Delete(context.Background(), id); err != nil {
				t.Fatalf("Delete() after start completed: %v", err)
			}
			if capacity.reservations != capacity.releases {
				t.Fatalf("reservations=%d releases=%d after delete", capacity.reservations, capacity.releases)
			}
		})
	}
}

func TestShardAcquireRequestReleaseIsSingleUseAcrossStop(t *testing.T) {
	manager, _, _, _, _ := newTestShardManager(t, "")
	group, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	assertSingleSlot := func(t *testing.T) {
		t.Helper()
		release, err := manager.AcquireRequest(context.Background(), group.ID)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		if _, err := manager.AcquireRequest(ctx, group.ID); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second concurrent AcquireRequest() error = %v, want deadline (single slot)", err)
		}
		release()
	}

	release, err := manager.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()
	assertSingleSlot(t)

	held, err := manager.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() {
		_, err := manager.Stop(context.Background(), group.ID)
		stopped <- err
	}()
	deadline := time.After(5 * time.Second)
	for current, _ := manager.Get(group.ID); current.State != ShardGroupStopping; current, _ = manager.Get(group.ID) {
		select {
		case <-deadline:
			t.Fatal("stop did not wait for the held request")
		case <-time.After(time.Millisecond):
		}
	}
	held()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	held()
	if _, err := manager.AcquireRequest(context.Background(), group.ID); !errors.Is(err, errShardGroupUnavailable) {
		t.Fatalf("AcquireRequest() after stop error = %v, want unavailable", err)
	}

	if _, err := manager.Start(context.Background(), group.ID); err != nil {
		t.Fatal(err)
	}
	assertSingleSlot(t)
}
