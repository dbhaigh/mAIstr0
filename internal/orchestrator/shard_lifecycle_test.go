package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
)

func TestShardGroupStopStartRestartRetainsConfiguration(t *testing.T) {
	m, _, engines, capacity, store := newTestShardManager(t, "")
	request := testShardRequest()
	request.PinnedNodes = []string{"node-1", "node-2"}
	request.HeadNodeID, request.HeadEngineID = "node-1", "llama-head"
	group, err := m.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := m.Stop(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != ShardGroupStopped || stopped.ID != group.ID || !reflect.DeepEqual(stopped.Model, group.Model) ||
		!reflect.DeepEqual(stopped.PinnedNodes, request.PinnedNodes) || !stopped.CreatedAt.Equal(group.CreatedAt) {
		t.Fatalf("stop lost configuration: %+v", stopped)
	}
	for _, member := range stopped.Members {
		if member.Started {
			t.Fatalf("stopped member still marked started: %+v", member)
		}
	}
	if _, err := m.AcquireRequest(context.Background(), group.ID); !errors.Is(err, errShardGroupUnavailable) {
		t.Fatalf("stopped group accepted inference: %v", err)
	}
	if capacity.releases != 1 || len(store.groups) != 1 {
		t.Fatalf("stop releases=%d persisted=%d", capacity.releases, len(store.groups))
	}
	count := len(engines.events)
	if _, err := m.Stop(context.Background(), group.ID); err != nil || len(engines.events) != count {
		t.Fatalf("stop was not idempotent: %v %v", err, engines.events)
	}
	plans := 0
	m.planner = func(model shard.ModelProfile, candidates []shard.CandidateNode, policy shard.Policy) (shard.ShardPlan, error) {
		plans++
		return testShardPlanner(model, candidates, policy)
	}
	started, err := m.Start(context.Background(), group.ID)
	if err != nil || started.State != ShardGroupReady || started.ID != group.ID {
		t.Fatalf("start=%+v err=%v", started, err)
	}
	count = len(engines.events)
	if _, err := m.Start(context.Background(), group.ID); err != nil || len(engines.events) != count {
		t.Fatalf("start was not idempotent: %v", err)
	}
	restarted, err := m.Restart(context.Background(), group.ID)
	if err != nil || restarted.State != ShardGroupReady || restarted.ID != group.ID {
		t.Fatalf("restart=%+v err=%v", restarted, err)
	}
	if want := []string{"stop:llama-head", "stop:rpc-2", "stop:rpc-1", "start:rpc-1", "start:rpc-2", "start:llama-head"}; !reflect.DeepEqual(engines.events[count:], want) {
		t.Fatalf("restart order = %v", engines.events[count:])
	}
	if plans != 2 || capacity.reservations != 3 || capacity.releases != 2 {
		t.Fatalf("replanning=%d reservations=%d releases=%d", plans, capacity.reservations, capacity.releases)
	}
}

func TestShardGroupRestartPreservesExplicitHead(t *testing.T) {
	m, _, engines, _, _ := newTestShardManager(t, "")
	engines.statuses["https://node-2:7451"]["selected-head"] = supervisor.Status{
		ID: "selected-head", Kind: "llama-server", State: "stopped", Endpoint: "http://127.0.0.1:8080",
	}
	request := testShardRequest()
	request.HeadNodeID, request.HeadEngineID = "node-2", "selected-head"
	group, err := m.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	group, err = m.Restart(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	head := group.Members[len(group.Members)-1]
	if head.EngineID != "selected-head" || head.NodeID != "node-2" {
		t.Fatalf("restart selected another head: %+v", head)
	}
}

func TestShardGroupStopCancellationAndOperationExclusion(t *testing.T) {
	m, _, engines, capacity, _ := newTestShardManager(t, "")
	group, err := m.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	release, err := m.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	stopped, err := m.Stop(ctx, group.ID)
	if !errors.Is(err, context.DeadlineExceeded) || stopped.State != ShardGroupReady || capacity.releases != 0 || len(engines.events) != 3 {
		t.Fatalf("canceled stop changed running group: %+v %v %v", stopped, err, engines.events)
	}
	if err := m.beginOperation(group.ID); err != nil {
		t.Fatal(err)
	}
	for _, action := range []func(context.Context, string) (ShardGroup, error){m.Start, m.Stop, m.Restart} {
		if _, err := action(context.Background(), group.ID); !errors.Is(err, errShardGroupOperation) {
			t.Fatalf("overlapping operation: %v", err)
		}
	}
	m.finishOperation(group.ID)
}

func TestShardGroupStopWaitsForInference(t *testing.T) {
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
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _, err := m.Stop(ctx, group.ID); done <- err }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, _ := m.Get(group.ID)
		if snapshot.State == ShardGroupStopping {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("stop did not enter stopping state")
		}
	}
	engines.mu.Lock()
	count := len(engines.events)
	engines.mu.Unlock()
	if count != 3 {
		t.Fatal("engines stopped before active inference drained")
	}
	if _, err := m.AcquireRequest(context.Background(), group.ID); !errors.Is(err, errShardGroupUnavailable) {
		t.Fatalf("new inference admitted: %v", err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestShardGroupLifecycleFailureAndRetry(t *testing.T) {
	m, _, engines, capacity, _ := newTestShardManager(t, "llama-head")
	group, err := m.Create(context.Background(), testShardRequest())
	if err == nil {
		t.Fatal("expected head startup failure")
	}
	engines.failStartID = ""
	group, err = m.Start(context.Background(), group.ID)
	if err != nil || group.State != ShardGroupReady {
		t.Fatalf("retry=%+v err=%v", group, err)
	}
	engines.failStopID = "llama-head"
	releases := capacity.releases
	failed, err := m.Restart(context.Background(), group.ID)
	if err == nil || failed.State != ShardGroupFailed || capacity.releases != releases {
		t.Fatalf("stop failure did not retain reservation: %+v %v", failed, err)
	}
	if !failed.Members[len(failed.Members)-1].Started {
		t.Fatal("failed stop lost running head flag")
	}
	engines.failStopID = ""
	started, err := m.Start(context.Background(), group.ID)
	if err != nil || started.State != ShardGroupReady {
		t.Fatalf("retry after cleanup: %+v %v", started, err)
	}
}

func TestShardGroupStoppedRecoveryAndShutdown(t *testing.T) {
	for _, state := range []ShardGroupState{ShardGroupStopped, ShardGroupStopping} {
		t.Run(string(state), func(t *testing.T) {
			m, registry, engines, _, store := newTestShardManager(t, "")
			group, err := m.Create(context.Background(), testShardRequest())
			if err != nil {
				t.Fatal(err)
			}
			if state == ShardGroupStopped {
				if _, err := m.Stop(context.Background(), group.ID); err != nil {
					t.Fatal(err)
				}
			} else if err := m.mutate(group.ID, func(g *ShardGroup) { g.State = ShardGroupStopping }); err != nil {
				t.Fatal(err)
			}
			capacity := &testShardCapacity{}
			recovered, err := NewShardManager(store, registry, engines, capacity, testShardPlanner)
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := recovered.CheckHealth(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := recovered.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			snapshot, ok := recovered.Get(group.ID)
			if !ok || snapshot.State != ShardGroupStopped || capacity.reservations != 0 {
				t.Fatalf("stopped recovery=%+v reserved=%d", snapshot, capacity.reservations)
			}
			started, err := recovered.Start(context.Background(), group.ID)
			if err != nil || started.State != ShardGroupReady {
				t.Fatalf("start recovered group: %+v %v", started, err)
			}
		})
	}
}

func TestShardGroupRejectsSharedManagedEngines(t *testing.T) {
	m, _, engines, _, _ := newTestShardManager(t, "")
	first, err := m.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	request := testShardRequest()
	request.Model.Name = "other-model"
	other, err := m.Create(context.Background(), request)
	if !errors.Is(err, errShardGroupConflict) || other.State != ShardGroupFailed || len(engines.events) != 3 {
		t.Fatalf("shared engines were altered: %+v %v %v", other, err, engines.events)
	}
	if _, err := m.Stop(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start(context.Background(), other.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start(context.Background(), first.ID); !errors.Is(err, errShardGroupConflict) {
		t.Fatalf("restart hijacked other group engines: %v", err)
	}
}

func TestShardGroupActionHandlers(t *testing.T) {
	m, _, _, _, _ := newTestShardManager(t, "")
	group, err := m.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{shards: m}
	for _, test := range []struct {
		action, id, body string
		code             int
		state            ShardGroupState
	}{
		{"stop", group.ID, "", 200, ShardGroupStopped},
		{"start", group.ID, "", 200, ShardGroupReady},
		{"restart", group.ID, "", 200, ShardGroupReady},
		{"start", group.ID, "{}", 400, ""},
		{"unknown", group.ID, "", 404, ""},
		{"start", "missing", "", 404, ""},
	} {
		t.Run(test.action+test.id+test.body, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/api/shards/"+test.id+"/"+test.action, strings.NewReader(test.body))
			r.SetPathValue("id", test.id)
			w := httptest.NewRecorder()
			s.handleShardGroupAction(w, r, test.action)
			if w.Code != test.code {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if test.code == 200 {
				var got ShardGroup
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.State != test.state || got.ID != group.ID {
					t.Fatalf("response=%+v err=%v", got, err)
				}
			}
		})
	}
}
