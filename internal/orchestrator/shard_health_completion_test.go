package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type healthCompletionStore struct {
	ShardGroupStore
	saving  chan struct{}
	release chan struct{}
	err     error
}

func (s *healthCompletionStore) SaveShardGroup(id string, payload []byte) error {
	var group ShardGroup
	if err := json.Unmarshal(payload, &group); err != nil {
		return err
	}
	if group.State != ShardGroupFailed {
		return s.ShardGroupStore.SaveShardGroup(id, payload)
	}
	close(s.saving)
	<-s.release
	if s.err != nil {
		return s.err
	}
	return s.ShardGroupStore.SaveShardGroup(id, payload)
}

func TestShardHealthFailureBecomesObservableWithOperationReleased(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "persisted"},
		{name: "persistence failure", err: errors.New("test disk failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, _, engines, _, store := newTestShardManager(t, "")
			group, err := manager.Create(context.Background(), testShardRequest())
			if err != nil {
				t.Fatal(err)
			}
			engines.setState("https://node-2:7451", "rpc-2", "failed")
			if err := manager.CheckHealth(context.Background()); err != nil {
				t.Fatal(err)
			}
			blocked := &healthCompletionStore{
				ShardGroupStore: store, saving: make(chan struct{}), release: make(chan struct{}), err: tc.err,
			}
			release := sync.OnceFunc(func() { close(blocked.release) })
			defer release()
			manager.store = blocked
			done := make(chan error, 1)
			go func() { done <- manager.CheckHealth(context.Background()) }()
			select {
			case <-blocked.saving:
			case <-time.After(2 * time.Second):
				t.Fatal("health teardown did not reach persistence")
			}
			current, _ := manager.Get(group.ID)
			if current.State != ShardGroupDegraded {
				t.Fatalf("state before persistence completed = %s, want degraded", current.State)
			}
			release()
			select {
			case err := <-done:
				if !errors.Is(err, tc.err) {
					t.Fatalf("health error = %v, want %v", err, tc.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("health teardown did not finish")
			}
			current, _ = manager.Get(group.ID)
			manager.mu.RLock()
			busy := manager.operations[group.ID]
			manager.mu.RUnlock()
			if current.State != ShardGroupFailed || busy {
				t.Fatalf("terminal group state = %s, operation claimed = %t", current.State, busy)
			}
			manager.store = store
			replacement, err := manager.Create(context.Background(), testShardRequest())
			if err != nil {
				t.Fatalf("cannot immediately replace failed group: %v", err)
			}
			if replacement.ID == group.ID || replacement.State != ShardGroupReady {
				t.Fatalf("replacement = %+v", replacement)
			}
		})
	}
}
