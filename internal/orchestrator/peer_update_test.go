package orchestrator

import (
	"context"
	"testing"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestPeerUpdateWaitsForShardGroupsAndOperations(t *testing.T) {
	manager, _, _, _, _ := newTestShardManager(t, "")
	tasks := taskmgr.NewManager()
	defer closeutil.Discard(tasks)
	server := &Server{shards: manager, tasks: tasks}
	if !server.UpdateIdle() {
		t.Fatal("empty coordinator is busy")
	}
	group, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}
	if server.UpdateIdle() {
		t.Fatal("active shard allowed an executable update")
	}
	if _, err := manager.Stop(context.Background(), group.ID); err != nil {
		t.Fatal(err)
	}
	if !server.UpdateIdle() {
		t.Fatal("stopped shard prevented idle update")
	}
	if err := manager.beginOperation(group.ID); err != nil {
		t.Fatal(err)
	}
	if server.UpdateIdle() {
		t.Fatal("shard mutation allowed an executable update")
	}
	manager.finishOperation(group.ID)
}
