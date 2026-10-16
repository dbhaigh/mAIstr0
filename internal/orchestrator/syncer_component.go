package orchestrator

import (
	"net/http"
	"sync"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type syncerAPI interface {
	taskHistorySyncLoop()
	syncTaskHistory()
	recoverTaskHistory()
	mergeMemorySnapshot(memory.Snapshot, string) error
	memorySnapshotForPeer(int, map[string]uint64) memory.Snapshot
}

type taskHistoryClient interface {
	Do(*http.Request) (*http.Response, error)
}

type syncerComponent struct {
	registry *cluster.Registry
	tasks    *taskmgr.Manager
	client   taskHistoryClient
	memory   memory.Store
	isLeader func() bool

	// syncMu serializes history passes and is outermost relative to registry/task/store operations; those operations never call back into the syncer.
	syncMu sync.Mutex
	// recoveryOnce ensures initial task-history recovery is launched only once.
	recoveryOnce sync.Once
}

func newSyncerComponent(registry *cluster.Registry, tasks *taskmgr.Manager, client taskHistoryClient, store memory.Store, isLeader func() bool) *syncerComponent {
	return &syncerComponent{registry: registry, tasks: tasks, client: client, memory: store, isLeader: isLeader}
}

func (s *syncerComponent) shouldSync() bool {
	return s.isLeader == nil || s.isLeader()
}

func (s *syncerComponent) mergeMemorySnapshot(snapshot memory.Snapshot, source string) error {
	if s.memory == nil {
		return nil
	}
	return s.memory.MergeSnapshotFrom(snapshot, source)
}

func (s *syncerComponent) memorySnapshotForPeer(version int, watermarks map[string]uint64) memory.Snapshot {
	if s.memory == nil {
		return memory.Snapshot{SyncVersion: memory.SyncProtocolVersion}
	}
	return memory.SnapshotForPeer(s.memory, version, watermarks)
}
