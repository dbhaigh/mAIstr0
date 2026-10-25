package orchestrator

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type syncerAPI interface {
	taskHistorySyncLoop(context.Context)
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
	// runRecovery is bound to the root lifecycle before the component is published.
	runRecovery func(func(context.Context))

	// syncing prevents overlapping history passes without holding a lock during peer I/O.
	syncing atomic.Bool
	// recoveryOnce ensures initial task-history recovery is launched only once.
	recoveryOnce sync.Once
	maxBytes     int64
}

func newSyncerComponent(registry *cluster.Registry, tasks *taskmgr.Manager, client taskHistoryClient, store memory.Store, isLeader func() bool, recoveryRunner ...func(func(context.Context))) *syncerComponent {
	syncer := &syncerComponent{
		registry: registry, tasks: tasks, client: client, memory: store,
		isLeader: isLeader, maxBytes: maxReplicatedTaskHistoryBytes,
	}
	if len(recoveryRunner) != 0 {
		syncer.runRecovery = recoveryRunner[0]
	}
	return syncer
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
