package orchestrator

import (
	"context"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/syncinterval"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/tasksync"
)

const maxReplicatedTaskHistoryBytes int64 = config.DefaultSyncMaxBytes

func (s *syncerComponent) taskHistorySyncLoop(ctx context.Context) {
	var backoff syncinterval.Backoff
	if syncinterval.WaitDebouncedContext(
		ctx,
		syncinterval.Jitter(3*time.Second),
		syncinterval.DebounceInterval,
		s.tasks.Changes(),
		s.tasks.Changes,
	) {
		backoff.Reset()
	}
	for ctx.Err() == nil {
		changes := s.tasks.Changes()
		if !s.shouldSync() {
			backoff.Reset()
			syncinterval.WaitDebouncedContext(
				ctx,
				syncinterval.Jitter(5*time.Second),
				syncinterval.DebounceInterval,
				changes,
				s.tasks.Changes,
			)
			continue
		}
		before := s.tasks.Watermarks()
		s.syncTaskHistoryWithContext(ctx)
		backoff.Observe(syncinterval.WatermarksAdvanced(before, s.tasks.Watermarks()))
		if syncinterval.WaitDebouncedContext(
			ctx,
			syncinterval.Jitter(backoff.Current()),
			syncinterval.DebounceInterval,
			changes,
			s.tasks.Changes,
		) {
			backoff.Reset()
		}
	}
}

func (s *syncerComponent) syncTaskHistory() {
	s.syncTaskHistoryWithContext(context.Background())
}

func (s *syncerComponent) syncTaskHistoryWithContext(ctx context.Context) {
	if ctx.Err() != nil || !s.shouldSync() {
		return
	}
	if !s.syncing.CompareAndSwap(false, true) {
		return
	}
	defer s.syncing.Store(false)
	if ctx.Err() != nil || !s.shouldSync() {
		return
	}

	nodes := s.registry.Active()
	remoteSnapshots := make(map[string]tasksync.Snapshot)
	for _, node := range nodes {
		if ctx.Err() != nil || !s.shouldSync() {
			return
		}
		if !node.Healthy || node.Address == "" {
			continue
		}
		var watermarks map[string]uint64
		if s.memory != nil {
			watermarks = taskmgr.FilterTaskWatermarks(s.memory.PeerWatermarks(node.ID))
		}
		requestID := syncLogRequestID(nil)
		started := time.Now()
		tasks, snapshot, err := (tasksync.Transfer{MaxBytes: s.maxBytes, RemoteName: "node"}).Fetch(
			ctx,
			s.client,
			strings.TrimRight(node.Address, "/")+"/tasks",
			watermarks,
		)
		if err != nil {
			logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=fetch_failed %s error=%v",
				syncLogFields(requestID, node.ID, time.Since(started), "fetch"), err)
			continue
		}
		version := snapshot.Version
		watermarks = snapshot.Watermarks
		if err := s.tasks.MergeFromReplicaFrom(tasks, "task:"+node.ID); err != nil {
			logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=import_failed %s error=%v",
				syncLogFields(requestID, node.ID, time.Since(started), "import"), err)
			continue
		}
		logging.FileInfof("orchestrator", "FILE_ONLY: orchestrator: task history sync event=fetch_complete %s records=%d version=%d",
			syncLogFields(requestID, node.ID, time.Since(started), "none"), len(tasks), version)
		remoteSnapshots[node.ID] = snapshot
		if s.memory != nil {
			if err := s.memory.SetPeerWatermarks(node.ID, watermarks); err != nil {
				logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=watermark_persist_failed %s error=%v",
					syncLogFields(requestID, node.ID, time.Since(started), "persistence"), err)
			}
		}
	}

	for _, node := range nodes {
		if ctx.Err() != nil || !s.shouldSync() {
			return
		}
		if !node.Healthy || node.Address == "" {
			continue
		}
		remote, fetched := remoteSnapshots[node.ID]
		version := 0
		var watermarks map[string]uint64
		if fetched {
			version = remote.Version
			watermarks = remote.Watermarks
		}
		tasks := s.tasks.SnapshotForPeer(version, watermarks)
		requestID := syncLogRequestID(nil)
		started := time.Now()
		err := postTaskHistoryPagesWithContext(
			ctx,
			s.client,
			strings.TrimRight(node.Address, "/")+"/tasks/replica",
			tasks,
			taskmgr.SnapshotWatermarks(s.tasks.All()),
			s.maxBytes,
			version,
		)
		if err != nil {
			logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=replicate_failed %s error=%v",
				syncLogFields(requestID, node.ID, time.Since(started), "replication"), err)
			continue
		}
		logging.FileInfof("orchestrator", "FILE_ONLY: orchestrator: task history sync event=replicate_complete %s records=%d",
			syncLogFields(requestID, node.ID, time.Since(started), "none"), len(tasks))
		if s.memory != nil && fetched && memory.SupportsIncrementalSync(version) {
			if err := s.memory.SetPeerWatermarks(node.ID, taskmgr.SnapshotWatermarks(s.tasks.All())); err != nil {
				logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=watermark_persist_failed %s error=%v",
					syncLogFields(requestID, node.ID, time.Since(started), "persistence"), err)
			}
		}
	}
}

func (s *syncerComponent) recoverTaskHistory() {
	if s.runRecovery == nil || !s.shouldSync() || len(s.registry.Active()) == 0 {
		return
	}
	s.recoveryOnce.Do(func() {
		s.runRecovery(s.syncTaskHistoryWithContext)
	})
}

func fetchReplicatedTaskHistoryWithContext(
	ctx context.Context,
	client tasksync.Client,
	endpoint string,
	requestWatermarks map[string]uint64,
	maxBytes int64,
) ([]*taskmgr.Task, int, map[string]uint64, error) {
	tasks, snapshot, err := (tasksync.Transfer{MaxBytes: maxBytes, RemoteName: "node"}).Fetch(
		ctx, client, endpoint, requestWatermarks,
	)
	return tasks, snapshot.Version, snapshot.Watermarks, err
}

func postTaskHistoryPagesWithContext(
	ctx context.Context,
	client tasksync.Client,
	endpoint string,
	tasks []*taskmgr.Task,
	watermarks map[string]uint64,
	maxBytes int64,
	peerVersion int,
) error {
	return (tasksync.Transfer{MaxBytes: maxBytes, RemoteName: "node"}).Dispatch(
		ctx, client, endpoint, tasks, watermarks, peerVersion,
	)
}
