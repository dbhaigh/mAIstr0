package orchestrator

import (
	"context"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/memory"
)

func (s *Server) taskHistorySyncLoop(ctx context.Context) {
	s.syncerComponent().taskHistorySyncLoop(ctx)
}

func (s *Server) syncTaskHistory() {
	s.syncerComponent().syncTaskHistory()
}

func (s *Server) recoverTaskHistory() {
	s.syncerComponent().recoverTaskHistory()
}

func (s *Server) mergeMemorySnapshot(snapshot memory.Snapshot, source string) error {
	return s.syncerComponent().mergeMemorySnapshot(snapshot, source)
}

func (s *Server) memorySnapshotForPeer(version int, watermarks map[string]uint64) memory.Snapshot {
	return s.syncerComponent().memorySnapshotForPeer(version, watermarks)
}

func (s *Server) syncerComponent() syncerAPI {
	if s.syncer != nil {
		return s.syncer
	}
	var isLeader func() bool
	if s.election != nil {
		isLeader = s.isElectedOrchestrator
	}
	var client taskHistoryClient
	if s.nodeClient != nil {
		client = s.nodeClient.sync
	}
	syncer := newSyncerComponent(s.registry, s.tasks, client, s.mem, isLeader, s.background.Go)
	syncer.maxBytes = s.maxSyncBytes()
	return syncer
}

func (s *Server) maxSyncBytes() int64 {
	if s.syncMaxBytes > 0 {
		return s.syncMaxBytes
	}
	return config.DefaultSyncMaxBytes
}
