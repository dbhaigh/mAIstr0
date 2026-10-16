package orchestrator

import "github.com/maistr0/maistr0/internal/memory"

func (s *Server) taskHistorySyncLoop() {
	s.syncerComponent().taskHistorySyncLoop()
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
	return newSyncerComponent(s.registry, s.tasks, s.nodeClient, s.mem, isLeader)
}
