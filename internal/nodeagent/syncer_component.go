package nodeagent

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/syncinterval"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type syncerComponent struct {
	nodeID           string
	tasks            *taskmgr.Manager
	memory           memory.Store
	peerClient       func() *http.Client
	memorySyncClient func() *http.Client
	peers            func() []cluster.NodeStatus
	standaloneLeader func() bool
	maxSyncBytes     int64

	memoryBufferMu  sync.Mutex
	memoryBuffer    []memory.Payload
	memoryRecordIDs map[string]struct{}
	memoryFlushMu   sync.Mutex
}

func newSyncerComponent(
	nodeID string,
	tasks *taskmgr.Manager,
	store memory.Store,
	peerClient func() *http.Client,
	memorySyncClient func() *http.Client,
	peers func() []cluster.NodeStatus,
	maxSyncBytes int64,
	standaloneLeader func() bool,
) *syncerComponent {
	if maxSyncBytes <= 0 {
		maxSyncBytes = maxTaskSyncBodyBytes
	}
	return &syncerComponent{
		nodeID: nodeID, tasks: tasks, memory: store,
		peerClient: peerClient, memorySyncClient: memorySyncClient,
		peers: peers, maxSyncBytes: maxSyncBytes, standaloneLeader: standaloneLeader,
	}
}

func (s *syncerComponent) taskHistorySyncLoop() {
	var backoff syncinterval.Backoff
	if syncinterval.WaitDebounced(
		syncinterval.Jitter(backoff.Current()),
		syncinterval.DebounceInterval,
		s.tasks.Changes(),
		s.tasks.Changes,
	) {
		backoff.Reset()
	}
	for {
		changes := s.tasks.Changes()
		before := s.tasks.Watermarks()
		s.syncPeerTaskHistory()
		backoff.Observe(syncinterval.WatermarksAdvanced(before, s.tasks.Watermarks()))
		if syncinterval.WaitDebounced(
			syncinterval.Jitter(backoff.Current()),
			syncinterval.DebounceInterval,
			changes,
			s.tasks.Changes,
		) {
			backoff.Reset()
		}
	}
}

func (s *syncerComponent) syncPeerTaskHistory() {
	if !s.standaloneLeader() {
		return
	}
	peers := s.peers()
	client := s.memorySyncClient()
	remoteSnapshots := make(map[string]taskSyncSnapshot)
	for _, peer := range peers {
		if !peer.Healthy || peer.ID == s.nodeID || peer.Address == "" {
			continue
		}
		var watermarks map[string]uint64
		if s.memory != nil {
			watermarks = taskmgr.FilterTaskWatermarks(s.memory.PeerWatermarks(peer.ID))
		}
		tasks, version, watermarks, err := fetchTaskSnapshot(
			client,
			strings.TrimRight(peer.Address, "/")+"/tasks",
			watermarks,
			s.maxSyncBytes,
		)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: task history fetch from %s failed: %v", peer.ID, err)
			continue
		}
		if err := s.tasks.MergeFromReplicaFrom(tasks, "task:"+peer.ID); err != nil {
			log.Printf("FILE_ONLY: nodeagent: task history import from %s failed: %v", peer.ID, err)
			continue
		}
		remoteSnapshots[peer.ID] = taskSyncSnapshot{version: version, watermarks: watermarks}
		if s.memory != nil {
			if err := s.memory.SetPeerWatermarks(peer.ID, watermarks); err != nil {
				log.Printf("FILE_ONLY: nodeagent: task history watermark persistence for %s failed: %v", peer.ID, err)
			}
		}
	}

	for _, peer := range peers {
		if !peer.Healthy || peer.ID == s.nodeID || peer.Address == "" {
			continue
		}
		remote, fetched := remoteSnapshots[peer.ID]
		version := 0
		var watermarks map[string]uint64
		if fetched {
			version = remote.version
			watermarks = remote.watermarks
		}
		tasks := s.tasks.SnapshotForPeer(version, watermarks)
		err := postTaskSnapshotPages(
			client,
			strings.TrimRight(peer.Address, "/")+"/tasks/replica",
			tasks,
			taskmgr.SnapshotWatermarks(s.tasks.All()),
			s.maxSyncBytes,
			version,
		)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: task history replication to %s failed: %v", peer.ID, err)
			continue
		}
		if s.memory != nil && fetched && memory.SupportsIncrementalSync(version) {
			if err := s.memory.SetPeerWatermarks(peer.ID, taskmgr.SnapshotWatermarks(s.tasks.All())); err != nil {
				log.Printf("FILE_ONLY: nodeagent: task history watermark persistence for %s failed: %v", peer.ID, err)
			}
		}
	}
}

func (s *syncerComponent) syncPeerMemoryLoop() {
	var backoff syncinterval.Backoff
	for {
		changes := memorySyncChanges(s.memory)
		before := memorySyncWatermarks(s.memory)
		s.syncPeerMemory()
		backoff.Observe(syncinterval.WatermarksAdvanced(before, memorySyncWatermarks(s.memory)))
		if syncinterval.WaitDebounced(
			syncinterval.Jitter(backoff.Current()),
			syncinterval.DebounceInterval,
			changes,
			func() <-chan struct{} { return memorySyncChanges(s.memory) },
		) {
			backoff.Reset()
		}
	}
}

func (s *syncerComponent) syncPeerMemory() {
	if s.memory == nil || !s.standaloneLeader() {
		return
	}
	peers := s.peers()
	client := s.memorySyncClient()
	remoteSnapshots := make(map[string]memory.Snapshot)
	peerVersions := make(map[string]int)
	for _, peer := range peers {
		if !peer.Healthy || peer.ID == s.nodeID || peer.Address == "" {
			continue
		}
		fetchStarted := time.Now()
		requestID := syncLogRequestID(nil)
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_start %s peer_id=%s body_bytes=unknown",
			syncLogFields(requestID, s.nodeID, time.Since(fetchStarted), "none"), peer.ID)
		snapshot, bodyBytes, status, err := fetchMemorySyncSnapshot(
			client,
			strings.TrimRight(peer.Address, "/")+"/memory/sync",
			memory.FilterMemoryWatermarks(s.memory.PeerWatermarks(peer.ID)),
			s.maxSyncBytes,
		)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_failed %s peer_id=%s body_bytes=0 elapsed=%s stage=request error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(fetchStarted), "fetch"), peer.ID, time.Since(fetchStarted), err)
			log.Printf("FILE_ONLY: nodeagent: cluster memory fetch from %s failed: %v", peer.ID, err)
			continue
		}
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_complete %s peer_id=%s status=%d body_bytes=%d records=%d elapsed=%s",
			syncLogFields(requestID, s.nodeID, time.Since(fetchStarted), "none"), peer.ID, status, bodyBytes, memorySyncRecordCount(snapshot), time.Since(fetchStarted))
		mergeStarted := time.Now()
		if err := s.memory.MergeSnapshotFrom(snapshot, peer.ID); err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=merge_failed %s peer_id=%s body_bytes=%d elapsed=%s error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(mergeStarted), "merge"), peer.ID, bodyBytes, time.Since(mergeStarted), err)
			log.Printf("FILE_ONLY: nodeagent: cluster memory import from %s failed: %v", peer.ID, err)
		} else {
			remoteSnapshots[peer.ID] = snapshot
			peerVersions[peer.ID] = snapshot.SyncVersion
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=merge_complete %s peer_id=%s body_bytes=%d records=%d elapsed=%s",
				syncLogFields(requestID, s.nodeID, time.Since(mergeStarted), "none"), peer.ID, bodyBytes, memorySyncRecordCount(snapshot), time.Since(mergeStarted))
		}
	}

	for _, peer := range peers {
		if !peer.Healthy || peer.ID == s.nodeID || peer.Address == "" {
			continue
		}
		remoteSnapshot, fetched := remoteSnapshots[peer.ID]
		peerVersion := 0
		var peerWatermarks map[string]uint64
		if fetched {
			peerVersion = remoteSnapshot.SyncVersion
			peerWatermarks = remoteSnapshot.Watermarks
		}
		outgoing := memory.SnapshotForPeer(s.memory, peerVersion, peerWatermarks)
		if fetched && !memory.SupportsIncrementalSync(peerVersion) {
			filtered, err := memory.SnapshotDifference(outgoing, remoteSnapshot)
			if err != nil {
				log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_failed %s peer_id=%s stage=deduplicate error=%v",
					syncLogFields(syncLogRequestID(nil), s.nodeID, 0, "deduplicate"), peer.ID, err)
				continue
			}
			outgoing = filtered
		}
		payload, err := memory.PayloadFromSnapshot(outgoing)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_failed %s peer_id=%s stage=buffer error=%v",
				syncLogFields(syncLogRequestID(nil), s.nodeID, 0, "buffer"), peer.ID, err)
			continue
		}
		s.bufferMemoryPayload(payload)
	}
	if err := s.flushToCluster(peerVersions); err != nil {
		log.Printf("FILE_ONLY: nodeagent: cluster memory batch flush failed: %v", err)
	}
}

func (s *syncerComponent) bufferMemoryPayload(payload memory.Payload) {
	if len(payload.RecordIDs) == 0 {
		return
	}
	s.memoryBufferMu.Lock()
	defer s.memoryBufferMu.Unlock()
	if s.memoryRecordIDs == nil {
		s.memoryRecordIDs = make(map[string]struct{})
	}
	filtered, err := memory.PayloadDifference(payload, s.memoryRecordIDs)
	if err != nil {
		log.Printf("FILE_ONLY: nodeagent: memory sync event=buffer_failed node_id=%s error=%v", s.nodeID, err)
		return
	}
	if len(filtered.RecordIDs) == 0 {
		return
	}
	for _, id := range filtered.RecordIDs {
		s.memoryRecordIDs[id] = struct{}{}
	}
	s.memoryBuffer = append(s.memoryBuffer, filtered)
}

func (s *syncerComponent) restoreMemoryPayloads(payloads []memory.Payload) {
	s.memoryBufferMu.Lock()
	defer s.memoryBufferMu.Unlock()
	if s.memoryRecordIDs == nil {
		s.memoryRecordIDs = make(map[string]struct{})
	}
	for _, payload := range payloads {
		for _, id := range payload.RecordIDs {
			s.memoryRecordIDs[id] = struct{}{}
		}
	}
	s.memoryBuffer = append(payloads, s.memoryBuffer...)
}

func (s *syncerComponent) FlushToCluster() error {
	return s.flushToCluster(nil)
}

func (s *syncerComponent) flushToCluster(peerVersions map[string]int) error {
	s.memoryFlushMu.Lock()
	defer s.memoryFlushMu.Unlock()

	s.memoryBufferMu.Lock()
	if len(s.memoryBuffer) == 0 {
		s.memoryBufferMu.Unlock()
		return nil
	}
	payloads := s.memoryBuffer
	s.memoryBuffer = nil
	s.memoryBufferMu.Unlock()

	requeue := func(err error) error {
		s.restoreMemoryPayloads(payloads)
		return err
	}
	complete := func() {
		s.memoryBufferMu.Lock()
		defer s.memoryBufferMu.Unlock()
		for _, payload := range payloads {
			for _, id := range payload.RecordIDs {
				delete(s.memoryRecordIDs, id)
			}
		}
	}
	if s.memory == nil {
		return requeue(errors.New("memory store unavailable"))
	}
	if s.memorySyncClient == nil {
		return requeue(errors.New("memory sync HTTP client unavailable"))
	}
	if s.peers == nil {
		return requeue(errors.New("cluster peer provider unavailable"))
	}
	batch, err := memory.CombinePayloads(payloads)
	if err != nil {
		return requeue(fmt.Errorf("combine memory sync batch: %w", err))
	}
	if len(batch.RecordIDs) == 0 {
		complete()
		return nil
	}
	bodies, err := marshalMemorySnapshotPages(batch.Snapshot, s.maxSyncBytes, func(snapshot memory.Snapshot) ([]byte, error) {
		return marshalMemorySnapshotWithLimit(snapshot, s.maxSyncBytes)
	})
	if err != nil {
		return requeue(fmt.Errorf("encode memory sync batch: %w", err))
	}
	var totalBodyBytes int64
	for _, body := range bodies {
		totalBodyBytes += int64(len(body))
	}
	peers := s.peers()
	client := s.memorySyncClient()
	if client == nil {
		return requeue(errors.New("memory sync HTTP client unavailable"))
	}
	successfulPeers := 0
	var flushErrors []error
	for _, peer := range peers {
		if !peer.Healthy || peer.ID == s.nodeID || peer.Address == "" {
			continue
		}
		started := time.Now()
		requestID := syncLogRequestID(nil)
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_start %s peer_id=%s body_bytes=%d pages=%d",
			syncLogFields(requestID, s.nodeID, time.Since(started), "none"), peer.ID, totalBodyBytes, len(bodies))
		sentBytes, err := postMemorySnapshotPages(
			client,
			strings.TrimRight(peer.Address, "/")+"/memory/sync",
			bodies,
			s.memory.Watermarks(),
			peerVersions[peer.ID],
		)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_failed %s peer_id=%s body_bytes=%d elapsed=%s error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(started), "send"), peer.ID, sentBytes, time.Since(started), err)
			flushErrors = append(flushErrors, fmt.Errorf("replicate memory to %s: %w", peer.ID, err))
			continue
		}
		successfulPeers++
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_complete %s peer_id=%s status=%d body_bytes=%d elapsed=%s records=%d",
			syncLogFields(requestID, s.nodeID, time.Since(started), "none"), peer.ID, http.StatusNoContent, sentBytes, time.Since(started), len(batch.RecordIDs))
		if err := s.memory.SetPeerWatermarks(peer.ID, batch.Snapshot.Watermarks); err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=watermark_persist_failed %s peer_id=%s error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(started), "persistence"), peer.ID, err)
			log.Printf("FILE_ONLY: nodeagent: persist memory watermarks for %s failed: %v", peer.ID, err)
		}
	}
	if successfulPeers == 0 {
		if len(flushErrors) == 0 {
			flushErrors = append(flushErrors, errors.New("no healthy cluster peers available for memory sync"))
		}
		return requeue(errors.Join(flushErrors...))
	}
	if len(flushErrors) > 0 {
		return requeue(errors.Join(flushErrors...))
	}
	complete()
	return nil
}
