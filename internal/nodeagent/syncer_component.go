package nodeagent

import (
	"log"
	"net/http"
	"strings"
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
}

func newSyncerComponent(
	nodeID string,
	tasks *taskmgr.Manager,
	store memory.Store,
	peerClient func() *http.Client,
	memorySyncClient func() *http.Client,
	peers func() []cluster.NodeStatus,
	standaloneLeader func() bool,
) *syncerComponent {
	return &syncerComponent{
		nodeID: nodeID, tasks: tasks, memory: store,
		peerClient: peerClient, memorySyncClient: memorySyncClient,
		peers: peers, standaloneLeader: standaloneLeader,
	}
}

func (s *syncerComponent) taskHistorySyncLoop() {
	var backoff syncinterval.Backoff
	if syncinterval.Wait(backoff.Current(), s.tasks.Changes()) {
		backoff.Reset()
	}
	for {
		changes := s.tasks.Changes()
		before := s.tasks.Watermarks()
		s.syncPeerTaskHistory()
		backoff.Observe(syncinterval.WatermarksAdvanced(before, s.tasks.Watermarks()))
		if syncinterval.Wait(backoff.Current(), changes) {
			backoff.Reset()
		}
	}
}

func (s *syncerComponent) syncPeerTaskHistory() {
	if !s.standaloneLeader() {
		return
	}
	peers := s.peers()
	client := s.peerClient()
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
		)
		if err != nil {
			log.Printf("nodeagent: task history fetch from %s failed: %v", peer.ID, err)
			continue
		}
		if err := s.tasks.MergeFromReplicaFrom(tasks, "task:"+peer.ID); err != nil {
			log.Printf("nodeagent: task history import from %s failed: %v", peer.ID, err)
			continue
		}
		remoteSnapshots[peer.ID] = taskSyncSnapshot{version: version, watermarks: watermarks}
		if s.memory != nil {
			if err := s.memory.SetPeerWatermarks(peer.ID, watermarks); err != nil {
				log.Printf("nodeagent: task history watermark persistence for %s failed: %v", peer.ID, err)
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
			maxTaskSyncBodyBytes,
		)
		if err != nil {
			log.Printf("nodeagent: task history replication to %s failed: %v", peer.ID, err)
			continue
		}
		if s.memory != nil && fetched && memory.SupportsIncrementalSync(version) {
			if err := s.memory.SetPeerWatermarks(peer.ID, taskmgr.SnapshotWatermarks(tasks)); err != nil {
				log.Printf("nodeagent: task history watermark persistence for %s failed: %v", peer.ID, err)
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
		if syncinterval.Wait(backoff.Current(), changes) {
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
	for _, peer := range peers {
		if !peer.Healthy || peer.ID == s.nodeID || peer.Address == "" {
			continue
		}
		fetchStarted := time.Now()
		requestID := syncLogRequestID(nil)
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_start %s peer_id=%s body_bytes=unknown",
			syncLogFields(requestID, s.nodeID, time.Since(fetchStarted), "none"), peer.ID)
		snapshot, bodyBytes, err := fetchMemorySyncSnapshot(
			client,
			strings.TrimRight(peer.Address, "/")+"/memory/sync",
			memory.FilterMemoryWatermarks(s.memory.PeerWatermarks(peer.ID)),
		)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_failed %s peer_id=%s body_bytes=0 elapsed=%s stage=request error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(fetchStarted), "fetch"), peer.ID, time.Since(fetchStarted), err)
			log.Printf("FILE_ONLY: nodeagent: cluster memory fetch from %s failed: %v", peer.ID, err)
			continue
		}
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_complete %s peer_id=%s status=%d body_bytes=%d records=%d elapsed=%s",
			syncLogFields(requestID, s.nodeID, time.Since(fetchStarted), "none"), peer.ID, http.StatusOK, bodyBytes, memorySyncRecordCount(snapshot), time.Since(fetchStarted))
		mergeStarted := time.Now()
		if err := s.memory.MergeSnapshotFrom(snapshot, peer.ID); err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=peer->node event=merge_failed %s peer_id=%s body_bytes=%d elapsed=%s error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(mergeStarted), "merge"), peer.ID, bodyBytes, time.Since(mergeStarted), err)
			log.Printf("FILE_ONLY: nodeagent: cluster memory import from %s failed: %v", peer.ID, err)
		} else {
			remoteSnapshots[peer.ID] = snapshot
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
		sendStarted := time.Now()
		requestID := syncLogRequestID(nil)
		bodies, err := marshalMemorySnapshotPages(outgoing, maxMemorySyncBodyBytes, marshalMemorySnapshot)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_failed %s peer_id=%s stage=encode error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(sendStarted), "encode"), peer.ID, err)
			log.Printf("FILE_ONLY: nodeagent: cluster memory encode for %s failed: %v", peer.ID, err)
			continue
		}
		var totalBodyBytes int64
		for _, body := range bodies {
			totalBodyBytes += int64(len(body))
		}
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_start %s peer_id=%s body_bytes=%d pages=%d",
			syncLogFields(requestID, s.nodeID, time.Since(sendStarted), "none"), peer.ID, totalBodyBytes, len(bodies))
		sentBytes, err := postMemorySnapshotPages(
			client,
			strings.TrimRight(peer.Address, "/")+"/memory/sync",
			bodies,
			s.memory.Watermarks(),
		)
		if err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_failed %s peer_id=%s body_bytes=%d elapsed=%s error=%v",
				syncLogFields(requestID, s.nodeID, time.Since(sendStarted), "send"), peer.ID, sentBytes, time.Since(sendStarted), err)
			log.Printf("FILE_ONLY: nodeagent: cluster memory replication to %s failed: %v", peer.ID, err)
			continue
		}
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_complete %s peer_id=%s status=%d body_bytes=%d elapsed=%s",
			syncLogFields(requestID, s.nodeID, time.Since(sendStarted), "none"), peer.ID, http.StatusNoContent, sentBytes, time.Since(sendStarted))
		if fetched && memory.SupportsIncrementalSync(peerVersion) {
			if err := s.memory.SetPeerWatermarks(peer.ID, outgoing.Watermarks); err != nil {
				log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->peer event=watermark_persist_failed %s peer_id=%s error=%v",
					syncLogFields(requestID, s.nodeID, time.Since(sendStarted), "persistence"), peer.ID, err)
				log.Printf("FILE_ONLY: nodeagent: persist memory watermarks for %s failed: %v", peer.ID, err)
			}
		}
	}
}
