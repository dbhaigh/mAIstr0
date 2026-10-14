package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

const maxReplicatedTaskHistoryBytes int64 = 64 << 20

func (s *Server) taskHistorySyncLoop() {
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	<-timer.C
	s.syncTaskHistory()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.syncTaskHistory()
	}
}

func (s *Server) syncTaskHistory() {
	s.taskHistorySyncMu.Lock()
	defer s.taskHistorySyncMu.Unlock()

	nodes := s.registry.Active()
	remoteSnapshots := make(map[string]taskSyncSnapshot)
	for _, node := range nodes {
		if !node.Healthy || node.Address == "" {
			continue
		}
		request, err := http.NewRequest(http.MethodGet, strings.TrimRight(node.Address, "/")+"/tasks", nil)
		if err == nil {
			var watermarks map[string]uint64
			if s.mem != nil {
				watermarks = taskmgr.FilterTaskWatermarks(s.mem.PeerWatermarks(node.ID))
			}
			err = memory.SetSyncRequestHeaders(request, watermarks)
		}
		if err != nil {
			log.Printf("orchestrator: task history request for %s failed: %v", node.ID, err)
			continue
		}
		resp, err := s.nodeClient.client.Do(request)
		if err != nil {
			log.Printf("orchestrator: task history fetch from %s failed: %v", node.ID, err)
			continue
		}
		tasks, version, watermarks, err := decodeReplicatedTaskHistory(resp)
		if err != nil {
			log.Printf("orchestrator: task history fetch from %s failed: %v", node.ID, err)
			continue
		}
		if err := s.tasks.MergeFromReplicaFrom(tasks, "task:"+node.ID); err != nil {
			log.Printf("orchestrator: task history import from %s failed: %v", node.ID, err)
			continue
		}
		remoteSnapshots[node.ID] = taskSyncSnapshot{version: version, watermarks: watermarks}
		if s.mem != nil {
			if err := s.mem.SetPeerWatermarks(node.ID, watermarks); err != nil {
				log.Printf("orchestrator: task history watermark persistence for %s failed: %v", node.ID, err)
			}
		}
	}

	for _, node := range nodes {
		if !node.Healthy || node.Address == "" {
			continue
		}
		remote, fetched := remoteSnapshots[node.ID]
		version := 0
		var watermarks map[string]uint64
		if fetched {
			version = remote.version
			watermarks = remote.watermarks
		}
		tasks := s.tasks.SnapshotForPeer(version, watermarks)
		body, err := json.Marshal(tasks)
		if err != nil {
			log.Printf("orchestrator: task history encode for %s failed: %v", node.ID, err)
			continue
		}
		if int64(len(body)) > maxReplicatedTaskHistoryBytes {
			log.Printf("orchestrator: task history payload for %s exceeds the %d-byte sync limit", node.ID, maxReplicatedTaskHistoryBytes)
			continue
		}
		req, err := http.NewRequest(http.MethodPost, strings.TrimRight(node.Address, "/")+"/tasks/replica", bytes.NewReader(body))
		if err != nil {
			log.Printf("orchestrator: task history request for %s failed: %v", node.ID, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if err := memory.SetSyncRequestHeaders(req, taskmgr.SnapshotWatermarks(s.tasks.All())); err != nil {
			log.Printf("orchestrator: task history sync headers for %s failed: %v", node.ID, err)
			continue
		}
		resp, err := s.nodeClient.client.Do(req)
		if err != nil {
			log.Printf("orchestrator: task history replication to %s failed: %v", node.ID, err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("orchestrator: task history replication to %s rejected: %s", node.ID, resp.Status)
			continue
		}
		if s.mem != nil && fetched && version == memory.SyncProtocolVersion {
			if err := s.mem.SetPeerWatermarks(node.ID, taskmgr.SnapshotWatermarks(tasks)); err != nil {
				log.Printf("orchestrator: task history watermark persistence for %s failed: %v", node.ID, err)
			}
		}
	}
}

func (s *Server) recoverTaskHistory() {
	if len(s.registry.Active()) == 0 {
		return
	}
	s.taskHistoryRecovery.Do(func() {
		go s.syncTaskHistory()
	})
}

type taskSyncSnapshot struct {
	version    int
	watermarks map[string]uint64
}

func decodeReplicatedTaskHistory(resp *http.Response) ([]*taskmgr.Task, int, map[string]uint64, error) {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		response, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, 0, nil, fmt.Errorf("node returned %s: %s", resp.Status, strings.TrimSpace(string(response)))
	}
	version, watermarks, err := memory.ReadSyncResponseHeaders(resp)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("read task sync response headers: %w", err)
	}
	var tasks []*taskmgr.Task
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxReplicatedTaskHistoryBytes+1))
	if err != nil {
		return nil, 0, nil, err
	}
	if int64(len(body)) > maxReplicatedTaskHistoryBytes {
		return nil, 0, nil, fmt.Errorf("node task snapshot exceeds the %d-byte sync limit", maxReplicatedTaskHistoryBytes)
	}
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, 0, nil, err
	}
	return tasks, version, watermarks, nil
}
