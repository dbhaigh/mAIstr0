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
		var watermarks map[string]uint64
		if s.mem != nil {
			watermarks = taskmgr.FilterTaskWatermarks(s.mem.PeerWatermarks(node.ID))
		}
		tasks, version, watermarks, err := fetchReplicatedTaskHistory(
			s.nodeClient.client,
			strings.TrimRight(node.Address, "/")+"/tasks",
			watermarks,
		)
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
		err := postTaskHistoryPages(
			s.nodeClient.client,
			strings.TrimRight(node.Address, "/")+"/tasks/replica",
			tasks,
			taskmgr.SnapshotWatermarks(s.tasks.All()),
			maxReplicatedTaskHistoryBytes,
		)
		if err != nil {
			log.Printf("orchestrator: task history replication to %s failed: %v", node.ID, err)
			continue
		}
		if s.mem != nil && fetched && memory.SupportsIncrementalSync(version) {
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

func fetchReplicatedTaskHistory(client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, requestWatermarks map[string]uint64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	tasks := make([]*taskmgr.Task, 0)
	var cursor, upper map[string]uint64
	var responseWatermarks map[string]uint64
	for {
		request, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, 0, nil, err
		}
		if err := memory.SetSyncRequestHeaders(request, requestWatermarks); err != nil {
			return nil, 0, nil, err
		}
		if cursor != nil {
			if err := memory.SetSyncPageRequestHeaders(request, cursor, upper); err != nil {
				return nil, 0, nil, err
			}
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, 0, nil, err
		}
		page, pageVersion, watermarks, nextCursor, more, err := decodeReplicatedTaskHistory(response)
		if err != nil {
			return nil, 0, nil, err
		}
		if more && !memory.SupportsSyncPages(pageVersion) {
			return nil, 0, nil, fmt.Errorf("node paginated task history without protocol version 2")
		}
		if more && len(nextCursor) == 0 {
			return nil, 0, nil, fmt.Errorf("node returned an empty task sync page cursor")
		}
		if more && len(watermarks) == 0 {
			return nil, 0, nil, fmt.Errorf("node omitted task sync watermarks during pagination")
		}
		tasks = append(tasks, page...)
		responseWatermarks = watermarks
		if !more {
			return tasks, pageVersion, responseWatermarks, nil
		}
		cursor = nextCursor
		upper = watermarks
	}
}

func decodeReplicatedTaskHistory(resp *http.Response) ([]*taskmgr.Task, int, map[string]uint64, map[string]uint64, bool, error) {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		response, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, 0, nil, nil, false, fmt.Errorf("node returned %s: %s", resp.Status, strings.TrimSpace(string(response)))
	}
	version, watermarks, err := memory.ReadSyncResponseHeaders(resp)
	if err != nil {
		return nil, 0, nil, nil, false, fmt.Errorf("read task sync response headers: %w", err)
	}
	cursor, more, err := memory.ReadSyncPageResponseHeader(resp)
	if err != nil {
		return nil, 0, nil, nil, false, fmt.Errorf("read task sync page header: %w", err)
	}
	var tasks []*taskmgr.Task
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxReplicatedTaskHistoryBytes+1))
	if err != nil {
		return nil, 0, nil, nil, false, err
	}
	if int64(len(body)) > maxReplicatedTaskHistoryBytes {
		return nil, 0, nil, nil, false, fmt.Errorf("node task snapshot exceeds the %d-byte sync limit", maxReplicatedTaskHistoryBytes)
	}
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, 0, nil, nil, false, err
	}
	return tasks, version, watermarks, cursor, more, nil
}

func postTaskHistoryPages(client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, tasks []*taskmgr.Task, watermarks map[string]uint64, maxBytes int64) error {
	pages, err := taskmgr.SplitTasks(tasks, maxBytes)
	if err != nil {
		return err
	}
	for _, page := range pages {
		body, err := json.Marshal(page)
		if err != nil {
			return err
		}
		if int64(len(body)) > maxBytes {
			return fmt.Errorf("task history page exceeds the %d-byte sync limit", maxBytes)
		}
		request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		if err := memory.SetSyncRequestHeaders(request, watermarks); err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			response.Body.Close()
			return fmt.Errorf("node returned %s: %s", response.Status, strings.TrimSpace(string(message)))
		}
		_, copyErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
