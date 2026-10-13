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
	for _, node := range nodes {
		if !node.Healthy || node.Address == "" {
			continue
		}
		resp, err := s.client.Get(strings.TrimRight(node.Address, "/") + "/tasks")
		if err != nil {
			log.Printf("orchestrator: task history fetch from %s failed: %v", node.ID, err)
			continue
		}
		tasks, err := decodeReplicatedTaskHistory(resp)
		if err != nil {
			log.Printf("orchestrator: task history fetch from %s failed: %v", node.ID, err)
			continue
		}
		if err := s.tasks.MergeFromReplica(tasks); err != nil {
			log.Printf("orchestrator: task history import from %s failed: %v", node.ID, err)
		}
	}

	body, err := json.Marshal(s.tasks.All())
	if err != nil {
		log.Printf("orchestrator: task history encode failed: %v", err)
		return
	}
	for _, node := range nodes {
		if !node.Healthy || node.Address == "" {
			continue
		}
		req, err := http.NewRequest(http.MethodPost, strings.TrimRight(node.Address, "/")+"/tasks/replica", bytes.NewReader(body))
		if err != nil {
			log.Printf("orchestrator: task history request for %s failed: %v", node.ID, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.client.Do(req)
		if err != nil {
			log.Printf("orchestrator: task history replication to %s failed: %v", node.ID, err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("orchestrator: task history replication to %s rejected: %s", node.ID, resp.Status)
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

func decodeReplicatedTaskHistory(resp *http.Response) ([]*taskmgr.Task, error) {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		response, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("node returned %s: %s", resp.Status, strings.TrimSpace(string(response)))
	}
	var tasks []*taskmgr.Task
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxReplicatedTaskHistoryBytes))
	if err := decoder.Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}
