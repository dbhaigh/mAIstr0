package nodeagent

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

const maxTaskSyncBodyBytes int64 = 64 << 20

func (a *Agent) handleTaskSync(w http.ResponseWriter, r *http.Request) {
	peerID := taskSyncPeerID(r)
	if peerID == "" || !a.isClusterMember(peerID) {
		http.Error(w, "task sync requires a paired cluster member", http.StatusForbidden)
		return
	}
	tasks, err := decodeTaskSync(w, r)
	if err != nil {
		http.Error(w, "invalid task sync payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.tasks.MergeFromReplicaFrom(tasks, "task:"+peerID); err != nil {
		http.Error(w, "could not merge task history: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleTaskReplica(w http.ResponseWriter, r *http.Request) {
	peerID := taskSyncPeerID(r)
	coordinatorID := a.mostCapableNode().ID
	if orchestratorID := a.getOrchestratorID(); orchestratorID != "" && peerID == orchestratorID {
		coordinatorID = peerID
	}
	if peerID == "" || peerID != coordinatorID {
		http.Error(w, "task replica update requires the current coordinator", http.StatusForbidden)
		return
	}
	tasks, err := decodeTaskSync(w, r)
	if err != nil {
		http.Error(w, "invalid task replica payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.tasks.ReplaceFromSyncFrom(tasks, "task:"+peerID); err != nil {
		http.Error(w, "could not apply task replica: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleTaskCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := a.tasks.Get(id); !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	cancelled, err := a.tasks.CancelChecked(id)
	if err != nil {
		http.Error(w, "could not persist cancellation: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !cancelled {
		http.Error(w, "task is already complete", http.StatusConflict)
		return
	}
	task, _ := a.tasks.Get(id)
	writeJSON(w, http.StatusOK, task)
}

func taskSyncPeerID(r *http.Request) string {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ""
	}
	return strings.TrimSpace(r.TLS.PeerCertificates[0].Subject.CommonName)
}

func (a *Agent) isClusterMember(id string) bool {
	if id == "" {
		return false
	}
	if id == a.getOrchestratorID() {
		return true
	}
	for _, peer := range a.Peers() {
		if peer.ID == id && peer.Healthy {
			return true
		}
	}
	return false
}

func decodeTaskSync(w http.ResponseWriter, r *http.Request) ([]*taskmgr.Task, error) {
	var tasks []*taskmgr.Task
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxTaskSyncBodyBytes))
	if err := decoder.Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

// taskHistorySyncLoop replicates task snapshots when this node is coordinating
// a standalone peer cluster. Joined clusters are synchronized by their
// orchestrator, which also owns task creation and execution.
func (a *Agent) taskHistorySyncLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		a.syncPeerTaskHistory()
	}
}

func (a *Agent) syncPeerTaskHistory() {
	if a.getOrchestratorAddr() != "" && a.getOrchestratorID() != "" {
		return
	}
	leader := a.mostCapableNode()
	if leader.ID != a.cfg.NodeID {
		return
	}
	peers := a.Peers()
	remoteSnapshots := make(map[string]taskSyncSnapshot)
	for _, peer := range peers {
		if !peer.Healthy || peer.ID == a.cfg.NodeID || peer.Address == "" {
			continue
		}
		request, err := http.NewRequest(http.MethodGet, strings.TrimRight(peer.Address, "/")+"/tasks", nil)
		if err == nil {
			var watermarks map[string]uint64
			if a.mem != nil {
				watermarks = taskmgr.FilterTaskWatermarks(a.mem.PeerWatermarks(peer.ID))
			}
			err = memory.SetSyncRequestHeaders(request, watermarks)
		}
		if err != nil {
			log.Printf("nodeagent: task history request for %s failed: %v", peer.ID, err)
			continue
		}
		resp, err := a.peerClient.Do(request)
		if err != nil {
			log.Printf("nodeagent: task history fetch from %s failed: %v", peer.ID, err)
			continue
		}
		tasks, version, watermarks, err := decodeTaskSnapshot(resp)
		if err != nil {
			log.Printf("nodeagent: task history fetch from %s failed: %v", peer.ID, err)
			continue
		}
		if err := a.tasks.MergeFromReplicaFrom(tasks, "task:"+peer.ID); err != nil {
			log.Printf("nodeagent: task history import from %s failed: %v", peer.ID, err)
			continue
		}
		remoteSnapshots[peer.ID] = taskSyncSnapshot{version: version, watermarks: watermarks}
		if a.mem != nil {
			if err := a.mem.SetPeerWatermarks(peer.ID, watermarks); err != nil {
				log.Printf("nodeagent: task history watermark persistence for %s failed: %v", peer.ID, err)
			}
		}
	}

	for _, peer := range peers {
		if !peer.Healthy || peer.ID == a.cfg.NodeID || peer.Address == "" {
			continue
		}
		remote, fetched := remoteSnapshots[peer.ID]
		version := 0
		var watermarks map[string]uint64
		if fetched {
			version = remote.version
			watermarks = remote.watermarks
		}
		tasks := a.tasks.SnapshotForPeer(version, watermarks)
		body, err := json.Marshal(tasks)
		if err != nil {
			log.Printf("nodeagent: task history encode for %s failed: %v", peer.ID, err)
			continue
		}
		if int64(len(body)) > maxTaskSyncBodyBytes {
			log.Printf("nodeagent: task history payload for %s exceeds the %d-byte sync limit", peer.ID, maxTaskSyncBodyBytes)
			continue
		}
		req, err := http.NewRequest(http.MethodPost, strings.TrimRight(peer.Address, "/")+"/tasks/replica", bytes.NewReader(body))
		if err != nil {
			log.Printf("nodeagent: task history request for %s failed: %v", peer.ID, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if err := memory.SetSyncRequestHeaders(req, taskmgr.SnapshotWatermarks(a.tasks.All())); err != nil {
			log.Printf("nodeagent: task history sync headers for %s failed: %v", peer.ID, err)
			continue
		}
		resp, err := a.peerClient.Do(req)
		if err != nil {
			log.Printf("nodeagent: task history replication to %s failed: %v", peer.ID, err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("nodeagent: task history replication to %s rejected: %s", peer.ID, resp.Status)
			continue
		}
		if a.mem != nil && fetched && version == memory.SyncProtocolVersion {
			if err := a.mem.SetPeerWatermarks(peer.ID, taskmgr.SnapshotWatermarks(tasks)); err != nil {
				log.Printf("nodeagent: task history watermark persistence for %s failed: %v", peer.ID, err)
			}
		}
	}
}

type taskSyncSnapshot struct {
	version    int
	watermarks map[string]uint64
}

func decodeTaskSnapshot(resp *http.Response) ([]*taskmgr.Task, int, map[string]uint64, error) {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		response, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, 0, nil, fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(response)))
	}
	version, watermarks, err := memory.ReadSyncResponseHeaders(resp)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("read task sync response headers: %w", err)
	}
	var tasks []*taskmgr.Task
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTaskSyncBodyBytes+1))
	if err != nil {
		return nil, 0, nil, err
	}
	if int64(len(body)) > maxTaskSyncBodyBytes {
		return nil, 0, nil, fmt.Errorf("peer task snapshot exceeds the %d-byte sync limit", maxTaskSyncBodyBytes)
	}
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, 0, nil, err
	}
	return tasks, version, watermarks, nil
}
