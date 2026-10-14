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
		var watermarks map[string]uint64
		if a.mem != nil {
			watermarks = taskmgr.FilterTaskWatermarks(a.mem.PeerWatermarks(peer.ID))
		}
		tasks, version, watermarks, err := fetchTaskSnapshot(
			a.peerClient,
			strings.TrimRight(peer.Address, "/")+"/tasks",
			watermarks,
		)
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
		err := postTaskSnapshotPages(
			a.peerClient,
			strings.TrimRight(peer.Address, "/")+"/tasks/replica",
			tasks,
			taskmgr.SnapshotWatermarks(a.tasks.All()),
			maxTaskSyncBodyBytes,
		)
		if err != nil {
			log.Printf("nodeagent: task history replication to %s failed: %v", peer.ID, err)
			continue
		}
		if a.mem != nil && fetched && memory.SupportsIncrementalSync(version) {
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

func fetchTaskSnapshot(client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, requestWatermarks map[string]uint64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	tasks := make([]*taskmgr.Task, 0)
	var cursor, upper map[string]uint64
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
		page, version, watermarks, nextCursor, more, err := decodeTaskSnapshot(response)
		if err != nil {
			return nil, 0, nil, err
		}
		if more && !memory.SupportsSyncPages(version) {
			return nil, 0, nil, fmt.Errorf("peer paginated task history without protocol version 2")
		}
		if more && len(nextCursor) == 0 {
			return nil, 0, nil, fmt.Errorf("peer returned an empty task sync page cursor")
		}
		if more && len(watermarks) == 0 {
			return nil, 0, nil, fmt.Errorf("peer omitted task sync watermarks during pagination")
		}
		tasks = append(tasks, page...)
		if !more {
			return tasks, version, watermarks, nil
		}
		cursor = nextCursor
		upper = watermarks
	}
}

func decodeTaskSnapshot(resp *http.Response) ([]*taskmgr.Task, int, map[string]uint64, map[string]uint64, bool, error) {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		response, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, 0, nil, nil, false, fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(response)))
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTaskSyncBodyBytes+1))
	if err != nil {
		return nil, 0, nil, nil, false, err
	}
	if int64(len(body)) > maxTaskSyncBodyBytes {
		return nil, 0, nil, nil, false, fmt.Errorf("peer task snapshot exceeds the %d-byte sync limit", maxTaskSyncBodyBytes)
	}
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, 0, nil, nil, false, err
	}
	return tasks, version, watermarks, cursor, more, nil
}

func postTaskSnapshotPages(client interface {
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
			return fmt.Errorf("peer returned %s: %s", response.Status, strings.TrimSpace(string(message)))
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
