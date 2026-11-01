package nodeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/tasksync"
)

const maxTaskSyncBodyBytes int64 = config.DefaultSyncMaxBytes

func (a *Agent) handleTaskSync(w http.ResponseWriter, r *http.Request) {
	peerID := taskSyncPeerID(r)
	if peerID == "" || !a.isClusterMember(peerID) {
		http.Error(w, "task sync requires a paired cluster member", http.StatusForbidden)
		return
	}
	tasks, err := decodeTaskSync(w, r, a.cfg.SyncMaxBytes)
	if err != nil {
		http.Error(w, "invalid task sync payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.orchestratorLink.tasks.MergeFromReplicaFrom(tasks, "task:"+peerID); err != nil {
		http.Error(w, "could not merge task history: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleTaskReplica(w http.ResponseWriter, r *http.Request) {
	peerID := taskSyncPeerID(r)
	coordinatorID := a.currentElectedOrchestratorID()
	if peerID == "" || peerID != coordinatorID {
		http.Error(w, "task replica update requires the current coordinator", http.StatusForbidden)
		return
	}
	tasks, err := decodeTaskSync(w, r, a.cfg.SyncMaxBytes)
	if err != nil {
		http.Error(w, "invalid task replica payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.orchestratorLink.tasks.ReplaceFromSyncFrom(tasks, "task:"+peerID); err != nil {
		http.Error(w, "could not apply task replica: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleTaskCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := a.orchestratorLink.tasks.Get(id); !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	cancelled, err := a.orchestratorLink.tasks.CancelChecked(id)
	if err != nil {
		http.Error(w, "could not persist cancellation: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !cancelled {
		http.Error(w, "task is already complete", http.StatusConflict)
		return
	}
	task, _ := a.orchestratorLink.tasks.Get(id)
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

func decodeTaskSync(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]*taskmgr.Task, error) {
	version, _, err := memory.ReadSyncRequestHeaders(r)
	if err != nil {
		return nil, err
	}
	body, err := memory.DecodeSyncBody(
		http.MaxBytesReader(w, r.Body, maxBytes),
		r.Header.Get("Content-Encoding"),
		version,
		maxBytes,
	)
	if err != nil {
		return nil, err
	}
	var tasks []*taskmgr.Task
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func fetchTaskSnapshot(client tasksync.Client, endpoint string, requestWatermarks map[string]uint64, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	return fetchTaskSnapshotWithContext(context.Background(), client, endpoint, requestWatermarks, maxBytes)
}

func fetchTaskSnapshotWithContext(ctx context.Context, client tasksync.Client, endpoint string, requestWatermarks map[string]uint64, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	tasks, snapshot, err := (tasksync.Transfer{MaxBytes: maxBytes, RemoteName: "peer"}).Fetch(
		ctx, client, endpoint, requestWatermarks,
	)
	return tasks, snapshot.Version, snapshot.Watermarks, err
}

func postTaskSnapshotPages(client tasksync.Client, endpoint string, tasks []*taskmgr.Task, watermarks map[string]uint64, maxBytes int64, peerVersion int) error {
	return postTaskSnapshotPagesWithContext(context.Background(), client, endpoint, tasks, watermarks, maxBytes, peerVersion)
}

func postTaskSnapshotPagesWithContext(ctx context.Context, client tasksync.Client, endpoint string, tasks []*taskmgr.Task, watermarks map[string]uint64, maxBytes int64, peerVersion int) error {
	return (tasksync.Transfer{MaxBytes: maxBytes, RemoteName: "peer"}).Dispatch(
		ctx, client, endpoint, tasks, watermarks, peerVersion,
	)
}
