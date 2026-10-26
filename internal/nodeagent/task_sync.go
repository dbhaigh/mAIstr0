package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
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

type taskSyncSnapshot struct {
	version    int
	watermarks map[string]uint64
}

func fetchTaskSnapshot(client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, requestWatermarks map[string]uint64, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	return fetchTaskSnapshotWithContext(context.Background(), client, endpoint, requestWatermarks, maxBytes)
}

func fetchTaskSnapshotWithContext(ctx context.Context, client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, requestWatermarks map[string]uint64, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	tasks := make([]*taskmgr.Task, 0)
	var cursor, upper map[string]uint64
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, 0, nil, err
		}
		if err := memory.SetSyncRequestHeaders(request, requestWatermarks); err != nil {
			return nil, 0, nil, err
		}
		memory.SetSyncCompressionRequestHeader(request)
		if cursor != nil {
			if err := memory.SetSyncPageRequestHeaders(request, cursor, upper); err != nil {
				return nil, 0, nil, err
			}
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, 0, nil, err
		}
		page, version, watermarks, nextCursor, more, err := decodeTaskSnapshot(response, maxBytes)
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

func decodeTaskSnapshot(resp *http.Response, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, map[string]uint64, bool, error) {
	defer closeutil.Discard(resp.Body)
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
	if resp.StatusCode == http.StatusNoContent {
		if !memory.SupportsEmptySync(version) {
			return nil, 0, nil, nil, false, fmt.Errorf("peer returned an empty task response without protocol version %d", memory.SyncCompressionProtocolVersion)
		}
		return []*taskmgr.Task{}, version, watermarks, cursor, false, nil
	}
	var tasks []*taskmgr.Task
	body, err := memory.DecodeSyncBody(resp.Body, resp.Header.Get("Content-Encoding"), version, maxBytes)
	if err != nil {
		return nil, 0, nil, nil, false, err
	}
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, 0, nil, nil, false, err
	}
	return tasks, version, watermarks, cursor, more, nil
}

func postTaskSnapshotPages(client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, tasks []*taskmgr.Task, watermarks map[string]uint64, maxBytes int64, peerVersion int) error {
	return postTaskSnapshotPagesWithContext(context.Background(), client, endpoint, tasks, watermarks, maxBytes, peerVersion)
}

func postTaskSnapshotPagesWithContext(ctx context.Context, client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, tasks []*taskmgr.Task, watermarks map[string]uint64, maxBytes int64, peerVersion int) error {
	if len(tasks) == 0 {
		return nil
	}
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
		body, compressed, err := memory.EncodeSyncBody(body, peerVersion, true)
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		if compressed {
			request.Header.Set("Content-Encoding", "gzip")
		}
		if err := memory.SetSyncRequestHeaders(request, watermarks); err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
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
