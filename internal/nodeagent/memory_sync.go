package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/memory"
)

const (
	maxMemorySyncBodyBytes int64 = config.DefaultSyncMaxBytes
)

type memorySyncCountingReader struct {
	io.ReadCloser
	bytesRead int64
}

func (r *memorySyncCountingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytesRead += int64(n)
	return n, err
}

type memorySyncCountingResponseWriter struct {
	http.ResponseWriter
	bytesWritten int64
	writeErr     error
}

func (w *memorySyncCountingResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.bytesWritten += int64(n)
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	return n, err
}

func (a *Agent) handlePeerMemorySyncGet(w http.ResponseWriter, r *http.Request) {
	peerID := taskSyncPeerID(r)
	if peerID == "" || !a.isClusterMember(peerID) {
		http.Error(w, "memory sync requires a paired cluster member", http.StatusForbidden)
		return
	}
	if a.mem == nil {
		http.Error(w, "memory store unavailable", http.StatusServiceUnavailable)
		return
	}
	peerVersion, watermarks, err := memory.ReadSyncRequestHeaders(r)
	if err != nil {
		http.Error(w, "invalid memory sync headers: "+err.Error(), http.StatusBadRequest)
		return
	}
	cursor, upper, err := memory.ReadSyncPageRequestHeaders(r)
	if err != nil {
		http.Error(w, "invalid memory sync page headers: "+err.Error(), http.StatusBadRequest)
		return
	}
	if cursor != nil && !memory.SupportsSyncPages(peerVersion) {
		http.Error(w, "memory sync pagination requires protocol version 2", http.StatusBadRequest)
		return
	}
	started := time.Now()
	requestID := syncLogRequestID(r)
	logging.FileInfof("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_start %s peer_id=%s body_bytes=unknown",
		syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "none"), peerID)
	response := &memorySyncCountingResponseWriter{ResponseWriter: w}
	snapshot := memory.SnapshotForPeer(a.mem, peerVersion, memory.FilterMemoryWatermarks(watermarks))
	responseWatermarks := snapshot.Watermarks
	if memory.SupportsSyncPages(peerVersion) {
		if cursor == nil {
			cursor = make(map[string]uint64)
			upper = snapshot.Watermarks
		}
		page, nextCursor, more, err := memory.PageSnapshot(snapshot, cursor, upper, a.cfg.SyncMaxBytes, 0)
		if err != nil {
			logging.FileErrorf("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_failed %s peer_id=%s status=%d stage=page error=%v",
				syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "page"), peerID, http.StatusRequestEntityTooLarge, err)
			http.Error(w, "could not page memory snapshot: "+err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		snapshot = page
		responseWatermarks = upper
		if more {
			if err := memory.SetSyncPageResponseHeader(w, nextCursor); err != nil {
				logging.FileErrorf("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_failed %s peer_id=%s status=%d stage=page_cursor error=%v",
					syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "page_cursor"), peerID, http.StatusInternalServerError, err)
				http.Error(w, "could not encode memory sync page cursor: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	if err := memory.SetSyncResponseHeadersForPeer(w, responseWatermarks, peerVersion); err != nil {
		http.Error(w, "could not encode memory sync watermarks: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if memory.SupportsEmptySync(peerVersion) && r.Header.Get(memory.SyncPageCursorHeader) == "" && memorySyncRecordCount(snapshot) == 0 {
		w.WriteHeader(http.StatusNoContent)
		logging.FileInfof("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_complete %s peer_id=%s status=%d body_bytes=0 records=0 elapsed=%s",
			syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "none"), peerID, http.StatusNoContent, time.Since(started))
		return
	}
	_, writeErr := memory.WriteSyncJSON(response, http.StatusOK, snapshot, peerVersion, memory.AcceptsSyncCompression(r))
	response.writeErr = writeErr
	failureClass := "none"
	if response.writeErr != nil {
		failureClass = "response_write"
	}
	logging.FileErrorf("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=node->peer event=send_complete %s peer_id=%s status=%d body_bytes=%d elapsed=%s error=%v",
		syncLogFields(requestID, a.cfg.NodeID, time.Since(started), failureClass), peerID, http.StatusOK, response.bytesWritten, time.Since(started), response.writeErr)
}

func (a *Agent) handlePeerMemorySyncPost(w http.ResponseWriter, r *http.Request) {
	peerID := taskSyncPeerID(r)
	coordinatorID := a.currentElectedOrchestratorID()
	if peerID == "" || peerID != coordinatorID {
		http.Error(w, "memory replica update requires the current coordinator", http.StatusForbidden)
		return
	}
	if a.mem == nil {
		http.Error(w, "memory store unavailable", http.StatusServiceUnavailable)
		return
	}
	started := time.Now()
	requestID := syncLogRequestID(r)
	logging.FileInfof("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_start %s peer_id=%s body_bytes_declared=%d",
		syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "none"), peerID, r.ContentLength)
	decodeStarted := time.Now()
	snapshot, bodyBytes, err := decodeMemorySyncRequest(w, r, a.cfg.SyncMaxBytes)
	if err != nil {
		logging.FileErrorf("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_failed %s peer_id=%s body_bytes=%d status=%d elapsed=%s stage=decode error=%v",
			syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "decode"), peerID, bodyBytes, http.StatusBadRequest, time.Since(started), err)
		http.Error(w, "invalid memory sync payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	decodeElapsed := time.Since(decodeStarted)
	mergeStarted := time.Now()
	if err := a.mem.MergeSnapshotFrom(snapshot, peerID); err != nil {
		logging.FileErrorf("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_failed %s peer_id=%s body_bytes=%d status=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s stage=merge error=%v",
			syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "merge"), peerID, bodyBytes, http.StatusInternalServerError, decodeElapsed, time.Since(mergeStarted), time.Since(started), err)
		http.Error(w, "could not merge cluster memory: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	logging.FileInfof("nodeagent", "FILE_ONLY: nodeagent: memory sync direction=peer->node event=receive_complete %s peer_id=%s body_bytes=%d status=%d records=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s",
		syncLogFields(requestID, a.cfg.NodeID, time.Since(started), "none"), peerID, bodyBytes, http.StatusNoContent, memorySyncRecordCount(snapshot), decodeElapsed, time.Since(mergeStarted), time.Since(started))
}

func decodeMemorySyncRequest(w http.ResponseWriter, r *http.Request, maxBytes int64) (memory.Snapshot, int64, error) {
	version, _, err := memory.ReadSyncRequestHeaders(r)
	if err != nil {
		return memory.Snapshot{}, 0, err
	}
	reader := &memorySyncCountingReader{ReadCloser: r.Body}
	body, err := memory.DecodeSyncBody(
		http.MaxBytesReader(w, reader, maxBytes),
		r.Header.Get("Content-Encoding"),
		version,
		maxBytes,
	)
	if err != nil {
		return memory.Snapshot{}, reader.bytesRead, err
	}
	var snapshot memory.Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return memory.Snapshot{}, reader.bytesRead, err
	}
	return snapshot, reader.bytesRead, nil
}

func marshalMemorySnapshotWithLimit(snapshot memory.Snapshot, maxBytes int64) ([]byte, error) {
	body, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("memory snapshot exceeds the %d-byte sync limit", maxBytes)
	}
	return body, nil
}

type memorySyncHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

func fetchMemorySyncSnapshot(client memorySyncHTTPClient, endpoint string, watermarks map[string]uint64, maxBytes int64) (memory.Snapshot, int64, int, error) {
	return fetchMemorySyncSnapshotWithContext(context.Background(), client, endpoint, watermarks, maxBytes)
}

func fetchMemorySyncSnapshotWithContext(ctx context.Context, client memorySyncHTTPClient, endpoint string, watermarks map[string]uint64, maxBytes int64) (memory.Snapshot, int64, int, error) {
	result := memory.Snapshot{
		Experiences:  make([]memory.Experience, 0),
		Facts:        make([]memory.Fact, 0),
		Performances: make([]memory.Performance, 0),
		Dialogues:    make([]memory.DialogueRecord, 0),
	}
	var cursor, upper map[string]uint64
	var totalBytes int64
	var lastStatus int
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return memory.Snapshot{}, totalBytes, lastStatus, err
		}
		if err := memory.SetSyncRequestHeaders(request, watermarks); err != nil {
			return memory.Snapshot{}, totalBytes, lastStatus, err
		}
		memory.SetSyncCompressionRequestHeader(request)
		if cursor != nil {
			if err := memory.SetSyncPageRequestHeaders(request, cursor, upper); err != nil {
				return memory.Snapshot{}, totalBytes, lastStatus, err
			}
		}
		response, err := client.Do(request)
		if err != nil {
			return memory.Snapshot{}, totalBytes, lastStatus, err
		}
		lastStatus = response.StatusCode
		version, responseWatermarks, err := memory.ReadSyncResponseHeaders(response)
		if err != nil {
			_ = response.Body.Close()
			return memory.Snapshot{}, totalBytes, lastStatus, err
		}
		nextCursor, more, err := memory.ReadSyncPageResponseHeader(response)
		if err != nil {
			_ = response.Body.Close()
			return memory.Snapshot{}, totalBytes, lastStatus, err
		}
		page, bodyBytes, err := decodeMemorySyncResponse(response, maxBytes, version)
		totalBytes += bodyBytes
		if err != nil {
			return memory.Snapshot{}, totalBytes, lastStatus, err
		}
		if version == 0 {
			version = page.SyncVersion
		}
		if more && !memory.SupportsSyncPages(version) {
			return memory.Snapshot{}, totalBytes, lastStatus, fmt.Errorf("peer paginated a memory snapshot without protocol version 2")
		}
		if more && len(nextCursor) == 0 {
			return memory.Snapshot{}, totalBytes, lastStatus, fmt.Errorf("peer returned an empty memory sync page cursor")
		}
		result.Experiences = append(result.Experiences, page.Experiences...)
		result.Facts = append(result.Facts, page.Facts...)
		result.Performances = append(result.Performances, page.Performances...)
		result.Dialogues = append(result.Dialogues, page.Dialogues...)
		if !more {
			result.SyncVersion = version
			result.Watermarks = page.Watermarks
			if result.Watermarks == nil {
				result.Watermarks = responseWatermarks
			}
			return result, totalBytes, lastStatus, nil
		}
		if len(responseWatermarks) == 0 {
			return memory.Snapshot{}, totalBytes, lastStatus, fmt.Errorf("peer omitted memory sync watermarks during pagination")
		}
		cursor = nextCursor
		upper = responseWatermarks
	}
}

func marshalMemorySnapshotPages(snapshot memory.Snapshot, maxBytes int64, wrap func(memory.Snapshot) ([]byte, error)) ([][]byte, error) {
	base := memory.Snapshot{
		Experiences:  make([]memory.Experience, 0),
		Facts:        make([]memory.Fact, 0),
		Performances: make([]memory.Performance, 0),
		Dialogues:    make([]memory.DialogueRecord, 0),
		SyncVersion:  snapshot.SyncVersion,
		Watermarks:   snapshot.Watermarks,
	}
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	wrappedBase, err := wrap(base)
	if err != nil {
		return nil, err
	}
	envelopeBytes := len(wrappedBase) - len(baseJSON)
	if envelopeBytes < 0 {
		return nil, fmt.Errorf("memory snapshot wrapper is smaller than its payload")
	}
	pages, err := memory.SplitSnapshot(snapshot, maxBytes, envelopeBytes)
	if err != nil {
		return nil, err
	}
	bodies := make([][]byte, 0, len(pages))
	for _, page := range pages {
		body, err := wrap(page)
		if err != nil {
			return nil, err
		}
		if int64(len(body)) > maxBytes {
			return nil, fmt.Errorf("memory snapshot page exceeds the %d-byte sync limit", maxBytes)
		}
		bodies = append(bodies, body)
	}
	return bodies, nil
}

func postMemorySnapshotPages(client memorySyncHTTPClient, endpoint string, bodies [][]byte, watermarks map[string]uint64, peerVersion int) (int64, error) {
	return postMemorySnapshotPagesWithContext(context.Background(), client, endpoint, bodies, watermarks, peerVersion)
}

func postMemorySnapshotPagesWithContext(ctx context.Context, client memorySyncHTTPClient, endpoint string, bodies [][]byte, watermarks map[string]uint64, peerVersion int) (int64, error) {
	var totalBytes int64
	for _, body := range bodies {
		body, compressed, err := memory.EncodeSyncBody(body, peerVersion, true)
		if err != nil {
			return totalBytes, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return totalBytes, err
		}
		request.Header.Set("Content-Type", "application/json")
		if compressed {
			request.Header.Set("Content-Encoding", "gzip")
		}
		if err := memory.SetSyncRequestHeaders(request, watermarks); err != nil {
			return totalBytes, err
		}
		response, err := client.Do(request)
		if err != nil {
			return totalBytes, err
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
			return totalBytes, fmt.Errorf("peer returned %s: %s", response.Status, strings.TrimSpace(string(message)))
		}
		_, copyErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		totalBytes += int64(len(body))
		if copyErr != nil {
			return totalBytes, copyErr
		}
		if closeErr != nil {
			return totalBytes, closeErr
		}
	}
	return totalBytes, nil
}

func decodeMemorySyncResponse(resp *http.Response, maxBytes int64, version int) (memory.Snapshot, int64, error) {
	defer closeutil.Discard(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return memory.Snapshot{}, int64(len(body)), fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if resp.StatusCode == http.StatusNoContent {
		if !memory.SupportsEmptySync(version) {
			return memory.Snapshot{}, 0, fmt.Errorf("peer returned an empty sync response without protocol version %d", memory.SyncCompressionProtocolVersion)
		}
		return memory.Snapshot{
			Experiences:  []memory.Experience{},
			Facts:        []memory.Fact{},
			Performances: []memory.Performance{},
			Dialogues:    []memory.DialogueRecord{},
		}, 0, nil
	}
	body, err := memory.DecodeSyncBody(resp.Body, resp.Header.Get("Content-Encoding"), version, maxBytes)
	if err != nil {
		return memory.Snapshot{}, 0, err
	}
	var snapshot memory.Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return memory.Snapshot{}, int64(len(body)), err
	}
	return snapshot, int64(len(body)), nil
}

func memorySyncRecordCount(snapshot memory.Snapshot) int {
	return len(snapshot.Experiences) + len(snapshot.Facts) + len(snapshot.Performances) + len(snapshot.Dialogues)
}

func memorySyncChanges(store memory.Store) <-chan struct{} {
	if source, ok := store.(memory.ChangeSource); ok {
		return source.Changes()
	}
	return nil
}

func memorySyncWatermarks(store memory.Store) map[string]uint64 {
	if store == nil {
		return nil
	}
	return store.Watermarks()
}

// syncPeerMemory makes the elected node a relay for memory in standalone
// peer clusters. Joined nodes use the orchestrator as their memory relay.
func (a *Agent) syncPeerMemory() {
	a.orchestratorLink.syncer.syncPeerMemory()
}
