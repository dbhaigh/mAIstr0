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
)

const maxMemorySyncBodyBytes int64 = 64 << 20

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
	log.Printf("nodeagent: memory sync direction=node->peer event=send_start node_id=%s peer_id=%s body_bytes=unknown", a.cfg.NodeID, peerID)
	response := &memorySyncCountingResponseWriter{ResponseWriter: w}
	snapshot := memory.SnapshotForPeer(a.mem, peerVersion, memory.FilterMemoryWatermarks(watermarks))
	responseWatermarks := snapshot.Watermarks
	if memory.SupportsSyncPages(peerVersion) {
		if cursor == nil {
			cursor = make(map[string]uint64)
			upper = snapshot.Watermarks
		}
		page, nextCursor, more, err := memory.PageSnapshot(snapshot, cursor, upper, maxMemorySyncBodyBytes, 0)
		if err != nil {
			http.Error(w, "could not page memory snapshot: "+err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		snapshot = page
		responseWatermarks = upper
		if more {
			if err := memory.SetSyncPageResponseHeader(w, nextCursor); err != nil {
				http.Error(w, "could not encode memory sync page cursor: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	if err := memory.SetSyncResponseHeaders(w, responseWatermarks); err != nil {
		http.Error(w, "could not encode memory sync watermarks: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(response, http.StatusOK, snapshot)
	log.Printf("nodeagent: memory sync direction=node->peer event=send_complete node_id=%s peer_id=%s status=%d body_bytes=%d elapsed=%s error=%v",
		a.cfg.NodeID, peerID, http.StatusOK, response.bytesWritten, time.Since(started), response.writeErr)
}

func (a *Agent) handlePeerMemorySyncPost(w http.ResponseWriter, r *http.Request) {
	peerID := taskSyncPeerID(r)
	coordinatorID := a.mostCapableNode().ID
	if orchestratorID := a.getOrchestratorID(); orchestratorID != "" && peerID == orchestratorID {
		coordinatorID = orchestratorID
	}
	if peerID == "" || peerID != coordinatorID {
		http.Error(w, "memory replica update requires the current coordinator", http.StatusForbidden)
		return
	}
	if a.mem == nil {
		http.Error(w, "memory store unavailable", http.StatusServiceUnavailable)
		return
	}
	started := time.Now()
	log.Printf("nodeagent: memory sync direction=peer->node event=receive_start node_id=%s peer_id=%s body_bytes_declared=%d",
		a.cfg.NodeID, peerID, r.ContentLength)
	decodeStarted := time.Now()
	snapshot, bodyBytes, err := decodeMemorySyncRequest(w, r)
	if err != nil {
		log.Printf("nodeagent: memory sync direction=peer->node event=receive_failed node_id=%s peer_id=%s body_bytes=%d status=%d elapsed=%s stage=decode error=%v",
			a.cfg.NodeID, peerID, bodyBytes, http.StatusBadRequest, time.Since(started), err)
		http.Error(w, "invalid memory sync payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	decodeElapsed := time.Since(decodeStarted)
	mergeStarted := time.Now()
	if err := a.mem.MergeSnapshotFrom(snapshot, peerID); err != nil {
		log.Printf("nodeagent: memory sync direction=peer->node event=receive_failed node_id=%s peer_id=%s body_bytes=%d status=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s stage=merge error=%v",
			a.cfg.NodeID, peerID, bodyBytes, http.StatusInternalServerError, decodeElapsed, time.Since(mergeStarted), time.Since(started), err)
		http.Error(w, "could not merge cluster memory: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	log.Printf("nodeagent: memory sync direction=peer->node event=receive_complete node_id=%s peer_id=%s body_bytes=%d status=%d records=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s",
		a.cfg.NodeID, peerID, bodyBytes, http.StatusNoContent, memorySyncRecordCount(snapshot), decodeElapsed, time.Since(mergeStarted), time.Since(started))
}

func decodeMemorySyncRequest(w http.ResponseWriter, r *http.Request) (memory.Snapshot, int64, error) {
	reader := &memorySyncCountingReader{ReadCloser: r.Body}
	body, err := io.ReadAll(http.MaxBytesReader(w, reader, maxMemorySyncBodyBytes))
	if err != nil {
		return memory.Snapshot{}, reader.bytesRead, err
	}
	var snapshot memory.Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return memory.Snapshot{}, reader.bytesRead, err
	}
	return snapshot, reader.bytesRead, nil
}

func marshalMemorySnapshot(snapshot memory.Snapshot) ([]byte, error) {
	body, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxMemorySyncBodyBytes {
		return nil, fmt.Errorf("memory snapshot exceeds the %d-byte sync limit", maxMemorySyncBodyBytes)
	}
	return body, nil
}

type memorySyncHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

func fetchMemorySyncSnapshot(client memorySyncHTTPClient, endpoint string, watermarks map[string]uint64) (memory.Snapshot, int64, error) {
	result := memory.Snapshot{
		Experiences:  make([]memory.Experience, 0),
		Facts:        make([]memory.Fact, 0),
		Performances: make([]memory.Performance, 0),
		Dialogues:    make([]memory.DialogueRecord, 0),
	}
	var cursor, upper map[string]uint64
	var totalBytes int64
	for {
		request, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return memory.Snapshot{}, totalBytes, err
		}
		if err := memory.SetSyncRequestHeaders(request, watermarks); err != nil {
			return memory.Snapshot{}, totalBytes, err
		}
		if cursor != nil {
			if err := memory.SetSyncPageRequestHeaders(request, cursor, upper); err != nil {
				return memory.Snapshot{}, totalBytes, err
			}
		}
		response, err := client.Do(request)
		if err != nil {
			return memory.Snapshot{}, totalBytes, err
		}
		version, responseWatermarks, err := memory.ReadSyncResponseHeaders(response)
		if err != nil {
			response.Body.Close()
			return memory.Snapshot{}, totalBytes, err
		}
		nextCursor, more, err := memory.ReadSyncPageResponseHeader(response)
		if err != nil {
			response.Body.Close()
			return memory.Snapshot{}, totalBytes, err
		}
		page, bodyBytes, err := decodeMemorySyncResponse(response)
		totalBytes += bodyBytes
		if err != nil {
			return memory.Snapshot{}, totalBytes, err
		}
		if version == 0 {
			version = page.SyncVersion
		}
		if more && !memory.SupportsSyncPages(version) {
			return memory.Snapshot{}, totalBytes, fmt.Errorf("peer paginated a memory snapshot without protocol version 2")
		}
		if more && len(nextCursor) == 0 {
			return memory.Snapshot{}, totalBytes, fmt.Errorf("peer returned an empty memory sync page cursor")
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
			return result, totalBytes, nil
		}
		if len(responseWatermarks) == 0 {
			return memory.Snapshot{}, totalBytes, fmt.Errorf("peer omitted memory sync watermarks during pagination")
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

func postMemorySnapshotPages(client memorySyncHTTPClient, endpoint string, bodies [][]byte, watermarks map[string]uint64) (int64, error) {
	var totalBytes int64
	for _, body := range bodies {
		request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return totalBytes, err
		}
		request.Header.Set("Content-Type", "application/json")
		if err := memory.SetSyncRequestHeaders(request, watermarks); err != nil {
			return totalBytes, err
		}
		response, err := client.Do(request)
		if err != nil {
			return totalBytes, err
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			response.Body.Close()
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

func decodeMemorySyncResponse(resp *http.Response) (memory.Snapshot, int64, error) {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return memory.Snapshot{}, int64(len(body)), fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMemorySyncBodyBytes+1))
	if err != nil {
		return memory.Snapshot{}, int64(len(body)), err
	}
	if int64(len(body)) > maxMemorySyncBodyBytes {
		return memory.Snapshot{}, int64(len(body)), fmt.Errorf("peer memory snapshot exceeds the %d-byte sync limit", maxMemorySyncBodyBytes)
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

func (a *Agent) syncPeerMemoryLoop() {
	a.syncPeerMemory()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		a.syncPeerMemory()
	}
}

// syncPeerMemory makes the elected node a relay for memory in standalone
// peer clusters. Joined nodes use the orchestrator as their memory relay.
func (a *Agent) syncPeerMemory() {
	if a.mem == nil || (a.getOrchestratorAddr() != "" && a.getOrchestratorID() != "") {
		return
	}
	if a.mostCapableNode().ID != a.cfg.NodeID {
		return
	}

	peers := a.Peers()
	remoteSnapshots := make(map[string]memory.Snapshot)
	for _, peer := range peers {
		if !peer.Healthy || peer.ID == a.cfg.NodeID || peer.Address == "" {
			continue
		}
		fetchStarted := time.Now()
		log.Printf("nodeagent: memory sync direction=peer->node event=receive_start node_id=%s peer_id=%s body_bytes=unknown",
			a.cfg.NodeID, peer.ID)
		snapshot, bodyBytes, err := fetchMemorySyncSnapshot(
			a.peerClient,
			strings.TrimRight(peer.Address, "/")+"/memory/sync",
			memory.FilterMemoryWatermarks(a.mem.PeerWatermarks(peer.ID)),
		)
		if err != nil {
			log.Printf("nodeagent: memory sync direction=peer->node event=receive_failed node_id=%s peer_id=%s body_bytes=0 elapsed=%s stage=request error=%v",
				a.cfg.NodeID, peer.ID, time.Since(fetchStarted), err)
			log.Printf("nodeagent: cluster memory fetch from %s failed: %v", peer.ID, err)
			continue
		}
		log.Printf("nodeagent: memory sync direction=peer->node event=receive_complete node_id=%s peer_id=%s status=%d body_bytes=%d records=%d elapsed=%s",
			a.cfg.NodeID, peer.ID, http.StatusOK, bodyBytes, memorySyncRecordCount(snapshot), time.Since(fetchStarted))
		mergeStarted := time.Now()
		if err := a.mem.MergeSnapshotFrom(snapshot, peer.ID); err != nil {
			log.Printf("nodeagent: memory sync direction=peer->node event=merge_failed node_id=%s peer_id=%s body_bytes=%d elapsed=%s error=%v",
				a.cfg.NodeID, peer.ID, bodyBytes, time.Since(mergeStarted), err)
			log.Printf("nodeagent: cluster memory import from %s failed: %v", peer.ID, err)
		} else {
			remoteSnapshots[peer.ID] = snapshot
			log.Printf("nodeagent: memory sync direction=peer->node event=merge_complete node_id=%s peer_id=%s body_bytes=%d records=%d elapsed=%s",
				a.cfg.NodeID, peer.ID, bodyBytes, memorySyncRecordCount(snapshot), time.Since(mergeStarted))
		}
	}

	for _, peer := range peers {
		if !peer.Healthy || peer.ID == a.cfg.NodeID || peer.Address == "" {
			continue
		}
		remoteSnapshot, fetched := remoteSnapshots[peer.ID]
		peerVersion := 0
		peerWatermarks := map[string]uint64(nil)
		if fetched {
			peerVersion = remoteSnapshot.SyncVersion
			peerWatermarks = remoteSnapshot.Watermarks
		}
		outgoing := memory.SnapshotForPeer(a.mem, peerVersion, peerWatermarks)
		bodies, err := marshalMemorySnapshotPages(outgoing, maxMemorySyncBodyBytes, func(page memory.Snapshot) ([]byte, error) {
			return marshalMemorySnapshot(page)
		})
		if err != nil {
			log.Printf("nodeagent: cluster memory encode for %s failed: %v", peer.ID, err)
			continue
		}
		sendStarted := time.Now()
		var totalBodyBytes int64
		for _, body := range bodies {
			totalBodyBytes += int64(len(body))
		}
		log.Printf("nodeagent: memory sync direction=node->peer event=send_start node_id=%s peer_id=%s body_bytes=%d pages=%d",
			a.cfg.NodeID, peer.ID, totalBodyBytes, len(bodies))
		sentBytes, err := postMemorySnapshotPages(
			a.peerClient,
			strings.TrimRight(peer.Address, "/")+"/memory/sync",
			bodies,
			a.mem.Watermarks(),
		)
		if err != nil {
			log.Printf("nodeagent: memory sync direction=node->peer event=send_failed node_id=%s peer_id=%s body_bytes=%d elapsed=%s error=%v",
				a.cfg.NodeID, peer.ID, sentBytes, time.Since(sendStarted), err)
			log.Printf("nodeagent: cluster memory replication to %s failed: %v", peer.ID, err)
			continue
		}
		log.Printf("nodeagent: memory sync direction=node->peer event=send_complete node_id=%s peer_id=%s status=%d body_bytes=%d elapsed=%s",
			a.cfg.NodeID, peer.ID, http.StatusNoContent, sentBytes, time.Since(sendStarted))
		if fetched && memory.SupportsIncrementalSync(peerVersion) {
			if err := a.mem.SetPeerWatermarks(peer.ID, outgoing.Watermarks); err != nil {
				log.Printf("nodeagent: persist memory watermarks for %s failed: %v", peer.ID, err)
			}
		}
	}
}
