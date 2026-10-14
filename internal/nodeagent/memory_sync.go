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
	started := time.Now()
	log.Printf("nodeagent: memory sync direction=node->peer event=send_start node_id=%s peer_id=%s body_bytes=unknown", a.cfg.NodeID, peerID)
	response := &memorySyncCountingResponseWriter{ResponseWriter: w}
	snapshot := memory.SnapshotForPeer(a.mem, peerVersion, memory.FilterMemoryWatermarks(watermarks))
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
		request, err := http.NewRequest(http.MethodGet, strings.TrimRight(peer.Address, "/")+"/memory/sync", nil)
		if err == nil {
			err = memory.SetSyncRequestHeaders(request, memory.FilterMemoryWatermarks(a.mem.PeerWatermarks(peer.ID)))
		}
		if err != nil {
			log.Printf("nodeagent: memory sync direction=peer->node event=receive_failed node_id=%s peer_id=%s body_bytes=0 elapsed=%s stage=request error=%v",
				a.cfg.NodeID, peer.ID, time.Since(fetchStarted), err)
			continue
		}
		resp, err := a.peerClient.Do(request)
		if err != nil {
			log.Printf("nodeagent: memory sync direction=peer->node event=receive_failed node_id=%s peer_id=%s body_bytes=0 elapsed=%s stage=request error=%v",
				a.cfg.NodeID, peer.ID, time.Since(fetchStarted), err)
			log.Printf("nodeagent: cluster memory fetch from %s failed: %v", peer.ID, err)
			continue
		}
		status := resp.StatusCode
		snapshot, bodyBytes, err := decodeMemorySyncResponse(resp)
		if err != nil {
			log.Printf("nodeagent: memory sync direction=peer->node event=receive_failed node_id=%s peer_id=%s status=%d body_bytes=%d elapsed=%s stage=decode error=%v",
				a.cfg.NodeID, peer.ID, status, bodyBytes, time.Since(fetchStarted), err)
			log.Printf("nodeagent: cluster memory fetch from %s failed: %v", peer.ID, err)
			continue
		}
		log.Printf("nodeagent: memory sync direction=peer->node event=receive_complete node_id=%s peer_id=%s status=%d body_bytes=%d records=%d elapsed=%s",
			a.cfg.NodeID, peer.ID, status, bodyBytes, memorySyncRecordCount(snapshot), time.Since(fetchStarted))
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
		body, err := marshalMemorySnapshot(outgoing)
		if err != nil {
			log.Printf("nodeagent: cluster memory encode for %s failed: %v", peer.ID, err)
			continue
		}
		sendStarted := time.Now()
		log.Printf("nodeagent: memory sync direction=node->peer event=send_start node_id=%s peer_id=%s body_bytes=%d",
			a.cfg.NodeID, peer.ID, len(body))
		req, err := http.NewRequest(http.MethodPost, strings.TrimRight(peer.Address, "/")+"/memory/sync", bytes.NewReader(body))
		if err != nil {
			log.Printf("nodeagent: memory sync direction=node->peer event=send_failed node_id=%s peer_id=%s body_bytes=%d elapsed=%s stage=request error=%v",
				a.cfg.NodeID, peer.ID, len(body), time.Since(sendStarted), err)
			log.Printf("nodeagent: cluster memory request for %s failed: %v", peer.ID, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if err := memory.SetSyncRequestHeaders(req, a.mem.Watermarks()); err != nil {
			log.Printf("nodeagent: memory sync direction=node->peer event=send_failed node_id=%s peer_id=%s body_bytes=%d elapsed=%s stage=headers error=%v",
				a.cfg.NodeID, peer.ID, len(body), time.Since(sendStarted), err)
			continue
		}
		resp, err := a.peerClient.Do(req)
		if err != nil {
			log.Printf("nodeagent: memory sync direction=node->peer event=send_failed node_id=%s peer_id=%s body_bytes=%d elapsed=%s stage=send error=%v",
				a.cfg.NodeID, peer.ID, len(body), time.Since(sendStarted), err)
			log.Printf("nodeagent: cluster memory replication to %s failed: %v", peer.ID, err)
			continue
		}
		status := resp.StatusCode
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		log.Printf("nodeagent: memory sync direction=node->peer event=send_complete node_id=%s peer_id=%s status=%d body_bytes=%d elapsed=%s",
			a.cfg.NodeID, peer.ID, status, len(body), time.Since(sendStarted))
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("nodeagent: cluster memory replication to %s rejected: %s", peer.ID, resp.Status)
			continue
		}
		if fetched && peerVersion == memory.SyncProtocolVersion {
			if err := a.mem.SetPeerWatermarks(peer.ID, outgoing.Watermarks); err != nil {
				log.Printf("nodeagent: persist memory watermarks for %s failed: %v", peer.ID, err)
			}
		}
	}
}
