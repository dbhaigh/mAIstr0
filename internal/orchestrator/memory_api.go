package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
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

func (s *Server) handleMemoryExperiences(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeJSON(w, http.StatusOK, []memory.Experience{})
		return
	}
	limit := intQuery(r, "limit", 50)
	if q := r.URL.Query().Get("q"); q != "" {
		writeJSON(w, http.StatusOK, s.mem.Recall(memory.SearchFilter{
			Query:       q,
			TaskType:    r.URL.Query().Get("task_type"),
			NodeID:      r.URL.Query().Get("node_id"),
			Model:       r.URL.Query().Get("model"),
			SuccessOnly: r.URL.Query().Get("success_only") == "true",
			Limit:       limit,
		}))
		return
	}
	writeJSON(w, http.StatusOK, s.mem.Experiences(limit))
}

func (s *Server) handleMemoryInsights(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeJSON(w, http.StatusOK, memory.Insights{})
		return
	}
	writeJSON(w, http.StatusOK, s.mem.Insights())
}

func (s *Server) handleMemoryRecommendations(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeJSON(w, http.StatusOK, []memory.Recommendation{})
		return
	}
	writeJSON(w, http.StatusOK, s.mem.Recommend(r.URL.Query().Get("task_type"), intQuery(r, "limit", 10)))
}

func (s *Server) handleMemoryFactsGet(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeJSON(w, http.StatusOK, []memory.Fact{})
		return
	}
	writeJSON(w, http.StatusOK, s.mem.Facts(r.URL.Query().Get("scope")))
}

func (s *Server) handleMemoryFactsPost(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		http.Error(w, "memory store unavailable", http.StatusServiceUnavailable)
		return
	}
	var f memory.Fact
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if f.Source == "" {
		f.Source = s.selfID
	}
	saved, err := s.mem.PutFact(f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.publishSnapshot()
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) handleMemoryFactDelete(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		http.Error(w, "memory store unavailable", http.StatusServiceUnavailable)
		return
	}
	scope := r.URL.Query().Get("scope")
	if err := s.mem.DeleteFact(scope, r.PathValue("key")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMemoryDialogues(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeJSON(w, http.StatusOK, []memory.DialogueRecord{})
		return
	}
	writeJSON(w, http.StatusOK, s.mem.Dialogues(intQuery(r, "limit", 30)))
}

func (s *Server) handleMemoryDialogueGet(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		http.Error(w, "memory store unavailable", http.StatusServiceUnavailable)
		return
	}
	d, err := s.mem.Dialogue(r.PathValue("id"))
	if err != nil {
		http.Error(w, "dialogue not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

type memorySyncRequest struct {
	NodeID       string               `json:"node_id"`
	Snapshot     memory.Snapshot      `json:"snapshot"`
	Performances []memory.Performance `json:"performances"` // legacy node payload
}

// handleMemorySync ingests a node's locally-learned performance aggregates
// so the orchestrator's routing improves from every node's experience, not
// just the work it dispatched itself.
func (s *Server) handleMemorySync(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	reader := &memorySyncCountingReader{ReadCloser: r.Body}
	log.Printf("orchestrator: memory sync direction=node->orchestrator event=receive_start remote=%s body_bytes_declared=%d",
		r.RemoteAddr, r.ContentLength)
	if s.mem == nil {
		w.WriteHeader(http.StatusNoContent)
		log.Printf("orchestrator: memory sync direction=node->orchestrator event=receive_complete remote=%s status=%d body_bytes=%d elapsed=%s",
			r.RemoteAddr, http.StatusNoContent, reader.bytesRead, time.Since(started))
		return
	}
	var req memorySyncRequest
	decodeStarted := time.Now()
	if err := json.NewDecoder(http.MaxBytesReader(w, reader, maxMemorySyncBodyBytes)).Decode(&req); err != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			status = http.StatusRequestEntityTooLarge
		}
		log.Printf("orchestrator: memory sync direction=node->orchestrator event=receive_failed node_id=%s remote=%s status=%d body_bytes=%d elapsed=%s stage=decode error=%v",
			req.NodeID, r.RemoteAddr, status, reader.bytesRead, time.Since(started), err)
		http.Error(w, err.Error(), status)
		return
	}
	decodeElapsed := time.Since(decodeStarted)
	snapshot := req.Snapshot
	if len(req.Performances) > 0 {
		snapshot.Performances = append(snapshot.Performances, req.Performances...)
	}
	mergeStarted := time.Now()
	if err := s.mem.MergeSnapshot(snapshot); err != nil {
		log.Printf("orchestrator: memory sync direction=node->orchestrator event=receive_failed node_id=%s remote=%s status=%d body_bytes=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s stage=merge error=%v",
			req.NodeID, r.RemoteAddr, http.StatusInternalServerError, reader.bytesRead, decodeElapsed, time.Since(mergeStarted), time.Since(started), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mergeElapsed := time.Since(mergeStarted)
	response := &memorySyncCountingResponseWriter{ResponseWriter: w}
	writeJSON(response, http.StatusOK, map[string]any{
		"merged":  len(snapshot.Experiences) + len(snapshot.Facts) + len(snapshot.Dialogues) + len(snapshot.Performances),
		"node_id": req.NodeID,
	})
	log.Printf("orchestrator: memory sync direction=node->orchestrator event=receive_complete node_id=%s remote=%s status=%d body_bytes=%d records=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s response_body_bytes=%d response_error=%v",
		req.NodeID, r.RemoteAddr, http.StatusOK, reader.bytesRead,
		len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Dialogues)+len(snapshot.Performances),
		decodeElapsed, mergeElapsed, time.Since(started), response.bytesWritten, response.writeErr)
}

func (s *Server) handleMemorySyncGet(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	log.Printf("orchestrator: memory sync direction=orchestrator->node event=send_start remote=%s body_bytes=unknown", r.RemoteAddr)
	snapshot := memory.Snapshot{}
	if s.mem == nil {
	} else {
		snapshot = s.mem.Snapshot()
	}
	response := &memorySyncCountingResponseWriter{ResponseWriter: w}
	writeJSON(response, http.StatusOK, snapshot)
	log.Printf("orchestrator: memory sync direction=orchestrator->node event=send_complete remote=%s status=%d body_bytes=%d records=%d elapsed=%s error=%v",
		r.RemoteAddr, http.StatusOK, response.bytesWritten,
		len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Performances)+len(snapshot.Dialogues),
		time.Since(started), response.writeErr)
}

func (s *Server) handleMemoryPrune(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		http.Error(w, "memory store unavailable", http.StatusServiceUnavailable)
		return
	}
	days := intQuery(r, "older_than_days", 30)
	removed, err := s.mem.Prune(time.Duration(days) * 24 * time.Hour)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"removed": removed})
}

// handleNodePeerConverse asks a node to open a direct LLM-to-LLM
// conversation with another node and relays the transcript back.
func (s *Server) handleNodePeerConverse(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok || !n.Healthy {
		http.Error(w, "node not found or offline", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.dispatcher.Post(n.Address+"/peer/converse", "application/json", bytes.NewReader(body))
	if err != nil {
		http.Error(w, "node unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func intQuery(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
