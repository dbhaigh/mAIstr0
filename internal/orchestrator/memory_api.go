package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
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
	pageRequest, err := parseHistoryPageRequest(r, "experiences", "q", "task_type", "node_id", "model", "tool", "success_only")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.mem == nil {
		if pageRequest.Active {
			writeHistoryPage(w, pageRequest, []memory.Experience{}, memory.HistoryCursor{}, false)
			return
		}
		writeJSON(w, http.StatusOK, []memory.Experience{})
		return
	}
	if pageRequest.Active {
		successOnly := r.URL.Query().Get("success_only")
		if successOnly != "" && successOnly != "true" && successOnly != "false" {
			http.Error(w, "success_only must be true or false", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Has("cursor") && pageRequest.Cursor.ID == 0 {
			http.Error(w, "cursor is missing its experience position", http.StatusBadRequest)
			return
		}
		pager, ok := s.mem.(memory.HistoryPager)
		if !ok {
			http.Error(w, "cursor paging is not supported by the memory store", http.StatusNotImplemented)
			return
		}
		page, err := pager.ExperiencesPage(memory.SearchFilter{
			Query:       r.URL.Query().Get("q"),
			TaskType:    r.URL.Query().Get("task_type"),
			NodeID:      r.URL.Query().Get("node_id"),
			Model:       r.URL.Query().Get("model"),
			Tool:        r.URL.Query().Get("tool"),
			SuccessOnly: successOnly == "true",
		}, memory.PageOptions{
			Cursor: pageRequest.memoryCursor(), Limit: pageRequest.Limit,
			MaxBytes: maxHistoryPageBytes - historyPageEnvelopeRoom,
		})
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, memory.ErrPageRecordTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			http.Error(w, "could not read experience page: "+err.Error(), status)
			return
		}
		writeHistoryPage(w, pageRequest, page.Items, page.NextCursor, page.HasMore)
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
	pageRequest, err := parseHistoryPageRequest(r, "facts", "scope")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.mem == nil {
		if pageRequest.Active {
			writeHistoryPage(w, pageRequest, []memory.Fact{}, memory.HistoryCursor{}, false)
			return
		}
		writeJSON(w, http.StatusOK, []memory.Fact{})
		return
	}
	if pageRequest.Active {
		if r.URL.Query().Has("cursor") && pageRequest.Cursor.Key == "" {
			http.Error(w, "cursor is missing its fact position", http.StatusBadRequest)
			return
		}
		pager, ok := s.mem.(memory.HistoryPager)
		if !ok {
			http.Error(w, "cursor paging is not supported by the memory store", http.StatusNotImplemented)
			return
		}
		page, err := pager.FactsPage(r.URL.Query().Get("scope"), memory.PageOptions{
			Cursor: pageRequest.memoryCursor(), Limit: pageRequest.Limit,
			MaxBytes: maxHistoryPageBytes - historyPageEnvelopeRoom,
		})
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, memory.ErrPageRecordTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			http.Error(w, "could not read fact page: "+err.Error(), status)
			return
		}
		writeHistoryPage(w, pageRequest, page.Items, page.NextCursor, page.HasMore)
		return
	}
	writeJSON(w, http.StatusOK, s.mem.Facts(r.URL.Query().Get("scope")))
}

func (s *Server) handleMemoryFactsPost(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeError(w, apperr.UnavailableError(errors.New("memory store unavailable")))
		return
	}
	var f memory.Fact
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	if f.Source == "" {
		f.Source = s.SelfID()
	}
	saved, err := s.mem.PutFact(f)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	s.publishSnapshot()
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) handleMemoryFactDelete(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeError(w, apperr.UnavailableError(errors.New("memory store unavailable")))
		return
	}
	scope := r.URL.Query().Get("scope")
	if err := s.mem.DeleteFact(scope, r.PathValue("key")); err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMemoryDialogues(w http.ResponseWriter, r *http.Request) {
	pageRequest, err := parseHistoryPageRequest(r, "dialogues")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.mem == nil {
		if pageRequest.Active {
			writeHistoryPage(w, pageRequest, []memory.DialogueRecord{}, memory.HistoryCursor{}, false)
			return
		}
		writeJSON(w, http.StatusOK, []memory.DialogueRecord{})
		return
	}
	if pageRequest.Active {
		if r.URL.Query().Has("cursor") && pageRequest.Cursor.Key == "" {
			http.Error(w, "cursor is missing its dialogue position", http.StatusBadRequest)
			return
		}
		pager, ok := s.mem.(memory.HistoryPager)
		if !ok {
			http.Error(w, "cursor paging is not supported by the memory store", http.StatusNotImplemented)
			return
		}
		page, err := pager.DialoguesPage(memory.PageOptions{
			Cursor: pageRequest.memoryCursor(), Limit: pageRequest.Limit,
			MaxBytes: maxHistoryPageBytes - historyPageEnvelopeRoom,
		})
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, memory.ErrPageRecordTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			http.Error(w, "could not read dialogue page: "+err.Error(), status)
			return
		}
		writeHistoryPage(w, pageRequest, page.Items, page.NextCursor, page.HasMore)
		return
	}
	writeJSON(w, http.StatusOK, s.mem.Dialogues(intQuery(r, "limit", 30)))
}

func (s *Server) handleMemoryDialogueGet(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeError(w, apperr.UnavailableError(errors.New("memory store unavailable")))
		return
	}
	d, err := s.mem.Dialogue(r.PathValue("id"))
	if err != nil {
		writeError(w, apperr.NotFoundError(errors.New("dialogue not found")))
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
	requestID := syncLogRequestID(r)
	nodeID := syncLogNodeID(r)
	reader := &memorySyncCountingReader{ReadCloser: r.Body}
	log.Printf("FILE_ONLY: orchestrator: memory sync direction=node->orchestrator event=receive_start %s remote=%s body_bytes_declared=%d",
		syncLogFields(requestID, nodeID, time.Since(started), "none"), r.RemoteAddr, r.ContentLength)
	if s.mem == nil {
		w.WriteHeader(http.StatusNoContent)
		log.Printf("FILE_ONLY: orchestrator: memory sync direction=node->orchestrator event=receive_complete %s remote=%s status=%d body_bytes=%d elapsed=%s",
			syncLogFields(requestID, nodeID, time.Since(started), "none"), r.RemoteAddr, http.StatusNoContent, reader.bytesRead, time.Since(started))
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
		if strings.TrimSpace(req.NodeID) != "" {
			nodeID = req.NodeID
		}
		log.Printf("FILE_ONLY: orchestrator: memory sync direction=node->orchestrator event=receive_failed %s remote=%s status=%d body_bytes=%d elapsed=%s stage=decode error=%v",
			syncLogFields(requestID, nodeID, time.Since(started), "decode"), r.RemoteAddr, status, reader.bytesRead, time.Since(started), err)
		if status == http.StatusBadRequest {
			writeError(w, apperr.Invalid(err))
		} else {
			http.Error(w, err.Error(), status)
		}
		return
	}
	decodeElapsed := time.Since(decodeStarted)
	if strings.TrimSpace(req.NodeID) != "" {
		nodeID = req.NodeID
	}
	snapshot := req.Snapshot
	if len(req.Performances) > 0 {
		snapshot.Performances = append(snapshot.Performances, req.Performances...)
	}
	mergeStarted := time.Now()
	if err := s.mergeMemorySnapshot(snapshot, req.NodeID); err != nil {
		log.Printf("FILE_ONLY: orchestrator: memory sync direction=node->orchestrator event=receive_failed %s remote=%s status=%d body_bytes=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s stage=merge error=%v",
			syncLogFields(requestID, nodeID, time.Since(started), "merge"), r.RemoteAddr, apperr.StatusCode(apperr.InternalError(err)), reader.bytesRead, decodeElapsed, time.Since(mergeStarted), time.Since(started), err)
		writeError(w, apperr.InternalError(err))
		return
	}
	mergeElapsed := time.Since(mergeStarted)
	response := &memorySyncCountingResponseWriter{ResponseWriter: w}
	writeJSON(response, http.StatusOK, map[string]any{
		"merged":  len(snapshot.Experiences) + len(snapshot.Facts) + len(snapshot.Dialogues) + len(snapshot.Performances),
		"node_id": req.NodeID,
	})
	log.Printf("FILE_ONLY: orchestrator: memory sync direction=node->orchestrator event=receive_complete %s remote=%s status=%d body_bytes=%d records=%d decode_elapsed=%s merge_elapsed=%s elapsed=%s response_body_bytes=%d response_error=%v",
		syncLogFields(requestID, nodeID, time.Since(started), "none"), r.RemoteAddr, http.StatusOK, reader.bytesRead,
		len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Dialogues)+len(snapshot.Performances),
		decodeElapsed, mergeElapsed, time.Since(started), response.bytesWritten, response.writeErr)
}

func (s *Server) handleMemorySyncGet(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	requestID := syncLogRequestID(r)
	nodeID := syncLogNodeID(r)
	log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_start %s remote=%s body_bytes=unknown",
		syncLogFields(requestID, nodeID, time.Since(started), "none"), r.RemoteAddr)
	snapshot := memory.Snapshot{}
	var responseWatermarks map[string]uint64
	if s.mem != nil {
		peerVersion, watermarks, err := memory.ReadSyncRequestHeaders(r)
		if err != nil {
			log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_failed %s remote=%s status=%d stage=headers error=%v",
				syncLogFields(requestID, nodeID, time.Since(started), "header_validation"), r.RemoteAddr, http.StatusBadRequest, err)
			http.Error(w, "invalid memory sync headers: "+err.Error(), http.StatusBadRequest)
			return
		}
		cursor, upper, err := memory.ReadSyncPageRequestHeaders(r)
		if err != nil {
			log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_failed %s remote=%s status=%d stage=page_headers error=%v",
				syncLogFields(requestID, nodeID, time.Since(started), "header_validation"), r.RemoteAddr, http.StatusBadRequest, err)
			http.Error(w, "invalid memory sync page headers: "+err.Error(), http.StatusBadRequest)
			return
		}
		if cursor != nil && !memory.SupportsSyncPages(peerVersion) {
			log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_failed %s remote=%s status=%d stage=page_version",
				syncLogFields(requestID, nodeID, time.Since(started), "unsupported_page_version"), r.RemoteAddr, http.StatusBadRequest)
			http.Error(w, "memory sync pagination requires protocol version 2", http.StatusBadRequest)
			return
		}
		snapshot = s.memorySnapshotForPeer(peerVersion, memory.FilterMemoryWatermarks(watermarks))
		responseWatermarks = snapshot.Watermarks
		if memory.SupportsSyncPages(peerVersion) {
			if cursor == nil {
				cursor = make(map[string]uint64)
				upper = snapshot.Watermarks
			}
			page, nextCursor, more, err := memory.PageSnapshot(snapshot, cursor, upper, maxMemorySyncBodyBytes, 0)
			if err != nil {
				log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_failed %s remote=%s status=%d stage=page error=%v",
					syncLogFields(requestID, nodeID, time.Since(started), "page"), r.RemoteAddr, http.StatusRequestEntityTooLarge, err)
				http.Error(w, "could not page memory snapshot: "+err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			snapshot = page
			responseWatermarks = upper
			if more {
				if err := memory.SetSyncPageResponseHeader(w, nextCursor); err != nil {
					log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_failed %s remote=%s status=%d stage=page_cursor error=%v",
						syncLogFields(requestID, nodeID, time.Since(started), "page_cursor"), r.RemoteAddr, http.StatusInternalServerError, err)
					http.Error(w, "could not encode memory sync page cursor: "+err.Error(), http.StatusInternalServerError)
					return
				}
			}
			s.metrics.recordSyncPage()
			log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=page_served %s remote=%s records=%d more=%t",
				syncLogFields(requestID, nodeID, time.Since(started), "none"), r.RemoteAddr,
				len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Performances)+len(snapshot.Dialogues), more)
		}
	}
	if err := memory.SetSyncResponseHeaders(w, responseWatermarks); err != nil {
		log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_failed %s remote=%s status=%d stage=watermarks error=%v",
			syncLogFields(requestID, nodeID, time.Since(started), "response_headers"), r.RemoteAddr, http.StatusInternalServerError, err)
		http.Error(w, "could not encode memory sync watermarks: "+err.Error(), http.StatusInternalServerError)
		return
	}
	response := &memorySyncCountingResponseWriter{ResponseWriter: w}
	writeJSON(response, http.StatusOK, snapshot)
	failureClass := "none"
	if response.writeErr != nil {
		failureClass = "response_write"
	}
	log.Printf("FILE_ONLY: orchestrator: memory sync direction=orchestrator->node event=send_complete %s remote=%s status=%d body_bytes=%d records=%d elapsed=%s error=%v",
		syncLogFields(requestID, nodeID, time.Since(started), failureClass), r.RemoteAddr, http.StatusOK, response.bytesWritten,
		len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Performances)+len(snapshot.Dialogues),
		time.Since(started), response.writeErr)
}

func (s *Server) handleMemoryPrune(w http.ResponseWriter, r *http.Request) {
	if s.mem == nil {
		writeError(w, apperr.UnavailableError(errors.New("memory store unavailable")))
		return
	}
	days := intQuery(r, "older_than_days", 30)
	removed, err := s.mem.Prune(time.Duration(days) * 24 * time.Hour)
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"removed": removed})
}

// handleNodePeerConverse asks a node to open a direct LLM-to-LLM
// conversation with another node and relays the transcript back.
func (s *Server) handleNodePeerConverse(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok || !n.Healthy {
		writeError(w, apperr.NotFoundError(errors.New("node not found or offline")))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	resp, err := s.nodeClientPort().PostDispatch(n.Address+"/peer/converse", "application/json", bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node unreachable: %w", err)))
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
