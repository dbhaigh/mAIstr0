package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/apperr"
)

func (s *Server) handleAgentSessionsList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.listAgentSessions())
}

func (s *Server) handleAgentSessionsCreate(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, apperr.Invalid(err))
			return
		}
	}
	harness, backend := s.currentAgent()
	session, err := backend.CreateSession(req.Title, agent.SessionConfig{
		CoordinatorModel: req.CoordinatorModel,
		MaxSteps:         req.MaxSteps,
		Temperature:      req.Temperature,
		SystemPrompt:     req.SystemPrompt,
	})
	if err != nil {
		writeError(w, apperr.UnavailableError(err))
		return
	}
	session.Harness = harness
	if err := s.persistAgentSession(session); err != nil {
		backend.DeleteSession(session.ID)
		writeError(w, apperr.InternalError(fmt.Errorf("could not persist agent session: %w", err)))
		return
	}
	s.harness.setSessionHarness(session.ID, harness)
	s.publishSnapshot()
	writeJSON(w, http.StatusCreated, session)
}

func (s *Server) handleAgentSessionGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	harness, backend, exists := s.agentForSession(id)
	if !exists {
		writeError(w, apperr.NotFoundError(errors.New("session not found")))
		return
	}
	session, ok := backend.GetSession(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("session not found")))
		return
	}
	session.Harness = harness
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) handleAgentSessionDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	harness, backend, exists := s.agentForSession(id)
	if !exists {
		writeError(w, apperr.NotFoundError(errors.New("session not found")))
		return
	}
	if err := s.harness.deletePersistedSession(harness, id); err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not delete persisted agent session: %w", err)))
		return
	}
	if !backend.DeleteSession(id) {
		writeError(w, apperr.NotFoundError(errors.New("session not found")))
		return
	}
	s.harness.removeSessionHarness(id, harness)
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleAgentMessagePost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	harness, backend, exists := s.agentForSession(id)
	if !exists {
		writeError(w, apperr.NotFoundError(errors.New("session not found")))
		return
	}
	var req agentMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	content := strings.TrimSpace(req.Content)
	if content == "" {
		content = strings.TrimSpace(req.Message)
	}
	if content == "" {
		writeError(w, apperr.Invalid(errors.New("content is required")))
		return
	}

	turnCtx, turnCancel := context.WithCancel(r.Context())
	s.harness.setTurnCancel(id, turnCancel)
	defer func() {
		s.harness.clearTurnCancel(id)
		turnCancel()
	}()

	isStream := r.URL.Query().Get("stream") == "true" || strings.Contains(r.Header.Get("Accept"), "text/event-stream")

	if isStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, apperr.InternalError(errors.New("streaming unsupported")))
			return
		}

		streamChan := make(chan agent.StreamEvent, 50)
		go func() {
			_, sendErr := backend.SendMessage(turnCtx, id, content, streamChan)
			if sendErr != nil {
				if !errors.Is(sendErr, context.Canceled) && turnCtx.Err() == nil {
					streamChan <- agent.StreamEvent{Type: agent.EventError, SessionID: id, Error: sendErr.Error()}
				}
			}
			close(streamChan)
		}()

		for ev := range streamChan {
			data, err := json.Marshal(ev)
			if err == nil {
				_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
		session, ok := backend.GetSession(id)
		if !ok {
			err := errors.New("agent session disappeared while processing the message")
			log.Printf("orchestrator: persist streamed agent session %s: %v", id, err)
			writeAgentStreamError(w, flusher, id, err)
		} else {
			session.Harness = harness
			if err := s.persistAgentSession(session); err != nil {
				log.Printf("orchestrator: persist streamed agent session %s: %v", id, err)
				writeAgentStreamError(w, flusher, id, err)
			}
		}
		s.publishSnapshot()
		return
	}

	msg, sendErr := backend.SendMessage(turnCtx, id, content, nil)
	session, ok := backend.GetSession(id)
	if !ok {
		writeError(w, apperr.InternalError(errors.New("agent session disappeared while processing the message")))
		return
	}
	session.Harness = harness
	if err := s.persistAgentSession(session); err != nil {
		if sendErr != nil {
			log.Printf("orchestrator: persist failed agent session %s: %v", id, err)
		}
		writeError(w, apperr.InternalError(fmt.Errorf("could not persist agent session: %w", err)))
		return
	}
	s.publishSnapshot()
	if sendErr != nil {
		if errors.Is(sendErr, context.Canceled) || turnCtx.Err() != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"session_id": id,
				"status":     "interrupted",
				"message":    msg,
			})
			return
		}
		writeError(w, apperr.InternalError(sendErr))
		return
	}
	writeJSON(w, http.StatusOK, msg)
}

func (s *Server) handleAgentSessionInterrupt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, apperr.Invalid(errors.New("session ID is required")))
		return
	}
	_, _, exists := s.agentForSession(id)
	if !exists {
		writeError(w, apperr.NotFoundError(errors.New("session not found")))
		return
	}

	interrupted := s.InterruptSession(id)
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id":  id,
		"interrupted": interrupted,
	})
}

func (s *Server) handleAgentToolsList(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	writeJSON(w, http.StatusOK, backend.ListTools())
}

func (s *Server) handleAgentModelsList(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	writeJSON(w, http.StatusOK, backend.ListClusterModels())
}

func (s *Server) handleAgentStatsGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.agentStats())
}

func (s *Server) handleAgentBackendGet(w http.ResponseWriter, r *http.Request) {
	harness, _ := s.currentAgent()
	writeJSON(w, http.StatusOK, map[string]any{
		"name": harness, "harnesses": []string{"pi", "deepseek"},
		"supports_coordinator_model": true,
	})
}

func (s *Server) handleAgentBackendPatch(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Harness string `json:"harness"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	request.Harness = strings.ToLower(strings.TrimSpace(request.Harness))
	if !s.selectHarness(request.Harness) {
		writeError(w, apperr.Invalid(errors.New(`unsupported harness (want "pi" or "deepseek")`)))
		return
	}
	s.handleAgentBackendGet(w, r)
}
