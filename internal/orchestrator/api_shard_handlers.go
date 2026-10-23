package orchestrator

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/shard"
)

func (s *Server) handleCreateShardGroup(w http.ResponseWriter, r *http.Request) {
	if s.shards == nil {
		writeError(w, apperr.UnavailableError(errors.New("shard group persistence is unavailable")))
		return
	}
	request, err := httpx.DecodeJSON[CreateShardGroupRequest](w, r, 2<<20)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	group, err := s.shards.Create(r.Context(), request)
	if err != nil {
		switch {
		case errors.Is(err, errShardGroupConflict):
			writeError(w, apperr.ConflictError(err))
		case errors.Is(err, errShardGroupInvalid):
			writeError(w, apperr.Invalid(err))
		case errors.Is(err, errShardGroupUnavailable):
			writeError(w, apperr.UnavailableError(err))
		case errors.Is(err, errShardGroupOperation):
			writeError(w, apperr.ConflictError(err))
		default:
			var refusal *shard.Refusal
			if errors.As(err, &refusal) {
				writeError(w, apperr.Invalid(err))
			} else {
				writeError(w, apperr.UpstreamError(fmt.Errorf("could not create shard group: %w", err)))
			}
		}
		return
	}
	writeJSON(w, http.StatusCreated, group)
}

func (s *Server) handleListShardGroups(w http.ResponseWriter, _ *http.Request) {
	if s.shards == nil {
		writeError(w, apperr.UnavailableError(errors.New("shard group persistence is unavailable")))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"groups": s.shards.List()})
}

func (s *Server) handleGetShardGroup(w http.ResponseWriter, r *http.Request) {
	if s.shards == nil {
		writeError(w, apperr.UnavailableError(errors.New("shard group persistence is unavailable")))
		return
	}
	group, ok := s.shards.Get(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errShardGroupNotFound))
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (s *Server) handleDeleteShardGroup(w http.ResponseWriter, r *http.Request) {
	if s.shards == nil {
		writeError(w, apperr.UnavailableError(errors.New("shard group persistence is unavailable")))
		return
	}
	if err := s.shards.Delete(r.Context(), r.PathValue("id")); err != nil {
		if errors.Is(err, errShardGroupNotFound) {
			writeError(w, apperr.NotFoundError(err))
			return
		}
		if errors.Is(err, errShardGroupOperation) {
			writeError(w, apperr.ConflictError(err))
			return
		}
		writeError(w, apperr.UpstreamError(fmt.Errorf("could not delete shard group: %w", err)))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
