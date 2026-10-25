package orchestrator

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/shard"
)

func (s *Server) handleStartShardGroup(w http.ResponseWriter, r *http.Request) {
	s.handleShardGroupAction(w, r, "start")
}

func (s *Server) handleStopShardGroup(w http.ResponseWriter, r *http.Request) {
	s.handleShardGroupAction(w, r, "stop")
}

func (s *Server) handleRestartShardGroup(w http.ResponseWriter, r *http.Request) {
	s.handleShardGroupAction(w, r, "restart")
}

func (s *Server) handleShardMembership(w http.ResponseWriter, r *http.Request) {
	if s.shards == nil {
		writeError(w, apperr.UnavailableError(errors.New("shard group persistence is unavailable")))
		return
	}
	request, err := httpx.DecodeJSON[ShardMembershipRequest](w, r, httpx.ControlJSONBodyBytes)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	group, err := s.shards.Reconfigure(r.Context(), r.PathValue("id"), request)
	if err != nil {
		switch {
		case errors.Is(err, errShardGroupInvalid):
			writeError(w, apperr.Invalid(err))
		case errors.Is(err, errShardGroupNotFound):
			writeError(w, apperr.NotFoundError(err))
		case errors.Is(err, errShardGroupOperation), errors.Is(err, errShardGroupConflict):
			writeError(w, apperr.ConflictError(err))
		case errors.Is(err, errShardGroupUnavailable):
			writeError(w, apperr.UnavailableError(err))
		default:
			var refusal *shard.Refusal
			if errors.As(err, &refusal) {
				writeError(w, apperr.Invalid(err))
			} else {
				writeError(w, apperr.UpstreamError(fmt.Errorf("reload shard membership: %w", err)))
			}
		}
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (s *Server) handleShardGroupAction(w http.ResponseWriter, r *http.Request, action string) {
	if s.shards == nil {
		writeError(w, apperr.UnavailableError(errors.New("shard group persistence is unavailable")))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1))
	if err != nil || len(body) != 0 {
		writeError(w, apperr.Invalid(errors.New("shard group actions require an empty request body")))
		return
	}
	var group ShardGroup
	switch action {
	case "start":
		group, err = s.shards.Start(r.Context(), r.PathValue("id"))
	case "stop":
		group, err = s.shards.Stop(r.Context(), r.PathValue("id"))
	case "restart":
		group, err = s.shards.Restart(r.Context(), r.PathValue("id"))
	default:
		writeError(w, apperr.NotFoundError(errors.New("unknown shard group action")))
		return
	}
	if err != nil {
		switch {
		case errors.Is(err, errShardGroupNotFound):
			writeError(w, apperr.NotFoundError(err))
		case errors.Is(err, errShardGroupOperation), errors.Is(err, errShardGroupConflict):
			writeError(w, apperr.ConflictError(err))
		case errors.Is(err, errShardGroupUnavailable):
			writeError(w, apperr.UnavailableError(err))
		default:
			var refusal *shard.Refusal
			if errors.As(err, &refusal) {
				writeError(w, apperr.Invalid(err))
			} else {
				writeError(w, apperr.UpstreamError(fmt.Errorf("could not %s shard group: %w", action, err)))
			}
		}
		return
	}
	writeJSON(w, http.StatusOK, group)
}

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
