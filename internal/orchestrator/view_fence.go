package orchestrator

import (
	"errors"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/apperr"
)

const (
	HeaderOrchestratorID    = "X-Maistr0-Orchestrator-ID"
	HeaderOrchestratorEpoch = "X-Maistr0-Orchestrator-Epoch"
)

func (s *Server) fenceStateChangingRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderOrchestratorID, s.SelfID())
		w.Header().Set(HeaderOrchestratorEpoch, s.orchestratorEpoch)

		if !stateChangingRequest(r) || isPairingRequest(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		id := r.Header.Get(HeaderOrchestratorID)
		epoch := r.Header.Get(HeaderOrchestratorEpoch)
		if id == "" && epoch == "" {
			next.ServeHTTP(w, r)
			return
		}
		if id == "" || epoch == "" || id != s.SelfID() || epoch != s.orchestratorEpoch {
			writeError(w, apperr.ConflictError(errors.New("orchestrator view is stale or incomplete; refresh the orchestrator view")))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func stateChangingRequest(r *http.Request) bool {
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func isPairingRequest(path string) bool {
	return strings.HasPrefix(path, "/api/pairing/") || strings.Contains(path, "/pairing/")
}
