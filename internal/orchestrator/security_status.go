package orchestrator

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/discovery"
)

func (s *Server) handleSecurityStatus(w http.ResponseWriter, _ *http.Request) {
	var key []byte
	if s.election != nil {
		key = s.election.discoveryHMACKey
	}
	writeJSON(w, http.StatusOK, discovery.SecurityStatusForKey(key))
}
