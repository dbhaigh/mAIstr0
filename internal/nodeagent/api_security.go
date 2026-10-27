package nodeagent

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/discovery"
)

func (a *Agent) handleAPISecurityStatus(w http.ResponseWriter, _ *http.Request) {
	cfg := a.configSnapshot()
	writeJSON(w, http.StatusOK, discovery.SecurityStatusForKey(cfg.DiscoveryHMACKey()))
}
