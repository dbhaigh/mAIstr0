package nodeagent

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/selfupdate"
)

func (a *Agent) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	if r.TLS != nil {
		if len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "upgrade requires the current paired orchestrator", http.StatusForbidden)
			return
		}
		peerID := strings.TrimSpace(r.TLS.PeerCertificates[0].Subject.CommonName)
		if !a.identityComponent.IsTrustedOrchestratorID(peerID) || peerID != a.currentElectedOrchestratorID() {
			http.Error(w, "upgrade requires the current paired orchestrator", http.StatusForbidden)
			return
		}
	}
	request, err := httpx.DecodeJSON[selfupdate.UpgradeRequest](w, r, 4<<10)
	if err != nil {
		http.Error(w, "invalid upgrade request", http.StatusBadRequest)
		return
	}
	if err := request.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.upgradeMu.RLock()
	requester := a.upgradeRequester
	a.upgradeMu.RUnlock()
	if requester == nil {
		http.Error(w, "upgrade service is unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := requester(r.Context(), request); err != nil {
		switch {
		case errors.Is(err, selfupdate.ErrUpgradeInProgress):
			http.Error(w, err.Error(), http.StatusConflict)
		case errors.Is(err, selfupdate.ErrPeerUpdatesDisabled):
			http.Error(w, err.Error(), http.StatusForbidden)
		default:
			http.Error(w, "upgrade request could not be queued", http.StatusServiceUnavailable)
		}
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"state": "queued"})
}

func (a *Agent) setUpgradeStatus(status cluster.UpgradeStatus) {
	if status.Progress < 0 {
		status.Progress = 0
	} else if status.Progress > 100 {
		status.Progress = 100
	}
	if status.UpdatedAt.IsZero() {
		status.UpdatedAt = time.Now().UTC()
	}
	a.upgradeMu.Lock()
	copy := status
	a.upgradeStatus = &copy
	a.upgradeMu.Unlock()
	if a.orchestratorLink != nil {
		a.orchestratorLink.requestStatusUpdate()
	}
}

func (a *Agent) upgradeStatusSnapshot() *cluster.UpgradeStatus {
	a.upgradeMu.RLock()
	defer a.upgradeMu.RUnlock()
	if a.upgradeStatus == nil {
		return nil
	}
	copy := *a.upgradeStatus
	return &copy
}
