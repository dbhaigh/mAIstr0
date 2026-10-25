package nodeagent

import (
	"github.com/maistr0/maistr0/internal/selfupdate"
)

func (a *Agent) UpdateSource() selfupdate.Source {
	result := selfupdate.Source{Identity: a.ClusterIdentity()}
	for _, peer := range a.peerComponent.peerSnapshot() {
		if peer.ID != a.ID() && peer.Healthy {
			result.Peers = append(result.Peers, selfupdate.Peer{ID: peer.ID, Address: peer.Address})
		}
	}
	if a.discoveryListener != nil {
		for _, peer := range a.discoveryListener.Snapshot() {
			if peer.ID != a.ID() {
				result.Peers = append(result.Peers, selfupdate.Peer{ID: peer.ID, Address: peer.PeerAddr})
			}
		}
	}
	if id := a.getOrchestratorID(); id != "" {
		result.Peers = append(result.Peers, selfupdate.Peer{ID: id, Address: a.getOrchestratorAddr()})
	}
	return result
}

func (a *Agent) UpdateIdle() bool {
	if a.activeTaskCount() != 0 {
		return false
	}
	if a.peerComponent.tasks.ActiveCount() != 0 {
		return false
	}
	if a.managedEngines != nil && !a.managedEngines.UpdateIdle() {
		return false
	}
	a.stageComponent.mu.Lock()
	loaded := len(a.stageComponent.stages) != 0 || len(a.stageComponent.requests) != 0
	a.stageComponent.mu.Unlock()
	return !loaded
}
