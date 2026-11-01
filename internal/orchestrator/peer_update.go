package orchestrator

import (
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/selfupdate"
	"net/http"
)

func (s *Server) handlePeerUpdate(w http.ResponseWriter, r *http.Request) {
	distribution, err := selfupdate.CurrentDistribution()
	if err != nil {
		logging.Errorf("update", "cannot serve peer executable: %v", err)
		http.Error(w, "peer executable unavailable", http.StatusServiceUnavailable)
		return
	}
	distribution.ServeHTTP(w, r)
}

// UpdateSource returns the local identity and currently healthy peers used to
// select update sources.
//
// Returns: an update source snapshot; peers are limited to healthy registry
// entries.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_test.go](server_test.go).
func (s *Server) UpdateSource() selfupdate.Source {
	result := selfupdate.Source{Identity: s.identity}
	for _, peer := range s.registry.All() {
		if peer.Healthy {
			result.Peers = append(result.Peers, selfupdate.Peer{ID: peer.ID, Address: peer.Address})
		}
	}
	return result
}

// UpdateIdle reports whether the node has no active tasks or shard engines
// that would make an in-place update unsafe.
//
// Returns: true only when task and shard activity is idle.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_test.go](server_test.go).
func (s *Server) UpdateIdle() bool {
	if s.tasks.ActiveCount() != 0 {
		return false
	}
	if s.shards != nil {
		s.shards.mu.RLock()
		defer s.shards.mu.RUnlock()
		for id, group := range s.shards.groups {
			if s.shards.operations[id] || (group.State != ShardGroupStopped && group.State != ShardGroupFailed) {
				return false
			}
			for _, member := range group.Members {
				if member.Started {
					return false
				}
			}
		}
	}
	return true
}
