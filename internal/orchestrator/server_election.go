package orchestrator

import (
	"github.com/maistr0/maistr0/internal/discovery"
)

func (s *Server) SetDiscoveryEnabled(enabled bool) { s.election.setDiscoveryEnabled(enabled) }

func (s *Server) SetDiscovery(listener *discovery.Listener) { s.election.setDiscovery(listener) }

func (s *Server) SelfID() string { return s.election.selfIDValue() }

func (s *Server) isElectedOrchestrator() bool { return s.election.isElectedOrchestrator() }

func (s *Server) discoverySyncLoop() {
	s.election.discoverySyncLoop(s.publishSnapshot)
}

func (s *Server) syncDiscoveredPeers() {
	s.election.syncDiscoveredPeers(s.publishSnapshot)
}
