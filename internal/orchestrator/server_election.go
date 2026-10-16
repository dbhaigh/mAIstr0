package orchestrator

import (
	"github.com/maistr0/maistr0/internal/discovery"
)

func (s *Server) SetDiscoveryEnabled(enabled bool) { s.electionPort().setDiscoveryEnabled(enabled) }

func (s *Server) SetDiscovery(listener *discovery.Listener) { s.electionPort().setDiscovery(listener) }

func (s *Server) SelfID() string { return s.electionPort().selfIDValue() }

func (s *Server) isElectedOrchestrator() bool {
	return s.election == nil || s.electionPort().isElectedOrchestrator()
}

func (s *Server) discoverySyncLoop() {
	s.electionPort().discoverySyncLoop(s.publishSnapshot)
}

func (s *Server) syncDiscoveredPeers() {
	s.electionPort().syncDiscoveredPeers(s.publishSnapshot)
}
