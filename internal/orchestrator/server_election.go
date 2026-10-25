package orchestrator

import (
	"fmt"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/discovery"
)

func (s *Server) SetDiscoveryEnabled(enabled bool) { s.electionPort().setDiscoveryEnabled(enabled) }

func (s *Server) SetDiscovery(listener *discovery.Listener) { s.electionPort().setDiscovery(listener) }

func (s *Server) SelfID() string { return s.electionPort().selfIDValue() }

// LeadershipAnnouncement reports this instance's ID, score, member count, and
// whether it currently considers itself the elected orchestrator.
func (s *Server) LeadershipAnnouncement() discovery.Announcement {
	if s.election == nil {
		return discovery.Announcement{}
	}
	return s.electionPort().selfAnnouncement()
}

// Retire makes a failover-promoted orchestrator step down: it stops LAN
// announcements and answers as a yielded instance until Reinstate is called.
// The HTTP listener stays bound so the same instance can be reinstated.
func (s *Server) Retire() {
	if s.election != nil {
		s.electionPort().setRetired(true)
	}
}

// Reinstate resumes leadership after Retire, subject to normal LAN ranking.
func (s *Server) Reinstate() {
	if s.election != nil {
		s.electionPort().setRetired(false)
	}
}

// Elected reports whether this instance currently serves as orchestrator.
func (s *Server) Elected() bool { return s.isElectedOrchestrator() }

func (s *Server) isElectedOrchestrator() bool {
	return s.election == nil || s.electionPort().isElectedOrchestrator()
}

func requireElectedOrchestrator(isElected func() bool, next, yielded http.Handler) http.Handler {
	if yielded == nil {
		yielded = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, apperr.UnavailableError(fmt.Errorf("this instance yielded orchestrator leadership to a higher-ranked cluster member")))
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isElected() {
			yielded.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) discoverySyncLoop() {
	s.electionPort().discoverySyncLoop(s.background.Context(), s.publishSnapshot)
}

func (s *Server) syncDiscoveredPeers() {
	s.electionPort().syncDiscoveredPeers(s.publishSnapshot)
}
