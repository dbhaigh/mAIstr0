package orchestrator

import (
	"fmt"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/discovery"
)

// SetDiscoveryEnabled enables or disables LAN discovery for this orchestrator.
//
// Parameters: enabled (bool) controls whether discovery announcements are
// active.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_election_test.go](server_election_test.go).
func (s *Server) SetDiscoveryEnabled(enabled bool) { s.electionPort().setDiscoveryEnabled(enabled) }

// SetDiscovery attaches the LAN discovery listener used for peer election.
//
// Parameters: listener (*discovery.Listener) supplies the discovery service.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_election_test.go](server_election_test.go).
func (s *Server) SetDiscovery(listener *discovery.Listener) { s.electionPort().setDiscovery(listener) }

// SelfID returns this orchestrator's current cluster identity.
//
// Returns: the local node ID used in discovery and election.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_election_test.go](server_election_test.go).
func (s *Server) SelfID() string { return s.electionPort().selfIDValue() }

// LeadershipAnnouncement reports this instance's ID, score, member count, and
// current election result.
//
// Returns: an empty announcement when election is not configured; otherwise
// the current local announcement.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_election_test.go](server_election_test.go).
func (s *Server) LeadershipAnnouncement() discovery.Announcement {
	if s.election == nil {
		return discovery.Announcement{}
	}
	return s.electionPort().selfAnnouncement()
}

// Retire makes a failover-promoted orchestrator step down. It stops LAN
// announcements and answers as yielded until Reinstate is called; the HTTP
// listener remains bound.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_election_test.go](server_election_test.go).
func (s *Server) Retire() {
	if s.election != nil {
		s.electionPort().setRetired(true)
	}
}

// Reinstate resumes election participation after Retire, subject to normal LAN
// ranking.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_election_test.go](server_election_test.go).
func (s *Server) Reinstate() {
	if s.election != nil {
		s.electionPort().setRetired(false)
	}
}

// Elected reports whether this instance currently serves as orchestrator.
//
// Returns: true when this instance is elected; an instance without election
// configuration is treated as elected.
//
// References: [startup and trust](../../docs/architecture.md#startup-and-trust),
// [server_election_test.go](server_election_test.go).
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
