package orchestrator

import (
	"errors"

	"github.com/maistr0/maistr0/internal/agent"
)

// HarnessName returns the name of the active agent harness, if enabled.
//
// Returns: the selected harness name, or an empty string when no harness is
// configured.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [server_test.go](server_test.go).
func (s *Server) HarnessName() string {
	if s.harness == nil {
		return ""
	}
	return s.harnessPort().harnessName()
}

func (s *Server) currentAgent() (string, agent.Backend) {
	if s.harness == nil {
		return "", nil
	}
	return s.harnessPort().currentAgent()
}

func (s *Server) agentForSession(id string) (string, agent.Backend, bool) {
	if s.harness == nil {
		return "", nil, false
	}
	return s.harnessPort().agentForSession(id)
}

func (s *Server) selectHarness(name string) bool {
	if s.harness == nil {
		return false
	}
	return s.harnessPort().selectHarness(name)
}

func (s *Server) listAgentSessions() []*agent.Session {
	if s.harness == nil {
		return nil
	}
	return s.harnessPort().listAgentSessions()
}

func (s *Server) persistAgentSession(session *agent.Session) error {
	if s.harness == nil {
		return errors.New("agent harness is disabled")
	}
	return s.harnessPort().persistAgentSession(session)
}

func (s *Server) agentStats() agent.AgentStats {
	if s.harness == nil {
		return agent.AgentStats{}
	}
	return s.harnessPort().agentStats()
}

// InterruptSession cancels an in-flight message turn for the given session ID.
//
// Parameters: id (string) identifies the agent session.
// Returns: true if an active turn was interrupted.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [harness_turn_ownership_test.go](harness_turn_ownership_test.go).
func (s *Server) InterruptSession(id string) bool {
	if s.harness == nil {
		return false
	}
	return s.harnessPort().interruptSession(id)
}
