package orchestrator

import (
	"errors"

	"github.com/maistr0/maistr0/internal/agent"
)

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

func (s *Server) restoreAgentSessions() error {
	if s.harness == nil {
		return nil
	}
	return s.harnessPort().restoreAgentSessions()
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
func (s *Server) InterruptSession(id string) bool {
	if s.harness == nil {
		return false
	}
	return s.harnessPort().interruptSession(id)
}
