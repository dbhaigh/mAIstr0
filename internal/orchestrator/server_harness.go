package orchestrator

import "github.com/maistr0/maistr0/internal/agent"

func (s *Server) HarnessName() string {
	return s.harness.harnessName()
}

func (s *Server) currentAgent() (string, agent.Backend) {
	return s.harness.currentAgent()
}

func (s *Server) agentForSession(id string) (string, agent.Backend, bool) {
	return s.harness.agentForSession(id)
}

func (s *Server) selectHarness(name string) bool {
	return s.harness.selectHarness(name)
}

func (s *Server) listAgentSessions() []*agent.Session {
	return s.harness.listAgentSessions()
}

func (s *Server) restoreAgentSessions() error {
	return s.harness.restoreAgentSessions()
}

func (s *Server) persistAgentSession(session *agent.Session) error {
	return s.harness.persistAgentSession(session)
}

func (s *Server) agentStats() agent.AgentStats {
	return s.harness.agentStats()
}

// InterruptSession cancels an in-flight message turn for the given session ID.
func (s *Server) InterruptSession(id string) bool {
	return s.harness.interruptSession(id)
}
