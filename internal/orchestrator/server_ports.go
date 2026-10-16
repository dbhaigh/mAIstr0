package orchestrator

// These consumer-facing ports keep Server wiring concrete while call sites
// depend on the behavior each component exposes.
func (s *Server) nodeClientPort() nodeClientAPI { return s.nodeClient }
func (s *Server) dispatcherPort() dispatcherAPI { return s.dispatcher }
func (s *Server) plannerPort() taskPlannerAPI   { return s.planner }
func (s *Server) electionPort() electionAPI     { return s.election }
func (s *Server) harnessPort() harnessAPI       { return s.harness }
