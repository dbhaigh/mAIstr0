package orchestrator

import (
	"context"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func (s *Server) schedulerNodes() []cluster.NodeStatus { return s.dispatcher.schedulerNodes() }

func (s *Server) dispatch(taskID string, assignment scheduler.Assignment) {
	s.dispatcher.dispatch(taskID, assignment)
}

func (s *Server) dispatchWithContext(ctx context.Context, taskID string, assignment scheduler.Assignment) {
	s.dispatcher.dispatchWithContext(ctx, taskID, assignment)
}

func (s *Server) nodeDispatchGate(nodeID string, limit int) *nodeDispatchGate {
	return s.dispatcher.nodeDispatchGate(nodeID, limit)
}

func (s *Server) dispatchCapacity(ctx context.Context, gate *nodeDispatchGate, nodeID, address string) (int, int, int, bool) {
	return s.dispatcher.dispatchCapacity(ctx, gate, nodeID, address)
}

func (s *Server) submitDispatch(taskID string, assignment scheduler.Assignment) error {
	return s.dispatcher.submitDispatch(taskID, assignment)
}

func (s *Server) dispatchSnapshot() *dispatchSnapshot { return s.dispatcher.snapshot() }

func (s *Server) executeSubtask(ctx context.Context, assignment scheduler.Assignment, prompt string) (string, error, bool) {
	return s.dispatcher.executeSubtask(ctx, assignment, prompt)
}

func (s *Server) retryAssignment(ctx context.Context, subtask scheduler.Subtask, excluded map[string]bool) (scheduler.Assignment, error) {
	return s.dispatcher.retryAssignment(ctx, subtask, excluded)
}

func (s *Server) experience() scheduler.Experience { return s.dispatcher.experience() }

func (s *Server) recordDispatch(assignment scheduler.Assignment, output string, err error, elapsed time.Duration) {
	s.dispatcher.recordDispatch(assignment, output, err, elapsed)
}
