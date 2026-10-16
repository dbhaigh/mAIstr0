package orchestrator

import (
	"context"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func (s *Server) schedulerNodes() []cluster.NodeStatus { return s.dispatcherPort().schedulerNodes() }

func (s *Server) dispatch(taskID string, assignment scheduler.Assignment) {
	s.dispatcherPort().dispatch(taskID, assignment)
}

func (s *Server) dispatchWithContext(ctx context.Context, taskID string, assignment scheduler.Assignment) {
	s.dispatcherPort().dispatchWithContext(ctx, taskID, assignment)
}

func (s *Server) submitDispatch(taskID string, assignment scheduler.Assignment) error {
	return s.dispatcherPort().submitDispatch(taskID, assignment)
}

func (s *Server) dispatchSnapshot() *dispatchSnapshot { return s.dispatcherPort().snapshot() }

func (s *Server) executeSubtask(ctx context.Context, assignment scheduler.Assignment, prompt string) (string, error, bool) {
	return s.dispatcherPort().executeSubtask(ctx, assignment, prompt)
}

func (s *Server) retryAssignment(ctx context.Context, subtask scheduler.Subtask, excluded map[string]bool) (scheduler.Assignment, error) {
	return s.dispatcherPort().retryAssignment(ctx, subtask, excluded)
}

func (s *Server) experience() scheduler.Experience { return s.dispatcherPort().experience() }

func (s *Server) recordDispatch(assignment scheduler.Assignment, output string, err error, elapsed time.Duration) {
	s.dispatcherPort().recordDispatch(assignment, output, err, elapsed)
}
