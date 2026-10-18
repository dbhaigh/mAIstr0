package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
)

const (
	maxDispatchAttempts = 5
	dispatchBackoff     = 100 * time.Millisecond
	maxDispatchBackoff  = 800 * time.Millisecond
)

type dispatchRetryPolicy struct {
	maxAttempts int
	backoff     time.Duration
	maxBackoff  time.Duration
}

var defaultDispatchRetryPolicy = dispatchRetryPolicy{
	maxAttempts: maxDispatchAttempts,
	backoff:     dispatchBackoff,
	maxBackoff:  maxDispatchBackoff,
}

func (p dispatchRetryPolicy) canRetry(retryable bool, attemptsCompleted int, ctx context.Context) bool {
	return retryable && attemptsCompleted < p.maxAttempts && ctx.Err() == nil
}

func (p dispatchRetryPolicy) delay(attempt int) time.Duration {
	backoff := p.backoff * time.Duration(1<<min(attempt, 3))
	return min(backoff, p.maxBackoff)
}

func dispatchContextFailure(ctx context.Context) error {
	cause := context.Cause(ctx)
	if errors.Is(cause, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.New("subtask dispatch deadline exceeded")
	}
	if errors.Is(cause, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	return nil
}

func classifyDispatchStatus(status int, body []byte) (string, bool) {
	switch {
	case status == http.StatusRequestTimeout:
		return "timeout", true
	case status == http.StatusTooManyRequests:
		return "rate_limit", true
	case status == http.StatusInternalServerError || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout:
		return "upstream_5xx", true
	case status == http.StatusNotFound && strings.Contains(strings.ToLower(string(body)), "model"):
		return "stale_model", true
	default:
		return "client_error", false
	}
}

func (d *dispatcherComponent) retryAssignment(
	ctx context.Context,
	subtask scheduler.Subtask,
	excluded map[string]bool,
) (scheduler.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return scheduler.Assignment{}, err
	}
	nodes := d.schedulerNodes()
	eligible := make([]cluster.NodeStatus, 0, len(nodes))
	for _, node := range nodes {
		models := make([]engine.Model, 0, len(node.Models))
		for _, model := range node.Models {
			if !excluded[routerPairKey(node.ID, model.Name)] {
				models = append(models, model)
			}
		}
		if len(models) > 0 {
			node.Models = models
			eligible = append(eligible, node)
		}
	}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{subtask}, eligible, d.experience())
	if err != nil {
		return scheduler.Assignment{}, err
	}
	if len(assignments) != 1 {
		return scheduler.Assignment{}, errors.New("scheduler returned no retry assignment")
	}
	if err := ctx.Err(); err != nil {
		return scheduler.Assignment{}, err
	}
	return assignments[0], nil
}
