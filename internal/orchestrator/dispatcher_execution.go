package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type dispatchPhase string

const (
	dispatchPhaseDependencyWait       dispatchPhase = "dependency_wait"
	dispatchPhaseAttemptBudget        dispatchPhase = "attempt_budget"
	dispatchPhaseRetryAssignment      dispatchPhase = "retry_assignment"
	dispatchPhaseRetryPersistence     dispatchPhase = "retry_persistence"
	dispatchPhaseDispatchContext      dispatchPhase = "dispatch_context"
	dispatchPhaseNodeSelection        dispatchPhase = "node_selection"
	dispatchPhaseCapacityReservation  dispatchPhase = "capacity_reservation"
	dispatchPhaseSubtaskStart         dispatchPhase = "subtask_start"
	dispatchPhaseSubtaskInput         dispatchPhase = "subtask_input"
	dispatchPhaseExecution            dispatchPhase = "execution"
	dispatchPhaseRetryBackoff         dispatchPhase = "retry_backoff"
)

type dispatchFailure struct {
	phase         dispatchPhase
	cause         error
	relatedErr    error
	assignment    scheduler.Assignment
	retryable     bool
	failureClass  string
	policyAttempt int
	httpStatus    int
}

// Error formats the dispatch phase and underlying cause for logs and callers.
//
// Returns: an empty string for a nil receiver; otherwise the phase-specific
// failure message.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [dispatcher_execution_test.go](dispatcher_execution_test.go).
func (e *dispatchFailure) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("dispatch %s phase failed: %v", e.phase, e.cause)
}

// Unwrap exposes the underlying cause for errors.Is and errors.As.
//
// Returns: the cause, or nil for a nil receiver.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [dispatcher_execution_test.go](dispatcher_execution_test.go).
func (e *dispatchFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func newDispatchFailure(phase dispatchPhase, cause error) *dispatchFailure {
	if cause == nil {
		return nil
	}
	return &dispatchFailure{phase: phase, cause: cause, failureClass: string(phase)}
}

type dispatchPlan struct {
	assignment   scheduler.Assignment
	attemptCount int
	alreadyTried map[string]bool
	failure      *dispatchFailure
}

type dispatchAttemptResult struct {
	assignment scheduler.Assignment
	completed  bool
	failure    *dispatchFailure
}

func (d *dispatcherComponent) dispatchWithContext(parent context.Context, taskID string, assignment scheduler.Assignment) {
	ctx, ok := d.tasks.Context(taskID)
	if !ok {
		return
	}
	dispatchCtx, cancel := context.WithCancelCause(ctx)
	stopParent := context.AfterFunc(parent, func() {
		cancel(context.Cause(parent))
	})
	defer func() {
		stopParent()
		cancel(context.Canceled)
	}()

	failure := d.waitForDispatchDependencies(dispatchCtx, taskID, assignment)
	if failure == nil {
		boundedCtx, cancelDeadline := context.WithTimeout(dispatchCtx, d.dispatchTimeout)
		defer cancelDeadline()
		dispatchCtx = boundedCtx
		failure = d.dispatchWithRetries(dispatchCtx, taskID, assignment)
	}
	if failure != nil {
		if failure.assignment.Subtask.ID == "" {
			failure.assignment = assignment
		}
		d.recordDispatchFailure(dispatchCtx, taskID, assignment.Subtask.ID, failure)
		d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", failure)
	}
}

func (d *dispatcherComponent) waitForDispatchDependencies(
	ctx context.Context,
	taskID string,
	assignment scheduler.Assignment,
) *dispatchFailure {
	dependencyStarted := time.Now()
	d.dependencyWaiters.Add(1)
	completed := d.tasks.WaitForDependencies(ctx, taskID, assignment.Subtask.ID)
	d.dependencyWaiters.Add(-1)
	elapsed := time.Since(dependencyStarted)
	d.dependencyWaitNanos.Add(elapsed.Nanoseconds())

	var failure *dispatchFailure
	if !completed {
		cause := dispatchContextFailure(ctx)
		if cause == nil {
			cause = errors.New("a required subtask did not complete")
		}
		failure = newDispatchFailure(dispatchPhaseDependencyWait, cause)
		failure.failureClass = dispatchContextClass(ctx)
		if failure.failureClass == "" {
			failure.failureClass = "dependency_failure"
		}
		failure.assignment = assignment
	} else if elapsed > 0 {
		logging.Infof("orchestrator", "orchestrator: dependency wait task=%s subtask=%s duration_ms=%d",
			taskID, assignment.Subtask.ID, elapsed.Milliseconds())
	}
	return failure
}

func (d *dispatcherComponent) prepareDispatchPlan(
	ctx context.Context,
	taskID string,
	assignment scheduler.Assignment,
) dispatchPlan {
	plan := dispatchPlan{
		assignment:   assignment,
		alreadyTried: make(map[string]bool),
	}
	if task, ok := d.tasks.Get(taskID); ok {
		for _, result := range task.Subtasks {
			if result.Subtask.ID != assignment.Subtask.ID {
				continue
			}
			plan.attemptCount = len(result.Attempts)
			for _, prior := range result.Attempts {
				plan.alreadyTried[routerPairKey(prior.NodeID, prior.Model)] = true
			}
			break
		}
	}
	if plan.attemptCount >= d.retryPolicy.maxAttempts {
		plan.failure = newDispatchFailure(
			dispatchPhaseAttemptBudget,
			errors.New("subtask dispatch attempt limit reached"),
		)
		plan.failure.failureClass = "attempt_limit"
	}
	if plan.failure == nil && plan.alreadyTried[routerPairKey(plan.assignment.NodeID, plan.assignment.Model)] {
		retry, retryErr := d.retryAssignment(ctx, plan.assignment.Subtask, plan.alreadyTried)
		if retryErr != nil {
			cause := dispatchContextFailure(ctx)
			if cause == nil {
				cause = errors.New("no untried node/model pair is available")
			}
			plan.failure = newDispatchFailure(dispatchPhaseRetryAssignment, cause)
			plan.failure.relatedErr = retryErr
			plan.failure.failureClass = dispatchContextClass(ctx)
			if plan.failure.failureClass == "" {
				plan.failure.failureClass = "no_retry_assignment"
			}
		} else if err := d.reassignForRetry(taskID, assignment.Subtask.ID, retry); err != nil {
			plan.failure = newDispatchFailure(dispatchPhaseRetryPersistence, err)
			plan.failure.failureClass = "persistence"
		} else {
			plan.assignment = retry
		}
	}
	if plan.failure != nil {
		plan.failure.assignment = plan.assignment
	}
	return plan
}

func (d *dispatcherComponent) dispatchWithRetries(
	ctx context.Context,
	taskID string,
	assignment scheduler.Assignment,
) *dispatchFailure {
	plan := d.prepareDispatchPlan(ctx, taskID, assignment)
	current := plan.assignment
	failure := plan.failure
	completed := false

	for attempt := plan.attemptCount; attempt < d.retryPolicy.maxAttempts && failure == nil && !completed; attempt++ {
		result := d.runDispatchAttempt(ctx, taskID, current, attempt, plan.alreadyTried)
		current = result.assignment
		completed = result.completed
		failure = result.failure
	}

	if failure == nil && !completed {
		failure = newDispatchFailure(
			dispatchPhaseAttemptBudget,
			errors.New("subtask dispatch attempt limit reached"),
		)
		failure.failureClass = "attempt_limit"
		failure.assignment = current
	}
	if failure != nil && failure.assignment.Subtask.ID == "" {
		failure.assignment = current
	}
	return failure
}

func (d *dispatcherComponent) runDispatchAttempt(
	ctx context.Context,
	taskID string,
	current scheduler.Assignment,
	attempt int,
	alreadyTried map[string]bool,
) dispatchAttemptResult {
	result := dispatchAttemptResult{assignment: current}
	if ctx.Err() != nil {
		result.failure = contextDispatchFailure(ctx, dispatchPhaseDispatchContext, ctx.Err())
	} else {
		node, healthy := d.registry.Get(current.NodeID)
		if !healthy || !node.Healthy || node.Address != current.Address {
			result = d.retryBeforeDispatch(
				ctx, taskID, current, alreadyTried,
				errors.New("node was ejected before dispatch"),
				dispatchPhaseNodeSelection, true, attempt,
			)
		} else {
			reservation, err := d.reserveTaskCapacity(ctx, current)
			if err != nil {
				if ctx.Err() != nil {
					result.failure = contextDispatchFailure(ctx, dispatchPhaseCapacityReservation, err)
				} else {
					result = d.retryBeforeDispatch(
						ctx, taskID, current, alreadyTried, err,
						dispatchPhaseCapacityReservation, false, attempt,
					)
				}
			} else {
				result = d.executeReservedDispatch(ctx, taskID, current, reservation, attempt, alreadyTried)
			}
		}
	}
	if result.failure != nil {
		result.failure.assignment = result.assignment
	}
	return result
}

func (d *dispatcherComponent) retryBeforeDispatch(
	ctx context.Context,
	taskID string,
	current scheduler.Assignment,
	alreadyTried map[string]bool,
	fallback error,
	phase dispatchPhase,
	requireActiveContext bool,
	attempt int,
) dispatchAttemptResult {
	result := dispatchAttemptResult{assignment: current}
	alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
	retry, retryErr := d.retryAssignment(ctx, current.Subtask, alreadyTried)
	retryAllowed := retryErr == nil && (!requireActiveContext || ctx.Err() == nil)
	if retryAllowed {
		if err := d.reassignForRetry(taskID, current.Subtask.ID, retry); err != nil {
			result.failure = newDispatchFailure(dispatchPhaseRetryPersistence, err)
			result.failure.failureClass = "persistence"
			result.failure.policyAttempt = attempt + 1
		} else {
			result.assignment = retry
		}
	} else {
		cause := dispatchContextFailure(ctx)
		if cause == nil {
			cause = fallback
		}
		result.failure = newDispatchFailure(phase, cause)
		result.failure.relatedErr = retryErr
		result.failure.policyAttempt = attempt + 1
		if contextClass := dispatchContextClass(ctx); contextClass != "" {
			result.failure.failureClass = contextClass
		}
	}
	return result
}

func (d *dispatcherComponent) executeReservedDispatch(
	ctx context.Context,
	taskID string,
	current scheduler.Assignment,
	reservation *dispatchReservation,
	attempt int,
	alreadyTried map[string]bool,
) dispatchAttemptResult {
	result := dispatchAttemptResult{assignment: current}
	defer reservation.Release()
	d.metrics.recordCapacityReservation()

	if err := d.tasks.StartSubtaskChecked(taskID, current.Subtask.ID); err != nil {
		result.failure = newDispatchFailure(dispatchPhaseSubtaskStart, err)
		result.failure.failureClass = "persistence"
		result.failure.policyAttempt = attempt + 1
	} else if prompt, ok := d.tasks.InputForSubtask(taskID, current.Subtask.ID); !ok {
		result.failure = newDispatchFailure(
			dispatchPhaseSubtaskInput,
			errors.New("subtask prerequisites are not available"),
		)
		result.failure.failureClass = "dependency_failure"
		result.failure.policyAttempt = attempt + 1
	} else {
		reservation.Start()
		candidateResult := d.executeCandidateWithHedge(ctx, taskID, current, prompt, attempt, alreadyTried)
		reservation.Release()
		for _, attempted := range candidateResult.attempted {
			alreadyTried[routerPairKey(attempted.NodeID, attempted.Model)] = true
		}

		attemptErr, retryable := candidateResult.err, candidateResult.retryable
		if attemptErr == nil || candidateResult.won {
			result.completed = true
		} else if !d.retryPolicy.canRetry(retryable, attempt+1, ctx) {
			result.failure = contextDispatchFailure(ctx, dispatchPhaseExecution, attemptErr)
			result.failure.retryable = retryable
			result.failure.failureClass = candidateFailureClass(candidateResult.failureClass)
			result.failure.httpStatus = candidateResult.httpStatus
			result.failure.policyAttempt = attempt + 1
		} else {
			alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
			if !sleepWithContext(ctx, d.retryPolicy.delay(attempt)) {
				result.failure = contextDispatchFailure(ctx, dispatchPhaseRetryBackoff, context.Canceled)
				result.failure.policyAttempt = attempt + 1
			} else {
				retry, retryErr := d.retryAssignment(ctx, current.Subtask, alreadyTried)
				if retryErr != nil {
					result.failure = contextDispatchFailure(ctx, dispatchPhaseRetryAssignment, attemptErr)
					result.failure.relatedErr = retryErr
					result.failure.retryable = retryable
					result.failure.failureClass = candidateFailureClass(candidateResult.failureClass)
					result.failure.httpStatus = candidateResult.httpStatus
					result.failure.policyAttempt = attempt + 1
				} else if err := d.reassignForRetry(taskID, current.Subtask.ID, retry); err != nil {
					result.failure = newDispatchFailure(dispatchPhaseRetryPersistence, err)
					result.failure.failureClass = "persistence"
					result.failure.policyAttempt = attempt + 1
				} else {
					logging.Errorf("orchestrator", "orchestrator: retrying subtask %s on node %s after node %s failed: %v",
						current.Subtask.ID, retry.NodeID, current.NodeID, attemptErr)
					result.assignment = retry
				}
			}
		}
	}
	if result.failure != nil {
		result.failure.assignment = result.assignment
	}
	return result
}

func contextDispatchFailure(ctx context.Context, phase dispatchPhase, fallback error) *dispatchFailure {
	cause := dispatchContextFailure(ctx)
	if cause == nil {
		cause = fallback
	}
	failure := newDispatchFailure(phase, cause)
	if failure != nil {
		if contextClass := dispatchContextClass(ctx); contextClass != "" {
			failure.failureClass = contextClass
		}
	}
	return failure
}

func dispatchContextClass(ctx context.Context) string {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return "timeout"
	case ctx.Err() != nil:
		return "cancelled"
	default:
		return ""
	}
}

func candidateFailureClass(failureClass string) string {
	if failureClass == "" {
		return "dispatch_error"
	}
	return failureClass
}

func (d *dispatcherComponent) recordDispatchFailure(
	ctx context.Context,
	taskID string,
	subtaskID string,
	failure *dispatchFailure,
) {
	if failure == nil {
		return
	}
	assignment := failure.assignment
	failureClass := failure.failureClass
	if failureClass == "" {
		failureClass = string(failure.phase)
	}
	attrs := []any{
		"component", "orchestrator",
		"event_type", "dispatch_error",
		"request_id", taskmgr.DispatchRequestID(taskID, subtaskID),
		"task_id", taskID,
		"subtask_id", subtaskID,
		"phase", string(failure.phase),
		"node_id", assignment.NodeID,
		"model", assignment.Model,
		"engine", assignment.Engine,
		"failure_class", failureClass,
		"retryable", failure.retryable,
	}
	if failure.cause != nil {
		attrs = append(attrs, "error_type", dispatchErrorType(failure.cause))
	}
	if failure.relatedErr != nil {
		attrs = append(attrs, "related_error_type", dispatchErrorType(failure.relatedErr))
	}
	if failure.policyAttempt > 0 {
		attrs = append(attrs, "policy_attempt", failure.policyAttempt)
	}
	if failure.httpStatus > 0 {
		attrs = append(attrs, "http_status", failure.httpStatus)
	}
	slog.Default().Log(ctx, slog.LevelError, "dispatch phase failed", attrs...)
}

func dispatchErrorType(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T", err)
}

func (d *dispatcherComponent) reassignForRetry(taskID, subtaskID string, assignment scheduler.Assignment) error {
	if err := d.tasks.ReassignSubtaskChecked(taskID, subtaskID, assignment); err != nil {
		return err
	}
	d.metrics.recordRetry()
	return nil
}

func validationRetryPrompt(prompt string, validationErr error, output string) string {
	const maxPriorOutputBytes = 8 * 1024
	if len(output) > maxPriorOutputBytes {
		output = output[:maxPriorOutputBytes]
	}
	return fmt.Sprintf("%s\n\nYour previous result did not pass validation: %s\nReturn exactly one corrected JSON object with no extra outer braces, markdown fences, or commentary. Previous result:\n%s",
		prompt, validationErr, output)
}

func (d *dispatcherComponent) executeSubtask(ctx context.Context, assignment scheduler.Assignment, prompt string) (string, error, bool) {
	outcome := d.executeSubtaskDetailed(ctx, assignment, prompt)
	return outcome.output, outcome.err, outcome.retryable
}

type dispatchOutcome struct {
	output       string
	err          error
	retryable    bool
	failureClass string
	httpStatus   int
}

func (d *dispatcherComponent) executeSubtaskDetailed(ctx context.Context, assignment scheduler.Assignment, prompt string) dispatchOutcome {
	return d.executeSubtaskWithIDs(ctx, assignment, prompt, "", "")
}

func (d *dispatcherComponent) executeSubtaskWithIDs(
	ctx context.Context,
	assignment scheduler.Assignment,
	prompt string,
	requestID string,
	attemptID string,
) dispatchOutcome {
	if strings.HasPrefix(assignment.Engine, "shard-group:") {
		if d.shards == nil || d.shardModel == nil {
			return dispatchOutcome{err: errors.New("shard task execution is unavailable"), failureClass: "shard_group_unavailable"}
		}
		group, ok := d.shards.Get(strings.TrimPrefix(assignment.Engine, "shard-group:"))
		if !ok || group.State != ShardGroupReady || !strings.EqualFold(group.Model.Name, assignment.Model) {
			return dispatchOutcome{err: errors.New("assigned shard group is not ready"), failureClass: "shard_group_unavailable"}
		}
		if owner, found := d.shards.GroupForModel(assignment.Model); !found || owner.ID != group.ID {
			return dispatchOutcome{err: errors.New("assigned shard group has been replaced"), failureClass: "shard_group_unavailable"}
		}
		d.metrics.recordDispatch()
		output, err := d.shardModel.generateShardGroupWithIDs(ctx, group, assignment.Address, prompt, 0, 0, requestID, attemptID)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return dispatchOutcome{err: err, failureClass: "cancelled"}
			}
			var upstream *nodeResponseError
			if errors.As(err, &upstream) {
				failureClass, retryable := classifyUpstreamStatus(upstream.statusCode, []byte(upstream.body))
				return dispatchOutcome{err: err, failureClass: failureClass, retryable: retryable, httpStatus: upstream.statusCode}
			}
			return dispatchOutcome{err: err, failureClass: "shard_execution_failure", retryable: !errors.Is(err, errShardGroupUnavailable) && !errors.Is(err, errShardGroupNotFound)}
		}
		return dispatchOutcome{output: output, httpStatus: http.StatusOK}
	}
	body, err := json.Marshal(struct {
		Engine    string `json:"engine"`
		Model     string `json:"model"`
		Prompt    string `json:"prompt"`
		SubtaskID string `json:"subtask_id"`
		TaskType  string `json:"task_type"`
	}{
		Engine: assignment.Engine, Model: assignment.Model, Prompt: prompt,
		SubtaskID: assignment.Subtask.ID, TaskType: assignment.Subtask.TaskType,
	})
	if err != nil {
		return dispatchOutcome{err: err, failureClass: "request_encoding"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, assignment.Address+"/execute", bytes.NewReader(body))
	if err != nil {
		return dispatchOutcome{err: err, failureClass: "request_creation"}
	}
	req.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		req.Header.Set(httpx.RequestIDHeader, requestID)
		req.Header.Set(inferroute.HeaderRequestID, requestID)
	}
	if attemptID != "" {
		req.Header.Set(inferroute.HeaderAttemptID, attemptID)
	}
	d.metrics.recordDispatch()
	resp, err := d.executeClient.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if errors.Is(err, context.Canceled) {
			return dispatchOutcome{err: err, failureClass: "cancelled"}
		}
		var networkErr net.Error
		if errors.As(err, &networkErr) && networkErr.Timeout() {
			return dispatchOutcome{err: errors.New("node request timed out"), retryable: true, failureClass: "timeout"}
		}
		return dispatchOutcome{err: errors.New("node request failed"), retryable: true, failureClass: "transport"}
	}
	defer closeutil.Discard(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		failureClass, retryable := classifyUpstreamStatus(resp.StatusCode, errBody)
		if readErr != nil {
			if retryable {
				failureClass = "upstream_read"
			}
			return dispatchOutcome{err: errors.New("read node error response failed"), retryable: retryable, failureClass: failureClass, httpStatus: resp.StatusCode}
		}
		return dispatchOutcome{
			err: errors.New("node returned " + resp.Status), retryable: retryable,
			failureClass: failureClass, httpStatus: resp.StatusCode,
		}
	}
	var out struct {
		Output string `json:"output"`
		Error  string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return dispatchOutcome{err: errors.New("invalid response from node"), retryable: true, failureClass: "invalid_response", httpStatus: resp.StatusCode}
	}
	if out.Error != "" {
		return dispatchOutcome{err: errors.New("node reported execution failure"), failureClass: "execution_failure", httpStatus: resp.StatusCode}
	}
	return dispatchOutcome{output: out.Output, httpStatus: resp.StatusCode}
}

func (d *dispatcherComponent) experience() scheduler.Experience {
	if d.memory == nil {
		return nil
	}
	return d.memory
}

func (d *dispatcherComponent) recordDispatch(assignment scheduler.Assignment, output string, dispatchErr error, elapsed time.Duration) {
	d.recordDispatchWithAttemptID(assignment, "", "", output, dispatchErr, elapsed, "", 0)
}

func (d *dispatcherComponent) recordDispatchWithAttemptID(
	assignment scheduler.Assignment,
	requestID string,
	attemptID string,
	output string,
	dispatchErr error,
	elapsed time.Duration,
	failureClass string,
	httpStatus int,
) {
	d.dispatchNanos.Add(elapsed.Nanoseconds())
	if dispatchErr == nil {
		failureClass = "none"
	} else if failureClass == "" {
		failureClass = "dispatch_error"
	}
	taskID, _, _ := strings.Cut(requestID, ":")
	level := slog.LevelInfo
	if dispatchErr != nil {
		level = slog.LevelError
	}
	attrs := []any{
		"component", "orchestrator",
		"event_type", "dispatch_attempt",
		"phase", string(dispatchPhaseExecution),
		"request_id", requestID,
		"attempt_id", attemptID,
		"task_id", taskID,
		"subtask_id", assignment.Subtask.ID,
		"task_type", assignment.Subtask.TaskType,
		"node_id", assignment.NodeID,
		"model", assignment.Model,
		"engine", assignment.Engine,
		"duration_ms", elapsed.Milliseconds(),
		"http_status", httpStatus,
		"failure_class", failureClass,
		"success", dispatchErr == nil,
	}
	if errorType := dispatchErrorType(dispatchErr); errorType != "" {
		attrs = append(attrs, "error_type", errorType)
	}
	slog.Default().Log(context.Background(), level, "dispatch completed", attrs...)
	if d.memory == nil || failureClass == "hedge_loser" {
		return
	}
	errMsg := ""
	if dispatchErr != nil {
		errMsg = dispatchErr.Error()
	}
	if _, err := d.memory.Record(memory.Experience{
		Kind:        "subtask",
		TaskType:    assignment.Subtask.TaskType,
		Description: assignment.Subtask.Description,
		Prompt:      assignment.Subtask.Description,
		Output:      output,
		NodeID:      assignment.NodeID,
		Model:       assignment.Model,
		Success:     dispatchErr == nil,
		Error:       errMsg,
		DurationMs:  elapsed.Milliseconds(),
		Tags:        append(append([]string{"dispatch", "role:" + assignment.Subtask.Role}, assignment.Subtask.Tags...), "stage:"+assignment.Subtask.TaskType),
	}); err != nil {
		logging.Errorf("orchestrator", "orchestrator: record subtask experience failed: %v", err)
	}
}
