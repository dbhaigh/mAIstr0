package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

func (d *dispatcherComponent) dispatchWithContext(parent context.Context, taskID string, assignment scheduler.Assignment) {
	ctx, ok := d.tasks.Context(taskID)
	if !ok {
		return
	}
	dispatchCtx, cancel := context.WithCancelCause(ctx)
	stopParent := context.AfterFunc(parent, func() {
		cancel(parent.Err())
	})
	defer func() {
		stopParent()
		cancel(context.Canceled)
	}()

	dependencyStarted := time.Now()
	d.dependencyWaiters.Add(1)
	if !d.tasks.WaitForDependencies(dispatchCtx, taskID, assignment.Subtask.ID) {
		d.dependencyWaiters.Add(-1)
		elapsed := time.Since(dependencyStarted)
		d.dependencyWaitNanos.Add(elapsed.Nanoseconds())
		failure := dispatchContextFailure(dispatchCtx)
		if failure == nil {
			failure = errors.New("a required subtask did not complete")
		}
		d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", failure)
		return
	}
	d.dependencyWaiters.Add(-1)
	dependencyElapsed := time.Since(dependencyStarted)
	d.dependencyWaitNanos.Add(dependencyElapsed.Nanoseconds())
	if dependencyElapsed > 0 {
		log.Printf("orchestrator: dependency wait task=%s subtask=%s duration_ms=%d", taskID, assignment.Subtask.ID, dependencyElapsed.Milliseconds())
	}

	boundedCtx, cancelDeadline := context.WithTimeout(dispatchCtx, d.dispatchTimeout)
	defer cancelDeadline()
	dispatchCtx = boundedCtx
	current := assignment
	alreadyTried := make(map[string]bool)
	attemptCount := 0
	if task, ok := d.tasks.Get(taskID); ok {
		for _, result := range task.Subtasks {
			if result.Subtask.ID != assignment.Subtask.ID {
				continue
			}
			attemptCount = len(result.Attempts)
			for _, prior := range result.Attempts {
				alreadyTried[routerPairKey(prior.NodeID, prior.Model)] = true
			}
			break
		}
	}
	if attemptCount >= d.retryPolicy.maxAttempts {
		d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("subtask dispatch attempt limit reached"))
		return
	}
	if alreadyTried[routerPairKey(current.NodeID, current.Model)] {
		retry, err := d.retryAssignment(dispatchCtx, current.Subtask, alreadyTried)
		if err != nil {
			if failure := dispatchContextFailure(dispatchCtx); failure != nil {
				d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", failure)
				return
			}
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("no untried node/model pair is available"))
			return
		}
		if err := d.reassignForRetry(taskID, assignment.Subtask.ID, retry); err != nil {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", err)
			return
		}
		current = retry
	}
	for attempt := attemptCount; attempt < d.retryPolicy.maxAttempts; attempt++ {
		if dispatchCtx.Err() != nil {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", dispatchContextFailure(dispatchCtx))
			return
		}
		node, healthy := d.registry.Get(current.NodeID)
		if !healthy || !node.Healthy || node.Address != current.Address {
			alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
			retry, err := d.retryAssignment(dispatchCtx, current.Subtask, alreadyTried)
			if err == nil && dispatchCtx.Err() == nil {
				if persistErr := d.reassignForRetry(taskID, assignment.Subtask.ID, retry); persistErr != nil {
					d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", persistErr)
					return
				}
				current = retry
				continue
			}
			failure := dispatchContextFailure(dispatchCtx)
			if failure == nil {
				failure = errors.New("node was ejected before dispatch")
			}
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", failure)
			return
		}
		reservation, err := reserveDispatchCapacity(dispatchCtx, d.capacityGate, d.registry, current)
		if err != nil {
			if dispatchCtx.Err() != nil {
				d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", dispatchContextFailure(dispatchCtx))
				return
			}
			alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
			retry, retryErr := d.retryAssignment(dispatchCtx, current.Subtask, alreadyTried)
			if retryErr == nil {
				if persistErr := d.reassignForRetry(taskID, assignment.Subtask.ID, retry); persistErr != nil {
					d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", persistErr)
					return
				}
				current = retry
				continue
			}
			failure := dispatchContextFailure(dispatchCtx)
			if failure == nil {
				failure = err
			}
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", failure)
			return
		}
		d.metrics.recordCapacityReservation()
		defer reservation.Release()
		if err := d.tasks.StartSubtaskChecked(taskID, assignment.Subtask.ID); err != nil {
			log.Printf("orchestrator: could not persist start of subtask %s: %v", assignment.Subtask.ID, err)
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", err)
			return
		}
		prompt, ok := d.tasks.InputForSubtask(taskID, assignment.Subtask.ID)
		if !ok {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("subtask prerequisites are not available"))
			return
		}
		reservation.Start()
		attemptPrompt := prompt
		var output string
		var attemptErr error
		retryable := true
		for validationAttempt := 0; ; validationAttempt++ {
			started := time.Now()
			attemptNumber, beginErr := d.tasks.BeginDispatchAttemptChecked(taskID, assignment.Subtask.ID, current)
			if beginErr != nil {
				attemptErr = beginErr
				retryable = false
				break
			}
			attemptID := taskmgr.DispatchAttemptID(taskID, assignment.Subtask.ID, attemptNumber)
			requestID := taskmgr.DispatchRequestID(taskID, assignment.Subtask.ID)
			outcome := d.executeSubtaskWithIDs(dispatchCtx, current, attemptPrompt, requestID, attemptID)
			output, attemptErr, retryable = outcome.output, outcome.err, outcome.retryable
			failureClass, httpStatus := outcome.failureClass, outcome.httpStatus
			if contextFailure := dispatchContextFailure(dispatchCtx); contextFailure != nil {
				attemptErr = contextFailure
				retryable = false
				if errors.Is(context.Cause(dispatchCtx), context.DeadlineExceeded) ||
					errors.Is(dispatchCtx.Err(), context.DeadlineExceeded) {
					failureClass = "timeout"
				} else {
					failureClass = "cancelled"
				}
			} else if attemptErr == nil {
				assembled := (dispatchResultAssembler{}).assemble(current.Subtask, output)
				output, attemptErr = assembled.output, assembled.err
				if attemptErr != nil {
					retryable = false
					failureClass = assembled.failureClass
				}
			}
			var persistErr error
			if attemptErr == nil {
				failureClass = "none"
				_, persistErr = d.tasks.CompleteDispatchAttemptChecked(
					taskID, assignment.Subtask.ID, attemptNumber,
					failureClass, httpStatus, time.Since(started), output,
				)
			} else {
				persistErr = d.tasks.FinishDispatchAttemptChecked(
					taskID, assignment.Subtask.ID, attemptNumber, taskmgr.DispatchAttemptFailed,
					failureClass, httpStatus, attemptErr, time.Since(started),
				)
			}
			if persistErr != nil {
				attemptErr = fmt.Errorf("persist dispatch attempt: %w", persistErr)
				retryable = false
				failureClass = "persistence"
			}
			if attemptErr != nil && failureClass == "" {
				failureClass = "dispatch_error"
			}
			d.recordDispatchWithAttemptID(current, requestID, attemptID, output, attemptErr, time.Since(started), failureClass, httpStatus)
			if attemptErr == nil || retryable || validationAttempt >= current.Subtask.MaxRetries || dispatchCtx.Err() != nil {
				break
			}
			attemptPrompt = validationRetryPrompt(prompt, attemptErr, output)
		}
		reservation.Release()

		if attemptErr == nil {
			return
		}
		if !d.retryPolicy.canRetry(retryable, attempt+1, dispatchCtx) {
			failure := dispatchContextFailure(dispatchCtx)
			if failure == nil {
				failure = attemptErr
			}
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", failure)
			return
		}

		alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
		delay := d.retryPolicy.delay(attempt)
		if !sleepWithContext(dispatchCtx, delay) {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", dispatchContextFailure(dispatchCtx))
			return
		}
		retry, err := d.retryAssignment(dispatchCtx, assignment.Subtask, alreadyTried)
		if err != nil {
			log.Printf("orchestrator: subtask %s retry unavailable after %v: %v", assignment.Subtask.ID, attemptErr, err)
			failure := dispatchContextFailure(dispatchCtx)
			if failure == nil {
				failure = attemptErr
			}
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", failure)
			return
		}
		log.Printf("orchestrator: retrying subtask %s on node %s after node %s failed: %v",
			assignment.Subtask.ID, retry.NodeID, current.NodeID, attemptErr)
		if err := d.reassignForRetry(taskID, assignment.Subtask.ID, retry); err != nil {
			log.Printf("orchestrator: could not persist retry assignment for subtask %s: %v", assignment.Subtask.ID, err)
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", err)
			return
		}
		current = retry
	}
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
	body, err := json.Marshal(map[string]string{
		"subtask_id": assignment.Subtask.ID,
		"model":      assignment.Model,
		"engine":     assignment.Engine,
		"prompt":     prompt,
		"task_type":  assignment.Subtask.TaskType,
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
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		failureClass, retryable := classifyDispatchStatus(resp.StatusCode, errBody)
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
	log.Printf("orchestrator: dispatch request_id=%s attempt_id=%s task_type=%s subtask=%s node_id=%s model=%s engine=%s duration_ms=%d status=%d failure_class=%s success=%t",
		requestID, attemptID, assignment.Subtask.TaskType, assignment.Subtask.ID, assignment.NodeID,
		assignment.Model, assignment.Engine, elapsed.Milliseconds(), httpStatus, failureClass, dispatchErr == nil)
	if d.memory == nil {
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
		log.Printf("orchestrator: record subtask experience failed: %v", err)
	}
}
