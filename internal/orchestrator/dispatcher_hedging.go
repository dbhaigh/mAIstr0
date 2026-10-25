package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

var errSpeculativeHedgeLost = errors.New("speculative dispatch lost")

type candidateDispatchResult struct {
	err       error
	retryable bool
	won       bool
	attempted []scheduler.Assignment
}

type hedgeReservationResult struct {
	reservation *dispatchReservation
	err         error
}

func (d *dispatcherComponent) executeCandidateWithHedge(
	ctx context.Context,
	taskID string,
	assignment scheduler.Assignment,
	prompt string,
	attemptCount int,
	alreadyTried map[string]bool,
) candidateDispatchResult {
	if !d.hedgingEnabled || d.hedgeSlots == nil ||
		attemptCount+2 > d.retryPolicy.maxAttempts {
		return d.runCandidateSeries(ctx, taskID, assignment, prompt, nil)
	}
	history, ok := d.memory.(interface {
		P95Duration(model, taskType string) time.Duration
	})
	if !ok {
		return d.runCandidateSeries(ctx, taskID, assignment, prompt, nil)
	}
	delay := history.P95Duration(assignment.Model, assignment.Subtask.TaskType)
	if delay <= 0 {
		return d.runCandidateSeries(ctx, taskID, assignment, prompt, nil)
	}
	select {
	case d.hedgeSlots <- struct{}{}:
	default:
		return d.runCandidateSeries(ctx, taskID, assignment, prompt, nil)
	}
	slotHeld := true
	releaseSlot := func() {
		if slotHeld {
			<-d.hedgeSlots
			slotHeld = false
		}
	}
	defer releaseSlot()

	primaryCtx, cancelPrimary := context.WithCancelCause(ctx)
	defer cancelPrimary(context.Canceled)

	var winner atomic.Int32
	var cancelMu sync.Mutex
	var cancelHedge context.CancelCauseFunc
	onPrimaryWinner := func() {
		if winner.CompareAndSwap(0, 1) {
			cancelMu.Lock()
			cancel := cancelHedge
			cancelMu.Unlock()
			if cancel != nil {
				cancel(errSpeculativeHedgeLost)
			}
		}
	}

	primaryResults := make(chan candidateDispatchResult, 1)
	go func() {
		primaryResults <- d.runCandidateSeries(primaryCtx, taskID, assignment, prompt, onPrimaryWinner)
	}()

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case result := <-primaryResults:
		return result
	case <-ctx.Done():
		return <-primaryResults
	case <-timer.C:
	}
	if result, ready := receiveCandidateResult(primaryResults); ready {
		return result
	}
	if winner.Load() != 0 {
		return <-primaryResults
	}

	hedge, err := d.hedgeAssignment(ctx, assignment, alreadyTried)
	if err != nil {
		releaseSlot()
		return <-primaryResults
	}
	if result, ready := receiveCandidateResult(primaryResults); ready {
		return result
	}

	reserveCtx, cancelReserve := context.WithCancel(ctx)
	reservationResults := make(chan hedgeReservationResult, 1)
	go func() {
		reservation, reserveErr := d.reserveTaskCapacity(reserveCtx, hedge)
		reservationResults <- hedgeReservationResult{reservation: reservation, err: reserveErr}
	}()

	var reserved hedgeReservationResult
	select {
	case result := <-primaryResults:
		cancelReserve()
		reserved = <-reservationResults
		if reserved.reservation != nil {
			reserved.reservation.Release()
		}
		return result
	case reserved = <-reservationResults:
		cancelReserve()
	case <-ctx.Done():
		cancelReserve()
		reserved = <-reservationResults
		if reserved.reservation != nil {
			reserved.reservation.Release()
		}
		return <-primaryResults
	}
	if reserved.err != nil || reserved.reservation == nil {
		releaseSlot()
		return <-primaryResults
	}
	if result, ready := receiveCandidateResult(primaryResults); ready || winner.Load() != 0 {
		reserved.reservation.Release()
		if ready {
			return result
		}
		return <-primaryResults
	}

	secondaryCtx, cancelSecondary := context.WithCancelCause(ctx)
	defer cancelSecondary(context.Canceled)
	cancelMu.Lock()
	cancelHedge = cancelSecondary
	cancelMu.Unlock()
	if winner.Load() == 1 {
		cancelSecondary(errSpeculativeHedgeLost)
		reserved.reservation.Release()
		return <-primaryResults
	}

	d.metrics.recordCapacityReservation()
	reserved.reservation.Start()
	defer reserved.reservation.Release()

	secondaryResults := make(chan candidateDispatchResult, 1)
	onSecondaryWinner := func() {
		if winner.CompareAndSwap(0, 2) {
			cancelPrimary(errSpeculativeHedgeLost)
		}
	}
	go func() {
		secondaryResults <- d.runCandidateSeries(secondaryCtx, taskID, hedge, prompt, onSecondaryWinner)
	}()

	var primaryResult, secondaryResult *candidateDispatchResult
	primaryPending, secondaryPending := true, true
	contextDone := ctx.Done()
	for primaryPending || secondaryPending {
		select {
		case result := <-primaryResults:
			primaryResult = &result
			primaryPending = false
		case result := <-secondaryResults:
			secondaryResult = &result
			secondaryPending = false
		case <-contextDone:
			cancelPrimary(context.Cause(ctx))
			cancelSecondary(context.Cause(ctx))
			contextDone = nil
		}
	}
	if primaryResult != nil && primaryResult.won {
		return *primaryResult
	}
	if secondaryResult != nil && secondaryResult.won {
		return *secondaryResult
	}
	if primaryResult != nil {
		return mergeAttempted(*primaryResult, secondaryResult)
	}
	if secondaryResult != nil {
		return *secondaryResult
	}
	return candidateDispatchResult{
		err: errors.New("speculative dispatch produced no result"),
	}
}

func receiveCandidateResult(results <-chan candidateDispatchResult) (candidateDispatchResult, bool) {
	select {
	case result := <-results:
		return result, true
	default:
		return candidateDispatchResult{}, false
	}
}

func mergeAttempted(primary candidateDispatchResult, secondary *candidateDispatchResult) candidateDispatchResult {
	if secondary != nil {
		primary.attempted = append(primary.attempted, secondary.attempted...)
	}
	return primary
}

func (d *dispatcherComponent) hedgeAssignment(
	ctx context.Context,
	primary scheduler.Assignment,
	alreadyTried map[string]bool,
) (scheduler.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return scheduler.Assignment{}, err
	}
	nodes := d.schedulerNodes()
	eligible := make([]cluster.NodeStatus, 0, len(nodes))
	for _, node := range nodes {
		if node.ID != primary.NodeID {
			candidate := node
			candidate.Models = make([]engine.Model, 0, len(node.Models))
			for _, model := range node.Models {
				if !alreadyTried[routerPairKey(node.ID, model.Name)] {
					candidate.Models = append(candidate.Models, model)
				}
			}
			if len(candidate.Models) > 0 {
				eligible = append(eligible, candidate)
			}
		}
	}
	assignments, err := scheduler.AssignWithExperience(
		[]scheduler.Subtask{primary.Subtask}, eligible, d.experience(),
	)
	if err != nil {
		return scheduler.Assignment{}, err
	}
	if len(assignments) != 1 || assignments[0].NodeID == primary.NodeID {
		return scheduler.Assignment{}, errors.New("scheduler returned no distinct hedge candidate")
	}
	return assignments[0], nil
}

func (d *dispatcherComponent) runCandidateSeries(
	ctx context.Context,
	taskID string,
	assignment scheduler.Assignment,
	prompt string,
	onWinner func(),
) candidateDispatchResult {
	result := candidateDispatchResult{retryable: true}
	attemptPrompt := prompt
	for validationAttempt := 0; ; validationAttempt++ {
		started := time.Now()
		requestID := taskmgr.DispatchRequestID(taskID, assignment.Subtask.ID)
		attemptNumber, beginErr := d.tasks.BeginDispatchAttemptChecked(taskID, assignment.Subtask.ID, assignment)
		if beginErr != nil {
			result.err = beginErr
			result.retryable = false
			failureClass := "dispatch_error"
			if errors.Is(context.Cause(ctx), errSpeculativeHedgeLost) {
				failureClass = "hedge_loser"
			}
			d.recordDispatchWithAttemptID(
				assignment, requestID, "", "", beginErr, time.Since(started), failureClass, 0,
			)
			return result
		}
		result.attempted = append(result.attempted, assignment)
		attemptID := taskmgr.DispatchAttemptID(taskID, assignment.Subtask.ID, attemptNumber)
		outcome := d.executeSubtaskWithIDs(ctx, assignment, attemptPrompt, requestID, attemptID)
		output, attemptErr, retryable := outcome.output, outcome.err, outcome.retryable
		failureClass, httpStatus := outcome.failureClass, outcome.httpStatus
		if contextFailure := dispatchContextFailure(ctx); contextFailure != nil {
			attemptErr = contextFailure
			retryable = false
			switch {
			case errors.Is(context.Cause(ctx), errSpeculativeHedgeLost):
				failureClass = "hedge_loser"
			case errors.Is(context.Cause(ctx), context.DeadlineExceeded) ||
				errors.Is(ctx.Err(), context.DeadlineExceeded):
				failureClass = "timeout"
			default:
				failureClass = "cancelled"
			}
		} else if attemptErr == nil {
			assembled := (dispatchResultAssembler{}).assemble(assignment.Subtask, output)
			output, attemptErr = assembled.output, assembled.err
			if attemptErr != nil {
				retryable = false
				failureClass = assembled.failureClass
			}
		}

		var persistErr error
		if attemptErr == nil {
			failureClass = "none"
			won, err := d.tasks.CompleteDispatchAttemptWithAssignmentChecked(
				taskID, assignment.Subtask.ID, attemptNumber, &assignment,
				failureClass, httpStatus, time.Since(started), output, onWinner,
			)
			result.won = won
			persistErr = err
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
		recordFailureClass := failureClass
		if errors.Is(context.Cause(ctx), errSpeculativeHedgeLost) {
			recordFailureClass = "hedge_loser"
		}
		d.recordDispatchWithAttemptID(
			assignment, requestID, attemptID, output, attemptErr,
			time.Since(started), recordFailureClass, httpStatus,
		)
		result.err, result.retryable = attemptErr, retryable
		if attemptErr == nil || retryable || validationAttempt >= assignment.Subtask.MaxRetries || ctx.Err() != nil {
			return result
		}
		attemptPrompt = validationRetryPrompt(prompt, attemptErr, output)
	}
}
