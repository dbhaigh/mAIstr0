package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/workpool"
)

const (
	maxDispatchAttempts = 5
	dispatchDeadline    = 3 * time.Minute
	dispatchBackoff     = 100 * time.Millisecond
	maxDispatchBackoff  = 800 * time.Millisecond
)

type dispatchJob struct {
	taskID     string
	assignment scheduler.Assignment
}

type dispatchHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type dispatchCapacityProbe interface {
	discoveredNodeStatusContext(context.Context, string) (cluster.NodeStatus, error)
}

type dispatcherComponent struct {
	registry            *cluster.Registry
	tasks               *taskmgr.Manager
	memory              memory.Store
	executeClient       dispatchHTTPClient
	capacityProbe       dispatchCapacityProbe
	pool                *workpool.Pool[dispatchJob]
	slots               chan struct{}
	limit               int
	nodeGateMu          sync.Mutex
	nodeGates           map[string]*nodeDispatchGate
	dependencyWaiters   atomic.Int64
	dependencyWaitNanos atomic.Int64
	dispatchNanos       atomic.Int64
}

func newDispatcherComponent(
	registry *cluster.Registry,
	tasks *taskmgr.Manager,
	store memory.Store,
	executeClient dispatchHTTPClient,
	capacityProbe dispatchCapacityProbe,
	limit int,
) *dispatcherComponent {
	dispatcher := &dispatcherComponent{
		registry: registry, tasks: tasks, memory: store,
		executeClient: executeClient, capacityProbe: capacityProbe,
		limit: limit, slots: make(chan struct{}, limit),
		nodeGates: make(map[string]*nodeDispatchGate),
	}
	// Dispatch reloads the task's canonical context instead of inheriting the submitter's request context.
	dispatcher.pool = workpool.NewWithOptions(
		limit,
		limit*64,
		func(ctx context.Context, job dispatchJob) {
			dispatcher.dispatchWithContext(ctx, job.taskID, job.assignment)
		},
		workpool.Options{PropagateSubmitterContext: false},
	)
	return dispatcher
}

func (d *dispatcherComponent) close() {
	if d.pool != nil {
		d.pool.Close()
	}
}

func (d *dispatcherComponent) schedulerNodes() []cluster.NodeStatus {
	nodes := d.registry.Active()
	byID := make(map[string]*cluster.NodeStatus, len(nodes))
	for i := range nodes {
		byID[nodes[i].ID] = &nodes[i]
	}
	for nodeID, pending := range d.tasks.PendingAssignmentsByNode() {
		if node, ok := byID[nodeID]; ok {
			node.QueuedTasks = pending
		}
	}
	return nodes
}

func (d *dispatcherComponent) dispatch(taskID string, assignment scheduler.Assignment) {
	d.dispatchWithContext(context.Background(), taskID, assignment)
}

func (d *dispatcherComponent) dispatchWithContext(parent context.Context, taskID string, assignment scheduler.Assignment) {
	ctx, ok := d.tasks.Context(taskID)
	if !ok {
		return
	}
	dispatchCtx, cancel := context.WithCancel(ctx)
	stopParent := context.AfterFunc(parent, cancel)
	defer func() {
		stopParent()
		cancel()
	}()

	dependencyStarted := time.Now()
	d.dependencyWaiters.Add(1)
	if !d.tasks.WaitForDependencies(dispatchCtx, taskID, assignment.Subtask.ID) {
		d.dependencyWaiters.Add(-1)
		elapsed := time.Since(dependencyStarted)
		d.dependencyWaitNanos.Add(elapsed.Nanoseconds())
		if dispatchCtx.Err() == nil {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("a required subtask did not complete"))
		}
		return
	}
	d.dependencyWaiters.Add(-1)
	dependencyElapsed := time.Since(dependencyStarted)
	d.dependencyWaitNanos.Add(dependencyElapsed.Nanoseconds())
	if dependencyElapsed > 0 {
		log.Printf("orchestrator: dependency wait task=%s subtask=%s duration_ms=%d", taskID, assignment.Subtask.ID, dependencyElapsed.Milliseconds())
	}

	boundedCtx, cancelDeadline := context.WithTimeout(dispatchCtx, dispatchDeadline)
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
	if attemptCount >= maxDispatchAttempts {
		d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("subtask dispatch attempt limit reached"))
		return
	}
	if alreadyTried[routerPairKey(current.NodeID, current.Model)] {
		retry, err := d.retryAssignment(dispatchCtx, current.Subtask, alreadyTried)
		if err != nil {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("no untried node/model pair is available"))
			return
		}
		if err := d.tasks.ReassignSubtaskChecked(taskID, assignment.Subtask.ID, retry); err != nil {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", err)
			return
		}
		current = retry
	}
	for attempt := attemptCount; attempt < maxDispatchAttempts; attempt++ {
		if dispatchCtx.Err() != nil {
			if errors.Is(dispatchCtx.Err(), context.DeadlineExceeded) {
				d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("subtask dispatch deadline exceeded"))
			}
			return
		}
		node, healthy := d.registry.Get(current.NodeID)
		if !healthy || !node.Healthy || node.Address != current.Address {
			alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
			retry, err := d.retryAssignment(dispatchCtx, current.Subtask, alreadyTried)
			if err == nil && dispatchCtx.Err() == nil {
				if persistErr := d.tasks.ReassignSubtaskChecked(taskID, assignment.Subtask.ID, retry); persistErr != nil {
					d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", persistErr)
					return
				}
				current = retry
				continue
			}
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("node was ejected before dispatch"))
			return
		}
		limit := node.MaxConcurrentTasks
		if limit <= 0 {
			limit = d.limit
		}
		gate := d.nodeDispatchGate(current.NodeID, limit)
		capacity := func() (int, int, int, bool) {
			return d.dispatchCapacity(dispatchCtx, gate, current.NodeID, current.Address)
		}
		if err := gate.acquire(dispatchCtx, capacity); err != nil {
			if dispatchCtx.Err() != nil {
				return
			}
			alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
			retry, retryErr := d.retryAssignment(dispatchCtx, current.Subtask, alreadyTried)
			if retryErr == nil {
				if persistErr := d.tasks.ReassignSubtaskChecked(taskID, assignment.Subtask.ID, retry); persistErr != nil {
					d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", persistErr)
					return
				}
				current = retry
				continue
			}
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", err)
			return
		}
		nodeSlotHeld := true
		globalSlotHeld := false
		defer func() {
			if nodeSlotHeld {
				gate.release()
			}
			if globalSlotHeld {
				<-d.slots
			}
		}()
		select {
		case d.slots <- struct{}{}:
			globalSlotHeld = true
		case <-dispatchCtx.Done():
			return
		}
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
		d.registry.IncrementLoad(current.NodeID, 1)
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
			outcome := d.executeSubtaskDetailed(dispatchCtx, current, attemptPrompt)
			output, attemptErr, retryable = outcome.output, outcome.err, outcome.retryable
			failureClass, httpStatus := outcome.failureClass, outcome.httpStatus
			if attemptErr == nil {
				output = normalizeTaskOutput(current.Subtask, output)
				attemptErr = validateTaskOutput(current.Subtask, output)
				if attemptErr != nil {
					retryable = false
					failureClass = "validation"
				}
			}
			attemptStatus := taskmgr.DispatchAttemptSucceeded
			if attemptErr != nil {
				attemptStatus = taskmgr.DispatchAttemptFailed
			}
			if persistErr := d.tasks.FinishDispatchAttemptChecked(
				taskID, assignment.Subtask.ID, attemptNumber, attemptStatus,
				failureClass, httpStatus, attemptErr, time.Since(started),
			); persistErr != nil {
				attemptErr = fmt.Errorf("persist dispatch attempt: %w", persistErr)
				retryable = false
			}
			d.recordDispatch(current, output, attemptErr, time.Since(started))
			if attemptErr == nil || retryable || validationAttempt >= current.Subtask.MaxRetries || dispatchCtx.Err() != nil {
				break
			}
			attemptPrompt = validationRetryPrompt(prompt, attemptErr, output)
		}
		d.registry.IncrementLoad(current.NodeID, -1)
		<-d.slots
		globalSlotHeld = false
		gate.release()
		nodeSlotHeld = false

		if attemptErr == nil {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, output, nil)
			return
		}
		if !retryable || attempt+1 >= maxDispatchAttempts || dispatchCtx.Err() != nil {
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", attemptErr)
			return
		}

		alreadyTried[routerPairKey(current.NodeID, current.Model)] = true
		delay := min(dispatchBackoff*time.Duration(1<<min(attempt, 3)), maxDispatchBackoff)
		if !sleepWithContext(dispatchCtx, delay) {
			if errors.Is(dispatchCtx.Err(), context.DeadlineExceeded) {
				d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", errors.New("subtask dispatch deadline exceeded"))
			}
			return
		}
		retry, err := d.retryAssignment(dispatchCtx, assignment.Subtask, alreadyTried)
		if err != nil {
			log.Printf("orchestrator: subtask %s retry unavailable after %v: %v", assignment.Subtask.ID, attemptErr, err)
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", attemptErr)
			return
		}
		log.Printf("orchestrator: retrying subtask %s on node %s after node %s failed: %v",
			assignment.Subtask.ID, retry.NodeID, current.NodeID, attemptErr)
		if err := d.tasks.ReassignSubtaskChecked(taskID, assignment.Subtask.ID, retry); err != nil {
			log.Printf("orchestrator: could not persist retry assignment for subtask %s: %v", assignment.Subtask.ID, err)
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", err)
			return
		}
		if err := d.tasks.ReassignSubtaskChecked(taskID, assignment.Subtask.ID, retry); err != nil {
			log.Printf("orchestrator: could not persist retry assignment for subtask %s: %v", assignment.Subtask.ID, err)
			d.tasks.CompleteSubtask(taskID, assignment.Subtask.ID, "", err)
			return
		}
		current = retry
	}
}

func (d *dispatcherComponent) nodeDispatchGate(nodeID string, limit int) *nodeDispatchGate {
	d.nodeGateMu.Lock()
	defer d.nodeGateMu.Unlock()
	gate, ok := d.nodeGates[nodeID]
	if !ok {
		gate = newNodeDispatchGate(limit)
		d.nodeGates[nodeID] = gate
	}
	return gate
}

func (d *dispatcherComponent) dispatchCapacity(ctx context.Context, gate *nodeDispatchGate, nodeID, address string) (int, int, int, bool) {
	select {
	case gate.refreshMu <- struct{}{}:
		defer func() { <-gate.refreshMu }()
	case <-ctx.Done():
		return 0, 0, 0, false
	}

	status, exists := d.registry.Get(nodeID)
	if !exists || !status.Healthy || status.Address != address {
		return 0, 0, 0, false
	}
	if time.Since(status.LastSeen) > nodeCapacityFreshnessWindow {
		refreshed, err := d.capacityProbe.discoveredNodeStatusContext(ctx, status.Address)
		if err != nil {
			if ctx.Err() != nil {
				return 0, 0, 0, false
			}
			log.Printf("orchestrator: refreshing capacity for node %s failed: %v", nodeID, err)
			d.registry.MarkUnhealthy(nodeID)
			return 0, 0, 0, false
		}
		if refreshed.ID != nodeID || refreshed.Address != address {
			log.Printf("orchestrator: capacity response identity mismatch for node %s", nodeID)
			d.registry.MarkUnhealthy(nodeID)
			return 0, 0, 0, false
		}
		d.registry.Upsert(refreshed)
		status = refreshed
	}
	limit := status.MaxConcurrentTasks
	available := status.AvailableCapacity
	if limit <= 0 {
		limit = d.limit
		available = limit
	}
	return limit, available, status.ActiveTasks, true
}

func (d *dispatcherComponent) submitDispatch(taskID string, assignment scheduler.Assignment) error {
	taskCtx, ok := d.tasks.Context(taskID)
	if !ok {
		return fmt.Errorf("task %s is no longer available", taskID)
	}
	if d.pool == nil {
		return errors.New("dispatch pool is unavailable")
	}
	return d.pool.Submit(taskCtx, dispatchJob{taskID: taskID, assignment: assignment})
}

type dispatchSnapshot struct {
	GlobalLimit           int   `json:"global_limit"`
	ActiveGlobalSlots     int   `json:"active_global_slots"`
	ActiveDispatchWorkers int   `json:"active_dispatch_workers"`
	QueueDepth            int   `json:"queue_depth"`
	DependencyWaiters     int64 `json:"dependency_waiters"`
	DependencyWaitMsTotal int64 `json:"dependency_wait_ms_total"`
	DispatchMsTotal       int64 `json:"dispatch_ms_total"`
	Goroutines            int   `json:"goroutines"`
}

func (d *dispatcherComponent) snapshot() *dispatchSnapshot {
	activeWorkers := 0
	queue := 0
	if d.pool != nil {
		activeWorkers = d.pool.Active()
		queue = d.pool.QueueDepth()
	}
	return &dispatchSnapshot{
		GlobalLimit:           d.limit,
		ActiveGlobalSlots:     len(d.slots),
		ActiveDispatchWorkers: activeWorkers,
		QueueDepth:            queue,
		DependencyWaiters:     d.dependencyWaiters.Load(),
		DependencyWaitMsTotal: time.Duration(d.dependencyWaitNanos.Load()).Milliseconds(),
		DispatchMsTotal:       time.Duration(d.dispatchNanos.Load()).Milliseconds(),
		Goroutines:            runtime.NumGoroutine(),
	}
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
	resp, err := d.executeClient.Do(req)
	if err != nil {
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

func (d *dispatcherComponent) retryAssignment(ctx context.Context, subtask scheduler.Subtask, excluded map[string]bool) (scheduler.Assignment, error) {
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

func (d *dispatcherComponent) experience() scheduler.Experience {
	if d.memory == nil {
		return nil
	}
	return d.memory
}

func (d *dispatcherComponent) recordDispatch(assignment scheduler.Assignment, output string, dispatchErr error, elapsed time.Duration) {
	d.dispatchNanos.Add(elapsed.Nanoseconds())
	log.Printf("orchestrator: dispatch task_type=%s subtask=%s node=%s model=%s duration_ms=%d success=%t",
		assignment.Subtask.TaskType, assignment.Subtask.ID, assignment.NodeID, assignment.Model, elapsed.Milliseconds(), dispatchErr == nil)
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
