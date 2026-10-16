package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/workpool"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
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

type dispatcherComponent struct {
	registry      *cluster.Registry
	tasks         *taskmgr.Manager
	memory        memory.Store
	executeClient dispatchHTTPClient
	capacityGate  workpool.CapacityGate
	pool          *workpool.Pool[dispatchJob]
	limit         int
	// These atomics are independent live metrics; they are not guarded by a
	// mutex and never impose lock ordering on dispatch paths.
	dependencyWaiters   atomic.Int64
	dependencyWaitNanos atomic.Int64
	dispatchNanos       atomic.Int64
}

type dispatcherAPI interface {
	close()
	schedulerNodes() []cluster.NodeStatus
	dispatch(string, scheduler.Assignment)
	dispatchWithContext(context.Context, string, scheduler.Assignment)
	submitDispatch(string, scheduler.Assignment) error
	snapshot() *dispatchSnapshot
	executeSubtask(context.Context, scheduler.Assignment, string) (string, error, bool)
	retryAssignment(context.Context, scheduler.Subtask, map[string]bool) (scheduler.Assignment, error)
	experience() scheduler.Experience
	recordDispatch(scheduler.Assignment, string, error, time.Duration)
	reservations() workpool.CapacityGate
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
		executeClient: executeClient, capacityGate: newDispatchCapacityGate(registry, capacityProbe, limit),
		limit: limit,
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

func (d *dispatcherComponent) reservations() workpool.CapacityGate {
	return d.capacityGate
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
		ActiveGlobalSlots:     d.capacityGate.Active(),
		ActiveDispatchWorkers: activeWorkers,
		QueueDepth:            queue,
		DependencyWaiters:     d.dependencyWaiters.Load(),
		DependencyWaitMsTotal: time.Duration(d.dependencyWaitNanos.Load()).Milliseconds(),
		DispatchMsTotal:       time.Duration(d.dispatchNanos.Load()).Milliseconds(),
		Goroutines:            runtime.NumGoroutine(),
	}
}
