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
	"log"
	"net/http"
	"runtime"
	"sync"
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

type dispatchCapacityProbe interface {
	discoveredNodeStatusContext(context.Context, string) (cluster.NodeStatus, error)
}

type dispatcherComponent struct {
	registry      *cluster.Registry
	tasks         *taskmgr.Manager
	memory        memory.Store
	executeClient dispatchHTTPClient
	capacityProbe dispatchCapacityProbe
	pool          *workpool.Pool[dispatchJob]
	slots         chan struct{}
	limit         int
	// nodeGateMu protects the per-node dispatch gate map.
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
