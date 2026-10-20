package orchestrator

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/inferroute"
)

type operationalMetrics struct {
	dispatches           atomic.Uint64
	retries              atomic.Uint64
	syncPages            atomic.Uint64
	capacityReservations atomic.Uint64
	pairingEvents        atomic.Uint64
	assignments          atomic.Uint64
	assignmentNanos      atomic.Uint64
}

type operationalMetricsSnapshot struct {
	dispatches           uint64
	retries              uint64
	syncPages            uint64
	capacityReservations uint64
	pairingEvents        uint64
	assignments          uint64
	assignmentNanos      uint64
}

func (m *operationalMetrics) recordDispatch() {
	if m != nil {
		m.dispatches.Add(1)
	}
}

func (m *operationalMetrics) recordRetry() {
	if m != nil {
		m.retries.Add(1)
	}
}

func (m *operationalMetrics) recordSyncPage() {
	if m != nil {
		m.syncPages.Add(1)
	}
}

func (m *operationalMetrics) recordCapacityReservation() {
	if m != nil {
		m.capacityReservations.Add(1)
	}
}

func (m *operationalMetrics) recordPairingEvent() {
	if m != nil {
		m.pairingEvents.Add(1)
	}
}

func (m *operationalMetrics) recordAssignment(duration time.Duration) {
	if m != nil {
		m.assignments.Add(1)
		m.assignmentNanos.Add(uint64(max(0, duration.Nanoseconds())))
	}
}

func (m *operationalMetrics) snapshot() operationalMetricsSnapshot {
	if m == nil {
		return operationalMetricsSnapshot{}
	}
	return operationalMetricsSnapshot{
		dispatches:           m.dispatches.Load(),
		retries:              m.retries.Load(),
		syncPages:            m.syncPages.Load(),
		capacityReservations: m.capacityReservations.Load(),
		pairingEvents:        m.pairingEvents.Load(),
		assignments:          m.assignments.Load(),
		assignmentNanos:      m.assignmentNanos.Load(),
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := writeOperationalMetrics(w, s.metrics, s.operationalMetricsGauges()); err != nil {
		log.Printf("orchestrator: write metrics response failed: %v", err)
	}
}

type operationalMetricsGauges struct {
	activeTasks               int
	queueDepth                int
	capacityReserved          int
	syncLagRecords            int64
	engineInventoryAgeSeconds float64
}

func (s *Server) operationalMetricsGauges() operationalMetricsGauges {
	gauges := operationalMetricsGauges{syncLagRecords: -1, engineInventoryAgeSeconds: -1}
	if s.tasks != nil {
		gauges.activeTasks = s.tasks.ActiveCount()
	}
	if s.dispatcher != nil {
		snapshot := s.dispatcher.snapshot()
		gauges.queueDepth = snapshot.QueueDepth
		gauges.capacityReserved = snapshot.ActiveGlobalSlots
	}
	if s.mem != nil && s.registry != nil {
		local := s.mem.Watermarks()
		var lag uint64
		for _, node := range s.registry.Active() {
			peerWatermarks := s.mem.PeerWatermarks(node.ID)
			for origin, current := range local {
				peer := peerWatermarks[origin]
				if current > peer {
					lag = max(lag, current-peer)
				}
			}
		}
		gauges.syncLagRecords = int64(lag)
	}
	if s.registry != nil {
		for _, node := range s.registry.Active() {
			for _, engine := range node.Engines {
				if engine.LastSuccessfulAt.IsZero() {
					continue
				}
				age := max(0, time.Since(engine.LastSuccessfulAt).Seconds())
				gauges.engineInventoryAgeSeconds = max(gauges.engineInventoryAgeSeconds, age)
			}
		}
	}
	return gauges
}

func writeOperationalMetrics(w http.ResponseWriter, metrics *operationalMetrics, gauges operationalMetricsGauges) error {
	snapshot := metrics.snapshot()
	_, err := fmt.Fprintf(w, `# HELP maistr0_dispatches_total Total dispatch attempts sent to worker nodes.
# TYPE maistr0_dispatches_total counter
maistr0_dispatches_total %d
# HELP maistr0_retries_total Total dispatch retries assigned to a different attempt.
# TYPE maistr0_retries_total counter
maistr0_retries_total %d
# HELP maistr0_sync_pages_total Total task-history and memory-sync pages served.
# TYPE maistr0_sync_pages_total counter
maistr0_sync_pages_total %d
# HELP maistr0_capacity_reservations_total Total successful worker-capacity reservations.
# TYPE maistr0_capacity_reservations_total counter
maistr0_capacity_reservations_total %d
# HELP maistr0_pairing_events_total Successful node pairing events.
# TYPE maistr0_pairing_events_total counter
maistr0_pairing_events_total %d
# HELP maistr0_scheduler_assignment_duration_seconds_count Scheduler assignment calls.
# TYPE maistr0_scheduler_assignment_duration_seconds_count counter
maistr0_scheduler_assignment_duration_seconds_count %d
# HELP maistr0_scheduler_assignment_duration_seconds_sum Total time spent assigning work.
# TYPE maistr0_scheduler_assignment_duration_seconds_sum counter
maistr0_scheduler_assignment_duration_seconds_sum %.9f
# HELP maistr0_active_tasks Number of tasks that have not reached a terminal state.
# TYPE maistr0_active_tasks gauge
maistr0_active_tasks %d
# HELP maistr0_dispatch_queue_depth Number of dispatch jobs waiting in the orchestrator queue.
# TYPE maistr0_dispatch_queue_depth gauge
maistr0_dispatch_queue_depth %d
# HELP maistr0_capacity_reserved Global dispatch slots currently reserved.
# TYPE maistr0_capacity_reserved gauge
maistr0_capacity_reserved %d
# HELP maistr0_sync_lag_records Maximum per-peer watermark deficit in records; -1 means unavailable.
# TYPE maistr0_sync_lag_records gauge
maistr0_sync_lag_records %d
# HELP maistr0_engine_inventory_age_seconds Age of the oldest successful engine inventory report; -1 means unavailable.
# TYPE maistr0_engine_inventory_age_seconds gauge
maistr0_engine_inventory_age_seconds %.3f
`, snapshot.dispatches, snapshot.retries, snapshot.syncPages, snapshot.capacityReservations,
		snapshot.pairingEvents, snapshot.assignments, float64(snapshot.assignmentNanos)/float64(time.Second),
		gauges.activeTasks, gauges.queueDepth, gauges.capacityReserved, gauges.syncLagRecords,
		gauges.engineInventoryAgeSeconds)
	return err
}

var syncLogSequence atomic.Uint64

func syncLogRequestID(r *http.Request) string {
	if r != nil {
		if requestID := strings.TrimSpace(r.Header.Get(inferroute.HeaderRequestID)); requestID != "" {
			return requestID
		}
	}
	return "sync-" + strconv.FormatUint(syncLogSequence.Add(1), 10)
}

func syncLogNodeID(r *http.Request) string {
	if r != nil && r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		if nodeID := strings.TrimSpace(r.TLS.PeerCertificates[0].Subject.CommonName); nodeID != "" {
			return nodeID
		}
	}
	return "unknown"
}

func syncLogFields(requestID, nodeID string, elapsed time.Duration, failureClass string) string {
	return fmt.Sprintf(
		"request_id=%q attempt_id=none node_id=%q model=none engine=none duration_ms=%d failure_class=%q",
		requestID, nodeID, elapsed.Milliseconds(), failureClass,
	)
}
