package orchestrator

import (
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/inferroute"
)

const (
	routerLatencyBucketCount = 14
	routerFailureClassCount  = 20
)

var routerLatencyBuckets = [...]float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10, 30, 60, 120,
}

var routerFailureClasses = [...]string{
	"invalid_request", "client_rate_limit", "client_concurrency_limit",
	"client_limit_capacity", "no_compatible_node", "attempts_exhausted",
	"node_unavailable", "reservation", "transport", "timeout", "cancelled",
	"upstream_5xx", "rate_limit", "stale_model", "client_error",
	"upstream_read", "time_to_first_byte_timeout", "idle_stream_timeout",
	"stream_interrupted", "client_write",
}

type operationalMetrics struct {
	dispatches           atomic.Uint64
	retries              atomic.Uint64
	syncPages            atomic.Uint64
	capacityReservations atomic.Uint64
	pairingEvents        atomic.Uint64
	assignments          atomic.Uint64
	assignmentNanos      atomic.Uint64
	routerAttempts       atomic.Uint64
	routerFailures       [routerFailureClassCount]atomic.Uint64
	routerQueueDepth     atomic.Int64
	routerActiveStreams  atomic.Int64
	routerRequestLatency routerDurationHistogram
	routerTTFB           routerDurationHistogram
}

type routerDurationHistogram struct {
	buckets  [routerLatencyBucketCount + 1]atomic.Uint64
	count    atomic.Uint64
	sumNanos atomic.Uint64
}

type routerDurationHistogramSnapshot struct {
	buckets  [routerLatencyBucketCount + 1]uint64
	count    uint64
	sumNanos uint64
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

func (m *operationalMetrics) recordRouterAttempt() {
	if m != nil {
		m.routerAttempts.Add(1)
	}
}

func (m *operationalMetrics) recordRouterFailure(class string) {
	if m == nil {
		return
	}
	index := len(routerFailureClasses) - 1
	for i, candidate := range routerFailureClasses {
		if class == candidate {
			index = i
			break
		}
	}
	m.routerFailures[index].Add(1)
}

func (m *operationalMetrics) changeRouterQueueDepth(delta int64) {
	if m != nil {
		m.routerQueueDepth.Add(delta)
	}
}

func (m *operationalMetrics) changeRouterActiveStreams(delta int64) {
	if m != nil {
		m.routerActiveStreams.Add(delta)
	}
}

func (m *operationalMetrics) observeRouterRequest(duration time.Duration) {
	if m != nil {
		m.routerRequestLatency.observe(duration)
	}
}

func (m *operationalMetrics) observeRouterTTFB(duration time.Duration) {
	if m != nil {
		m.routerTTFB.observe(duration)
	}
}

func (h *routerDurationHistogram) observe(duration time.Duration) {
	nanoseconds := uint64(max(0, duration.Nanoseconds()))
	seconds := float64(nanoseconds) / float64(time.Second)
	h.buckets[routerLatencyBucketCount].Add(1)
	for i := len(routerLatencyBuckets) - 1; i >= 0; i-- {
		bound := routerLatencyBuckets[i]
		if seconds <= bound {
			h.buckets[i].Add(1)
		}
	}
	h.count.Add(1)
	h.sumNanos.Add(nanoseconds)
}

func (h *routerDurationHistogram) snapshot() routerDurationHistogramSnapshot {
	var snapshot routerDurationHistogramSnapshot
	for i := range snapshot.buckets {
		snapshot.buckets[i] = h.buckets[i].Load()
	}
	snapshot.count = h.count.Load()
	snapshot.sumNanos = h.sumNanos.Load()
	return snapshot
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
		logging.Errorf("orchestrator", "orchestrator: write metrics response failed: %v", err)
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
	if _, err := fmt.Fprintf(w, `# HELP maistr0_dispatches_total Total dispatch attempts sent to worker nodes.
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
		gauges.engineInventoryAgeSeconds); err != nil {
		return err
	}
	return writeRouterMetrics(w, metrics)
}

func writeRouterMetrics(w http.ResponseWriter, metrics *operationalMetrics) error {
	if metrics == nil {
		metrics = &operationalMetrics{}
	}
	_, err := fmt.Fprintf(w, `# HELP maistr0_router_attempts_total Total transparent-router attempts sent to worker nodes.
# TYPE maistr0_router_attempts_total counter
maistr0_router_attempts_total %d
# HELP maistr0_router_failures_total Transparent-router failure events by bounded failure class.
# TYPE maistr0_router_failures_total counter
`, metrics.routerAttempts.Load())
	if err != nil {
		return err
	}
	for i, class := range routerFailureClasses {
		if _, err := fmt.Fprintf(w, "maistr0_router_failures_total{class=%q} %d\n", class, metrics.routerFailures[i].Load()); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, `# HELP maistr0_router_queue_depth Requests waiting for worker capacity in the transparent router.
# TYPE maistr0_router_queue_depth gauge
maistr0_router_queue_depth %d
# HELP maistr0_router_active_streams Successful upstream streams currently being copied to clients.
# TYPE maistr0_router_active_streams gauge
maistr0_router_active_streams %d
`, metrics.routerQueueDepth.Load(), metrics.routerActiveStreams.Load()); err != nil {
		return err
	}
	if err := writeRouterHistogram(w, "maistr0_router_request_duration_seconds",
		"Transparent-router request duration in seconds.", metrics.routerRequestLatency.snapshot()); err != nil {
		return err
	}
	return writeRouterHistogram(w, "maistr0_router_time_to_first_byte_seconds",
		"Transparent-router time to first response byte in seconds.", metrics.routerTTFB.snapshot())
}

func writeRouterHistogram(
	w http.ResponseWriter,
	name, help string,
	histogram routerDurationHistogramSnapshot,
) error {
	if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name); err != nil {
		return err
	}
	for i, bound := range routerLatencyBuckets {
		if _, err := fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name,
			strconv.FormatFloat(bound, 'f', -1, 64), histogram.buckets[i]); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %.9f\n%s_count %d\n",
		name, histogram.buckets[routerLatencyBucketCount],
		name, float64(histogram.sumNanos)/float64(time.Second),
		name, histogram.count); err != nil {
		return err
	}
	return nil
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
