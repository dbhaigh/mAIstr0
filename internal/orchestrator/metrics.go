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
}

type operationalMetricsSnapshot struct {
	dispatches           uint64
	retries              uint64
	syncPages            uint64
	capacityReservations uint64
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

func (m *operationalMetrics) snapshot() operationalMetricsSnapshot {
	if m == nil {
		return operationalMetricsSnapshot{}
	}
	return operationalMetricsSnapshot{
		dispatches:           m.dispatches.Load(),
		retries:              m.retries.Load(),
		syncPages:            m.syncPages.Load(),
		capacityReservations: m.capacityReservations.Load(),
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := writeOperationalMetrics(w, s.metrics); err != nil {
		log.Printf("orchestrator: write metrics response failed: %v", err)
	}
}

func writeOperationalMetrics(w http.ResponseWriter, metrics *operationalMetrics) error {
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
`, snapshot.dispatches, snapshot.retries, snapshot.syncPages, snapshot.capacityReservations)
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
