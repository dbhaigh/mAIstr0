package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpointExposesOperationalCounters(t *testing.T) {
	srv := newTestServer(t)
	srv.metrics.recordDispatch()
	srv.metrics.recordDispatch()
	srv.metrics.recordRetry()
	srv.metrics.recordSyncPage()
	srv.metrics.recordCapacityReservation()
	srv.metrics.recordPairingEvent()
	srv.metrics.recordAssignment(10_000_000)

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /metrics returned %d: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("metrics content type = %q", got)
	}
	for _, expected := range []string{
		"# TYPE maistr0_dispatches_total counter\nmaistr0_dispatches_total 2",
		"# TYPE maistr0_retries_total counter\nmaistr0_retries_total 1",
		"# TYPE maistr0_sync_pages_total counter\nmaistr0_sync_pages_total 1",
		"# TYPE maistr0_capacity_reservations_total counter\nmaistr0_capacity_reservations_total 1",
		"# TYPE maistr0_pairing_events_total counter\nmaistr0_pairing_events_total 1",
		"# TYPE maistr0_scheduler_assignment_duration_seconds_count counter\nmaistr0_scheduler_assignment_duration_seconds_count 1",
		"maistr0_scheduler_assignment_duration_seconds_sum 0.010000000",
		"# TYPE maistr0_active_tasks gauge\nmaistr0_active_tasks 0",
		"# TYPE maistr0_dispatch_queue_depth gauge\nmaistr0_dispatch_queue_depth 0",
		"# TYPE maistr0_capacity_reserved gauge\nmaistr0_capacity_reserved 0",
		"# TYPE maistr0_sync_lag_records gauge",
		"# TYPE maistr0_engine_inventory_age_seconds gauge",
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("metrics response missing %q:\n%s", expected, response.Body.String())
		}
	}

	legacyResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(legacyResponse, httptest.NewRequest(http.MethodGet, "/api/metrics", nil))
	if legacyResponse.Code != http.StatusOK {
		t.Fatalf("legacy GET /api/metrics returned %d: %s", legacyResponse.Code, legacyResponse.Body.String())
	}
}
