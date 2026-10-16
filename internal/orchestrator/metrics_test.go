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

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/metrics returned %d: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("metrics content type = %q", got)
	}
	for _, expected := range []string{
		"# TYPE maistr0_dispatches_total counter\nmaistr0_dispatches_total 2",
		"# TYPE maistr0_retries_total counter\nmaistr0_retries_total 1",
		"# TYPE maistr0_sync_pages_total counter\nmaistr0_sync_pages_total 1",
		"# TYPE maistr0_capacity_reservations_total counter\nmaistr0_capacity_reservations_total 1",
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("metrics response missing %q:\n%s", expected, response.Body.String())
		}
	}
}
