package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/maistr0/maistr0/internal/memory"
)

// Regression for F8: an unreadable memory store must fail node pulls instead
// of answering "no changes", so nodes keep their previous resume point.
func TestMemorySyncGetReportsUnreadableStore(t *testing.T) {
	for _, test := range []struct {
		name       string
		versioned  bool
		watermarks map[string]uint64
	}{
		{name: "protocol v3 incremental", versioned: true, watermarks: map[string]uint64{}},
		{name: "protocol v3 reset fallback", versioned: true, watermarks: map[string]uint64{"orchestrator": 99}},
		{name: "legacy full snapshot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			srv := newTestServer(t)
			store, err := memory.Open(filepath.Join(t.TempDir(), "unreadable.db"), "orchestrator")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Record(memory.Experience{Description: "hidden"}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			srv.syncer = newSyncerComponent(srv.registry, srv.tasks, nil, store, nil)

			request := httptest.NewRequest(http.MethodGet, "/api/memory/sync", nil)
			if test.versioned {
				if err := memory.SetSyncRequestHeaders(request, test.watermarks); err != nil {
					t.Fatal(err)
				}
			}
			response := httptest.NewRecorder()
			srv.Mux().ServeHTTP(response, request)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("unreadable store sync status = %d body %q, want %d", response.Code, response.Body.String(), http.StatusInternalServerError)
			}
			if got := response.Header().Get(memory.SyncWatermarksHeader); got != "" {
				t.Fatalf("failed sync advertised watermarks %q", got)
			}
		})
	}
}
