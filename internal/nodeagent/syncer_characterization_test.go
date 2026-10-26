package nodeagent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
)

func withPeerCertificate(r *http.Request, commonName string) {
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{
		Subject: pkix.Name{CommonName: commonName},
	}}}
}

// openUnreadableMemoryStore returns a bbolt store holding records whose
// database has been closed, so every uncached read fails.
func openUnreadableMemoryStore(t *testing.T, owner string) memory.Store {
	t.Helper()
	store, err := memory.Open(filepath.Join(t.TempDir(), owner+"-unreadable.db"), owner)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 2 {
		if _, err := store.Record(memory.Experience{Description: fmt.Sprintf("hidden %d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return store
}

// Regression for F8: a peer whose store cannot be read must fail the sync
// request instead of answering "no changes" with an empty snapshot.
func TestPeerMemorySyncGetReportsUnreadableStore(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    bool
		watermarks map[string]uint64
	}{
		{name: "protocol v3 incremental", version: true, watermarks: map[string]uint64{}},
		{name: "protocol v3 reset fallback", version: true, watermarks: map[string]uint64{"node-a": 99}},
		{name: "legacy full snapshot", version: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newTestAgent(t, "node-a")
			a.mu.Lock()
			a.peers = []cluster.NodeStatus{{ID: "node-b", Healthy: true}}
			a.mu.Unlock()
			original := a.mem
			a.mem = openUnreadableMemoryStore(t, "node-a")
			t.Cleanup(func() { a.mem = original })

			request := httptest.NewRequest(http.MethodGet, "/memory/sync", nil)
			withPeerCertificate(request, "node-b")
			if test.version {
				if err := memory.SetSyncRequestHeaders(request, test.watermarks); err != nil {
					t.Fatal(err)
				}
			}
			response := httptest.NewRecorder()
			a.handlePeerMemorySyncGet(response, request)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("unreadable store sync status = %d body %q, want %d", response.Code, response.Body.String(), http.StatusInternalServerError)
			}
			if response.Header().Get(memory.SyncWatermarksHeader) != "" {
				t.Fatalf("failed sync advertised watermarks %q", response.Header().Get(memory.SyncWatermarksHeader))
			}
		})
	}
}

func TestFetchMemorySyncFromUnreadablePeerDoesNotAdvanceWatermarks(t *testing.T) {
	peer := newTestAgent(t, "node-b")
	peer.mu.Lock()
	peer.peers = []cluster.NodeStatus{{ID: "node-a", Healthy: true}}
	peer.mu.Unlock()
	original := peer.mem
	peer.mem = openUnreadableMemoryStore(t, "node-b")
	t.Cleanup(func() { peer.mem = original })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		withPeerCertificate(r, "node-a")
		if r.Method == http.MethodGet {
			peer.handlePeerMemorySyncGet(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	local := memory.NewInMemoryStore()
	if err := local.SetPeerWatermarks("node-b", map[string]uint64{"node-b": 1}); err != nil {
		t.Fatal(err)
	}
	syncer := characterizationSyncer(local, server)
	syncer.syncPeerMemoryWithContext(context.Background())

	if got := local.PeerWatermarks("node-b"); !reflect.DeepEqual(got, map[string]uint64{"node-b": 1}) {
		t.Fatalf("peer watermarks after failed fetch = %v, want unchanged", got)
	}
	if got := memorySyncRecordCount(local.Snapshot()); got != 0 {
		t.Fatalf("failed fetch merged %d records", got)
	}
	_, _, status, err := fetchMemorySyncSnapshot(server.Client(), server.URL+"/memory/sync", nil, maxMemorySyncBodyBytes)
	if err == nil || status != http.StatusInternalServerError {
		t.Fatalf("fetch from unreadable peer = status %d, err %v; want 500 error", status, err)
	}
}

func characterizationSyncer(store memory.Store, server *httptest.Server) *syncerComponent {
	return newSyncerComponent(
		"node-a",
		nil,
		store,
		nil,
		func() *http.Client { return server.Client() },
		func() []cluster.NodeStatus {
			return []cluster.NodeStatus{{ID: "node-b", Address: server.URL, Healthy: true}}
		},
		0,
		func() bool { return true },
	)
}

type truncatingResponseWriter struct {
	http.ResponseWriter
}

func (w truncatingResponseWriter) Write(p []byte) (int, error) {
	if _, err := w.ResponseWriter.Write(p[:len(p)/2]); err != nil {
		return 0, err
	}
	return len(p), nil
}

// memorySyncFaultServer serves the real peer GET handler and injects one
// fault into the first follow-up page request while armed.
type memorySyncFaultServer struct {
	peer   *Agent
	cancel context.CancelFunc

	mu       sync.Mutex
	fault    string
	statuses []int
	pages    int
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (f *memorySyncFaultServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	withPeerCertificate(r, "node-a")
	if r.Method != http.MethodGet {
		closeutil.Discard(r.Body)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	followUp := r.Header.Get(memory.SyncPageCursorHeader) != ""
	f.mu.Lock()
	if followUp {
		f.pages++
	}
	fault := ""
	if f.fault != "" && (followUp || f.fault == "first page server error") {
		fault = f.fault
		f.fault = ""
	}
	f.mu.Unlock()

	recorder := &statusRecorder{ResponseWriter: w}
	switch fault {
	case "":
		f.peer.handlePeerMemorySyncGet(recorder, r)
	case "first page server error", "follow-up page server error":
		http.Error(recorder, "injected failure", http.StatusInternalServerError)
	case "truncated follow-up page":
		f.peer.handlePeerMemorySyncGet(&statusRecorder{ResponseWriter: truncatingResponseWriter{w}}, r)
		recorder.status = -1
	case "malformed follow-up page":
		recorder.Header().Set("Content-Type", "application/json")
		recorder.Header().Set(memory.SyncVersionHeader, fmt.Sprint(memory.SyncProtocolVersion))
		recorder.WriteHeader(http.StatusOK)
		_, _ = recorder.Write([]byte(`{"experiences":[`))
	case "cancelled during follow-up page":
		f.cancel()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		recorder.status = -1
	default:
		panic("unknown fault " + fault)
	}
	f.mu.Lock()
	f.statuses = append(f.statuses, recorder.status)
	f.mu.Unlock()
}

func (f *memorySyncFaultServer) arm(fault string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fault
	f.statuses = nil
	f.pages = 0
}

func (f *memorySyncFaultServer) observed() ([]int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.statuses), f.pages
}

func sortedRecordIDs(t *testing.T, snapshot memory.Snapshot) []string {
	t.Helper()
	ids, err := memory.RecordIDs(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	return ids
}

// An interrupted multi-page pull must leave the local store and the per-peer
// watermark untouched; a retry restarts from the old watermark, converges,
// and later passes are incremental.
func TestSyncPeerMemoryInterruptedPagedPullDoesNotAdvanceWatermarks(t *testing.T) {
	stores := []struct {
		name string
		open func(t *testing.T) memory.Store
	}{
		{name: "in-memory", open: func(*testing.T) memory.Store { return memory.NewInMemoryStore() }},
		{name: "bbolt", open: func(t *testing.T) memory.Store {
			store, err := memory.Open(filepath.Join(t.TempDir(), "leader.db"), "node-a")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeutil.Discard(store) })
			return store
		}},
	}
	faults := []string{
		"first page server error",
		"follow-up page server error",
		"truncated follow-up page",
		"malformed follow-up page",
		"cancelled during follow-up page",
	}
	for _, storeCase := range stores {
		for _, fault := range faults {
			t.Run(storeCase.name+"/"+fault, func(t *testing.T) {
				peer := newTestAgent(t, "node-b")
				peer.mu.Lock()
				peer.peers = []cluster.NodeStatus{{ID: "node-a", Healthy: true}}
				peer.cfg.SyncMaxBytes = 1500
				peer.mu.Unlock()
				for index := range 8 {
					if _, err := peer.mem.Record(memory.Experience{
						Description: fmt.Sprintf("paged record %d %s", index, strings.Repeat("x", 300)),
					}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := peer.mem.PutFact(memory.Fact{Key: "paged-fact", Value: "value"}); err != nil {
					t.Fatal(err)
				}
				want := sortedRecordIDs(t, peer.mem.Snapshot())
				wantWatermarks := memory.FilterMemoryWatermarks(peer.mem.Watermarks())

				local := storeCase.open(t)
				beforeWatermarks := local.Watermarks()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				faultServer := &memorySyncFaultServer{peer: peer, cancel: cancel}
				server := httptest.NewServer(faultServer)
				defer server.Close()
				syncer := characterizationSyncer(local, server)

				faultServer.arm(fault)
				syncer.syncPeerMemoryWithContext(ctx)
				if _, pages := faultServer.observed(); fault != "first page server error" && pages == 0 {
					t.Fatalf("fault %q never reached a follow-up page", fault)
				}
				if got := local.PeerWatermarks("node-b"); len(got) != 0 {
					t.Fatalf("interrupted pull advanced peer watermarks to %v", got)
				}
				if got := local.Watermarks(); !reflect.DeepEqual(got, beforeWatermarks) {
					t.Fatalf("interrupted pull changed local watermarks from %v to %v", beforeWatermarks, got)
				}
				if got := memorySyncRecordCount(local.Snapshot()); got != 0 {
					t.Fatalf("interrupted pull merged %d records from partial pages", got)
				}

				faultServer.arm("")
				syncer.syncPeerMemoryWithContext(context.Background())
				if got := sortedRecordIDs(t, local.Snapshot()); !reflect.DeepEqual(got, want) {
					t.Fatalf("retry converged to %v, want %v", got, want)
				}
				if got := memory.FilterMemoryWatermarks(local.PeerWatermarks("node-b")); !reflect.DeepEqual(got, wantWatermarks) {
					t.Fatalf("retry peer watermarks = %v, want %v", got, wantWatermarks)
				}
				if statuses, pages := faultServer.observed(); pages == 0 || slices.Contains(statuses, http.StatusInternalServerError) {
					t.Fatalf("retry statuses = %v with %d follow-up pages, want a clean paged pull", statuses, pages)
				}

				faultServer.arm("")
				syncer.syncPeerMemoryWithContext(context.Background())
				if statuses, pages := faultServer.observed(); !reflect.DeepEqual(statuses, []int{http.StatusNoContent}) || pages != 0 {
					t.Fatalf("pass after convergence statuses = %v, follow-up pages %d; want one incremental 204", statuses, pages)
				}
			})
		}
	}
}
