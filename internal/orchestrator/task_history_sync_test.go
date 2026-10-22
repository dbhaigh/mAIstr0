package orchestrator

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestTaskListSyncNegotiatesEmptyAndGzip(t *testing.T) {
	srv := newTestServer(t)
	for _, test := range []struct {
		version int
		status  int
	}{
		{version: memory.SyncPageProtocolVersion, status: http.StatusOK},
		{version: memory.SyncProtocolVersion, status: http.StatusNoContent},
	} {
		request := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
		if err := memory.SetSyncHeadersVersion(request.Header, nil, test.version); err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		srv.Mux().ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("protocol %d empty task status = %d, want %d", test.version, response.Code, test.status)
		}
		version, _, err := memory.ReadSyncHeaders(response.Header())
		if err != nil {
			t.Fatal(err)
		}
		if version != test.version {
			t.Fatalf("protocol %d task response version = %d", test.version, version)
		}
	}

	if _, err := srv.tasks.CreateChecked(strings.Repeat("task-data ", 300), nil); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	if err := memory.SetSyncHeadersVersion(request.Header, nil, memory.SyncProtocolVersion); err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", "gzip")
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, request)
	if got := response.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("task sync Content-Encoding = %q, want gzip", got)
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	var tasks []*taskmgr.Task
	if err := json.Unmarshal(body, &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("decoded task count = %d, want 1", len(tasks))
	}
}

func TestSyncTaskHistoryImportsAndReplicatesCompleteHistory(t *testing.T) {
	peerTasks := taskmgr.NewManager()
	for _, description := range []string{"Older task", "Recent task"} {
		if _, err := peerTasks.CreateChecked(description, nil); err != nil {
			t.Fatal(err)
		}
	}

	var replicated []*taskmgr.Task
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tasks":
			_ = json.NewEncoder(w).Encode(peerTasks.All())
		case "/tasks/replica":
			if err := json.NewDecoder(r.Body).Decode(&replicated); err != nil {
				t.Errorf("decode replicated task history: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer peer.Close()

	server := &Server{
		registry: cluster.NewRegistry(),
		tasks:    taskmgr.NewManager(),
		nodeClient: &nodeClient{
			client: peer.Client(),
			sync:   peer.Client(),
		},
	}
	server.registry.Upsert(cluster.NodeStatus{ID: "worker", Address: peer.URL})
	server.syncTaskHistory()

	if got := len(server.tasks.All()); got != 2 {
		t.Fatalf("coordinator imported %d tasks, want full history of 2", got)
	}
	if len(replicated) != 2 {
		t.Fatalf("replica received %d tasks, want full history of 2", len(replicated))
	}
}

func TestSyncTaskHistorySkipsPeersWhenNotElected(t *testing.T) {
	requests := 0
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
	}))
	defer peer.Close()

	syncer := newSyncerComponent(
		cluster.NewRegistry(),
		taskmgr.NewManager(),
		&nodeClient{client: peer.Client()},
		nil,
		func() bool { return false },
	)
	syncer.registry.Upsert(cluster.NodeStatus{ID: "worker", Address: peer.URL, Healthy: true})
	syncer.syncTaskHistory()
	if requests != 0 {
		t.Fatalf("yielded orchestrator made %d peer requests, want none", requests)
	}
}

func TestTaskListDoesNotWaitForPeerHistoryRecovery(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	releasePeer := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tasks":
			requestStarted <- struct{}{}
			<-releasePeer
			_ = json.NewEncoder(w).Encode([]*taskmgr.Task{})
		case "/tasks/replica":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer peer.Close()
	defer close(releasePeer)

	server := &Server{
		registry: cluster.NewRegistry(),
		tasks:    taskmgr.NewManager(),
		nodeClient: &nodeClient{
			client: peer.Client(),
			sync:   peer.Client(),
		},
	}
	server.registry.Upsert(cluster.NodeStatus{ID: "worker", Address: peer.URL})

	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.handleListTasks(response, httptest.NewRequest(http.MethodGet, "/api/tasks", nil))
		close(done)
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("task history recovery did not contact the peer")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task list waited for peer history recovery")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("task list returned %d: %s", response.Code, strings.TrimSpace(response.Body.String()))
	}
}
