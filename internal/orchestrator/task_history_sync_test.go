package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

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
		client:   peer.Client(),
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
		client:   peer.Client(),
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
