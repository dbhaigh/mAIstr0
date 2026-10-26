package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/workpool"
)

func TestDependenciesRemainLiveWithOneDispatchWorker(t *testing.T) {
	ran := make(chan string, 2)
	var workerURL string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			_ = json.NewEncoder(w).Encode(cluster.NodeStatus{
				ID: "worker", Address: workerURL, Healthy: true,
				MaxConcurrentTasks: 1, AvailableCapacity: 1,
			})
			return
		}
		var request struct {
			SubtaskID string `json:"subtask_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		ran <- request.SubtaskID
		_, _ = w.Write([]byte(`{"output":"completed"}`))
	}))
	defer worker.Close()
	workerURL = worker.URL
	srv := newTestServer(t)
	srv.tasks.SetFinalizer(nil)
	srv.dispatcher.pool.Close()
	srv.dispatcher.pool = workpool.NewWithOptions[dispatchJob](1, 64, func(ctx context.Context, job dispatchJob) {
		srv.dispatcher.dispatchWithContext(ctx, job.taskID, job.assignment)
	}, workpool.Options{})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "worker", Address: worker.URL, Healthy: true, MaxConcurrentTasks: 1, AvailableCapacity: 1,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	})
	task, err := srv.createTask(context.Background(), createTaskRequest{
		Description: "dependency ordering",
		Subtasks: []scheduler.Subtask{
			{ID: "child", Description: "use parent", TaskType: "general", DependsOn: []string{"parent"}},
			{ID: "parent", Description: "produce input", TaskType: "general"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	completed, err := srv.tasks.Wait(ctx, task.ID)
	if err != nil || completed.Status != taskmgr.StatusCompleted {
		current, _ := srv.tasks.Get(task.ID)
		t.Fatalf("prerequisite was starved: task=%+v err=%v active=%d queued=%d", current, err, srv.dispatcher.pool.Active(), srv.dispatcher.pool.QueueDepth())
	}
	if first, second := <-ran, <-ran; first != "parent" || second != "child" {
		t.Fatalf("execution order = %s, %s", first, second)
	}
	if task.Subtasks[0].Subtask.ID != "parent" {
		t.Fatal("scheduler did not preserve prerequisite-first assignment order")
	}
}

func saturateDispatchQueue(t *testing.T, srv *Server) {
	t.Helper()
	srv.dispatcher.pool.Close()
	started := make(chan struct{})
	srv.dispatcher.pool = workpool.NewWithOptions[dispatchJob](1, 1, func(ctx context.Context, _ dispatchJob) {
		close(started)
		<-ctx.Done()
	}, workpool.Options{})
	if err := srv.dispatcher.pool.Submit(context.Background(), dispatchJob{}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := srv.dispatcher.pool.Submit(context.Background(), dispatchJob{}); err != nil {
		t.Fatal(err)
	}
}

func TestTaskCreationRejectsSaturatedQueueAndPersistsFailure(t *testing.T) {
	srv := newTestServer(t)
	saturateDispatchQueue(t, srv)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "worker", Address: "http://worker.invalid", Healthy: true,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	})
	rec := httptest.NewRecorder()
	srv.handleCreateTask(rec, httptest.NewRequest(http.MethodPost, "/api/tasks",
		strings.NewReader(`{"description":"one operation","subtasks":[{"id":"one","description":"one operation","task_type":"general"}]}`)))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), workpool.ErrFull.Error()) {
		t.Fatalf("overload = %d %s", rec.Code, rec.Body.String())
	}
	tasks := srv.tasks.All()
	if len(tasks) != 1 || tasks[0].Status != taskmgr.StatusFailed ||
		!strings.Contains(rec.Body.String(), tasks[0].ID) || !tasks[0].Resumable {
		t.Fatalf("rejected dispatch was not recorded as resumable: %+v", tasks)
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "contracts", "fixtures", "concurrency", "task-create-overload.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Response struct {
			Status      int    `json:"status"`
			ContentType string `json:"content_type"`
			Body        string `json:"body"`
		} `json:"expected_response"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	body := strings.ReplaceAll(rec.Body.String(), tasks[0].ID, "<TASK_ID>")
	if rec.Code != fixture.Response.Status || rec.Header().Get("Content-Type") != fixture.Response.ContentType || body != fixture.Response.Body {
		t.Fatalf("overload contract drift: status=%d content-type=%q body=%q", rec.Code, rec.Header().Get("Content-Type"), body)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/"+tasks[0].ID+"/resume", nil)
	req.SetPathValue("id", tasks[0].ID)
	rec = httptest.NewRecorder()
	srv.handleResumeTask(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("resume overload = %d %s", rec.Code, rec.Body.String())
	}
}

func TestCanceledTaskCreationDoesNotPersistOrDispatch(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "worker", Address: "http://worker.invalid", Healthy: true,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := srv.createTask(ctx, createTaskRequest{
		Description: "one operation",
		Subtasks:    []scheduler.Subtask{{ID: "one", Description: "one operation", TaskType: "general"}},
	})
	if !errors.Is(err, context.Canceled) || len(srv.tasks.All()) != 0 || srv.dispatcher.pool.QueueDepth() != 0 {
		t.Fatalf("canceled creation: err=%v tasks=%d", err, len(srv.tasks.All()))
	}
}
