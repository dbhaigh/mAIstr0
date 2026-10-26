package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/workpool"
)

const characterizationWait = 5 * time.Second

func characterizationNode(id, address, model string, capacity int) cluster.NodeStatus {
	return cluster.NodeStatus{
		ID: id, Address: address, Healthy: true,
		MaxConcurrentTasks: capacity, AvailableCapacity: capacity,
		Models: []engine.Model{{Name: model, Engine: "ollama", Tags: []string{"general"}}},
	}
}

func characterizationAssignment(id string, node cluster.NodeStatus, dependsOn ...string) scheduler.Assignment {
	return scheduler.Assignment{
		Subtask: scheduler.Subtask{
			ID: id, Description: "Run " + id, TaskType: "general", Tags: []string{"general"}, DependsOn: dependsOn,
		},
		NodeID: node.ID, Address: node.Address, Model: node.Models[0].Name, Engine: node.Models[0].Engine,
	}
}

type executeRequest struct {
	SubtaskID string `json:"subtask_id"`
	Prompt    string `json:"prompt"`
}

func decodeExecuteRequest(t *testing.T, r *http.Request) executeRequest {
	t.Helper()
	var request executeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Errorf("decode execute request: %v", err)
	}
	return request
}

func runDispatch(srv *Server, taskID string, assignment scheduler.Assignment) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.dispatcher.dispatchWithContext(context.Background(), taskID, assignment)
	}()
	return done
}

func awaitSignal[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(characterizationWait):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// assertDispatchCapacityReleased checks the dispatch gate's observable in-use
// counts: global slots, each node gate's local reservations, and registry load.
func assertDispatchCapacityReleased(t *testing.T, srv *Server, gate *dispatchCapacityGate, nodeIDs ...string) {
	t.Helper()
	if active := gate.Active(); active != 0 {
		t.Errorf("global dispatch slots in use = %d, want 0", active)
	}
	if waiters := srv.dispatcher.dependencyWaiters.Load(); waiters != 0 {
		t.Errorf("dependency waiters = %d, want 0", waiters)
	}
	for _, nodeID := range nodeIDs {
		gate.nodeGateMu.Lock()
		nodeGate := gate.nodeGates[nodeID]
		gate.nodeGateMu.Unlock()
		if nodeGate != nil {
			nodeGate.mu.Lock()
			active := nodeGate.active
			nodeGate.mu.Unlock()
			if active != 0 {
				t.Errorf("node %s reservations in use = %d, want 0", nodeID, active)
			}
		}
		if node, ok := srv.registry.Get(nodeID); ok && node.ActiveTasks != 0 {
			t.Errorf("node %s registry load = %d, want 0", nodeID, node.ActiveTasks)
		}
	}
}

func testCapacityGate(t *testing.T, srv *Server) *dispatchCapacityGate {
	t.Helper()
	gate, ok := srv.dispatcher.capacityGate.(*dispatchCapacityGate)
	if !ok {
		t.Fatalf("dispatcher capacity gate = %T", srv.dispatcher.capacityGate)
	}
	return gate
}

func TestTaskCancellationDuringDispatchCancelsNodeRequestAndReleasesCapacity(t *testing.T) {
	entered := make(chan struct{})
	requestCancelled := make(chan struct{})
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(requestCancelled)
	}))
	defer worker.Close()

	srv := newTestServer(t)
	gate := testCapacityGate(t, srv)
	node := characterizationNode("node-a", worker.URL, "general", 2)
	srv.registry.Upsert(node)
	task, err := srv.tasks.CreateChecked("cancel in flight", []scheduler.Assignment{characterizationAssignment("work", node)})
	if err != nil {
		t.Fatal(err)
	}

	done := runDispatch(srv, task.ID, task.Subtasks[0].Assignment)
	awaitSignal(t, entered, "worker request")
	if active := gate.Active(); active != 1 {
		t.Fatalf("global slots during dispatch = %d, want 1", active)
	}
	if loaded, _ := srv.registry.Get("node-a"); loaded.ActiveTasks != 1 {
		t.Fatalf("node load during dispatch = %d, want 1", loaded.ActiveTasks)
	}

	if cancelled, err := srv.tasks.CancelChecked(task.ID); !cancelled || err != nil {
		t.Fatalf("cancel = %t, %v", cancelled, err)
	}
	awaitSignal(t, requestCancelled, "node request cancellation")
	awaitSignal(t, done, "dispatch return")

	final, _ := srv.tasks.Get(task.ID)
	if final.Status != taskmgr.StatusCancelled || final.Subtasks[0].Status != taskmgr.StatusCancelled {
		t.Fatalf("cancelled task = %s/%s", final.Status, final.Subtasks[0].Status)
	}
	attempts := final.Subtasks[0].Attempts
	if len(attempts) != 1 || attempts[0].Status != taskmgr.DispatchAttemptFailed || attempts[0].FailureClass != "cancelled" {
		t.Fatalf("cancelled attempts = %+v", attempts)
	}
	if calls.Load() != 1 {
		t.Fatalf("worker calls = %d, want 1 with no retry after cancellation", calls.Load())
	}
	assertDispatchCapacityReleased(t, srv, gate, "node-a")
}

type reserveSignalGate struct {
	workpool.CapacityGate
	entered chan struct{}
	once    sync.Once
}

func (g *reserveSignalGate) Reserve(ctx context.Context, targets []workpool.CapacityTarget) (workpool.CapacityReservation, error) {
	g.once.Do(func() { close(g.entered) })
	return g.CapacityGate.Reserve(ctx, targets)
}

func TestTaskCancellationWhileWaitingForCapacityDispatchesNothing(t *testing.T) {
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "must not run"})
	}))
	defer worker.Close()

	srv := newTestServer(t)
	gate := testCapacityGate(t, srv)
	signal := &reserveSignalGate{CapacityGate: gate, entered: make(chan struct{})}
	srv.dispatcher.capacityGate = signal
	node := characterizationNode("node-a", worker.URL, "general", 1)
	srv.registry.Upsert(node)
	held, err := gate.Reserve(context.Background(), []workpool.CapacityTarget{{NodeID: node.ID, Address: node.Address}})
	if err != nil {
		t.Fatal(err)
	}
	task, err := srv.tasks.CreateChecked("cancel while queued", []scheduler.Assignment{characterizationAssignment("work", node)})
	if err != nil {
		t.Fatal(err)
	}

	done := runDispatch(srv, task.ID, task.Subtasks[0].Assignment)
	awaitSignal(t, signal.entered, "capacity reservation wait")
	if cancelled, err := srv.tasks.CancelChecked(task.ID); !cancelled || err != nil {
		t.Fatalf("cancel = %t, %v", cancelled, err)
	}
	awaitSignal(t, done, "dispatch return")

	final, _ := srv.tasks.Get(task.ID)
	if final.Status != taskmgr.StatusCancelled || len(final.Subtasks[0].Attempts) != 0 || calls.Load() != 0 {
		t.Fatalf("queued cancellation = status %s attempts %+v worker calls %d", final.Status, final.Subtasks[0].Attempts, calls.Load())
	}
	if active := gate.Active(); active != 1 {
		t.Fatalf("global slots after cancelled wait = %d, want only the test's reservation", active)
	}
	held.Release()
	assertDispatchCapacityReleased(t, srv, gate, "node-a")
}

func TestDispatchRetriesAreBoundedByConfiguredAttempts(t *testing.T) {
	for _, maxAttempts := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("max_%d", maxAttempts), func(t *testing.T) {
			const nodeCount = 4
			var calls [nodeCount]atomic.Int32
			srv := newTestServer(t)
			srv.dispatcher.retryPolicy.maxAttempts = maxAttempts
			srv.dispatcher.retryPolicy.backoff = time.Millisecond
			srv.dispatcher.retryPolicy.maxBackoff = time.Millisecond
			nodes := make([]cluster.NodeStatus, nodeCount)
			nodeIDs := make([]string, nodeCount)
			for i := range nodeCount {
				index := i
				worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls[index].Add(1)
					http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				}))
				t.Cleanup(worker.Close)
				nodes[i] = characterizationNode(fmt.Sprintf("node-%d", i), worker.URL, fmt.Sprintf("model-%d", i), 2)
				nodeIDs[i] = nodes[i].ID
				srv.registry.Upsert(nodes[i])
			}
			task, err := srv.tasks.CreateChecked("bounded retries", []scheduler.Assignment{characterizationAssignment("work", nodes[0])})
			if err != nil {
				t.Fatal(err)
			}

			awaitSignal(t, runDispatch(srv, task.ID, task.Subtasks[0].Assignment), "dispatch return")

			final, _ := srv.tasks.Get(task.ID)
			if final.Status != taskmgr.StatusFailed || final.Subtasks[0].Status != taskmgr.StatusFailed {
				t.Fatalf("exhausted task = %s/%s", final.Status, final.Subtasks[0].Status)
			}
			attempts := final.Subtasks[0].Attempts
			if len(attempts) != maxAttempts {
				t.Fatalf("attempts = %d, want %d: %+v", len(attempts), maxAttempts, attempts)
			}
			pairs := make(map[string]bool, len(attempts))
			for i, attempt := range attempts {
				pair := routerPairKey(attempt.NodeID, attempt.Model)
				if pairs[pair] {
					t.Fatalf("attempt %d reused node/model pair %s: %+v", i+1, pair, attempts)
				}
				pairs[pair] = true
				if attempt.Number != i+1 || attempt.Status != taskmgr.DispatchAttemptFailed ||
					attempt.FailureClass != "upstream_5xx" || attempt.HTTPStatus != http.StatusServiceUnavailable ||
					attempt.ID != taskmgr.DispatchAttemptID(task.ID, "work", i+1) {
					t.Fatalf("attempt %d = %+v", i+1, attempt)
				}
			}
			var totalCalls int32
			for i := range calls {
				if got := calls[i].Load(); got > 1 {
					t.Fatalf("node-%d called %d times, want at most once", i, got)
				}
				totalCalls += calls[i].Load()
			}
			if int(totalCalls) != maxAttempts {
				t.Fatalf("worker calls = %d, want %d", totalCalls, maxAttempts)
			}
			assertDispatchCapacityReleased(t, srv, testCapacityGate(t, srv), nodeIDs...)
		})
	}
}

// resumeAfterFailedAttempts records prior attempts for the first subtask, fails
// the task, and resumes it, mirroring a retry after restart or failure.
func resumeAfterFailedAttempts(t *testing.T, srv *Server, taskID string, prior []scheduler.Assignment) *taskmgr.Task {
	t.Helper()
	if err := srv.tasks.StartSubtaskChecked(taskID, "work"); err != nil {
		t.Fatal(err)
	}
	for _, assignment := range prior {
		number, err := srv.tasks.BeginDispatchAttemptChecked(taskID, "work", assignment)
		if err != nil {
			t.Fatal(err)
		}
		if err := srv.tasks.FinishDispatchAttemptChecked(taskID, "work", number, taskmgr.DispatchAttemptFailed,
			"orchestrator_restart", 0, errors.New("indeterminate"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := srv.tasks.CompleteSubtaskChecked(taskID, "work", "", errors.New("interrupted")); err != nil {
		t.Fatal(err)
	}
	resumed, err := srv.tasks.ResumeTaskChecked(taskID)
	if err != nil {
		t.Fatal(err)
	}
	return resumed
}

func TestRedispatchAfterResumeHonorsPersistedAttemptHistory(t *testing.T) {
	for _, test := range []struct {
		name        string
		priorOnA    int
		maxAttempts int
		wantStatus  taskmgr.Status
		wantCallsA  int32
		wantCallsB  int32
		wantError   string
	}{
		{name: "untried pair is preferred", priorOnA: 1, maxAttempts: 3, wantStatus: taskmgr.StatusCompleted, wantCallsB: 1},
		{name: "attempt budget is exhausted", priorOnA: 2, maxAttempts: 2, wantStatus: taskmgr.StatusFailed, wantError: "attempt limit reached"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var callsA, callsB atomic.Int32
			workerA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				callsA.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]string{"output": "from a"})
			}))
			defer workerA.Close()
			workerB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				callsB.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]string{"output": "from b"})
			}))
			defer workerB.Close()

			srv := newTestServer(t)
			srv.dispatcher.retryPolicy.maxAttempts = test.maxAttempts
			nodeA := characterizationNode("node-a", workerA.URL, "model-a", 2)
			nodeB := characterizationNode("node-b", workerB.URL, "model-b", 2)
			srv.registry.Upsert(nodeA)
			srv.registry.Upsert(nodeB)
			assignment := characterizationAssignment("work", nodeA)
			task, err := srv.tasks.CreateChecked("redispatch", []scheduler.Assignment{assignment})
			if err != nil {
				t.Fatal(err)
			}
			prior := make([]scheduler.Assignment, test.priorOnA)
			for i := range prior {
				prior[i] = assignment
			}
			resumed := resumeAfterFailedAttempts(t, srv, task.ID, prior)

			awaitSignal(t, runDispatch(srv, task.ID, resumed.Subtasks[0].Assignment), "dispatch return")

			final, _ := srv.tasks.Get(task.ID)
			if final.Status != test.wantStatus || callsA.Load() != test.wantCallsA || callsB.Load() != test.wantCallsB {
				t.Fatalf("redispatch = status %s, calls a=%d b=%d; want %s, a=%d b=%d",
					final.Status, callsA.Load(), callsB.Load(), test.wantStatus, test.wantCallsA, test.wantCallsB)
			}
			if test.wantError != "" && !strings.Contains(final.Subtasks[0].Error, test.wantError) {
				t.Fatalf("subtask error = %q, want %q", final.Subtasks[0].Error, test.wantError)
			}
			attempts := final.Subtasks[0].Attempts
			if want := test.priorOnA + int(test.wantCallsA+test.wantCallsB); len(attempts) != want {
				t.Fatalf("attempt history = %+v, want %d attempts", attempts, want)
			}
			if test.wantStatus == taskmgr.StatusCompleted {
				last := attempts[len(attempts)-1]
				if last.NodeID != "node-b" || last.Status != taskmgr.DispatchAttemptSucceeded ||
					last.Number != test.priorOnA+1 || final.Subtasks[0].NodeID != "node-b" {
					t.Fatalf("resumed attempt = %+v, assignment %s", last, final.Subtasks[0].NodeID)
				}
			}
			assertDispatchCapacityReleased(t, srv, testCapacityGate(t, srv), "node-a", "node-b")
		})
	}
}

func TestDuplicateDispatchOfCompletedSubtaskIsIgnored(t *testing.T) {
	for _, nodeCount := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d_nodes", nodeCount), func(t *testing.T) {
			var calls atomic.Int32
			worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				n := calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]string{"output": fmt.Sprintf("output %d", n)})
			}))
			defer worker.Close()

			srv := newTestServer(t)
			nodeIDs := make([]string, 0, nodeCount)
			var first cluster.NodeStatus
			for i := range nodeCount {
				node := characterizationNode(fmt.Sprintf("node-%d", i), worker.URL, fmt.Sprintf("model-%d", i), 2)
				srv.registry.Upsert(node)
				nodeIDs = append(nodeIDs, node.ID)
				if i == 0 {
					first = node
				}
			}
			task, err := srv.tasks.CreateChecked("duplicate", []scheduler.Assignment{characterizationAssignment("work", first)})
			if err != nil {
				t.Fatal(err)
			}
			awaitSignal(t, runDispatch(srv, task.ID, task.Subtasks[0].Assignment), "first dispatch")
			completed, _ := srv.tasks.Get(task.ID)
			if completed.Status != taskmgr.StatusCompleted || calls.Load() != 1 {
				t.Fatalf("first dispatch = %s after %d calls", completed.Status, calls.Load())
			}

			awaitSignal(t, runDispatch(srv, task.ID, task.Subtasks[0].Assignment), "duplicate dispatch")

			final, _ := srv.tasks.Get(task.ID)
			if final.Status != taskmgr.StatusCompleted || final.Subtasks[0].Status != taskmgr.StatusCompleted ||
				final.Subtasks[0].Output != completed.Subtasks[0].Output || final.Subtasks[0].Error != "" ||
				len(final.Subtasks[0].Attempts) != 1 || final.Subtasks[0].NodeID != completed.Subtasks[0].NodeID {
				t.Fatalf("duplicate dispatch changed completed task: before %+v after %+v", completed.Subtasks[0], final.Subtasks[0])
			}
			if calls.Load() != 1 {
				t.Fatalf("worker calls after duplicate dispatch = %d, want 1", calls.Load())
			}
			assertDispatchCapacityReleased(t, srv, testCapacityGate(t, srv), nodeIDs...)
		})
	}
}

// Dispatch has no per-subtask lease: the dispatcher assumes one dispatcher per
// subtask, which the planner and resume handler provide. This documents what a
// concurrent duplicate still guarantees: a single recorded winner and released
// capacity, not a single node execution.
func TestConcurrentDuplicateDispatchRecordsSingleWinner(t *testing.T) {
	type arrival struct {
		node    string
		release chan string
	}
	arrivals := make(chan arrival, 2)
	newWorker := func(node string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			release := make(chan string, 1)
			arrivals <- arrival{node: node, release: release}
			select {
			case output := <-release:
				_ = json.NewEncoder(w).Encode(map[string]string{"output": output})
			case <-r.Context().Done():
			}
		}))
	}
	workerA, workerB := newWorker("node-a"), newWorker("node-b")
	defer workerA.Close()
	defer workerB.Close()

	srv := newTestServer(t)
	nodeA := characterizationNode("node-a", workerA.URL, "model-a", 2)
	nodeB := characterizationNode("node-b", workerB.URL, "model-b", 2)
	srv.registry.Upsert(nodeA)
	srv.registry.Upsert(nodeB)
	task, err := srv.tasks.CreateChecked("concurrent duplicate", []scheduler.Assignment{characterizationAssignment("work", nodeA)})
	if err != nil {
		t.Fatal(err)
	}

	firstDone := runDispatch(srv, task.ID, task.Subtasks[0].Assignment)
	first := awaitSignal(t, arrivals, "first execution")
	secondDone := runDispatch(srv, task.ID, task.Subtasks[0].Assignment)
	second := awaitSignal(t, arrivals, "duplicate execution")
	if first.node != "node-a" || second.node != "node-b" {
		t.Fatalf("executions = %s then %s; duplicate should avoid the in-flight pair", first.node, second.node)
	}
	first.release <- "first output"
	awaitSignal(t, firstDone, "first dispatch")
	second.release <- "second output"
	awaitSignal(t, secondDone, "duplicate dispatch")

	final, _ := srv.tasks.Get(task.ID)
	result := final.Subtasks[0]
	if final.Status != taskmgr.StatusCompleted || result.Output != "first output" || result.NodeID != "node-a" {
		t.Fatalf("winner = status %s output %q node %s", final.Status, result.Output, result.NodeID)
	}
	if len(result.Attempts) != 2 || result.Attempts[0].NodeID == result.Attempts[1].NodeID {
		t.Fatalf("duplicate attempts = %+v", result.Attempts)
	}
	assertDispatchCapacityReleased(t, srv, testCapacityGate(t, srv), "node-a", "node-b")
}

func TestPartialWorkflowFailureReleasesDispatchResources(t *testing.T) {
	var mu sync.Mutex
	calls := make(map[string]int)
	prompts := make(map[string]string)
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := decodeExecuteRequest(t, r)
		mu.Lock()
		calls[request.SubtaskID]++
		prompts[request.SubtaskID] = request.Prompt
		mu.Unlock()
		if request.SubtaskID == "broken" {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": request.SubtaskID + " output"})
	}))
	defer worker.Close()

	srv := newTestServer(t)
	node := characterizationNode("node-a", worker.URL, "general", 4)
	srv.registry.Upsert(node)
	assignments := []scheduler.Assignment{
		characterizationAssignment("source", node),
		characterizationAssignment("broken", node),
		characterizationAssignment("after-broken", node, "broken"),
		characterizationAssignment("after-source", node, "source"),
	}
	task, err := srv.tasks.CreateChecked("partial failure", assignments)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, assignment := range task.Subtasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.dispatcher.dispatchWithContext(context.Background(), task.ID, assignment.Assignment)
		}()
	}
	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()
	awaitSignal(t, allDone, "workflow dispatches")

	final, _ := srv.tasks.Get(task.ID)
	if final.Status != taskmgr.StatusFailed || !final.Resumable {
		t.Fatalf("partial failure task = %s resumable=%t", final.Status, final.Resumable)
	}
	want := map[string]taskmgr.Status{
		"source": taskmgr.StatusCompleted, "broken": taskmgr.StatusFailed,
		"after-broken": taskmgr.StatusFailed, "after-source": taskmgr.StatusCompleted,
	}
	for _, subtask := range final.Subtasks {
		if subtask.Status != want[subtask.Subtask.ID] {
			t.Errorf("subtask %s = %s, want %s", subtask.Subtask.ID, subtask.Status, want[subtask.Subtask.ID])
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["broken"] != 1 || calls["after-broken"] != 0 || calls["source"] != 1 || calls["after-source"] != 1 {
		t.Fatalf("worker calls = %v; failed prerequisite must block its dependent", calls)
	}
	if !strings.Contains(prompts["after-source"], "Prerequisite source:\nsource output") {
		t.Fatalf("dependent prompt omitted prerequisite output: %q", prompts["after-source"])
	}
	assertDispatchCapacityReleased(t, srv, testCapacityGate(t, srv), "node-a")
}

func TestResumeHTTPRejectsRunningTaskWithoutRedispatch(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		select {
		case <-release:
			_ = json.NewEncoder(w).Encode(map[string]string{"output": "single output"})
		case <-r.Context().Done():
		}
	}))
	defer worker.Close()

	srv := newTestServer(t)
	node := characterizationNode("node-a", worker.URL, "general", 2)
	srv.registry.Upsert(node)
	task, err := srv.tasks.CreateChecked("resume while running", []scheduler.Assignment{characterizationAssignment("work", node)})
	if err != nil {
		t.Fatal(err)
	}
	taskCtx, _ := srv.tasks.Context(task.ID)
	done := runDispatch(srv, task.ID, task.Subtasks[0].Assignment)
	awaitSignal(t, entered, "worker request")

	response := serveContractRequest(srv.Mux(), http.MethodPost, "/api/tasks/"+task.ID+"/resume", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("resume running task returned %d, want 409: %s", response.Code, response.Body.String())
	}
	running, _ := srv.tasks.Get(task.ID)
	if running.Status != taskmgr.StatusRunning || running.Subtasks[0].Status != taskmgr.StatusRunning || taskCtx.Err() != nil {
		t.Fatalf("rejected resume disturbed running task: %s/%s ctx=%v", running.Status, running.Subtasks[0].Status, taskCtx.Err())
	}
	close(release)
	awaitSignal(t, done, "dispatch return")

	final, _ := srv.tasks.Get(task.ID)
	if final.Status != taskmgr.StatusCompleted || calls.Load() != 1 || len(final.Subtasks[0].Attempts) != 1 {
		t.Fatalf("task = %s after %d worker calls, attempts %+v", final.Status, calls.Load(), final.Subtasks[0].Attempts)
	}
	assertDispatchCapacityReleased(t, srv, testCapacityGate(t, srv), "node-a")
}

func TestConcurrentResumeHTTPDispatchesPendingWorkOnce(t *testing.T) {
	var mu sync.Mutex
	calls := make(map[string]int)
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := decodeExecuteRequest(t, r)
		mu.Lock()
		calls[request.SubtaskID]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"output": request.SubtaskID + " output"})
	}))
	defer worker.Close()

	srv := newTestServer(t)
	node := characterizationNode("node-a", worker.URL, "general", 4)
	srv.registry.Upsert(node)
	task, err := srv.tasks.CreateChecked("concurrent resume", []scheduler.Assignment{
		characterizationAssignment("kept", node),
		characterizationAssignment("retry", node, "kept"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.tasks.CompleteSubtaskChecked(task.ID, "kept", "kept output", nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.tasks.CompleteSubtaskChecked(task.ID, "retry", "", errors.New("transient")); err != nil {
		t.Fatal(err)
	}

	const callers = 6
	codes := make(chan int, callers)
	var start sync.WaitGroup
	start.Add(1)
	mux := srv.Mux()
	for range callers {
		go func() {
			start.Wait()
			codes <- serveContractRequest(mux, http.MethodPost, "/api/tasks/"+task.ID+"/resume", "").Code
		}()
	}
	start.Done()
	accepted := 0
	for range callers {
		switch code := <-codes; code {
		case http.StatusOK:
			accepted++
		case http.StatusConflict:
		default:
			t.Fatalf("concurrent resume returned %d", code)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted resumes = %d, want 1", accepted)
	}
	final := waitForCompletedTask(t, srv, task.ID)
	mu.Lock()
	defer mu.Unlock()
	if calls["retry"] != 1 || calls["kept"] != 0 {
		t.Fatalf("worker calls after concurrent resume = %v, want one retry dispatch only", calls)
	}
	if final.Subtasks[0].Output != "kept output" || final.Subtasks[1].Output != "retry output" {
		t.Fatalf("resumed outputs = %q, %q", final.Subtasks[0].Output, final.Subtasks[1].Output)
	}
}
