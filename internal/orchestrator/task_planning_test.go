package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestAutoPlanRunsRoleTasksWithValidationAndReview(t *testing.T) {
	var executeCalls atomic.Int32
	var receivedPrompt string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/generate":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"output": `{"subtasks":[{"id":"implementation","description":"Implement the requested change","role":"coder","task_type":"code","expected_output":"A JSON object with an answer field","output_format":"json","required_fields":["answer"]}]}`,
			})
		case "/execute":
			var request struct {
				Prompt string `json:"prompt"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode execute request: %v", err)
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			receivedPrompt = request.Prompt
			call := executeCalls.Add(1)
			output := `{"answer":"implemented"}`
			if strings.Contains(request.Prompt, "You are the reviewer agent") {
				output = `{"approved":true,"issues":[]}`
			} else if call == 1 {
				output = `{"other":"invalid first attempt"}`
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"output": output})
		default:
			http.NotFound(w, r)
		}
	}))
	defer worker.Close()

	srv := newTestServer(t)
	srv.tasks.SetFinalizer(func(_ context.Context, task *taskmgr.Task) (string, error) {
		if len(task.Subtasks) != 2 {
			t.Errorf("finalizer received %d subtasks, want work and review", len(task.Subtasks))
		}
		return "reviewed result", nil
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "workflow-worker", Address: worker.URL, Healthy: true,
		Models: []engine.Model{{Name: "code-model", Tags: []string{"code"}}},
	})

	task, err := srv.createTask(context.Background(), createTaskRequest{
		Description: "Implement and review the requested change",
		PlanMode:    "auto", RequireReview: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed, err := srv.tasks.Wait(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != taskmgr.StatusCompleted || completed.Result != "reviewed result" {
		t.Fatalf("workflow did not complete: %+v", completed)
	}
	if completed.Subtasks[0].Subtask.Role != "coder" || completed.Subtasks[0].Subtask.ExpectedOutput == "" {
		t.Fatalf("planner role/output contract was not preserved: %+v", completed.Subtasks[0].Subtask)
	}
	reviewer := completed.Subtasks[1]
	if reviewer.Subtask.Role != "reviewer" || len(reviewer.Subtask.DependsOn) != 1 || reviewer.Status != taskmgr.StatusCompleted {
		t.Fatalf("review task did not gate synthesis: %+v", reviewer)
	}
	if executeCalls.Load() != 3 {
		t.Fatalf("worker calls = %d, want failed validation, retry, reviewer", executeCalls.Load())
	}
	if !strings.Contains(receivedPrompt, "approved") || !strings.Contains(receivedPrompt, "You are the reviewer agent") {
		t.Fatalf("reviewer instructions missing from dispatched prompt: %s", receivedPrompt)
	}
}

func TestTaskApprovalEndpointResumesSynthesis(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "worker output"})
	}))
	defer worker.Close()
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "approval-worker", Address: worker.URL, Healthy: true,
		Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
	})
	requestBody := bytes.NewBufferString(`{"description":"Review this task","require_approval":true,"subtasks":[{"id":"work","description":"Do the work"}]}`)
	createdResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(createdResponse, httptest.NewRequest(http.MethodPost, "/api/tasks", requestBody))
	if createdResponse.Code != http.StatusAccepted {
		t.Fatalf("task create returned %d: %s", createdResponse.Code, createdResponse.Body.String())
	}
	var created taskmgr.Task
	if err := json.Unmarshal(createdResponse.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waiting, err := srv.tasks.Wait(ctx, created.ID)
	if err == nil || waiting.Status != taskmgr.StatusAwaitingApproval {
		t.Fatalf("task did not pause for approval: %+v, err=%v", waiting, err)
	}

	approvedResponse := httptest.NewRecorder()
	srv.Mux().ServeHTTP(approvedResponse, httptest.NewRequest(http.MethodPost, "/api/tasks/"+created.ID+"/approve", nil))
	if approvedResponse.Code != http.StatusOK {
		t.Fatalf("task approval returned %d: %s", approvedResponse.Code, approvedResponse.Body.String())
	}
	var approved taskmgr.Task
	if err := json.Unmarshal(approvedResponse.Body.Bytes(), &approved); err != nil {
		t.Fatal(err)
	}
	if approved.Status != taskmgr.StatusCompleted || approved.Result != "worker output" {
		t.Fatalf("approval did not finish task synthesis: %+v", approved)
	}
}

func TestBothAgentBackendsExposeDurableWorkflowTool(t *testing.T) {
	srv := newTestServer(t)
	for _, backendName := range []string{"deepseek", "pi"} {
		backend := srv.agents[backendName]
		found := false
		for _, tool := range backend.ListTools() {
			if tool.Name == "execute_workflow" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s backend is missing execute_workflow", backendName)
		}
	}
}

func TestReviewerMustExplicitlyApprove(t *testing.T) {
	reviewer, err := scheduler.ValidatePlan([]scheduler.Subtask{{
		ID: "review", Description: "Review the results", Role: "reviewer",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTaskOutput(reviewer[0], `{"approved":false,"issues":["missing source"]}`); err == nil || !strings.Contains(err.Error(), "missing source") {
		t.Fatalf("negative review was not surfaced: %v", err)
	}
	if err := validateTaskOutput(reviewer[0], `{"approved":true,"issues":[]}`); err != nil {
		t.Fatalf("positive review was rejected: %v", err)
	}
	if err := validateTaskOutput(reviewer[0], "```json\n{\"approved\":true,\"issues\":[]}\n```"); err != nil {
		t.Fatalf("fenced positive review was rejected: %v", err)
	}
	if got := normalizeTaskOutput(reviewer[0], `{{"approved":true,"issues":[]}}`); got != `{"approved":true,"issues":[]}` {
		t.Fatalf("redundant JSON braces were not normalized: %q", got)
	}
	if err := validateTaskOutput(reviewer[0], `{{"approved":true,"issues":[]}}`); err != nil {
		t.Fatalf("redundantly-braced positive review was rejected: %v", err)
	}
	if got := normalizeTaskOutput(reviewer[0], "```json\n{\"approved\":true,\"issues\":[]}\n```"); got != `{"approved":true,"issues":[]}` {
		t.Fatalf("fenced JSON output was not normalized: %q", got)
	}
}

func TestFencedJSONOutputAllowsDependentSubtaskToRun(t *testing.T) {
	var dependentPrompt atomic.Value
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			SubtaskID string `json:"subtask_id"`
			Prompt    string `json:"prompt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode execute request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		output := "```json\n{\"answer\":\"ready\"}\n```"
		if request.SubtaskID == "follow-up" {
			dependentPrompt.Store(request.Prompt)
			output = "follow-up completed"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": output})
	}))
	defer worker.Close()

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "workflow-worker", Address: worker.URL, Healthy: true,
		Models: []engine.Model{{Name: "general-model", Tags: []string{"general"}}},
	})
	task, err := srv.createTask(context.Background(), createTaskRequest{
		Description: "Run a coordinator step and then a dependent step",
		Subtasks: []scheduler.Subtask{
			{
				ID: "coordinator", Description: "Produce the coordinator result", Role: "general",
				TaskType: "general", OutputFormat: "json", RequiredFields: []string{"answer"},
			},
			{
				ID: "follow-up", Description: "Use the coordinator result", Role: "general",
				TaskType: "general", DependsOn: []string{"coordinator"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed, err := srv.tasks.Wait(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != taskmgr.StatusCompleted {
		t.Fatalf("workflow did not complete: %+v", completed)
	}
	if prompt, ok := dependentPrompt.Load().(string); !ok || !strings.Contains(prompt, "Prerequisite coordinator:\n{\"answer\":\"ready\"}") {
		t.Fatalf("dependent step did not receive normalized coordinator output: %q", prompt)
	}
}

func TestTaskGraphCreationAndResumeEndpoints(t *testing.T) {
	srv := newTestServer(t)
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "step output"})
	}))
	defer worker.Close()

	srv.registry.Upsert(cluster.NodeStatus{
		ID: "test-node", Address: worker.URL, Healthy: true,
		Capabilities: []string{"coder", "researcher"},
		Models:       []engine.Model{{Name: "test-model", Tags: []string{"code"}}},
	})

	// 1. Create a DAG task via POST /api/tasks
	graph := scheduler.NewTaskGraph()
	_ = graph.AddNode(scheduler.Subtask{ID: "n1", Description: "Step 1", Role: "coder", RequiredCapabilities: []string{"coder"}})
	_ = graph.AddNode(scheduler.Subtask{ID: "n2", Description: "Step 2", Role: "researcher", DependsOn: []string{"n1"}})
	_ = graph.AddEdge("n1", "n2")

	reqBody, _ := json.Marshal(map[string]any{
		"description": "Two-step DAG task",
		"graph":       graph,
	})

	createRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(createRec, httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(reqBody)))
	if createRec.Code != http.StatusAccepted {
		t.Fatalf("create task returned %d: %s", createRec.Code, createRec.Body.String())
	}

	var created taskmgr.Task
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatalf("expected non-empty task ID")
	}

	// 2. Query checkpoints
	cpRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(cpRec, httptest.NewRequest(http.MethodGet, "/api/tasks/"+created.ID+"/checkpoints", nil))
	if cpRec.Code != http.StatusOK {
		t.Fatalf("get checkpoints returned %d: %s", cpRec.Code, cpRec.Body.String())
	}
	var cps []taskmgr.Checkpoint
	if err := json.Unmarshal(cpRec.Body.Bytes(), &cps); err != nil {
		t.Fatal(err)
	}
	if len(cps) == 0 {
		t.Fatalf("expected at least one checkpoint, got %d", len(cps))
	}

	// 3. Complete step n1, then cancel
	srv.tasks.CompleteSubtask(created.ID, "n1", "n1 output", nil)
	cancelRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(cancelRec, httptest.NewRequest(http.MethodPost, "/api/tasks/"+created.ID+"/cancel", nil))
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("cancel returned %d: %s", cancelRec.Code, cancelRec.Body.String())
	}

	// 4. Query resumable tasks
	resumableRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(resumableRec, httptest.NewRequest(http.MethodGet, "/api/tasks/resumable", nil))
	if resumableRec.Code != http.StatusOK {
		t.Fatalf("get resumable returned %d: %s", resumableRec.Code, resumableRec.Body.String())
	}
	var resumable []*taskmgr.Task
	if err := json.Unmarshal(resumableRec.Body.Bytes(), &resumable); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range resumable {
		if r.ID == created.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected task %s to be listed as resumable, got: %+v", created.ID, resumable)
	}

	// 5. Resume task
	resumeRec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(resumeRec, httptest.NewRequest(http.MethodPost, "/api/tasks/"+created.ID+"/resume", nil))
	if resumeRec.Code != http.StatusOK {
		t.Fatalf("resume returned %d: %s", resumeRec.Code, resumeRec.Body.String())
	}
	var resumed taskmgr.Task
	if err := json.Unmarshal(resumeRec.Body.Bytes(), &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.Status != taskmgr.StatusRunning {
		t.Fatalf("expected resumed status running, got %s", resumed.Status)
	}
}

func TestCycleDetectionInCreateTask(t *testing.T) {
	srv := newTestServer(t)

	// Create cyclic graph: a -> b -> a
	graph := scheduler.NewTaskGraph()
	_ = graph.AddNode(scheduler.Subtask{ID: "a", Description: "A", DependsOn: []string{"b"}})
	_ = graph.AddNode(scheduler.Subtask{ID: "b", Description: "B", DependsOn: []string{"a"}})
	_ = graph.AddEdge("b", "a")
	_ = graph.AddEdge("a", "b")

	reqBody, _ := json.Marshal(map[string]any{
		"description": "Cyclic task",
		"graph":       graph,
	})

	rec := httptest.NewRecorder()
	srv.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(reqBody)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for cycle, got %d: %s", rec.Code, rec.Body.String())
	}
}
