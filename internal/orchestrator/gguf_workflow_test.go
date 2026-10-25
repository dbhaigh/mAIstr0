package orchestrator

import (
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

type planningModelAcquirer func(context.Context, []scheduler.Subtask) error

func (f planningModelAcquirer) provision(ctx context.Context, tasks []scheduler.Subtask) error {
	return f(ctx, tasks)
}

func TestWorkflowAcquiresInitialPlanningModelAndRunsCreativeShardTasks(t *testing.T) {
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/proxy/v1/chat/completions") {
			t.Errorf("unexpected standalone call %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		var request struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		output := "Five spring haiku completed"
		if len(request.Messages) > 0 && strings.Contains(request.Messages[0].Content, "mAIstr0 workflow planner") {
			tasks := make([]scheduler.Subtask, 5)
			for i := range tasks {
				tasks[i] = scheduler.Subtask{ID: string(rune('a' + i)), Description: "Write one haiku about girls and spring", TaskType: "creative", Role: "writer", ExpectedOutput: "One haiku", RequiredCapabilities: []string{"writer"}}
			}
			data, err := json.Marshal(map[string]any{"subtasks": tasks})
			if err != nil {
				t.Error(err)
			}
			output = string(data)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": output}}}})
	}))
	defer worker.Close()
	server := newTestServer(t)
	var acquisitions atomic.Int32
	server.planner.provisioner = planningModelAcquirer(func(ctx context.Context, tasks []scheduler.Subtask) error {
		acquisitions.Add(1)
		if len(tasks) != 1 || tasks[0].ID != "plan" {
			t.Errorf("unexpected acquisition tasks: %+v", tasks)
		}
		addTaskTestShardGroup(server, worker.URL)
		node, _ := server.registry.Get("head")
		node.Capabilities = []string{"gpu"}
		node.Engines = []engine.Status{{Name: "standalone", Ready: false}}
		server.registry.Upsert(node)
		return nil
	})
	// Registration has not yet loaded any model; acquisition must bootstrap planning.
	server.registry.Upsert(cluster.NodeStatus{ID: "head", Address: worker.URL, Healthy: true})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := server.createTask(ctx, createTaskRequest{Description: "generate 5 haiku about girls and spring", PlanMode: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	done, err := server.tasks.Wait(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != taskmgr.StatusCompleted || len(done.Subtasks) != 5 || done.Result != "Five spring haiku completed" || acquisitions.Load() != 1 || calls.Load() != 7 {
		t.Fatalf("workflow=%+v acquisitions=%d calls=%d", done, acquisitions.Load(), calls.Load())
	}
}
