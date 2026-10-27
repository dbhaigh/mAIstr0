package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestTaskHTTPAPIReturnsOrderedSimulatedSynthesis(t *testing.T) {
	simulated := engine.NewSimulated()
	models, err := simulated.ListModels()
	if err != nil {
		t.Fatal(err)
	}

	var synthesisMu sync.Mutex
	synthesisCalls := 0
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/execute" {
			http.NotFound(w, r)
			return
		}
		var request struct {
			SubtaskID string `json:"subtask_id"`
			Model     string `json:"model"`
			Prompt    string `json:"prompt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid simulated execution request", http.StatusBadRequest)
			return
		}
		generated, err := simulated.Generate(request.Model, request.Prompt)
		if err != nil {
			http.Error(w, "simulated generation failed", http.StatusBadGateway)
			return
		}

		output := generated
		switch request.SubtaskID {
		case "sub-a":
			output = "alpha-result\n" + generated
		case "sub-b":
			output = "beta-result\n" + generated
		case "synthesis":
			alpha := strings.Index(request.Prompt, "alpha-result")
			beta := strings.Index(request.Prompt, "beta-result")
			if alpha < 0 || beta < 0 || alpha >= beta {
				http.Error(w, "synthesis inputs were missing or out of order", http.StatusUnprocessableEntity)
				return
			}
			synthesisMu.Lock()
			synthesisCalls++
			synthesisMu.Unlock()
			output = "alpha-result\nbeta-result\n" + generated
		}
		if err := json.NewEncoder(w).Encode(map[string]string{"output": output}); err != nil {
			t.Errorf("encode simulated execution response: %v", err)
		}
	}))
	t.Cleanup(worker.Close)

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "simulated-worker", Address: worker.URL, Healthy: true,
		Models: models,
		Engines: []engine.Status{{
			Name: simulated.Name(), Ready: true, ModelCount: len(models),
		}},
		MaxConcurrentTasks: 4, AvailableCapacity: 4, LastSeen: time.Now(),
	})
	api := httptest.NewServer(srv.Mux())
	t.Cleanup(api.Close)

	body, err := json.Marshal(map[string]string{
		"description": "- Analyze alpha\n- Analyze beta",
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := api.Client().Post(api.URL+"/api/tasks", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close task submission response: %v", err)
		}
	}()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /api/tasks returned %d", response.StatusCode)
	}
	var accepted taskmgr.Task
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.ID == "" {
		t.Fatal("POST /api/tasks returned an empty task ID")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed, err := srv.tasks.Wait(ctx, accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != taskmgr.StatusCompleted {
		t.Fatalf("task status = %q, want completed: %s", completed.Status, completed.Error)
	}
	alpha := strings.Index(completed.Result, "alpha-result")
	beta := strings.Index(completed.Result, "beta-result")
	if strings.TrimSpace(completed.Result) == "" || alpha < 0 || beta < 0 || alpha >= beta {
		t.Fatalf("final result is empty or out of order: %q", completed.Result)
	}
	synthesisMu.Lock()
	gotSynthesisCalls := synthesisCalls
	synthesisMu.Unlock()
	if gotSynthesisCalls != 1 {
		t.Fatalf("simulated synthesis calls = %d, want 1", gotSynthesisCalls)
	}
}
