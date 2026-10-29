package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func TestModelCandidatesRespectNodeMemoryAndSubtaskCapabilities(t *testing.T) {
	node := cluster.NodeStatus{
		ID: "worker", Address: "http://worker", Healthy: true,
		Hardware: hardware.Info{TotalRAMMB: 4 * 1024, MemoryStatsAvailable: true, MemoryUsage: 10},
		Engines:  []engine.Status{{Name: "ollama", Ready: true}},
	}
	subtask := scheduler.Subtask{
		ID: "code", TaskType: "code", RequiredCapabilities: []string{"coder"},
	}

	candidates := modelCandidates(subtask, []cluster.NodeStatus{node})
	if len(candidates) == 0 {
		t.Fatal("expected at least one fitting code model")
	}
	for _, candidate := range candidates {
		if candidate.model.SizeGB*modelMemoryReserve*1024 > 4*1024*0.9 {
			t.Errorf("candidate %q exceeds available memory", candidate.model.Name)
		}
		candidateNode := node
		candidateNode.Models = []engine.Model{candidate.model}
		if _, err := scheduler.Assign([]scheduler.Subtask{subtask}, []cluster.NodeStatus{candidateNode}); err != nil {
			t.Errorf("candidate %q is not compatible with the requested task: %v", candidate.model.Name, err)
		}
	}

	node.Hardware.TotalRAMMB = 512
	if got := modelCandidates(subtask, []cluster.NodeStatus{node}); len(got) != 0 {
		t.Fatalf("models were offered to an undersized node: %+v", got)
	}
}

func TestModelCandidatesTreatTextRolesAsAdvisory(t *testing.T) {
	node := cluster.NodeStatus{
		ID: "worker", Address: "http://worker", Healthy: true,
		Hardware: hardware.Info{TotalRAMMB: 4 * 1024},
		Engines:  []engine.Status{{Name: "ollama", Ready: true}},
	}
	for _, capability := range []string{"researcher", "translator", "editor"} {
		t.Run(capability, func(t *testing.T) {
			candidates := modelCandidates(scheduler.Subtask{
				ID: "text", TaskType: "general", RequiredCapabilities: []string{capability},
			}, []cluster.NodeStatus{node})
			if len(candidates) == 0 {
				t.Fatalf("no general Ollama model considered for advisory %q capability", capability)
			}
			for _, candidate := range candidates {
				if candidate.model.Engine != "ollama" {
					t.Errorf("candidate engine = %q, want ollama", candidate.model.Engine)
				}
			}
		})
	}
}

func TestAutomaticModelProvisioningPullsAndRefreshesBeforeTaskAssignment(t *testing.T) {
	var mu sync.Mutex
	var installed []engine.Model
	var pullCount atomic.Int32
	var nodeAddress string
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/models/pull":
			var request struct {
				Model  string `json:"model"`
				Engine string `json:"engine"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var tags []string
			for _, catalogModel := range engine.ModelCatalog("ollama", "") {
				if strings.EqualFold(catalogModel.Name, request.Model) {
					tags = append([]string(nil), catalogModel.Tags...)
					break
				}
			}
			codeCapable := false
			for _, tag := range tags {
				if tag == "code" {
					codeCapable = true
				}
			}
			if request.Engine != "ollama" || !codeCapable {
				http.Error(w, "unexpected model pull", http.StatusBadRequest)
				return
			}
			mu.Lock()
			installed = append(installed, engine.Model{
				Name: request.Model, Engine: request.Engine, Tags: tags,
			})
			mu.Unlock()
			pullCount.Add(1)
			w.WriteHeader(http.StatusOK)
		case "/status":
			mu.Lock()
			models := append([]engine.Model(nil), installed...)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(cluster.NodeStatus{
				ID: "provisioning-worker", Address: nodeAddress,
				Hardware: hardware.Info{TotalRAMMB: 4 * 1024, MemoryStatsAvailable: true, MemoryUsage: 10},
				Engines:  []engine.Status{{Name: "ollama", Ready: true}},
				Models:   models,
			})
		case "/execute":
			_ = json.NewEncoder(w).Encode(map[string]string{"output": "done"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer worker.Close()
	nodeAddress = worker.URL

	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "provisioning-worker", Address: worker.URL, Healthy: true,
		Hardware: hardware.Info{TotalRAMMB: 4 * 1024, MemoryStatsAvailable: true, MemoryUsage: 10},
		Engines:  []engine.Status{{Name: "ollama", Ready: true}},
	})

	_, err := srv.createTask(context.Background(), createTaskRequest{
		Description: "Write code",
		Subtasks: []scheduler.Subtask{{
			ID: "code", Description: "Write code", TaskType: "code",
			RequiredCapabilities: []string{"coder"},
		}},
	})
	if err != nil {
		t.Fatalf("create task after provisioning: %v", err)
	}
	if pullCount.Load() != 1 {
		t.Fatalf("automatic model pulls = %d, want 1", pullCount.Load())
	}
	status, ok := srv.registry.Get("provisioning-worker")
	if !ok || len(status.Models) != 1 {
		t.Fatalf("node inventory was not refreshed after pull: %+v", status)
	}
	assignments, err := scheduler.Assign([]scheduler.Subtask{{
		ID: "code", Description: "Write code", TaskType: "code",
		RequiredCapabilities: []string{"coder"},
	}}, []cluster.NodeStatus{status})
	if err != nil || len(assignments) != 1 {
		t.Fatalf("task was not assignable after model pull: assignments=%+v err=%v", assignments, err)
	}
}

func TestEstimatedModelSizeUsesParameterCount(t *testing.T) {
	size, ok := estimatedModelSizeGB("qwen3:30b-a3b")
	if !ok || size < 19.7 || size > 19.9 {
		t.Fatalf("estimated size = %.2f GB, ok=%v; want about 19.8 GB", size, ok)
	}
	if _, ok := estimatedModelSizeGB("unknown-model"); ok {
		t.Fatal("unknown model size was treated as assessable")
	}
	if size, ok := estimatedModelSizeGB("phi4-mini"); !ok || size != 2.8 {
		t.Fatalf("phi4-mini estimated size = %.1f GB, ok=%v", size, ok)
	}
}
