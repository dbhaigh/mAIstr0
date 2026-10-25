package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/shard"
)

func TestGGUFProvisioningLoadsLocalBeforeDownloading(t *testing.T) {
	for _, test := range []struct {
		name          string
		local, busy   bool
		wantDownloads int
		vision        bool
	}{
		{"local model", true, false, 0, false},
		{"download missing model", false, false, 1, false},
		{"never replace active model", false, true, 0, false},
		{"unsupported vision never downloads text weights", false, false, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			profile := testShardRequest().Model
			profile.Name = "qwen2.5-test"
			profile.RequestedContext = 4096
			profile.FileSizeBytes = 1 << 20
			model := supervisor.LocalHeadModel{ID: strings.Repeat("a", 64), Name: profile.Name, Source: "lmstudio", Profile: profile}
			downloads, configured, started := 0, 0, 0
			states := map[string]string{"maistr0-auto-rpc": "stopped", "maistr0-auto-head": "stopped"}
			if test.busy {
				states["maistr0-auto-head"] = "ready"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/engines/managed":
					_ = json.NewEncoder(w).Encode([]supervisor.Status{
						{ID: "maistr0-auto-rpc", Kind: "ggml-rpc-server", State: states["maistr0-auto-rpc"], RelayEndpoint: "192.0.2.1:7452"},
						{ID: "maistr0-auto-head", Kind: "llama-server", State: states["maistr0-auto-head"]},
					})
				case "/api/engines/managed/head-models":
					models := []supervisor.LocalHeadModel{}
					if test.local {
						models = append(models, model)
					}
					_ = json.NewEncoder(w).Encode(supervisor.LocalHeadModels{Models: models, Warnings: []string{}})
				case "/api/engines/managed/download-model":
					downloads++
					_ = json.NewEncoder(w).Encode(model)
				case "/api/engines/managed/install-worker":
					_ = json.NewEncoder(w).Encode(supervisor.WorkerInstallResult{Source: "official"})
				case "/api/engines/managed/configure-head":
					configured++
					_ = json.NewEncoder(w).Encode(supervisor.Status{ID: "maistr0-auto-head", Kind: "llama-server", State: "stopped"})
				case "/api/engines/managed/maistr0-auto-head/profile":
					_ = json.NewEncoder(w).Encode(profile)
				case "/api/engines/managed/maistr0-auto-rpc/start", "/api/engines/managed/maistr0-auto-head/start":
					started++
					id := "maistr0-auto-head"
					kind := "llama-server"
					if strings.Contains(r.URL.Path, "auto-rpc") {
						id = "maistr0-auto-rpc"
						kind = "ggml-rpc-server"
					}
					states[id] = "ready"
					_ = json.NewEncoder(w).Encode(supervisor.Status{ID: id, Kind: kind, State: "ready", RelayEndpoint: "192.0.2.1:7452"})
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			registry := cluster.NewRegistry()
			registry.Upsert(cluster.NodeStatus{ID: "worker", Address: server.URL, Healthy: true, TelemetryAt: time.Now(),
				Capabilities: []string{"gpu", "fast"}, Hardware: hardware.Info{OS: "linux", TotalRAMMB: 32 << 10, HasGPU: true,
					Devices: []hardware.Device{{Kind: "gpu", Vendor: "nvidia", Name: "test GPU", MemoryMB: 16 << 10, FreeMemoryMB: 16 << 10}},
				}})
			client := newNodeClient(nil, nil, nil)
			manager, err := NewShardManager(&testShardStore{groups: map[string][]byte{}}, registry, client, &testShardCapacity{}, shard.Plan)
			if err != nil {
				t.Fatal(err)
			}
			p := newAutoModelProvisioner(registry, client)
			p.shards = manager
			task := scheduler.Subtask{ID: "haiku", TaskType: "creative", RequiredCapabilities: []string{"writer"}}
			if test.vision {
				task.TaskType = "vision"
				task.RequiredCapabilities = []string{"vision"}
			}
			err = p.provisionGGUF(context.Background(), task)
			if test.busy || test.vision {
				if err == nil || started != 0 || configured != 0 || downloads != 0 {
					t.Fatalf("active model replaced: start=%d configure=%d download=%d error=%v", started, configured, downloads, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if downloads != test.wantDownloads || configured != 1 || started != 2 {
				t.Fatalf("downloads=%d configured=%d starts=%d", downloads, configured, started)
			}
			if err := p.provisionGGUF(context.Background(), task); err != nil {
				t.Fatal(err)
			}
			if configured != 1 || downloads != test.wantDownloads {
				t.Fatal("ready compatible model was provisioned again")
			}
		})
	}
}

func TestReadyShardAdvertisesTextCapabilitiesWithoutClaimingVision(t *testing.T) {
	node := cluster.NodeStatus{ID: "head", Address: "http://head", Healthy: true, Capabilities: []string{"gpu"}}
	group := ShardGroup{ID: "group", State: ShardGroupReady, Model: shard.ModelProfile{Name: "Llama-3.2-3B-Instruct"},
		Members: []ShardGroupMember{{NodeID: "head", Address: node.Address, Role: "head"}}}
	nodes := shardSchedulerNodes([]cluster.NodeStatus{node}, []ShardGroup{group})
	for _, capability := range []string{"writer", "summarizer", "translator"} {
		if _, err := scheduler.Assign([]scheduler.Subtask{{ID: "text", TaskType: "creative", RequiredCapabilities: []string{capability}}}, nodes); err != nil {
			t.Fatalf("%s rejected: %v", capability, err)
		}
	}
	if _, err := scheduler.Assign([]scheduler.Subtask{{ID: "image", TaskType: "vision", RequiredCapabilities: []string{"vision"}}}, nodes); err == nil {
		t.Fatal("text-only shard advertised vision")
	}
	if len(node.Capabilities) != 1 || len(node.Models) != 0 {
		t.Fatal("scheduler enrichment modified input node slices")
	}
}
