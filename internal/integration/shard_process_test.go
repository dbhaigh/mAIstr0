package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/orchestrator"
	"github.com/maistr0/maistr0/internal/shard"
)

func TestManagedShardProcessLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("fake-process lifecycle is covered by full qualification")
	}
	binaries := buildShardFakes(t)
	for _, mode := range []shard.Mode{shard.ModeLlamaCPPRPC, shard.ModeVLLMMP} {
		t.Run(string(mode), func(t *testing.T) {
			if mode == shard.ModeVLLMMP && runtime.GOOS != "linux" {
				t.Skip("managed vLLM distributed launch requires Linux; no native Windows support")
			}
			address := freeTCPAddress(t)
			process := startOrchestratorProcess(t, address, filepath.Join(t.TempDir(), "shards.db"))
			baseURL := "http://" + address
			client := &http.Client{Timeout: 15 * time.Second}
			nodes := newShardFixtureNodes(t, mode, binaries)
			// Stop the producer before fixture cleanup, including assertion failures.
			t.Cleanup(func() { process.stop(t) })
			nodeStatuses := make([]cluster.NodeStatus, len(nodes))
			for i, node := range nodes {
				nodeStatuses[i] = cluster.NodeStatus{
					ID: fmt.Sprintf("node-%d", i), Address: node.http.URL,
					Hardware: hardware.Info{
						OS: "linux", Devices: []hardware.Device{{
							Kind: "gpu", Vendor: "nvidia", Name: "fixture GPU", Index: 0,
							FreeMemoryMB: 8192,
						}},
						NetworkLinks: []hardware.NetworkLink{{Name: "fixture", Trusted: true, SpeedMbps: 10000}},
					},
					Engines: []engine.Status{{
						Name:  map[shard.Mode]string{shard.ModeLlamaCPPRPC: "llamacpp", shard.ModeVLLMMP: "vllm"}[mode],
						Ready: true, Capabilities: engine.Capabilities{
							Devices: []string{"gpu"}, SupportsSharding: []string{string(mode)}, OS: []string{"linux"},
						},
					}},
					MaxConcurrentTasks: 2, AvailableCapacity: 2, TelemetryAt: time.Now(),
				}
			}
			request := orchestrator.CreateShardGroupRequest{
				Mode: mode, PinnedNodes: []string{"node-0", "node-1"},
				Model: shard.ModelProfile{
					Name: "fake-sharded", Architecture: "llama", Parameters: 1_000_000,
					LayerCount: 8, HiddenSize: 128, KVHeads: 2, HeadDim: 64,
					VocabSize: 1024, Quantization: "f16", WeightsSizeBytes: 2_000_000, RequestedContext: 128,
				},
			}
			create := func() orchestrator.ShardGroup {
				t.Helper()
				for _, status := range nodeStatuses {
					status.TelemetryAt = time.Now()
					registerPipelineNode(t, client, baseURL, status)
				}
				var group orchestrator.ShardGroup
				shardJSON(t, client, http.MethodPost, baseURL+"/api/shards", request, http.StatusCreated, &group)
				if group.State != orchestrator.ShardGroupReady || len(group.Plan.Nodes) != 2 {
					t.Fatalf("created group = %+v, want ready on two nodes", group)
				}
				for _, member := range group.Members {
					if !member.Started {
						t.Fatalf("member not started: %+v", member)
					}
				}
				return group
			}
			group := create()
			assertShardChat(t, client, baseURL, http.StatusOK)
			for _, action := range []string{"stop", "start", "restart"} {
				var updated orchestrator.ShardGroup
				shardJSON(t, client, http.MethodPost, baseURL+"/api/shards/"+group.ID+"/"+action, nil, http.StatusOK, &updated)
				wantState := orchestrator.ShardGroupReady
				wantChat := http.StatusOK
				if action == "stop" {
					wantState, wantChat = orchestrator.ShardGroupStopped, http.StatusServiceUnavailable
					for _, node := range nodes {
						for _, status := range node.manager.Statuses() {
							if status.State != "stopped" || status.PID != 0 {
								t.Fatalf("stop left a child running: %+v", status)
							}
						}
					}
				}
				if updated.ID != group.ID || updated.State != wantState || !updated.CreatedAt.Equal(group.CreatedAt) {
					t.Fatalf("%s did not retain group: %+v", action, updated)
				}
				assertShardChat(t, client, baseURL, wantChat)
				group = updated
			}
			// Kill a real managed child without marking the node unhealthy. A long
			// restart backoff keeps the loss observable over both health sweeps.
			worker := group.Members[0]
			var statuses []struct {
				ID  string `json:"id"`
				PID int    `json:"pid"`
			}
			shardJSON(t, client, http.MethodGet, worker.Address+"/api/engines/managed", nil, http.StatusOK, &statuses)
			pid := 0
			for _, status := range statuses {
				if status.ID == worker.EngineID {
					pid = status.PID
				}
			}
			if pid <= 0 {
				t.Fatalf("worker %s has no running child: %+v", worker.EngineID, statuses)
			}
			child, err := os.FindProcess(pid)
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = child.Release()
			waitShardState(t, client, baseURL, group.ID, orchestrator.ShardGroupDegraded)
			assertShardChat(t, client, baseURL, http.StatusServiceUnavailable)
			waitShardState(t, client, baseURL, group.ID, orchestrator.ShardGroupFailed)
			assertShardChat(t, client, baseURL, http.StatusServiceUnavailable)
			for _, node := range nodes {
				for _, status := range node.manager.Statuses() {
					if status.State != "stopped" || status.PID != 0 {
						t.Fatalf("failed group left a child behind: %+v", status)
					}
				}
			}
			// Retain the failed group to prove model lookup selects the replacement.
			replacement := create()
			if replacement.ID == group.ID {
				t.Fatal("rebuild reused the failed group ID")
			}
			assertShardChat(t, client, baseURL, http.StatusOK)
			for _, id := range []string{replacement.ID, group.ID} {
				shardJSON(t, client, http.MethodDelete, baseURL+"/api/shards/"+id, nil, http.StatusNoContent, nil)
			}
			var list struct {
				Groups []orchestrator.ShardGroup `json:"groups"`
			}
			shardJSON(t, client, http.MethodGet, baseURL+"/api/shards", nil, http.StatusOK, &list)
			if len(list.Groups) != 0 {
				t.Fatalf("deleted groups remain: %+v", list.Groups)
			}
			process.stop(t)
		})
	}
}

func buildShardFakes(t *testing.T) map[string]string {
	t.Helper()
	binaries := make(map[string]string)
	for _, name := range []string{"fakerpc", "fakellama", "fakevllm"} {
		filename := name
		if runtime.GOOS == "windows" {
			filename += ".exe"
		}
		binary := filepath.Join(t.TempDir(), filename)
		cmd := exec.Command("go", "build", "-o", binary, "."+string(filepath.Separator)+filepath.Join("testdata", name))
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, output)
		}
		binaries[name] = binary
	}
	return binaries
}

func shardJSON(t *testing.T, client *http.Client, method, endpoint string, body any, want int, result any) {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, endpoint, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(response.Body)
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("%s %s: status %d, want %d: %s", method, endpoint, response.StatusCode, want, data)
	}
	if result != nil {
		if err := json.Unmarshal(data, result); err != nil {
			t.Fatalf("decode %s: %v: %s", endpoint, err, data)
		}
	}
}

func waitShardState(t *testing.T, client *http.Client, baseURL, id string, want orchestrator.ShardGroupState) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	poll := time.NewTicker(25 * time.Millisecond)
	defer poll.Stop()
	var group orchestrator.ShardGroup
	for time.Now().Before(deadline) {
		shardJSON(t, client, http.MethodGet, baseURL+"/api/shards/"+id, nil, http.StatusOK, &group)
		if group.State == want {
			return
		}
		if group.State == orchestrator.ShardGroupFailed && want != group.State {
			t.Fatalf("missed state %s: %+v", want, group)
		}
		<-poll.C
	}
	t.Fatalf("waiting for %s: %+v", want, group)
}

func assertShardChat(t *testing.T, client *http.Client, baseURL string, want int) {
	t.Helper()
	for _, stream := range []bool{false, true} {
		body := fmt.Sprintf(`{"model":"fake-sharded","stream":%t,"messages":[{"role":"user","content":"prove sharding"}]}`, stream)
		response, err := client.Post(baseURL+"/router/v1/chat/completions", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			t.Fatalf("chat stream=%t: %d, want %d: %s", stream, response.StatusCode, want, data)
		}
		if want == http.StatusOK {
			if response.Header.Get("X-Fake-Engine") == "" || !strings.Contains(string(data), "shard-process-ok") ||
				(stream && (!strings.Contains(string(data), "data: [DONE]") ||
					!strings.Contains(response.Header.Get("Content-Type"), "text/event-stream"))) {
				t.Fatalf("chat did not traverse fake head: headers=%v body=%s", response.Header, data)
			}
			if !stream {
				var completion struct {
					Choices []struct {
						Message struct {
							Role    string `json:"role"`
							Content string `json:"content"`
						} `json:"message"`
					} `json:"choices"`
				}
				if err := json.Unmarshal(data, &completion); err != nil {
					t.Fatalf("invalid chat response JSON: %v: %s", err, data)
				}
				if !strings.Contains(response.Header.Get("Content-Type"), "application/json") ||
					len(completion.Choices) != 1 || completion.Choices[0].Message.Role != "assistant" ||
					completion.Choices[0].Message.Content != "shard-process-ok" {
					t.Fatalf("invalid chat response shape: headers=%v body=%s", response.Header, data)
				}
			}
		}
	}
}
