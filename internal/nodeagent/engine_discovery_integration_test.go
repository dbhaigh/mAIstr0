package nodeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type nodeAgentFakeInventory struct {
	Models      []string `json:"models"`
	Unavailable bool     `json:"unavailable,omitempty"`
}

type nodeAgentProcessEngine struct {
	name         string
	backend      *engine.LMStudio
	capabilities engine.Capabilities
}

func (e *nodeAgentProcessEngine) Name() string { return e.name }

func (e *nodeAgentProcessEngine) Available() bool { return e.backend.Available() }

func (e *nodeAgentProcessEngine) URL() string { return e.backend.URL() }

func (e *nodeAgentProcessEngine) ListModels() ([]engine.Model, error) {
	return e.backend.ListModels()
}

func (e *nodeAgentProcessEngine) Generate(model, prompt string) (string, error) {
	return e.backend.Generate(model, prompt)
}

func (e *nodeAgentProcessEngine) Chat(model string, messages []engine.ChatMessage) (string, error) {
	return e.backend.Chat(model, messages)
}

func (e *nodeAgentProcessEngine) Capabilities() engine.Capabilities {
	return engine.CloneCapabilities(e.capabilities)
}

func TestEngineDiscoveryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and manages the fake engine process")
	}

	binary := buildNodeAgentFakeEngine(t)
	tempDir := t.TempDir()
	inventoryPath := filepath.Join(tempDir, "inventory.json")
	pidPath := filepath.Join(tempDir, "fake-engine.pid")
	writeNodeAgentFakeInventory(t, inventoryPath, nodeAgentFakeInventory{
		Models: []string{"shared-model", "old-model"},
	})

	port := reserveNodeAgentPort(t)
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "http")
	t.Setenv("MAISTR0_FAKE_ENGINE_MODELS_FILE", inventoryPath)
	t.Setenv("MAISTR0_FAKE_ENGINE_PID_FILE", pidPath)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	backends := []engine.Engine{
		&nodeAgentProcessEngine{
			name:         "lmstudio",
			backend:      engine.NewLMStudio(baseURL),
			capabilities: engine.Capabilities{Devices: []string{"cpu"}},
		},
		&nodeAgentProcessEngine{
			name:         "llamacpp",
			backend:      engine.NewLMStudio(baseURL),
			capabilities: engine.Capabilities{Devices: []string{"gpu"}},
		},
	}
	var detectorCalls atomic.Int32
	detector := func() []engine.Engine {
		detectorCalls.Add(1)
		found := make([]engine.Engine, 0, len(backends))
		for _, backend := range backends {
			if backend.Available() {
				found = append(found, backend)
			}
		}
		return found
	}

	cfg := config.DefaultNodeConfig()
	cfg.NodeID = "engine-discovery-integration"
	cfg.MemoryPath = filepath.Join(tempDir, "node.db")
	cfg.AdvertiseAddr = "https://127.0.0.1:7451"
	cfg.DiscoveryEnabled = false
	cfg.DefaultModel = "shared-model"
	cfg.ManagedEngineBinaryAllowlist = []string{filepath.Dir(binary)}
	cfg.ManagedEngines = []config.ManagedEngineConfig{{
		ID:         "engine-discovery-fixture",
		Kind:       config.ManagedEngineLlamaServer,
		BinaryPath: binary,
		ModelID:    "fixture-model",
		Options: config.ManagedEngineOptions{
			LlamaServer: &config.LlamaServerOptions{Host: "127.0.0.1", Port: port},
		},
	}}
	agent := mustNewTestAgentWithEngineDetector(t, cfg, taskmgr.NewManager(), detector)
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := agent.Close(); err != nil {
				t.Errorf("close agent: %v", err)
			}
		}
	})

	startCtx, cancelStart := context.WithTimeout(context.Background(), 10*time.Second)
	err := agent.managedEngines.Start(startCtx, "engine-discovery-fixture", nil)
	cancelStart()
	if err != nil {
		t.Fatalf("start fake engine: %v", err)
	}
	processStatus, statusErr := agent.managedEngines.Status("engine-discovery-fixture")
	if statusErr != nil || processStatus.State != "ready" || processStatus.PID <= 0 {
		t.Fatalf("managed engine status = %+v, want ready with a PID", processStatus)
	}
	pidData, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatalf("read fake engine PID file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil || pid != processStatus.PID {
		t.Fatalf("fake engine PID file = %q, manager PID = %d, parse error = %v", pidData, processStatus.PID, err)
	}
	if detectorCalls.Load() != 0 {
		t.Fatalf("detector calls before model request = %d, want deferred discovery", detectorCalls.Load())
	}

	setNodeAgentHardware(agent, hardware.Device{Kind: "cpu"})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /models/all", agent.handleModelsAllGet)
	server := httptest.NewServer(mux)
	defer server.Close()

	initial := requestNodeAgentModelStates(t, server.Client(), server.URL)
	assertNodeAgentInventory(t, initial, "shared-model", "old-model")
	if detectorCalls.Load() != 1 {
		t.Fatalf("detector calls after first model request = %d, want 1", detectorCalls.Load())
	}
	assertNodeAgentSelectedEngine(t, agent, "shared-model", "lmstudio")

	setNodeAgentHardware(agent, hardware.Device{Kind: "gpu"})
	agent.rescanDetectedEngines()
	assertNodeAgentSelectedEngine(t, agent, "shared-model", "llamacpp")

	writeNodeAgentFakeInventory(t, inventoryPath, nodeAgentFakeInventory{
		Models: []string{"shared-model", "new-model"},
	})
	agent.rescanDetectedEngines()
	updated := requestNodeAgentModelStates(t, server.Client(), server.URL)
	assertNodeAgentInventory(t, updated, "shared-model", "new-model")

	writeNodeAgentFakeInventory(t, inventoryPath, nodeAgentFakeInventory{Unavailable: true})
	expireNodeAgentEngineScan(agent)
	agent.refreshDetectedEngines()
	agent.engineComponent.refreshReportedModels()
	statuses := agent.reportedEngineList()
	if len(statuses) != 2 {
		t.Fatalf("engine statuses during inventory failure = %d, want 2", len(statuses))
	}
	for _, status := range statuses {
		if status.Ready || status.InventoryState != engine.ModelInventoryError {
			t.Errorf("engine status during inventory failure = %+v, want unavailable inventory", status)
		}
	}
	assertNodeAgentInventory(t, agent.engineComponent.reportedAllModelList(), "shared-model", "new-model")
	if selected := agent.engineForModel("shared-model", ""); selected != nil {
		t.Fatalf("selected engine during inventory failure = %s, want none", selected.Name())
	}

	writeNodeAgentFakeInventory(t, inventoryPath, nodeAgentFakeInventory{
		Models: []string{"shared-model", "recovered-model"},
	})
	expireNodeAgentEngineScan(agent)
	agent.refreshDetectedEngines()
	agent.engineComponent.refreshReportedModels()
	recovered := requestNodeAgentModelStates(t, server.Client(), server.URL)
	assertNodeAgentInventory(t, recovered, "shared-model", "recovered-model")
	assertNodeAgentSelectedEngine(t, agent, "shared-model", "llamacpp")

	refreshStarted := make(chan struct{})
	refreshStopped := make(chan struct{})
	agent.orchestratorLink.background.Go(func(ctx context.Context) {
		close(refreshStarted)
		defer close(refreshStopped)
		agent.engineComponent.modelRefreshLoopContext(ctx)
	})
	<-refreshStarted
	if err := agent.Close(); err != nil {
		closed = true
		t.Fatalf("close agent: %v", err)
	}
	closed = true
	select {
	case <-refreshStopped:
	default:
		t.Fatal("model refresh loop was not joined during agent shutdown")
	}
	processStatus, statusErr = agent.managedEngines.Status("engine-discovery-fixture")
	if statusErr != nil || processStatus.State != "stopped" || processStatus.PID != 0 {
		t.Fatalf("managed engine status after shutdown = %+v, want stopped with no PID", processStatus)
	}
	if backends[0].Available() || backends[1].Available() {
		t.Fatal("fake engine remained available after agent shutdown")
	}
}

func buildNodeAgentFakeEngine(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source path")
	}
	binary := filepath.Join(t.TempDir(), "fakeengine")
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("locate go command: %v", err)
	}
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	command := exec.Command(goBinary, "build", "-o", binary, "./supervisor/testdata/fakeengine")
	command.Dir = filepath.Dir(currentFile)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build fake engine fixture: %v\n%s", err, output)
	}
	return binary
}

func reserveNodeAgentPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve fake engine port: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close reserved fake engine port: %v", err)
		}
	}()
	return listener.Addr().(*net.TCPAddr).Port
}

func writeNodeAgentFakeInventory(t *testing.T, path string, inventory nodeAgentFakeInventory) {
	t.Helper()
	data, err := json.Marshal(inventory)
	if err != nil {
		t.Fatalf("marshal fake engine inventory: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fake engine inventory: %v", err)
	}
}

func setNodeAgentHardware(agent *Agent, devices ...hardware.Device) {
	agent.telemetryComponent.hwMu.Lock()
	agent.telemetryComponent.hw = hardware.Info{OS: runtime.GOOS, Devices: devices}
	agent.telemetryComponent.hwGeneration++
	agent.telemetryComponent.hwMu.Unlock()
}

func requestNodeAgentModelStates(t *testing.T, client *http.Client, serverURL string) []ModelState {
	t.Helper()
	response, err := client.Get(serverURL + "/models/all")
	if err != nil {
		t.Fatalf("request model states: %v", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close model states response: %v", err)
		}
	}()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("model states status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var states []ModelState
	if err := json.NewDecoder(response.Body).Decode(&states); err != nil {
		t.Fatalf("decode model states: %v", err)
	}
	return states
}

func assertNodeAgentInventory(t *testing.T, states []ModelState, modelNames ...string) {
	t.Helper()
	if len(states) != 2*len(modelNames) {
		t.Fatalf("model state count = %d, want %d: %+v", len(states), 2*len(modelNames), states)
	}
	seen := make(map[string]bool, len(states))
	for _, state := range states {
		seen[state.Engine+"/"+state.Name] = true
		if state.Name == "shared-model" && (!state.Enabled || !state.Default) {
			t.Errorf("shared model state = %+v, want enabled and default", state)
		}
	}
	for _, engineName := range []string{"lmstudio", "llamacpp"} {
		for _, modelName := range modelNames {
			if !seen[engineName+"/"+modelName] {
				t.Errorf("missing model state %s/%s in %+v", engineName, modelName, states)
			}
		}
	}
}

func assertNodeAgentSelectedEngine(t *testing.T, agent *Agent, modelName, wantEngine string) {
	t.Helper()
	selected := agent.engineForModel(modelName, "")
	if selected == nil || selected.Name() != wantEngine {
		if selected == nil {
			t.Fatalf("selected engine for %s = nil, want %s", modelName, wantEngine)
		}
		t.Fatalf("selected engine for %s = %s, want %s", modelName, selected.Name(), wantEngine)
	}
}

func expireNodeAgentEngineScan(agent *Agent) {
	agent.enginesMu.Lock()
	agent.lastEngineScan = time.Now().Add(-engineDetectionInterval)
	agent.enginesMu.Unlock()
}
