package nodeagent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/peeridentity"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type blockingExecutionEngine struct {
	started chan struct{}
	release chan struct{}
}

func mustNewTestAgent(t *testing.T, cfg config.NodeConfig) *Agent {
	t.Helper()
	if cfg.MemoryPath == "" {
		cfg.MemoryPath = filepath.Join(t.TempDir(), "node.db")
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = "https://127.0.0.1:7451"
	}
	agent := mustNewTestAgentWithEngineDetector(t, cfg, taskmgr.NewManager(), func() []engine.Engine { return nil })
	t.Cleanup(func() { _ = agent.Close() })
	return agent
}

func mustNewTestAgentWithEngineDetector(t *testing.T, cfg config.NodeConfig, tasks *taskmgr.Manager, detector func() []engine.Engine) *Agent {
	t.Helper()
	// Inventory probing belongs to hardware tests, not these agent fixtures.
	t.Setenv("PATH", "")
	agent, err := newAgentWithEngineDetector(cfg, tasks, detector)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func TestNodeSyncConfigControlsPageLimitAndTimeout(t *testing.T) {
	cfg := config.DefaultNodeConfig()
	cfg.NodeID = "sync-config-test"
	cfg.MemoryPath = filepath.Join(t.TempDir(), "sync.db")
	cfg.SyncMaxBytes = 4096
	cfg.SyncTimeoutSeconds = 37
	agent := mustNewTestAgentWithEngineDetector(t, cfg, taskmgr.NewManager(), func() []engine.Engine { return nil })
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close agent: %v", err)
		}
	})
	if agent.orchestratorLink.syncer.maxSyncBytes != 4096 {
		t.Fatalf("sync page limit = %d, want 4096", agent.orchestratorLink.syncer.maxSyncBytes)
	}
	if agent.memorySyncClient.Timeout != 37*time.Second {
		t.Fatalf("memory sync timeout = %s, want 37s", agent.memorySyncClient.Timeout)
	}
}

func TestMemorySyncProxyUsesConfiguredBodyLimit(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "sync-proxy-body-limit-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
		SyncMaxBytes: 4,
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close agent: %v", err)
		}
	})
	upstreamReached := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamReached = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/api/memory/sync", strings.NewReader("12345"))
	response := httptest.NewRecorder()
	agent.peerComponent.proxy(response, request, upstream.URL, "/api/memory/sync")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("oversized sync proxy returned %d, want 400", response.Code)
	}
	if upstreamReached {
		t.Fatal("oversized sync request reached the upstream server")
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	_, err := New(config.NodeConfig{ListenAddr: "invalid"})
	if err == nil || !strings.Contains(err.Error(), "listen_addr") {
		t.Fatalf("New error = %v, want listen_addr validation error", err)
	}
}

func (e *blockingExecutionEngine) Name() string    { return "blocking" }
func (e *blockingExecutionEngine) Available() bool { return true }
func (e *blockingExecutionEngine) ListModels() ([]engine.Model, error) {
	return []engine.Model{{Name: "model"}}, nil
}
func (e *blockingExecutionEngine) Generate(string, string) (string, error) {
	return "done", nil
}
func (e *blockingExecutionEngine) Chat(string, []engine.ChatMessage) (string, error) {
	return "done", nil
}
func (e *blockingExecutionEngine) GenerateWithOptions(ctx context.Context, _, _ string, _ engine.GenerationOptions) (string, error) {
	e.started <- struct{}{}
	select {
	case <-e.release:
		return "done", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

type pullTestEngine struct {
	name  string
	pulls []string
}

func (e *pullTestEngine) Name() string                        { return e.name }
func (e *pullTestEngine) Available() bool                     { return true }
func (e *pullTestEngine) ListModels() ([]engine.Model, error) { return nil, nil }
func (e *pullTestEngine) Generate(string, string) (string, error) {
	return "", nil
}
func (e *pullTestEngine) Chat(string, []engine.ChatMessage) (string, error) {
	return "", nil
}
func (e *pullTestEngine) Pull(model string) error {
	e.pulls = append(e.pulls, model)
	return nil
}

func TestHandleExecuteEnforcesAdvertisedConcurrencyLimit(t *testing.T) {
	agent := mustNewTestAgentWithEngineDetector(t, config.NodeConfig{
		NodeID: "execution-limit-test", MaxConcurrentTasks: 1,
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}, nil, func() []engine.Engine { return nil })
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close test agent: %v", err)
		}
	})
	eng := &blockingExecutionEngine{started: make(chan struct{}, 2), release: make(chan struct{})}
	agent.replaceConfiguredEngine(eng)

	runRequest := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/execute", strings.NewReader(`{"subtask_id":"work","model":"model","prompt":"test"}`))
		agent.handleExecute(response, request)
		return response
	}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- runRequest() }()
	select {
	case <-eng.started:
	case <-time.After(time.Second):
		select {
		case response := <-firstDone:
			t.Fatalf("first execution returned before engine start: %d: %s", response.Code, response.Body.String())
		default:
			t.Fatal("first execution did not start")
		}
	}
	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { secondDone <- runRequest() }()
	select {
	case <-eng.started:
		t.Fatal("second execution started while node capacity was full")
	case <-time.After(30 * time.Millisecond):
	}
	status := agent.status()
	if status.ActiveTasks != 1 || status.AvailableCapacity != 0 || status.MaxConcurrentTasks != 1 {
		t.Fatalf("status does not advertise saturated capacity: %+v", status)
	}

	close(eng.release)
	for range 2 {
		select {
		case response := <-firstDone:
			if response.Code != http.StatusOK {
				t.Fatalf("first execution returned %d: %s", response.Code, response.Body.String())
			}
		case response := <-secondDone:
			if response.Code != http.StatusOK {
				t.Fatalf("second execution returned %d: %s", response.Code, response.Body.String())
			}
		case <-time.After(time.Second):
			t.Fatal("execution did not complete after capacity was released")
		}
	}
}

func TestHandlePropertiesLoadsAndSavesCompleteProperties(t *testing.T) {
	engineURL := "http://127.0.0.1:1235"
	agent := mustNewTestAgentWithEngineDetector(t, config.NodeConfig{
		NodeID:           "node-properties-test",
		ListenAddr:       ":7451",
		AdvertiseAddr:    "https://10.0.0.2:7451",
		OrchestratorAddr: "https://orchestrator:7450",
		DefaultModel:     "qwen3-4b",
		DisabledModels:   []string{"disabled-model"},
		Engines:          []config.EngineConfig{{Name: "lmstudio", URL: engineURL}},
		DiscoveryEnabled: true,
		TrayMode:         "hidden",
		MemoryPath:       filepath.Join(t.TempDir(), "node.db"),
	}, nil, func() []engine.Engine { return nil })
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close test agent: %v", err)
		}
	})

	getProperties := func() nodeProperties {
		response := httptest.NewRecorder()
		agent.handleProperties(response, httptest.NewRequest(http.MethodGet, "/properties", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET properties returned %d: %s", response.Code, response.Body.String())
		}
		var props nodeProperties
		if err := json.NewDecoder(response.Body).Decode(&props); err != nil {
			t.Fatal(err)
		}
		return props
	}

	props := getProperties()
	if props.ID != "node-properties-test" || props.ListenAddr != ":7451" ||
		props.AdvertiseAddr != "https://10.0.0.2:7451" ||
		props.OrchestratorAddr != "https://orchestrator:7450" ||
		props.DefaultModel != "qwen3-4b" || props.EngineName != "lmstudio" ||
		props.EngineURL != engineURL || !props.DiscoveryEnabled || props.TrayMode != "hidden" ||
		len(props.DisabledModels) != 1 || props.DisabledModels[0] != "disabled-model" {
		t.Fatalf("GET properties did not return the populated node configuration: %+v", props)
	}

	update := nodeProperties{
		AdvertiseAddr:    "https://10.0.0.3:7451",
		OrchestratorAddr: "https://new-orchestrator:7450",
		DefaultModel:     "new-model",
		DisabledModels:   []string{"old-model", "new-model"},
		EngineName:       "lmstudio",
		EngineURL:        engineURL,
		AutoOpenBrowser:  true,
		TrayMode:         "taskbar",
		MemoryPath:       "C:\\node\\memory.db",
	}
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	agent.handleProperties(response, httptest.NewRequest(http.MethodPatch, "/properties", bytes.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("PATCH properties returned %d: %s", response.Code, response.Body.String())
	}

	props = getProperties()
	if props.AdvertiseAddr != update.AdvertiseAddr ||
		props.OrchestratorAddr != update.OrchestratorAddr ||
		props.DefaultModel != update.DefaultModel ||
		props.EngineName != update.EngineName || props.EngineURL != update.EngineURL ||
		!props.AutoOpenBrowser || props.TrayMode != update.TrayMode ||
		props.MemoryPath != update.MemoryPath || len(props.DisabledModels) != 2 {
		t.Fatalf("saved node properties were not retained: %+v", props)
	}
}

type capabilityTestEngine struct {
	engine.Engine
	capabilities engine.Capabilities
}

func (e capabilityTestEngine) Capabilities() engine.Capabilities { return e.capabilities }

func TestPrioritizeEnginesByCapabilitiesAndReadiness(t *testing.T) {
	engines := []engine.Engine{
		capabilityTestEngine{
			Engine:       &pullTestEngine{name: "cpu"},
			capabilities: engine.Capabilities{Devices: []string{"cpu"}},
		},
		capabilityTestEngine{
			Engine:       &pullTestEngine{name: "gpu"},
			capabilities: engine.Capabilities{},
		},
		capabilityTestEngine{
			Engine:       &pullTestEngine{name: "npu"},
			capabilities: engine.Capabilities{},
		},
	}
	tests := []struct {
		name     string
		hw       hardware.Info
		statuses []engine.Status
		want     string
	}{
		{
			name: "best available device wins among ready engines",
			hw: hardware.Info{Devices: []hardware.Device{
				{Kind: "cpu"}, {Kind: "gpu", Backends: []string{"cuda"}}, {Kind: "npu", Backends: []string{"openvino"}},
			}},
			statuses: []engine.Status{
				{Name: "cpu", Ready: true, ModelCount: 1, Capabilities: engine.Capabilities{Devices: []string{"cpu"}}},
				{Name: "gpu", Ready: true, ModelCount: 1, Capabilities: engine.Capabilities{Devices: []string{"gpu"}, Backends: []string{"cuda"}}},
				{Name: "npu", Ready: true, ModelCount: 1, Capabilities: engine.Capabilities{Devices: []string{"npu"}, Backends: []string{"openvino"}}},
			},
			want: "npu,gpu,cpu",
		},
		{
			name: "ready model outranks unsupported unready accelerator",
			hw: hardware.Info{Devices: []hardware.Device{
				{Kind: "gpu", Backends: []string{"cuda"}}, {Kind: "npu", Backends: []string{"openvino"}},
			}},
			statuses: []engine.Status{
				{Name: "gpu", Ready: true, ModelCount: 1, Capabilities: engine.Capabilities{Devices: []string{"gpu"}, Backends: []string{"cuda"}}},
				{Name: "npu", Capabilities: engine.Capabilities{Devices: []string{"npu"}, Backends: []string{"openvino"}}},
			},
			want: "gpu,npu,cpu",
		},
		{
			name: "backend mismatch does not match device",
			hw:   hardware.Info{Devices: []hardware.Device{{Kind: "gpu", Backends: []string{"rocm"}}}},
			statuses: []engine.Status{
				{Name: "gpu", Ready: true, ModelCount: 1, Capabilities: engine.Capabilities{Devices: []string{"gpu"}, Backends: []string{"cuda"}}},
				{Name: "cpu", Ready: true, ModelCount: 1, Capabilities: engine.Capabilities{Devices: []string{"cpu"}}},
			},
			want: "cpu,gpu,npu",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ordered := prioritizeEnginesByCapabilities(engines, tt.hw, tt.statuses)
			var names []string
			for _, e := range ordered {
				names = append(names, e.Name())
			}
			if got := strings.Join(names, ","); got != tt.want {
				t.Fatalf("accelerated engine order = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNPUEngineCapabilitiesMatchHardwareVendor(t *testing.T) {
	tests := []struct {
		name    string
		vendor  string
		backend string
	}{
		{name: "Intel OpenVINO", vendor: "intel-npu", backend: "openvino"},
		{name: "AMD XDNA", vendor: "amd-npu", backend: "xdna"},
		{name: "Qualcomm Hexagon", vendor: "qualcomm-npu", backend: "hexagon"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capabilities := engine.Capabilities{
				Devices: []string{"npu"}, Backends: []string{test.backend},
				NPUVendors: []string{test.vendor},
			}
			hardwareInfo := hardware.Info{HasNPU: true, NPUVendor: test.vendor, Devices: []hardware.Device{
				{Kind: "npu", Vendor: test.vendor, Backends: []string{test.backend}},
			}}
			if got := engineDeviceMatchScore(capabilities, hardwareInfo); got != deviceKindPriority("npu") {
				t.Fatalf("matching NPU score = %d, want %d", got, deviceKindPriority("npu"))
			}
			mismatchedVendor := "qualcomm-npu"
			if test.vendor == mismatchedVendor {
				mismatchedVendor = "intel-npu"
			}
			hardwareInfo.NPUVendor = mismatchedVendor
			hardwareInfo.Devices[0].Vendor = mismatchedVendor
			if got := engineDeviceMatchScore(capabilities, hardwareInfo); got != 0 {
				t.Fatalf("mismatched NPU vendor score = %d, want 0", got)
			}
		})
	}
}

func TestValidatedRPCCapabilityRequiresOperatorOptIn(t *testing.T) {
	for _, test := range []struct {
		name string
		set  bool
		want bool
	}{
		{name: "default is disabled"},
		{name: "operator opt-in", set: true, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			component := &engineComponent{
				engines: []engine.Engine{
					capabilityTestEngine{
						Engine: &pullTestEngine{name: "llamacpp"},
						capabilities: engine.Capabilities{
							Devices: []string{"npu"}, SupportsSharding: []string{"llamacpp-rpc"},
						},
					},
				},
				telemetry: &telemetryComponent{},
				operatorCapabilities: map[string]engine.Capabilities{
					"llamacpp": {ValidatedRPC: test.set},
				},
			}
			_, statuses := component.inspectInventory()
			if len(statuses) != 1 || statuses[0].Capabilities.ValidatedRPC != test.want {
				t.Fatalf("engine status capabilities = %+v, want ValidatedRPC=%t", statuses, test.want)
			}
		})
	}
}

func TestRefreshDetectedEnginesFindsNewEngineAlongsideExistingOne(t *testing.T) {
	detected := []engine.Engine{&pullTestEngine{name: "ollama"}}
	agent := mustNewTestAgentWithEngineDetector(t, config.NodeConfig{
		NodeID: "engine-rescan-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}, nil, func() []engine.Engine {
		return append([]engine.Engine(nil), detected...)
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close test agent: %v", err)
		}
	})

	detected = append(detected, &pullTestEngine{name: "lmstudio"})
	agent.enginesMu.Lock()
	agent.lastEngineScan = time.Now().Add(-engineDetectionInterval)
	agent.enginesMu.Unlock()
	agent.refreshDetectedEngines()

	foundLMStudio := false
	for _, e := range agent.engineSnapshot() {
		if e.Name() == "lmstudio" {
			foundLMStudio = true
			break
		}
	}
	if !foundLMStudio {
		t.Fatal("LM Studio was not discovered while another engine was already available")
	}
}

func TestPairingConnectEndpointEnrollsNode(t *testing.T) {
	orchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-pair-test")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pairing/join" {
			t.Errorf("pairing path = %q", r.URL.Path)
		}
		var request nodePairingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Role != "node" || request.PIN != "123456" || request.ID != "node-pair-test" || request.CertificatePEM == "" {
			t.Errorf("unexpected pairing request: %#v", request)
		}
		_ = json.NewEncoder(w).Encode(nodePairingResponse{
			ID: orchestrator.ID(), Role: "orchestrator", CertificatePEM: orchestrator.CertificatePEM(),
		})
	}))
	server.TLS = orchestrator.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-pair-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	requestBody, _ := json.Marshal(map[string]string{"address": server.URL, "pin": "123456", "fingerprint": securityFingerprint(t, orchestrator)})
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/connect", bytes.NewReader(requestBody))
	response := httptest.NewRecorder()
	agent.handlePairingConnect(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("pairing returned %d: %s", response.Code, response.Body.String())
	}
	if !agent.identity.IsTrustedID(orchestrator.ID()) || agent.getOrchestratorAddr() != server.URL {
		t.Fatal("node did not trust and select the paired orchestrator")
	}
	status := httptest.NewRecorder()
	agent.handlePairingStatus(status, httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil))
	if status.Code != http.StatusOK ||
		!strings.Contains(status.Body.String(), `"trusted":true`) ||
		!strings.Contains(status.Body.String(), `"pairing_required":false`) {
		t.Fatalf("pairing status returned %d: %s", status.Code, status.Body.String())
	}
}

func TestFirstContactSameSubnetAutoPairsWithoutFingerprintOrPIN(t *testing.T) {
	orchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-auto-test")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pairing/join" {
			t.Errorf("pairing path = %q", r.URL.Path)
		}
		var request nodePairingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Role != "node" || request.ID != "node-auto-test" || request.CertificatePEM == "" {
			t.Errorf("unexpected pairing request: %#v", request)
		}
		// PIN is empty for auto-join on same subnet
		if request.PIN != "" {
			t.Errorf("expected empty PIN, got %q", request.PIN)
		}
		_ = json.NewEncoder(w).Encode(nodePairingResponse{
			ID: orchestrator.ID(), Role: "orchestrator", CertificatePEM: orchestrator.CertificatePEM(),
		})
	}))
	server.TLS = orchestrator.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-auto-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	agent.setOrchestratorAddr(server.URL)
	agent.setOrchestratorID(orchestrator.ID())
	requestBody, _ := json.Marshal(map[string]string{
		"address": server.URL, "pin": "123456", "orchestrator_id": orchestrator.ID(),
	})
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/connect", bytes.NewReader(requestBody))
	response := httptest.NewRecorder()
	agent.handlePairingConnect(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("same-subnet first contact returned %d: %s", response.Code, response.Body.String())
	}
	if !agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatal("same-subnet auto-pair did not trust the discovered orchestrator")
	}
}

func TestPairingFailureDoesNotLogOrReturnReflectedPIN(t *testing.T) {
	const pin = "654321"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid PIN "+pin, http.StatusUnauthorized)
	}))
	defer server.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "pairing-error-redaction-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close agent: %v", err)
		}
	})

	var logs bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previousOutput)

	err := agent.orchestratorLink.pairWithAddress(server.URL, "", pin)
	if err == nil {
		t.Fatal("pairing unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), pin) {
		t.Fatalf("pairing error leaked PIN %q: %v", pin, err)
	}
	if strings.Contains(logs.String(), pin) {
		t.Fatalf("pairing log leaked PIN %q: %s", pin, logs.String())
	}
}

func TestPairingConnectRejectsStaleSelectedOrchestrator(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-stale-orchestrator", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	agent.setOrchestratorAddr("https://127.0.0.1:7450")
	agent.setOrchestratorID("visible-orchestrator")
	body, err := json.Marshal(map[string]string{
		"address": "https://127.0.0.1:7450", "orchestrator_id": "stale-orchestrator",
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	agent.handlePairingConnect(response, httptest.NewRequest(http.MethodPost, "/api/pairing/connect", bytes.NewReader(body)))
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "no longer discovered") {
		t.Fatalf("stale selected orchestrator returned %d: %s", response.Code, response.Body.String())
	}
}

func TestPairingStatusRequestsPINForUntrustedOrchestrator(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-awaiting-pairing", OrchestratorAddr: "https://orchestrator.local:7450",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}

	response := httptest.NewRecorder()
	agent.handlePairingStatus(response, httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"pairing_required":true`) {
		t.Fatalf("pairing status returned %d: %s", response.Code, response.Body.String())
	}
}

func TestPairingStatusOffersSameSubnetAutoJoinForDiscoveredOrchestrator(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-same-subnet", OrchestratorAddr: "https://localhost:7450",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}

	response := httptest.NewRecorder()
	agent.handlePairingStatus(response, httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil))
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"auto_join_available":true`) {
		t.Fatalf("pairing status omitted same-subnet auto-join: %d: %s", response.Code, response.Body.String())
	}
}

func TestFormerOrchestratorWorkerJoinsNewLeader(t *testing.T) {
	oldLeader, err := peeridentity.Open(filepath.Join(t.TempDir(), "old.json"), "old-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	newLeader, err := peeridentity.Open(filepath.Join(t.TempDir(), "new.json"), "new-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "former-orchestrator-worker", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(agent.Memory())
	if err := agent.PairLocalOrchestrator(oldLeader.ID(), oldLeader.CertificatePEM(), "https://127.0.0.1:7450"); err != nil {
		t.Fatal(err)
	}
	localCandidates := agent.orchestratorCandidates()
	if len(localCandidates) != 1 || localCandidates[0].id != oldLeader.ID() ||
		localCandidates[0].score != cluster.SelfScore(agent.hardwareSnapshot()) {
		t.Fatalf("co-located coordinator candidate was not ranked by its capability: %#v", localCandidates)
	}
	agent.markJoined()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pairing/join", func(w http.ResponseWriter, r *http.Request) {
		var request nodePairingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if request.PIN != "123456" || request.ID != agent.cfg.NodeID || request.Role != "node" {
			http.Error(w, "pairing rejected", http.StatusUnauthorized)
			return
		}
		if err := newLeader.TrustPeer(request.ID, request.CertificatePEM); err != nil {
			t.Error(err)
			http.Error(w, "invalid certificate", http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, nodePairingResponse{
			ID: newLeader.ID(), Role: "orchestrator", CertificatePEM: newLeader.CertificatePEM(),
		})
	})
	registered := false
	mux.Handle("POST /api/nodes/register", newLeader.RequireTrustedPeer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var node cluster.NodeStatus
		if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
			t.Error(err)
			http.Error(w, "invalid node", http.StatusBadRequest)
			return
		}
		registered = node.ID == agent.cfg.NodeID
		w.WriteHeader(http.StatusNoContent)
	})))
	server := httptest.NewUnstartedServer(mux)
	server.TLS = newLeader.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	agent.joinedMu.Lock()
	agent.joined = false
	agent.joinedMu.Unlock()
	agent.setOrchestratorAddr(server.URL)
	agent.setOrchestratorID(newLeader.ID())
	response := httptest.NewRecorder()
	agent.handlePairingStatus(response, httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil))
	if !strings.Contains(response.Body.String(), `"pairing_required":true`) ||
		!strings.Contains(response.Body.String(), `"trusted":false`) {
		t.Fatalf("former local pairing hid the new leader's pairing requirement: %s", response.Body.String())
	}
	body, err := json.Marshal(map[string]string{"address": server.URL, "pin": "123456", "fingerprint": securityFingerprint(t, newLeader)})
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	agent.handlePairingConnect(response, httptest.NewRequest(http.MethodPost, "/api/pairing/connect", bytes.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("former orchestrator worker could not pair: %d: %s", response.Code, response.Body.String())
	}
	status, err := json.Marshal(agent.status())
	if err != nil {
		t.Fatal(err)
	}
	registration, err := agent.peerClient.Post(server.URL+"/api/nodes/register", "application/json", bytes.NewReader(status))
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(registration.Body)
	if registration.StatusCode != http.StatusNoContent || !registered {
		t.Fatalf("former orchestrator worker could not register: %s", registration.Status)
	}
	if agent.getOrchestratorID() != newLeader.ID() || agent.getOrchestratorAddr() != server.URL {
		t.Fatal("worker retained its surrendered local orchestrator")
	}
	if agent.localOrchestratorIDValue() != oldLeader.ID() {
		t.Fatalf("local coordinator identity changed after joining a new leader: %q", agent.localOrchestratorIDValue())
	}
}

func TestOrchestratorCandidatesUseCapabilityElectionOrder(t *testing.T) {
	candidates := []orchestratorCandidate{
		{id: "cluster-leader-with-more-members", address: "https://192.168.1.65:7450", score: 80},
		{id: "local-higher-capability", address: "https://192.168.1.189:7450", score: 100},
		{id: "tie-break-a", address: "https://192.168.1.10:7450", score: 80},
	}

	sortOrchestratorCandidates(candidates)
	if candidates[0].id != "local-higher-capability" {
		t.Fatalf("top candidate = %q, want highest-capability coordinator", candidates[0].id)
	}
	if candidates[1].id != "cluster-leader-with-more-members" ||
		candidates[2].id != "tie-break-a" {
		t.Fatalf("equal-score candidates are not ordered by stable ID: %#v", candidates)
	}
}

func TestPersistentTaskHistorySurvivesAgentRestart(t *testing.T) {
	cfg := config.NodeConfig{
		NodeID:     "persistent-task-history-test",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}
	agent, err := NewWithPersistentTasks(cfg)
	if err != nil {
		t.Fatal(err)
	}
	created, err := agent.orchestratorLink.tasks.CreateChecked("Persist this task", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewWithPersistentTasks(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(restarted)
	restored, ok := restarted.orchestratorLink.tasks.Get(created.ID)
	if !ok || restored.Description != "Persist this task" {
		t.Fatalf("task history was not restored: %#v", restored)
	}
}

func TestTaskReplicaRequiresCurrentOrchestrator(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID:     "task-replica-test",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.mem != nil {
		defer closeutil.Discard(agent.mem)
	}
	agent.setOrchestratorID("current-orchestrator")
	source := taskmgr.NewManager()
	task, err := source.CreateChecked("Replicated from coordinator", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(source.All())
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/tasks/replica", bytes.NewReader(body))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: "old-orchestrator"}}}}
	response := httptest.NewRecorder()
	agent.handleTaskReplica(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("replica from former coordinator returned %d, want forbidden", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/tasks/replica", bytes.NewReader(body))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: "current-orchestrator"}}}}
	response = httptest.NewRecorder()
	agent.handleTaskReplica(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("replica from current coordinator returned %d: %s", response.Code, response.Body.String())
	}
	replica, ok := agent.orchestratorLink.tasks.Get(task.ID)
	if !ok || replica.Description != task.Description {
		t.Fatalf("coordinator task was not replicated: %#v", replica)
	}
}

func TestNodeControlInspectsLocalEngineWithoutSelfTLSRequest(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-local-control-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(agent.Memory())
	agent.engines = []engine.Engine{engine.NewSimulated()}
	agent.detectedEngines = nil

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = agent.identity.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()
	agent.cfg.AdvertiseAddr = server.URL

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/nodes/node-local-control-test/engines", nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/nodes/{id}/engines", agent.handleAPINodeControl)
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("local engine inspection returned %d: %s", response.Code, response.Body.String())
	}
	var engines []engineInfo
	if err := json.NewDecoder(response.Body).Decode(&engines); err != nil {
		t.Fatal(err)
	}
	if len(engines) != 1 || engines[0].Name != "simulated" {
		t.Fatalf("local engine inspection returned %#v", engines)
	}
}

func TestNodeControlRoutesRemoteInspectionThroughOrchestrator(t *testing.T) {
	orchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-control-test")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/nodes/remote-control-test/engines" {
			t.Errorf("orchestrator path = %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode([]engineInfo{{Name: "remote-engine"}})
	}))
	server.TLS = orchestrator.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-control-client", OrchestratorAddr: server.URL,
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(agent.Memory())
	if err := agent.identity.TrustPeerDetails(orchestrator.ID(), orchestrator.CertificatePEM(), "orchestrator", server.URL); err != nil {
		t.Fatal(err)
	}
	agent.peers = []cluster.NodeStatus{{
		ID: "remote-control-test", Address: "https://127.0.0.1:1", Healthy: true,
	}}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/nodes/remote-control-test/engines", nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/nodes/{id}/engines", agent.handleAPINodeControl)
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("remote engine inspection returned %d: %s", response.Code, response.Body.String())
	}
	var engines []engineInfo
	if err := json.NewDecoder(response.Body).Decode(&engines); err != nil {
		t.Fatal(err)
	}
	if len(engines) != 1 || engines[0].Name != "remote-engine" {
		t.Fatalf("remote engine inspection returned %#v", engines)
	}
}

func TestDeferredAndManualRescanKeepConfiguredAndDiscoveredEngines(t *testing.T) {
	configuredServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"configured-model"}]}`))
	}))
	defer configuredServer.Close()

	discovered := []engine.Engine{&pullTestEngine{name: "detected-at-start"}}
	scans := 0
	agent := mustNewTestAgentWithEngineDetector(t, config.NodeConfig{
		NodeID:     "engine-discovery-test",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
		Engines: []config.EngineConfig{{
			Name: "lmstudio",
			URL:  configuredServer.URL,
		}},
	}, nil, func() []engine.Engine {
		scans++
		return append([]engine.Engine(nil), discovered...)
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close agent: %v", err)
		}
	})

	engineNames := func() []string {
		names := make([]string, 0)
		for _, configured := range agent.engineSnapshot() {
			names = append(names, configured.Name())
		}
		return names
	}
	if scans != 0 {
		t.Fatalf("constructor discovery scans = %d, want 0", scans)
	}
	if got := strings.Join(engineNames(), ","); got != "lmstudio" {
		t.Fatalf("engines before the deferred scan = %q, want the configured engine", got)
	}

	agent.refreshDetectedEngines()
	if scans != 1 {
		t.Fatalf("initial deferred discovery scans = %d, want 1", scans)
	}
	if got := strings.Join(engineNames(), ","); got != "lmstudio,detected-at-start" {
		t.Fatalf("engines after deferred scan = %q, want configured and discovered engines", got)
	}

	discovered = []engine.Engine{&pullTestEngine{name: "detected-after-install"}}
	response := httptest.NewRecorder()
	agent.handleModelsRefresh(response, httptest.NewRequest(http.MethodPost, "/models/refresh", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("manual rescan returned %d: %s", response.Code, response.Body.String())
	}
	if scans != 2 {
		t.Fatalf("discovery scans after manual rescan = %d, want 2", scans)
	}
	if got := strings.Join(engineNames(), ","); got != "lmstudio,detected-after-install" {
		t.Fatalf("engines after rescan = %q, want configured and newly discovered engines", got)
	}
}

func TestNodeModelsLocalRequestsWithoutSelfTLS(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-local-models-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(agent.Memory())
	agent.engines = []engine.Engine{engine.NewSimulated()}
	agent.detectedEngines = nil

	selfRequests := 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		selfRequests++
		http.Error(w, "unexpected self request", http.StatusInternalServerError)
	}))
	server.TLS = agent.identity.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()
	agent.cfg.AdvertiseAddr = server.URL
	if agent.identity.IsTrustedID(agent.cfg.NodeID) {
		t.Fatal("test requires an unpinned self identity")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/nodes/{id}/models", agent.handleAPINodeModelsGet)
	mux.HandleFunc("PATCH /api/nodes/{id}/models", agent.handleAPINodeModelsPatch)
	path := "/api/nodes/" + agent.cfg.NodeID + "/models"
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("local model inspection returned %d: %s", response.Code, response.Body.String())
	}
	models := agent.allModelsWithState()
	if len(models) == 0 {
		t.Fatal("simulated engine must provide models")
	}
	modelName := models[0].Name
	body, err := json.Marshal(modelsPatchRequest{DisabledModels: []string{modelName}})
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, path, bytes.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("local model update returned %d: %s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("local model inspection returned %d: %s", response.Code, response.Body.String())
	}
	var inspected []struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.NewDecoder(response.Body).Decode(&inspected); err != nil {
		t.Fatal(err)
	}
	if len(inspected) != len(models) {
		t.Fatalf("inspection returned %d models, want %d including disabled models", len(inspected), len(models))
	}
	foundDisabled := false
	for _, model := range inspected {
		if model.Name == modelName && !model.Enabled {
			foundDisabled = true
		}
	}
	if !foundDisabled {
		t.Fatalf("inspection did not include disabled model %q: %#v", modelName, inspected)
	}
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, path, strings.NewReader("{")))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid local update returned %d: %s", response.Code, response.Body.String())
	}
	if selfRequests != 0 {
		t.Fatalf("local handlers made %d self HTTPS requests", selfRequests)
	}
}

func TestNodeModelsRemoteRequestsRequirePinnedPeer(t *testing.T) {
	remote, err := peeridentity.Open(filepath.Join(t.TempDir(), "remote.json"), "remote-models-test")
	if err != nil {
		t.Fatal(err)
	}
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-models-client", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(agent.Memory())
	if err := remote.TrustPeer(agent.cfg.NodeID, agent.identity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	remoteRequests := 0
	server := httptest.NewUnstartedServer(remote.RequireTrustedPeer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteRequests++
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path != "/models/all" {
				t.Errorf("remote inspection path = %q", r.URL.Path)
			}
		case http.MethodPatch:
			if r.URL.Path != "/models" {
				t.Errorf("remote update path = %q", r.URL.Path)
			}
			var body modelsPatchRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if len(body.DisabledModels) != 1 || body.DisabledModels[0] != "remote-model" {
				t.Errorf("remote update body = %#v", body)
			}
		default:
			t.Errorf("unexpected remote method = %q", r.Method)
		}
		writeJSON(w, http.StatusOK, []map[string]any{{"name": "remote-model", "enabled": false}})
	})))
	server.TLS = remote.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()
	agent.peers = []cluster.NodeStatus{{ID: remote.ID(), Address: server.URL, Healthy: true}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/nodes/{id}/models", agent.handleAPINodeModelsGet)
	mux.HandleFunc("PATCH /api/nodes/{id}/models", agent.handleAPINodeModelsPatch)

	for _, method := range []string{http.MethodGet, http.MethodPatch} {
		for _, target := range []struct {
			id   string
			code int
		}{
			{id: "unknown-node", code: http.StatusNotFound},
			{id: remote.ID(), code: http.StatusBadGateway},
		} {
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(method, "/api/nodes/"+target.id+"/models", strings.NewReader(`{"disabled_models":["remote-model"]}`)))
			if response.Code != target.code {
				t.Fatalf("%s to %s returned %d, want %d: %s", method, target.id, response.Code, target.code, response.Body.String())
			}
		}
	}
	if remoteRequests != 0 {
		t.Fatal("unpinned requests reached remote handler")
	}
	if err := agent.identity.TrustPeer(remote.ID(), remote.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodGet, http.MethodPatch} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(method, "/api/nodes/"+remote.ID()+"/models", strings.NewReader(`{"disabled_models":["remote-model"]}`)))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"remote-model"`) {
			t.Fatalf("pinned remote %s returned %d: %s", method, response.Code, response.Body.String())
		}
	}
	if remoteRequests != 2 {
		t.Fatalf("remote handler received %d requests, want 2", remoteRequests)
	}
}

func TestModelPullUsesRequestedEngine(t *testing.T) {
	first := &pullTestEngine{name: "first"}
	target := &pullTestEngine{name: "target"}
	agent := &Agent{
		engineComponent:    &engineComponent{engines: []engine.Engine{first, target}, telemetry: &telemetryComponent{}},
		telemetryComponent: &telemetryComponent{},
		agentState:         &agentState{},
	}
	request := httptest.NewRequest(http.MethodPost, "/models/pull", strings.NewReader(`{"model":"qwen2.5:7b","engine":"target"}`))
	response := httptest.NewRecorder()
	agent.handleModelPull(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("pull returned %d: %s", response.Code, response.Body.String())
	}
	if len(first.pulls) != 0 || len(target.pulls) != 1 || target.pulls[0] != "qwen2.5:7b" {
		t.Fatalf("pull routed to wrong engine: first=%v target=%v", first.pulls, target.pulls)
	}
	if !strings.Contains(response.Body.String(), `"engine":"target"`) {
		t.Fatalf("pull response did not identify selected engine: %s", response.Body.String())
	}
}

func TestModelPullRejectsUnknownRequestedEngine(t *testing.T) {
	agent := &Agent{
		engineComponent:    &engineComponent{engines: []engine.Engine{&pullTestEngine{name: "configured"}}, telemetry: &telemetryComponent{}},
		telemetryComponent: &telemetryComponent{},
		agentState:         &agentState{},
	}
	request := httptest.NewRequest(http.MethodPost, "/models/pull", strings.NewReader(`{"model":"model","engine":"missing"}`))
	response := httptest.NewRecorder()
	agent.handleModelPull(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "engine is not configured") {
		t.Fatalf("unknown engine pull returned %d: %s", response.Code, response.Body.String())
	}
}

func TestPairingInvitationRequiresLocalAcceptance(t *testing.T) {
	orchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-invitation-test")
	if err != nil {
		t.Fatal(err)
	}
	otherOrchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "other-orchestrator.json"), "different-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	pairServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pairing/join" {
			t.Errorf("pairing path = %q", r.URL.Path)
		}
		var request nodePairingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.PIN != "123456" || request.ID != "node-invitation-test" {
			t.Errorf("unexpected pairing request: %#v", request)
		}
		_ = json.NewEncoder(w).Encode(nodePairingResponse{
			ID: orchestrator.ID(), Role: "orchestrator", CertificatePEM: orchestrator.CertificatePEM(),
		})
	}))
	pairServer.TLS = orchestrator.ServerTLSConfig()
	pairServer.StartTLS()
	defer pairServer.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "node-invitation-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(agent.Memory())
	wrongServerReceivedRequest := false
	wrongServer := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		wrongServerReceivedRequest = true
	}))
	wrongServer.TLS = otherOrchestrator.ServerTLSConfig()
	wrongServer.StartTLS()
	defer wrongServer.Close()
	if err := agent.pairWithAddress(wrongServer.URL, orchestrator.ID(), "123456"); err == nil {
		t.Fatal("pairing sent a PIN to a TLS endpoint with the wrong identity")
	}
	if wrongServerReceivedRequest || agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatal("wrong-identity bootstrap reached the endpoint or changed the trust roster")
	}
	agent.setOrchestratorAddr("https://orchestrator.local:7450")
	agent.setOrchestratorID(orchestrator.ID())
	certBlock, _ := pem.Decode([]byte(orchestrator.CertificatePEM()))
	if certBlock == nil {
		t.Fatal("orchestrator identity certificate is not PEM encoded")
	}
	certificate, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(pairingInvitationRequest{
		NodeID: "node-invitation-test", OrchestratorID: orchestrator.ID(),
		Address: pairServer.URL, PIN: "123456", ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/invitation", bytes.NewReader(payload))
	otherCertBlock, _ := pem.Decode([]byte(otherOrchestrator.CertificatePEM()))
	if otherCertBlock == nil {
		t.Fatal("other orchestrator identity certificate is not PEM encoded")
	}
	otherCertificate, err := x509.ParseCertificate(otherCertBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{otherCertificate}}
	agent.setOrchestratorID("different-orchestrator")
	rejectedResponse := httptest.NewRecorder()
	agent.handlePairingInvitation(rejectedResponse, request)
	if rejectedResponse.Code != http.StatusBadRequest {
		t.Fatalf("invitation with a mismatched TLS identity returned %d: %s", rejectedResponse.Code, rejectedResponse.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/pairing/invitation", bytes.NewReader(payload))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
	invitationResponse := httptest.NewRecorder()
	agent.handlePairingInvitation(invitationResponse, request)
	if invitationResponse.Code != http.StatusAccepted {
		t.Fatalf("invitation from an undiscovered orchestrator returned %d: %s", invitationResponse.Code, invitationResponse.Body.String())
	}
	if agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatal("node paired before local acceptance")
	}

	statusResponse := httptest.NewRecorder()
	agent.handlePairingInvitationStatus(statusResponse, httptest.NewRequest(http.MethodGet, "/api/pairing/invitation", nil))
	if statusResponse.Code != http.StatusOK || !strings.Contains(statusResponse.Body.String(), `"pending":true`) ||
		strings.Contains(statusResponse.Body.String(), "123456") {
		t.Fatalf("invitation status exposed invalid state: %d: %s", statusResponse.Code, statusResponse.Body.String())
	}

	for _, test := range []struct {
		name        string
		fingerprint string
		status      int
	}{
		{"missing anchor", "", http.StatusBadRequest},
		{"malformed anchor", strings.Repeat("z", 64), http.StatusBadRequest},
		{"wrong anchor", strings.Repeat("0", 64), http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(map[string]string{"fingerprint": test.fingerprint})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			agent.handlePairingInvitationAccept(response, httptest.NewRequest(http.MethodPost, "/api/pairing/invitation/accept", bytes.NewReader(payload)))
			if response.Code != test.status || agent.identity.IsTrustedID(orchestrator.ID()) {
				t.Fatalf("invalid acceptance = %d: %s", response.Code, response.Body.String())
			}
			if agent.orchestratorLink.pairingInvitationStatus(time.Now()) == nil {
				t.Fatal("rejected acceptance discarded the pending invitation")
			}
		})
	}
	acceptPayload, err := json.Marshal(map[string]string{"fingerprint": securityFingerprint(t, orchestrator)})
	if err != nil {
		t.Fatal(err)
	}
	acceptResponse := httptest.NewRecorder()
	agent.handlePairingInvitationAccept(acceptResponse, httptest.NewRequest(http.MethodPost, "/api/pairing/invitation/accept", bytes.NewReader(acceptPayload)))
	if acceptResponse.Code != http.StatusOK || !agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatalf("local acceptance returned %d: %s", acceptResponse.Code, acceptResponse.Body.String())
	}
	gone := httptest.NewRecorder()
	agent.handlePairingInvitationAccept(gone, httptest.NewRequest(http.MethodPost, "/api/pairing/invitation/accept", bytes.NewReader(acceptPayload)))
	if gone.Code != http.StatusGone {
		t.Fatalf("consumed invitation returned %d: %s", gone.Code, gone.Body.String())
	}
}

func TestStaleOrchestratorCannotMutateAfterYieldButCanServeItsPartition(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "stale-orchestrator-fence-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.mem != nil {
		defer closeutil.Discard(agent.mem)
	}
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	oldLeader, err := peeridentity.Open(filepath.Join(t.TempDir(), "old-leader.json"), "old-leader")
	if err != nil {
		t.Fatal(err)
	}
	newLeader, err := peeridentity.Open(filepath.Join(t.TempDir(), "new-leader.json"), "new-leader")
	if err != nil {
		t.Fatal(err)
	}
	for _, leader := range []*peeridentity.Identity{oldLeader, newLeader} {
		if err := agent.identity.TrustPeerDetails(leader.ID(), leader.CertificatePEM(), "orchestrator", "https://127.0.0.1:7450"); err != nil {
			t.Fatal(err)
		}
	}
	block, _ := pem.Decode([]byte(oldLeader.CertificatePEM()))
	if block == nil {
		t.Fatal("old leader certificate is not PEM encoded")
	}
	oldCertificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name       string
		electedID  string
		wantStatus int
		wantWrite  bool
	}{
		{name: "new higher-ranked leader is visible", electedID: newLeader.ID(), wantStatus: http.StatusForbidden},
		{name: "old leader remains elected in an isolated partition", electedID: oldLeader.ID(), wantStatus: http.StatusNoContent, wantWrite: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrote := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				wrote = true
				w.WriteHeader(http.StatusNoContent)
			})
			fenced := currentCoordinatorWriteFence(next, agent.identity.IsTrustedOrchestratorID, func() string {
				return test.electedID
			})
			handler := agent.identity.RequireTrustedPeer(fenced)
			request := httptest.NewRequest(http.MethodPost, "/api/models", nil)
			request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{oldCertificate}}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("stale coordinator mutation returned %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if wrote != test.wantWrite {
				t.Fatalf("mutation reached handler = %t, want %t", wrote, test.wantWrite)
			}
		})
	}
}

func TestDefaultNodeIDStable(t *testing.T) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node"
	}
	if got := DefaultNodeID(); got != hostname {
		t.Fatalf("expected stable hostname ID %q, got %q", hostname, got)
	}
	first, second := DefaultNodeID(), DefaultNodeID()
	if first != second {
		t.Fatalf("expected default node ID to remain stable, got %q then %q", first, second)
	}
}

func TestAppendGPUUsageSampleKeepsBoundedRecentHistory(t *testing.T) {
	var history []hardware.GPUUsageSample
	for i := 0; i < gpuHistoryLimit+3; i++ {
		history = appendGPUUsageSample(history, hardware.GPUUsageSample{
			Timestamp: time.Unix(int64(i), 0), Available: true, Utilization: i,
		})
	}
	if len(history) != gpuHistoryLimit {
		t.Fatalf("history length = %d, want %d", len(history), gpuHistoryLimit)
	}
	if got := history[0].Utilization; got != 3 {
		t.Fatalf("oldest retained utilization = %d, want 3", got)
	}
	if got := history[len(history)-1].Utilization; got != gpuHistoryLimit+2 {
		t.Fatalf("newest retained utilization = %d, want %d", got, gpuHistoryLimit+2)
	}
}

func TestAppendNPUUsageSampleKeepsBoundedRecentHistory(t *testing.T) {
	var history []hardware.NPUUsageSample
	for i := 0; i < npuHistoryLimit+3; i++ {
		history = appendNPUUsageSample(history, hardware.NPUUsageSample{
			Timestamp: time.Unix(int64(i), 0), Available: true, Utilization: i,
		})
	}
	if len(history) != npuHistoryLimit {
		t.Fatalf("history length = %d, want %d", len(history), npuHistoryLimit)
	}
	if got := history[0].Utilization; got != 3 {
		t.Fatalf("oldest retained utilization = %d, want 3", got)
	}
	if got := history[len(history)-1].Utilization; got != npuHistoryLimit+2 {
		t.Fatalf("newest retained utilization = %d, want %d", got, npuHistoryLimit+2)
	}
}

func TestAppendSystemUsageSampleKeepsBoundedRecentHistory(t *testing.T) {
	history := make([]hardware.SystemUsageSample, systemHistoryLimit)
	for index := range history {
		history[index] = hardware.SystemUsageSample{CPUUtilization: index}
	}
	history = appendSystemUsageSample(history, hardware.SystemUsageSample{CPUUtilization: 100})
	if len(history) != systemHistoryLimit {
		t.Fatalf("history length = %d, want %d", len(history), systemHistoryLimit)
	}
	if history[0].CPUUtilization != 1 || history[len(history)-1].CPUUtilization != 100 {
		t.Fatalf("history endpoints = (%d, %d), want (1, 100)", history[0].CPUUtilization, history[len(history)-1].CPUUtilization)
	}
}

func TestSystemStatusReportsPersistenceAvailability(t *testing.T) {
	for _, test := range []struct {
		name      string
		memory    string
		available bool
	}{
		{name: "available", memory: filepath.Join(t.TempDir(), "node.db"), available: true},
		{name: "unavailable", memory: t.TempDir(), available: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := mustNewTestAgent(t, config.NodeConfig{NodeID: "system-status-test", MemoryPath: test.memory})
			if agent.Memory() != nil {
				t.Cleanup(func() { _ = agent.Memory().Close() })
			}
			response := httptest.NewRecorder()
			agent.handleSystemStatus(response, httptest.NewRequest(http.MethodGet, "/api/status", nil))
			if response.Code != http.StatusOK {
				t.Fatalf("GET /api/status returned %d", response.Code)
			}
			var status struct {
				Role        string `json:"role"`
				Persistence struct {
					Available bool   `json:"available"`
					Warning   string `json:"warning"`
				} `json:"persistence"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
				t.Fatal(err)
			}
			if status.Role != "node" || status.Persistence.Available != test.available {
				t.Fatalf("unexpected status: %+v", status)
			}
			if test.available && status.Persistence.Warning != "" {
				t.Fatalf("available persistence has warning %q", status.Persistence.Warning)
			}
			if !test.available && status.Persistence.Warning == "" {
				t.Fatal("unavailable persistence did not include a user-facing warning")
			}
		})
	}
}

func TestNodeChatAndDialogueEndpoints(t *testing.T) {
	cfg := config.NodeConfig{
		NodeID:     "node-chat-test",
		ListenAddr: ":0",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}

	agent := mustNewTestAgent(t, cfg)
	t.Cleanup(func() { _ = agent.Memory().Close() })
	// Ensure simulated engine is present for deterministic unit testing
	agent.engines = append([]engine.Engine{engine.NewSimulated()}, agent.engines...)

	// 1. Test POST /chat (stateless multi-turn array)
	chatReqBody, _ := json.Marshal(chatRequest{
		Model: "simulated-general",
		Messages: []engine.ChatMessage{
			{Role: "user", Content: "Hello model on node"},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/chat", bytes.NewReader(chatReqBody))
	rec := httptest.NewRecorder()
	agent.handleChat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /chat returned %d", rec.Code)
	}
	var chatResp chatResponse
	if err := json.NewDecoder(rec.Body).Decode(&chatResp); err != nil {
		t.Fatalf("decode chatResp failed: %v", err)
	}
	if !strings.Contains(chatResp.Message.Content, "simulated") {
		t.Errorf("unexpected chat reply: %q", chatResp.Message.Content)
	}

	// 2. Test POST /dialogues (start a conversation)
	createReqBody, _ := json.Marshal(createDialogueRequest{
		ID:     "test-dlg-1",
		Model:  "simulated-general",
		System: "You are a helpful coding assistant",
	})
	req = httptest.NewRequest(http.MethodPost, "/dialogues", bytes.NewReader(createReqBody))
	rec = httptest.NewRecorder()
	agent.handleDialogueCreate(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /dialogues returned %d", rec.Code)
	}
	var dlg Dialogue
	if err := json.NewDecoder(rec.Body).Decode(&dlg); err != nil {
		t.Fatalf("decode dialogue failed: %v", err)
	}
	if dlg.ID != "test-dlg-1" || len(dlg.Messages) != 1 {
		t.Errorf("unexpected created dialogue: %+v", dlg)
	}

	// 3. Test POST /dialogues/{id}/messages (Turn 1)
	msgReqBody1, _ := json.Marshal(dialogueMessageRequest{
		Content: "What is concurrency in Go?",
	})
	req = httptest.NewRequest(http.MethodPost, "/dialogues/test-dlg-1/messages", bytes.NewReader(msgReqBody1))
	req.SetPathValue("id", "test-dlg-1")
	rec = httptest.NewRecorder()
	agent.handleDialogueMessage(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST turn 1 returned %d", rec.Code)
	}
	var msgResp1 dialogueMessageResponse
	if err := json.NewDecoder(rec.Body).Decode(&msgResp1); err != nil {
		t.Fatalf("decode turn 1 response failed: %v", err)
	}
	if len(msgResp1.Messages) != 3 { // system + user + assistant
		t.Errorf("expected 3 messages after turn 1, got %d", len(msgResp1.Messages))
	}

	// 4. Test POST /dialogues/{id}/messages (Turn 2 - Follow-up in ongoing conversation)
	msgReqBody2, _ := json.Marshal(dialogueMessageRequest{
		Content: "Can you provide an example with goroutines and channels?",
	})
	req = httptest.NewRequest(http.MethodPost, "/dialogues/test-dlg-1/messages", bytes.NewReader(msgReqBody2))
	req.SetPathValue("id", "test-dlg-1")
	rec = httptest.NewRecorder()
	agent.handleDialogueMessage(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST turn 2 returned %d", rec.Code)
	}
	var msgResp2 dialogueMessageResponse
	if err := json.NewDecoder(rec.Body).Decode(&msgResp2); err != nil {
		t.Fatalf("decode turn 2 response failed: %v", err)
	}
	if len(msgResp2.Messages) != 5 { // system + user1 + asst1 + user2 + asst2
		t.Errorf("expected 5 messages after turn 2, got %d", len(msgResp2.Messages))
	}

	// 5. Test GET /dialogues/{id}
	req = httptest.NewRequest(http.MethodGet, "/dialogues/test-dlg-1", nil)
	req.SetPathValue("id", "test-dlg-1")
	rec = httptest.NewRecorder()
	agent.handleDialogueGet(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET dialogue returned %d", rec.Code)
	}
	var fetchedDlg Dialogue
	if err := json.NewDecoder(rec.Body).Decode(&fetchedDlg); err != nil {
		t.Fatalf("decode fetched dialogue failed: %v", err)
	}
	if len(fetchedDlg.Messages) != 5 {
		t.Errorf("expected 5 messages in fetched dialogue, got %d", len(fetchedDlg.Messages))
	}
}

func TestDialogueCreateRejectsMalformedAndOversizedBodies(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "dialogue-body-limit-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close agent: %v", err)
		}
	})

	for _, body := range []string{`{"model":`, strings.Repeat(" ", 64<<10+1)} {
		request := httptest.NewRequest(http.MethodPost, "/dialogues", strings.NewReader(body))
		response := httptest.NewRecorder()
		agent.handleDialogueCreate(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("dialogue create with %d-byte body returned %d, want 400",
				len(body), response.Code)
		}
	}
}

func TestTaskSubmissionProxyUsesLongTimeoutClient(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer upstream.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{})
	agent.orchestratorAddr = upstream.URL
	agent.peerClient = &http.Client{Timeout: 10 * time.Millisecond}
	agent.dispatchClient = &http.Client{Timeout: time.Second}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/tasks", strings.NewReader(`{"description":"test"}`))
	agent.handleAPITasksPost(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("proxied task submission returned %d: %s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/nodes/worker/engines/managed/install-worker", strings.NewReader(`{"source":"official"}`))
	agent.handleAPINodeControl(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("proxied worker installation returned %d: %s", response.Code, response.Body.String())
	}
}

func TestReadOnlyClusterViewsProxyToOrchestrator(t *testing.T) {
	var gotURI string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.Method + " " + r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	tests := []struct {
		name       string
		joined     bool
		target     string
		handler    func(*Agent) http.HandlerFunc
		wantStatus int
		wantURI    string
	}{
		{name: "cluster health", joined: true, target: "/api/cluster/health", handler: func(a *Agent) http.HandlerFunc { return a.handleAPIOrchestratorRead }, wantStatus: http.StatusOK, wantURI: "GET /api/cluster/health"},
		{name: "shard groups", joined: true, target: "/api/shards", handler: func(a *Agent) http.HandlerFunc { return a.handleAPIOrchestratorRead }, wantStatus: http.StatusOK, wantURI: "GET /api/shards"},
		{name: "task page keeps query", joined: true, target: "/api/tasks?page_size=50&cursor=abc", handler: func(a *Agent) http.HandlerFunc { return a.handleAPITasksList }, wantStatus: http.StatusOK, wantURI: "GET /api/tasks?page_size=50&cursor=abc"},
		{name: "not joined", target: "/api/cluster/health", handler: func(a *Agent) http.HandlerFunc { return a.handleAPIOrchestratorRead }, wantStatus: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURI = ""
			agent := mustNewTestAgent(t, config.NodeConfig{})
			if tt.joined {
				agent.orchestratorAddr = upstream.URL
			}
			agent.peerClient = upstream.Client()
			response := httptest.NewRecorder()
			tt.handler(agent)(response, httptest.NewRequest(http.MethodGet, tt.target, nil))
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d: %s", response.Code, tt.wantStatus, response.Body.String())
			}
			if gotURI != tt.wantURI {
				t.Fatalf("upstream request = %q, want %q", gotURI, tt.wantURI)
			}
		})
	}
}

func TestNodeDashboardProxiesShardCreationAndProfileReadsToOrchestrator(t *testing.T) {
	var gotMethod, gotURI, gotBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotURI = r.URL.RequestURI()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read proxied request body: %v", err)
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{})
	agent.orchestratorAddr = upstream.URL
	agent.peerClient = upstream.Client()

	createResponse := httptest.NewRecorder()
	createRequest := httptest.NewRequest(http.MethodPost, "/api/shards", strings.NewReader(`{"mode":"llamacpp-rpc"}`))
	agent.handleAPIOrchestratorProxy(createResponse, createRequest)
	if createResponse.Code != http.StatusOK || gotMethod != http.MethodPost ||
		gotURI != "/api/shards" || gotBody != `{"mode":"llamacpp-rpc"}` {
		t.Fatalf("shard create proxy = %d %s %s %q", createResponse.Code, gotMethod, gotURI, gotBody)
	}

	gotMethod, gotURI, gotBody = "", "", ""
	profileResponse := httptest.NewRecorder()
	profileRequest := httptest.NewRequest(
		http.MethodGet,
		"/api/nodes/worker/engines/managed/engine/profile?requested_context=4096",
		nil,
	)
	agent.handleAPINodeControl(profileResponse, profileRequest)
	if profileResponse.Code != http.StatusOK || gotMethod != http.MethodGet ||
		gotURI != "/api/nodes/worker/engines/managed/engine/profile?requested_context=4096" {
		t.Fatalf("model profile proxy = %d %s %s", profileResponse.Code, gotMethod, gotURI)
	}
	installResponse := httptest.NewRecorder()
	installRequest := httptest.NewRequest(
		http.MethodPost, "/api/nodes/worker/engines/managed/install-worker",
		strings.NewReader(`{"source":"official"}`),
	)
	agent.handleAPINodeControl(installResponse, installRequest)
	if installResponse.Code != http.StatusOK || gotMethod != http.MethodPost ||
		gotURI != "/api/nodes/worker/engines/managed/install-worker" || gotBody != `{"source":"official"}` {
		t.Fatalf("worker install proxy = %d %s %s %q", installResponse.Code, gotMethod, gotURI, gotBody)
	}
}
