package supervisor

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestManagerStartStopAndCloseKillProcessTree(t *testing.T) {
	binary := buildFakeEngine(t)
	pidFile := filepath.Join(t.TempDir(), "parent.pid")
	childPIDFile := filepath.Join(t.TempDir(), "child.pid")
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "tree")
	t.Setenv("MAISTR0_FAKE_ENGINE_PID_FILE", pidFile)
	t.Setenv("MAISTR0_FAKE_ENGINE_CHILD_PID_FILE", childPIDFile)

	manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), testOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("cleanup managed engine supervisor: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.Start(ctx, "test-engine", nil); err != nil {
		t.Fatalf("start managed engine: %v", err)
	}
	if status, err := manager.Status("test-engine"); err != nil || status.State != stateReady {
		t.Fatalf("ready status = %+v, error = %v", status, err)
	}
	parentPID := readPID(t, pidFile)
	childPID := waitPIDFile(t, childPIDFile)
	if !processAlive(parentPID) || !processAlive(childPID) {
		t.Fatalf("fake engine tree did not stay alive after readiness (parent %d, child %d)", parentPID, childPID)
	}
	if err := manager.Stop(ctx, "test-engine"); err != nil {
		t.Fatalf("stop managed engine: %v", err)
	}
	waitProcessDead(t, parentPID)
	waitProcessDead(t, childPID)
	status, err := manager.Status("test-engine")
	if err != nil || status.State != stateStopped || status.PID != 0 {
		t.Fatalf("stopped status = %+v, error = %v", status, err)
	}

	secondChildPIDFile := filepath.Join(t.TempDir(), "second-child.pid")
	t.Setenv("MAISTR0_FAKE_ENGINE_CHILD_PID_FILE", secondChildPIDFile)
	if err := manager.Start(ctx, "test-engine", nil); err != nil {
		t.Fatalf("restart after stop: %v", err)
	}
	secondChildPID := waitPIDFile(t, secondChildPIDFile)
	if err := manager.Close(); err != nil {
		t.Fatalf("close managed engine supervisor: %v", err)
	}
	waitProcessDead(t, secondChildPID)
}

func TestManagerTCPReadinessAndRPCLoopbackDefault(t *testing.T) {
	binary := buildFakeEngine(t)
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "tcp")
	managerConfig := config.NodeConfig{
		ManagedEngineBinaryAllowlist: []string{filepath.Dir(binary)},
		ManagedEngines: []config.ManagedEngineConfig{{
			ID: "rpc-engine", Kind: config.ManagedEngineGGMLRPCServer, BinaryPath: binary,
			Options: config.ManagedEngineOptions{GGMLRPCServer: &config.GGMLRPCServerOptions{Port: freePort(t)}},
		}},
	}
	manager, err := New(managerConfig, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	manager.SetRPCRelayController(testRPCRelayController{})
	defer manager.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Start(ctx, "rpc-engine", nil); err != nil {
		t.Fatalf("start RPC managed engine: %v", err)
	}
	status, err := manager.Status("rpc-engine")
	if err != nil || status.State != stateReady {
		t.Fatalf("RPC readiness status = %+v, error = %v", status, err)
	}
	if status.Warnings != nil {
		t.Fatalf("loopback RPC bind produced warnings: %v", status.Warnings)
	}
}

func TestManagerReadinessTimeoutStopsProcess(t *testing.T) {
	binary := buildFakeEngine(t)
	pidFile := filepath.Join(t.TempDir(), "never-ready.pid")
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "never-ready")
	t.Setenv("MAISTR0_FAKE_ENGINE_PID_FILE", pidFile)
	manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), Options{
		ReadinessTimeout: 120 * time.Millisecond,
		ProbeInterval:    10 * time.Millisecond,
		StopTimeout:      time.Second,
		Restart:          RestartPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	err = manager.Start(context.Background(), "test-engine", nil)
	if !errors.Is(err, ErrReadinessTimeout) {
		t.Fatalf("start error = %v, want readiness timeout", err)
	}
	pid := readPID(t, pidFile)
	waitProcessDead(t, pid)
	status, err := manager.Status("test-engine")
	if err != nil || status.State != stateFailed || status.PID != 0 {
		t.Fatalf("timeout status = %+v, error = %v", status, err)
	}
}

func TestManagerAutomaticallyRestartsCrashLoopWithBackoffLimit(t *testing.T) {
	binary := buildFakeEngine(t)
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "crash")
	t.Setenv("MAISTR0_FAKE_ENGINE_EXIT_AFTER_MS", "100")
	manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), Options{
		ReadinessTimeout: time.Second,
		ProbeInterval:    10 * time.Millisecond,
		StopTimeout:      time.Second,
		Restart:          RestartPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Start(ctx, "test-engine", nil); err != nil {
		t.Fatalf("initial start: %v", err)
	}
	status := waitForStatus(t, manager, "test-engine", func(status Status) bool {
		return status.State == stateFailed && status.RestartCount == 2
	})
	if status.LastError != "restart limit exceeded" {
		t.Fatalf("crash-loop status error = %q", status.LastError)
	}
}

func TestVLLMArgsUseTypedShardPlanAndWindowsRejectsNativeLaunch(t *testing.T) {
	configured := config.ManagedEngineConfig{
		ID: "vllm-worker", Kind: config.ManagedEngineVLLM, BinaryPath: "vllm",
		ModelID: "repo/model",
		Options: config.ManagedEngineOptions{VLLM: &config.VLLMOptions{Port: 8111, TensorParallelSize: 2}},
	}
	plan := &ShardPlan{NNodes: 3, NodeRank: 1, MasterAddr: "192.0.2.10"}
	args, err := buildArgs(configured, plan)
	if runtime.GOOS == "windows" {
		if !errors.Is(err, ErrUnsupportedPlatform) {
			t.Fatalf("native Windows vLLM error = %v, want unsupported platform", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, "\x00")
	for _, required := range []string{"--headless", "--nnodes\x003", "--node-rank\x001", "--master-addr\x00192.0.2.10", "--tensor-parallel-size\x002"} {
		if !strings.Contains(joined, required) {
			t.Errorf("argv %q does not contain %q", args, required)
		}
	}
	if args[1] != configured.ModelID {
		t.Fatalf("model ID was not passed as a single argv value: %#v", args)
	}
	headPlan := *plan
	headPlan.NodeRank = 0
	headArgs, err := buildArgs(configured, &headPlan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(headArgs, "\x00"), "--headless") {
		t.Fatalf("rank-zero head must expose the inference API, argv = %#v", headArgs)
	}
	if err := validatePlatformAndPlan(configured, &ShardPlan{NNodes: 2, NodeRank: 2, MasterAddr: "node"}); !errors.Is(err, ErrInvalidShardPlan) && runtime.GOOS != "windows" {
		t.Fatalf("invalid node rank error = %v, want invalid plan", err)
	}
}

func TestLlamaServerNPUTemplatesApplyFixedLaunchSettings(t *testing.T) {
	openVINO, ok := engine.ManagedEngineTemplateFor(engine.TemplateLlamaCPPOpenVINONPU)
	if !ok {
		t.Fatal("OpenVINO template is missing")
	}
	environment := applyTemplateEnvironment(
		[]string{"GGML_OPENVINO_DEVICE=CPU", "KEEP=value"},
		openVINO.Environment,
	)
	if len(environment) != 2 || environment[0] != "GGML_OPENVINO_DEVICE=NPU" || environment[1] != "KEEP=value" {
		t.Fatalf("OpenVINO template environment = %#v", environment)
	}
	openVINOArgs, err := buildArgs(config.ManagedEngineConfig{
		ID: "openvino", Kind: config.ManagedEngineLlamaServer, ModelID: "model",
		Template: engine.TemplateLlamaCPPOpenVINONPU,
		Options:  config.ManagedEngineOptions{LlamaServer: &config.LlamaServerOptions{ContextSize: 8192}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if joined := strings.Join(openVINOArgs, "\x00"); !strings.Contains(joined, "--parallel\x001") ||
		!strings.Contains(joined, "--ctx-size\x008192") {
		t.Fatalf("OpenVINO template argv = %#v, want one sequence and explicit context size", openVINOArgs)
	}

	args, err := buildArgs(config.ManagedEngineConfig{
		ID: "hexagon", Kind: config.ManagedEngineLlamaServer, ModelID: "model",
		Template: engine.TemplateLlamaCPPHexagonNPU,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, "\x00")
	if !strings.Contains(joined, "--device\x00HTP0") {
		t.Fatalf("Hexagon template argv = %#v, want the documented HTP0 device selection", args)
	}

	args, err = buildArgs(config.ManagedEngineConfig{
		ID: "xdna", Kind: config.ManagedEngineLlamaServer, ModelID: "model",
		Template: engine.TemplateLlamaCPPXDNA2NPU,
		Options:  config.ManagedEngineOptions{LlamaServer: &config.LlamaServerOptions{ContextSize: 8192}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(args, "\x00")
	if !strings.Contains(joined, "--device\x00XDNA0") ||
		!strings.Contains(joined, "--parallel\x001") ||
		!strings.Contains(joined, "-nkvo") ||
		!strings.Contains(joined, "--ctx-size\x008192") {
		t.Fatalf("XDNA template argv = %#v, want explicit device, one sequence, no KV offload, and context size", args)
	}
}

func TestVLLMHeadlessWorkersUseProcessReadinessAndHeadRequiresHTTP(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("vLLM sharding is unsupported on native Windows")
	}
	binary := buildFakeEngine(t)
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "never-ready")
	cfg := config.NodeConfig{
		ManagedEngineBinaryAllowlist: []string{filepath.Dir(binary)},
		TrustedNetworkInterfaces:     []string{"test0"},
		ManagedEngines: []config.ManagedEngineConfig{
			{
				ID: "vllm-worker", Kind: config.ManagedEngineVLLM, BinaryPath: binary, ModelID: "repo/model",
				Options: config.ManagedEngineOptions{VLLM: &config.VLLMOptions{Port: freePort(t)}},
			},
			{
				ID: "vllm-head", Kind: config.ManagedEngineVLLM, BinaryPath: binary, ModelID: "repo/model",
				Options: config.ManagedEngineOptions{VLLM: &config.VLLMOptions{Port: freePort(t)}},
			},
		},
	}
	options := testOptions()
	options.ReadinessTimeout = 80 * time.Millisecond
	environmentFile := filepath.Join(t.TempDir(), "vllm-host-ip")
	t.Setenv("MAISTR0_FAKE_ENGINE_ENV_FILE", environmentFile)
	manager, err := New(cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	workerPlan := &ShardPlan{NNodes: 2, NodeRank: 1, MasterAddr: "127.0.0.1"}
	if err := manager.Start(context.Background(), "vllm-worker", workerPlan); err != nil {
		t.Fatalf("headless worker start: %v", err)
	}
	// Process readiness can precede the child's first environment-file write.
	waitForStatus(t, manager, "vllm-worker", func(status Status) bool {
		got, err := os.ReadFile(environmentFile)
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		if err != nil {
			t.Fatalf("read VLLM_HOST_IP: %v", err)
		}
		if len(got) == 0 {
			return false
		}
		if string(got) != "10.20.0.1" {
			t.Fatalf("VLLM_HOST_IP = %q; want trusted-interface address", got)
		}
		return status.State == stateReady
	})
	if err := manager.Stop(context.Background(), "vllm-worker"); err != nil {
		t.Fatalf("stop headless worker: %v", err)
	}

	headPlan := &ShardPlan{NNodes: 2, NodeRank: 0, MasterAddr: "127.0.0.1"}
	err = manager.Start(context.Background(), "vllm-head", headPlan)
	if !errors.Is(err, ErrReadinessTimeout) {
		t.Fatalf("rank-zero head start error = %v, want HTTP readiness timeout", err)
	}
}

func TestVLLMStartRefusesUntrustedInterfaceBeforeLaunching(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("vLLM sharding is unsupported on native Windows")
	}
	binary := buildFakeEngine(t)
	cfg := config.NodeConfig{
		ManagedEngineBinaryAllowlist: []string{filepath.Dir(binary)},
		ManagedEngines: []config.ManagedEngineConfig{{
			ID: "vllm-worker", Kind: config.ManagedEngineVLLM, BinaryPath: binary, ModelID: "repo/model",
			Options: config.ManagedEngineOptions{VLLM: &config.VLLMOptions{Port: freePort(t)}},
		}},
	}
	pidFile := filepath.Join(t.TempDir(), "unexpected.pid")
	t.Setenv("MAISTR0_FAKE_ENGINE_PID_FILE", pidFile)
	options := testOptions()
	options.NetworkAddresses = staticNetworkAddressSource{{Interface: "eth0", IP: net.ParseIP("10.20.0.1")}}
	manager, err := New(cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	err = manager.Start(context.Background(), "vllm-worker", &ShardPlan{
		NNodes: 2, NodeRank: 1, MasterAddr: "10.20.0.2",
	})
	if !errors.Is(err, ErrUntrustedShardNetwork) {
		t.Fatalf("Start() error = %v, want untrusted-network refusal", err)
	}
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fake vLLM process was launched before preflight: stat error = %v", err)
	}
}

func TestRemoteRPCBindIsRejectedEvenWithCompatibilityFlag(t *testing.T) {
	binary := buildFakeEngine(t)
	cfg := config.NodeConfig{
		ManagedEngineBinaryAllowlist: []string{filepath.Dir(binary)},
		ManagedEngines: []config.ManagedEngineConfig{{
			ID: "rpc-remote", Kind: config.ManagedEngineGGMLRPCServer, BinaryPath: binary,
			Options: config.ManagedEngineOptions{GGMLRPCServer: &config.GGMLRPCServerOptions{
				BindAddress: "0.0.0.0", Port: 50052, AllowRemoteBind: true,
			}},
		}},
	}
	if _, err := New(cfg, Options{}); err == nil || !strings.Contains(err.Error(), "must be loopback") {
		t.Fatalf("remote RPC config error = %v, want loopback-only rejection", err)
	}
}

func TestLlamaServerRPCRelayPlanBecomesLoopbackArgv(t *testing.T) {
	binary := buildFakeEngine(t)
	cfg := testConfig(binary, config.ManagedEngineLlamaServer, freePort(t))
	manager, err := New(cfg, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	manager.SetRPCRelayController(testRPCRelayController{})
	engine, err := manager.engine("test-engine")
	if err != nil {
		t.Fatal(err)
	}
	plan := &ShardPlan{
		TensorSplit: "2",
		RPCRelays: []RPCRelayTarget{
			{PeerID: "worker-a", Address: "10.0.0.2:7452"},
		},
	}
	launchPlan, err := manager.prepareLaunchPlan(context.Background(), engine, plan)
	if err != nil {
		t.Fatal(err)
	}
	args, err := buildArgs(engine.config, launchPlan)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, "\x00")
	if !strings.Contains(joined, "--rpc\x00127.0.0.1:50000") {
		t.Fatalf("llama-server RPC argv = %#v, want loopback relay endpoint", args)
	}
	if strings.Contains(joined, "10.0.0.2") {
		t.Fatalf("llama-server argv exposed worker's LAN address: %#v", args)
	}
}

type testRPCRelayController struct{}

func (testRPCRelayController) StartWorker(context.Context, string, string) (string, error) {
	return "192.0.2.10:7452", nil
}

func (testRPCRelayController) StartHead(_ context.Context, _ string, targets []RPCRelayTarget) ([]string, error) {
	addresses := make([]string, len(targets))
	for index := range targets {
		addresses[index] = net.JoinHostPort("127.0.0.1", strconv.Itoa(50000+index))
	}
	return addresses, nil
}

func (testRPCRelayController) Stop(context.Context, string) error { return nil }

func TestSanitizedProcessOutputRedactsCredentialForms(t *testing.T) {
	line := []byte(`api_key="secret-one" AWS_SECRET_ACCESS_KEY=secret-five Authorization: Bearer secret-two URL=https://user:secret-three@example.test/?token=secret-four`)
	sanitized := sanitizedProcessOutput(line, false)
	for _, secret := range []string{"secret-one", "secret-two", "secret-three", "secret-four", "secret-five"} {
		if strings.Contains(sanitized, secret) {
			t.Errorf("sanitized output %q contains %q", sanitized, secret)
		}
	}
}

func testOptions() Options {
	return Options{
		ReadinessTimeout: 2 * time.Second,
		ProbeInterval:    10 * time.Millisecond,
		StopTimeout:      2 * time.Second,
		Restart:          RestartPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond},
		NetworkAddresses: staticNetworkAddressSource{{Interface: "test0", IP: net.ParseIP("10.20.0.1")}},
	}
}

func testConfig(binary string, kind config.ManagedEngineKind, port int) config.NodeConfig {
	return config.NodeConfig{
		ManagedEngineBinaryAllowlist: []string{filepath.Dir(binary)},
		ManagedEngines: []config.ManagedEngineConfig{{
			ID: "test-engine", Kind: kind, BinaryPath: binary, ModelID: "fake/model",
			Options: config.ManagedEngineOptions{LlamaServer: &config.LlamaServerOptions{
				Host: "127.0.0.1", Port: port,
			}},
		}},
	}
}

func buildFakeEngine(t *testing.T) string {
	t.Helper()
	name := "fake-engine"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-o", binary, "./testdata/fakeengine")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build fake engine: %v\n%s", err, output)
	}
	return binary
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(strings.Split(listener.Addr().String(), ":")[1])
	closeErr := listener.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return port
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parse pid file: %v", err)
	}
	return pid
}

func waitPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return readPID(t, path)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pid file %q was not created", path)
	return 0
}

func waitForStatus(t *testing.T, manager *Manager, id string, ready func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := manager.Status(id)
		if err != nil {
			t.Fatal(err)
		}
		if ready(status) {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, _ := manager.Status(id)
	t.Fatalf("managed engine status did not reach the expected state: %+v", status)
	return Status{}
}

func waitProcessDead(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d is still alive after managed engine shutdown", pid)
}
