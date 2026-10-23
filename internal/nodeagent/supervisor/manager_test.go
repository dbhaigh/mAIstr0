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
	if err := validatePlatformAndPlan(configured, &ShardPlan{NNodes: 2, NodeRank: 2, MasterAddr: "node"}); !errors.Is(err, ErrInvalidShardPlan) && runtime.GOOS != "windows" {
		t.Fatalf("invalid node rank error = %v, want invalid plan", err)
	}
}

func TestRemoteRPCBindWarningIsRecorded(t *testing.T) {
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
	manager, err := New(cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	status, err := manager.Status("rpc-remote")
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Warnings) != 1 || !strings.Contains(status.Warnings[0], "non-loopback") {
		t.Fatalf("remote RPC warning = %v", status.Warnings)
	}
}

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
