package supervisor

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
)

func TestManagerControlCharacterizationWithoutRunningProcess(t *testing.T) {
	binary := buildFakeEngine(t)
	for _, test := range []struct {
		name string
		run  func(*testing.T, *Manager)
	}{
		{
			name: "stop before any start is idempotent",
			run: func(t *testing.T, manager *Manager) {
				for range 2 {
					if err := manager.Stop(context.Background(), "test-engine"); err != nil {
						t.Fatalf("Stop() error = %v, want nil", err)
					}
					status, err := manager.Status("test-engine")
					if err != nil || status.State != stateStopped || status.PID != 0 || status.LastError != "" {
						t.Fatalf("status = %+v, error = %v; want clean stopped state", status, err)
					}
				}
			},
		},
		{
			name: "unknown engine is not found for every control",
			run: func(t *testing.T, manager *Manager) {
				if err := manager.Start(context.Background(), "missing", nil); !errors.Is(err, ErrNotFound) {
					t.Errorf("Start() error = %v, want ErrNotFound", err)
				}
				if err := manager.Stop(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
					t.Errorf("Stop() error = %v, want ErrNotFound", err)
				}
				if err := manager.Restart(context.Background(), "missing", nil); !errors.Is(err, ErrNotFound) {
					t.Errorf("Restart() error = %v, want ErrNotFound", err)
				}
				if _, err := manager.Status("missing"); !errors.Is(err, ErrNotFound) {
					t.Errorf("Status() error = %v, want ErrNotFound", err)
				}
			},
		},
		{
			name: "closed supervisor refuses controls and close is idempotent",
			run: func(t *testing.T, manager *Manager) {
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				if err := manager.Start(context.Background(), "test-engine", nil); !errors.Is(err, ErrClosed) {
					t.Errorf("Start() after Close error = %v, want ErrClosed", err)
				}
				if err := manager.Stop(context.Background(), "test-engine"); !errors.Is(err, ErrClosed) {
					t.Errorf("Stop() after Close error = %v, want ErrClosed", err)
				}
				if err := manager.Restart(context.Background(), "test-engine", nil); !errors.Is(err, ErrClosed) {
					t.Errorf("Restart() after Close error = %v, want ErrClosed", err)
				}
				if err := manager.Close(); err != nil {
					t.Errorf("second Close() error = %v, want nil", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), testOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer closeutil.Discard(manager)
			test.run(t, manager)
		})
	}
}

func TestManagerStartupFailureLeavesRetryableFailedState(t *testing.T) {
	for _, test := range []struct {
		name          string
		prepare       func(t *testing.T, binary string)
		wantErr       error
		wantLastError string
	}{
		{
			name: "process exits before readiness",
			prepare: func(t *testing.T, _ string) {
				t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "exit-immediately")
			},
			wantErr:       ErrProcessExited,
			wantLastError: "process exited before readiness",
		},
		{
			name: "binary removed after configuration",
			prepare: func(t *testing.T, binary string) {
				if err := os.Rename(binary, binary+".moved"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Rename(binary+".moved", binary) })
			},
			wantErr:       ErrStart,
			wantLastError: "managed engine startup failed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			binary := buildFakeEngine(t)
			pidFile := filepath.Join(t.TempDir(), "engine.pid")
			t.Setenv("MAISTR0_FAKE_ENGINE_PID_FILE", pidFile)
			manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), testOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer closeutil.Discard(manager)
			test.prepare(t, binary)

			err = manager.Start(context.Background(), "test-engine", nil)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Start() error = %v, want %v", err, test.wantErr)
			}
			status, statusErr := manager.Status("test-engine")
			if statusErr != nil || status.State != stateFailed || status.PID != 0 ||
				status.RestartCount != 0 || status.LastError != test.wantLastError {
				t.Fatalf("failed status = %+v, error = %v", status, statusErr)
			}
			if _, err := os.Stat(pidFile); err == nil {
				waitProcessDead(t, readPID(t, pidFile))
			}

			if _, err := os.Stat(binary); err != nil {
				if err := os.Rename(binary+".moved", binary); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "http")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := manager.Start(ctx, "test-engine", nil); err != nil {
				t.Fatalf("Start() retry after failure error = %v", err)
			}
			status, statusErr = manager.Status("test-engine")
			if statusErr != nil || status.State != stateReady || status.PID == 0 || status.LastError != "" {
				t.Fatalf("retried status = %+v, error = %v", status, statusErr)
			}
		})
	}
}

func TestManagerRejectsDuplicateStartAndStopsIdempotently(t *testing.T) {
	binary := buildFakeEngine(t)
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "http")
	manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(manager)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.Start(ctx, "test-engine", nil); err != nil {
		t.Fatal(err)
	}
	running, err := manager.Status("test-engine")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(ctx, "test-engine", nil); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("duplicate Start() error = %v, want ErrAlreadyRunning", err)
	}
	if current, _ := manager.Status("test-engine"); current.PID != running.PID || current.State != stateReady {
		t.Fatalf("duplicate start changed running process: before %+v after %+v", running, current)
	}
	for range 2 {
		if err := manager.Stop(ctx, "test-engine"); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
		status, _ := manager.Status("test-engine")
		if status.State != stateStopped || status.PID != 0 {
			t.Fatalf("stopped status = %+v", status)
		}
	}
	waitProcessDead(t, running.PID)
}

// TestVLLMNetworkPreflightFailureClosesOutputPipe is a regression test for a
// descriptor leak where the output pipe was opened before the vLLM network
// preflight and not closed when the preflight failed.
func TestVLLMNetworkPreflightFailureClosesOutputPipe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("vLLM launch is rejected on native Windows before the network preflight runs")
	}
	binary := buildFakeEngine(t)
	var (
		mu    sync.Mutex
		pipes []*os.File
	)
	original := openProcessOutputPipe
	openProcessOutputPipe = func() (*os.File, *os.File, error) {
		reader, writer, err := original()
		if err == nil {
			mu.Lock()
			pipes = append(pipes, reader, writer)
			mu.Unlock()
		}
		return reader, writer, err
	}
	t.Cleanup(func() { openProcessOutputPipe = original })

	options := testOptions()
	options.NetworkAddresses = staticNetworkAddressSource{{Interface: "eth0", IP: net.ParseIP("10.20.0.1")}}
	manager, err := New(config.NodeConfig{
		ManagedEngineBinaryAllowlist: []string{filepath.Dir(binary)},
		ManagedEngines: []config.ManagedEngineConfig{{
			ID: "vllm-worker", Kind: config.ManagedEngineVLLM, BinaryPath: binary, ModelID: "repo/model",
			Options: config.ManagedEngineOptions{VLLM: &config.VLLMOptions{Port: freePort(t)}},
		}},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(manager)

	plan := &ShardPlan{NNodes: 2, NodeRank: 1, MasterAddr: "10.20.0.2"}
	for range 3 {
		if err := manager.Start(context.Background(), "vllm-worker", plan); !errors.Is(err, ErrUntrustedShardNetwork) {
			t.Fatalf("Start() error = %v, want untrusted-network refusal", err)
		}
		if status, _ := manager.Status("vllm-worker"); status.State != stateFailed || status.PID != 0 {
			t.Fatalf("status after refused start = %+v, want failed without a process", status)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, file := range pipes {
		if err := file.Close(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("output pipe %s was left open after a refused start (Close error = %v)", file.Name(), err)
		}
	}
}
