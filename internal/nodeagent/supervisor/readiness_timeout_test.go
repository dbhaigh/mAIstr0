package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
)

func TestReadinessTimeoutByEngineKind(t *testing.T) {
	for _, test := range []struct {
		name          string
		options       Options
		worker, model time.Duration
	}{
		{"defaults", Options{}, 30 * time.Second, 10 * time.Minute},
		{"explicit common override", Options{ReadinessTimeout: time.Second}, time.Second, time.Second},
		{"separate model override", Options{ReadinessTimeout: time.Second, ModelReadinessTimeout: time.Minute}, time.Second, time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := &Manager{options: normalizeOptions(test.options)}
			for _, entry := range []struct {
				kind config.ManagedEngineKind
				want time.Duration
			}{
				{config.ManagedEngineGGMLRPCServer, test.worker},
				{config.ManagedEngineLlamaServer, test.model},
				{config.ManagedEngineVLLM, test.model},
			} {
				got := manager.readinessTimeout(&managedEngine{config: config.ManagedEngineConfig{Kind: entry.kind}})
				if got != entry.want {
					t.Fatalf("%s readiness timeout=%s want=%s", entry.kind, got, entry.want)
				}
			}
		})
	}
}

func TestModelStartupAndRestartOutliveWorkerReadinessWindow(t *testing.T) {
	binary := buildFakeEngine(t)
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "http")
	t.Setenv("MAISTR0_FAKE_ENGINE_START_DELAY_MS", "60")
	manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), Options{
		ReadinessTimeout: 10 * time.Millisecond, ModelReadinessTimeout: 2 * time.Second,
		ProbeInterval: 5 * time.Millisecond, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(manager)
	if err := manager.Start(context.Background(), "test-engine", nil); err != nil {
		t.Fatalf("slow initial model load: %v", err)
	}
	if err := manager.Restart(context.Background(), "test-engine", nil); err != nil {
		t.Fatalf("slow restarted model load: %v", err)
	}
	status, err := manager.Status("test-engine")
	if err != nil || status.State != stateReady {
		t.Fatalf("status=%+v err=%v", status, err)
	}
}

func TestLongModelStartupStillHonorsCallerDeadline(t *testing.T) {
	binary := buildFakeEngine(t)
	t.Setenv("MAISTR0_FAKE_ENGINE_MODE", "never-ready")
	manager, err := New(testConfig(binary, config.ManagedEngineLlamaServer, freePort(t)), Options{
		ProbeInterval: 5 * time.Millisecond, StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(manager)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err = manager.Start(ctx, "test-engine", nil)
	if !errors.Is(err, ErrReadinessTimeout) {
		t.Fatalf("caller deadline=%v", err)
	}
	status, statusErr := manager.Status("test-engine")
	if statusErr != nil || status.PID != 0 || status.State != stateFailed {
		t.Fatalf("startup not cleaned up: %+v err=%v", status, statusErr)
	}
}
