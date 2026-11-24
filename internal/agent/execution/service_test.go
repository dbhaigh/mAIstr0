package execution

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServiceProcessStopsAndJoinsOwnedProcessTree(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	workingDirectory := t.TempDir()
	resultDirectory := t.TempDir()
	started := filepath.Join(resultDirectory, "started")
	orphaned := filepath.Join(resultDirectory, "orphaned")
	process, err := StartServiceProcess(ServiceConfig{
		Executable: executable,
		Args: []string{
			"-test.run=^TestExecutionProcessHelper$", "--", "--execution-helper", "spawn", orphaned, started,
		},
		WorkingDirectory: workingDirectory,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Close(); err != nil {
			t.Errorf("close service process: %v", err)
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("service helper did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if exited, err := process.Status(); exited || err != nil {
		t.Fatalf("running service status = exited %t, error %v", exited, err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("stop service process: %v", err)
	}
	if exited, err := process.Status(); !exited || err != nil {
		t.Fatalf("stopped service status = exited %t, error %v", exited, err)
	}
	time.Sleep(850 * time.Millisecond)
	if _, err := os.Stat(orphaned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service process outlived Close: %v", err)
	}
}

func TestStartServiceProcessRejectsInvalidConfiguration(t *testing.T) {
	if _, err := StartServiceProcess(ServiceConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing service configuration error = %v", err)
	}
}
