package nodecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
)

func TestChildArgsApplyPersistedRuntimeSettings(t *testing.T) {
	settings := config.RuntimeSettings{
		Role:                   stringValue("node"),
		NodeConfigPath:         stringValue(`C:\maistr0\node.json`),
		OrchestratorConfigPath: stringValue(`C:\maistr0\orchestrator.json`),
		NodeListenAddr:         stringValue(":7551"),
		OrchestratorListenAddr: stringValue(":7550"),
		OrchestratorAddr:       stringValue("https://192.0.2.10:7550"),
		TrayMode:               stringValue("hidden"),
		AutoOpenBrowser:        testBoolValue(true),
		DiscoveryEnabled:       testBoolValue(false),
		Harness:                stringValue("deepseek"),
		MemoryPath:             stringValue(`C:\maistr0\memory.db`),
		AllowPeerUpdates:       testBoolValue(true),
		NoPeerUpdates:          testBoolValue(true),
	}

	restartArgs := []string{"controller", "--runtime-config", `C:\maistr0\runtime.json`}
	args, err := childArgs(`C:\maistr0\runtime.json`, "", "https://192.0.2.20:7453", settings, 42, restartArgs, true)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, "\n")
	for _, expected := range []string{
		"--runtime-config\nC:\\maistr0\\runtime.json",
		"--role\nnode",
		"--node-config\nC:\\maistr0\\node.json",
		"--orchestrator-config\nC:\\maistr0\\orchestrator.json",
		"--node-listen\n:7551",
		"--orchestrator-listen\n:7550",
		"--orchestrator-addr\nhttps://192.0.2.10:7550",
		"--tray-mode\nhidden",
		"--auto-open-browser",
		"--no-discovery",
		"--harness\ndeepseek",
		"--memory-path\nC:\\maistr0\\memory.db",
		"--allow-peer-updates=true",
		"--no-peer-updates=true",
		"--host-control-addr\nhttps://192.0.2.20:7453",
		"--host-controller-pid\n42",
		"--host-controller-managed=true",
		"--host-controller-restart-args\n[\"controller\",\"--runtime-config\",\"C:\\\\maistr0\\\\runtime.json\"]",
	} {
		if !strings.Contains(joined, expected) {
			t.Errorf("child arguments do not include %q:\n%s", expected, joined)
		}
	}
	if strings.Contains(joined, "pairing-pin") || strings.Contains(joined, "orchestrator-fingerprint") {
		t.Fatal("one-time pairing credentials must not be persisted or forwarded")
	}
}

func TestChildArgsDisableBrowserByDefault(t *testing.T) {
	args, err := childArgs("runtime.json", "", "https://192.0.2.20:7453", config.RuntimeSettings{}, 42,
		[]string{"controller"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(args, "--no-browser") {
		t.Fatalf("headless controller should suppress browser startup by default: %v", args)
	}
}

func TestSupervisorStopsWhenChildRequestsUpdateHandoff(t *testing.T) {
	target := filepath.Join(t.TempDir(), "maistr0")
	controllerPID := os.Getpid()
	const childPID = 83
	markerPath := filepath.Join(filepath.Dir(target), fmt.Sprintf(".maistr0-handoff-%d-%d.json", controllerPID, childPID))
	marker, err := json.Marshal(map[string]int{"controller_pid": controllerPID, "parent_pid": childPID})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(markerPath, marker, 0600); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan struct{})
	close(childDone)
	controller := &Controller{
		executablePath: target,
		child: &childProcess{
			cmd:  &exec.Cmd{Process: &os.Process{Pid: childPID}},
			done: childDone,
		},
		desiredRunning: true,
	}
	if err := controller.supervise(context.Background()); !errors.Is(err, errPeerUpdateHandoff) {
		t.Fatalf("supervise error = %v, want peer-update handoff", err)
	}
	if controller.desiredRunning {
		t.Fatal("controller still wants to restart the child during update handoff")
	}
}

func TestAdvertisedControlAddress(t *testing.T) {
	got, err := advertisedControlAddress("https://[2001:db8::1]:7451", ":7453")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://[2001:db8::1]:7453" {
		t.Fatalf("advertisedControlAddress() = %q", got)
	}
	if _, err := advertisedControlAddress("https://192.0.2.1:7451", ":0"); err == nil {
		t.Fatal("expected ephemeral controller port to be rejected")
	}
}

func TestLoopbackNodeURL(t *testing.T) {
	got, err := loopbackNodeURL(":7451")
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://127.0.0.1:7451" {
		t.Fatalf("loopbackNodeURL() = %q", got)
	}
	if _, err := loopbackNodeURL(":0"); err == nil {
		t.Fatal("expected ephemeral node port to be rejected")
	}
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func stringValue(value string) *string { return &value }

func testBoolValue(value bool) *bool { return &value }
