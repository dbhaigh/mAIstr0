package nodecontrol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestUpgradeActionForwardsOneValidatedSourceToRunningNode(t *testing.T) {
	var calls int
	var received string
	var contentType string
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		contentType = r.Header.Get("Content-Type")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded upgrade request: %v", err)
		}
		received = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"state":"queued"}`))
	}))
	defer node.Close()
	runtimePath := filepath.Join(t.TempDir(), "runtime.json")
	if err := config.SaveRuntimeSettings(runtimePath, config.RuntimeSettings{
		AllowPeerUpdates: testBoolValue(true),
	}); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{
		localNodeURL: node.URL,
		runtimePath:  runtimePath,
		child:        &childProcess{},
	}

	request := httptest.NewRequest(http.MethodPost, "/api/service/upgrade", strings.NewReader(`{"peer_id":"trusted-node"}`))
	request.SetPathValue("action", "upgrade")
	response := httptest.NewRecorder()
	controller.handleAction(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("upgrade response status = %d, want 202: %s", response.Code, response.Body.String())
	}
	if calls != 1 || contentType != "application/json" || received != `{"peer_id":"trusted-node"}` {
		t.Fatalf("forwarded request: calls=%d content-type=%q body=%q", calls, contentType, received)
	}

	invalid := httptest.NewRequest(http.MethodPost, "/api/service/upgrade", strings.NewReader(`{"path":"relative.exe"}`))
	invalid.SetPathValue("action", "upgrade")
	invalidResponse := httptest.NewRecorder()
	controller.handleAction(invalidResponse, invalid)
	if invalidResponse.Code != http.StatusBadRequest {
		t.Fatalf("invalid upgrade source status = %d, want 400", invalidResponse.Code)
	}
	if calls != 1 {
		t.Fatalf("invalid upgrade request reached node controller; calls = %d", calls)
	}
}

func TestPeerUpgradeGateUsesCurrentPersistedRuntimeSettings(t *testing.T) {
	var calls int
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"state":"queued"}`))
	}))
	defer node.Close()

	runtimePath := filepath.Join(t.TempDir(), "runtime.json")
	if err := config.SaveRuntimeSettings(runtimePath, config.RuntimeSettings{
		AllowPeerUpdates: testBoolValue(false),
	}); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{
		localNodeURL: node.URL,
		runtimePath:  runtimePath,
		child:        &childProcess{},
		runtimeSettings: config.RuntimeSettings{
			AllowPeerUpdates: testBoolValue(false),
		},
	}

	if err := config.SaveRuntimeSettings(runtimePath, config.RuntimeSettings{
		AllowPeerUpdates: testBoolValue(true),
	}); err != nil {
		t.Fatal(err)
	}
	enabledRequest := httptest.NewRequest(http.MethodPost, "/api/service/upgrade",
		strings.NewReader(`{"peer_id":"trusted-node"}`))
	enabledRequest.SetPathValue("action", "upgrade")
	enabled := httptest.NewRecorder()
	controller.handleAction(enabled, enabledRequest)
	if enabled.Code != http.StatusAccepted || calls != 1 {
		t.Fatalf("updated peer policy response = %d, forwarded calls = %d, body = %s", enabled.Code, calls, enabled.Body.String())
	}

	if err := config.SaveRuntimeSettings(runtimePath, config.RuntimeSettings{
		AllowPeerUpdates: testBoolValue(true),
		NoPeerUpdates:    testBoolValue(true),
	}); err != nil {
		t.Fatal(err)
	}
	disabledRequest := httptest.NewRequest(http.MethodPost, "/api/service/upgrade",
		strings.NewReader(`{"peer_id":"trusted-node"}`))
	disabledRequest.SetPathValue("action", "upgrade")
	disabled := httptest.NewRecorder()
	controller.handleAction(disabled, disabledRequest)
	if disabled.Code != http.StatusForbidden || calls != 1 {
		t.Fatalf("disabled peer policy response = %d, forwarded calls = %d, body = %s", disabled.Code, calls, disabled.Body.String())
	}
}

func TestStoppedUpgradeStartsNodeAndForwardsAfterCurrentAuthorityIsReady(t *testing.T) {
	var upgradeCalls int
	var received string
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/lifecycle/authority":
			_, _ = io.WriteString(w, `{"orchestrator_id":"current-orchestrator"}`)
		case "/api/upgrade":
			upgradeCalls++
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read forwarded upgrade request: %v", err)
			}
			received = string(body)
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"state":"queued"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer node.Close()
	controller := &Controller{
		localNodeURL: node.URL,
		wake:         make(chan struct{}, 1),
	}

	request := httptest.NewRequest(http.MethodPost, "/api/service/upgrade", strings.NewReader(`{"path":"C:\\maistr0\\next.exe"}`))
	request.SetPathValue("action", "upgrade")
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{
		Subject: pkix.Name{CommonName: "current-orchestrator"},
	}}}
	response := httptest.NewRecorder()
	controller.handleAction(response, request)
	if response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), `"state":"starting"`) {
		t.Fatalf("stopped upgrade response = %d %s", response.Code, response.Body.String())
	}

	child := &childProcess{done: make(chan struct{})}
	controller.mu.Lock()
	controller.child = child
	controller.mu.Unlock()
	deadline := time.Now().Add(4 * time.Second)
	for {
		controller.mu.RLock()
		queued := controller.pendingUpgrade == nil && controller.upgradeStatus != nil &&
			controller.upgradeStatus.State == "queued"
		controller.mu.RUnlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("controller did not forward the upgrade after the child authority became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if upgradeCalls != 1 || received != `{"path":"C:\\maistr0\\next.exe"}` {
		t.Fatalf("forwarded upgrade calls=%d body=%q", upgradeCalls, received)
	}
	if !controller.actionMu.TryLock() {
		t.Fatal("upgrade operation did not release the lifecycle action lock")
	}
	controller.actionMu.Unlock()
	close(child.done)
}

func TestStoppedUpgradeRejectsChangedOrchestratorAuthority(t *testing.T) {
	var upgradeCalls int
	var childDone chan struct{}
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/lifecycle/authority":
			_, _ = io.WriteString(w, `{"orchestrator_id":"different-orchestrator"}`)
		case "/api/upgrade":
			upgradeCalls++
			w.WriteHeader(http.StatusAccepted)
		case "/api/lifecycle/stop":
			if childDone != nil {
				select {
				case <-childDone:
				default:
					close(childDone)
				}
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer node.Close()
	controller := &Controller{
		localNodeURL: node.URL,
		wake:         make(chan struct{}, 1),
	}
	request := httptest.NewRequest(http.MethodPost, "/api/service/upgrade", strings.NewReader(`{"path":"C:\\maistr0\\next.exe"}`))
	request.SetPathValue("action", "upgrade")
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{
		Subject: pkix.Name{CommonName: "current-orchestrator"},
	}}}
	response := httptest.NewRecorder()
	controller.handleAction(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("stopped upgrade response = %d %s", response.Code, response.Body.String())
	}
	child := &childProcess{done: make(chan struct{})}
	childDone = child.done
	controller.mu.Lock()
	controller.child = child
	controller.mu.Unlock()

	deadline := time.Now().Add(4 * time.Second)
	for {
		controller.mu.RLock()
		failed := controller.upgradeStatus != nil && controller.upgradeStatus.State == "failed"
		controller.mu.RUnlock()
		if failed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("controller did not reject the changed orchestrator authority")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if upgradeCalls != 0 {
		t.Fatalf("stale orchestrator request was forwarded %d times", upgradeCalls)
	}
	controller.mu.RLock()
	failureReason := controller.upgradeStatus.FailureReason
	controller.mu.RUnlock()
	if !strings.Contains(failureReason, "authority changed") {
		t.Fatalf("upgrade failure reason = %q", failureReason)
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
