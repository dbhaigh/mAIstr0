package integration

import (
	"bytes"
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
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/orchestrator"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestUpgradeRejoinTwoNodeLocalCluster(t *testing.T) {
	directory := t.TempDir()
	dataDir := filepath.Join(directory, "user-data")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
		"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME",
	} {
		t.Setenv(name, dataDir)
	}

	root := repositoryRoot(t)
	installedDir := filepath.Join(directory, "installed")
	sourceDir := filepath.Join(directory, "source")
	for _, path := range []string{installedDir, sourceDir} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	installedBinary := filepath.Join(installedDir, "maistr0"+extension)
	sourceBinary := filepath.Join(sourceDir, "maistr0"+extension)
	buildUpgradeBinary(t, root, installedBinary, "0.1.1.16")
	buildUpgradeBinary(t, root, sourceBinary, "0.1.1.17")

	orchestratorConfig := config.DefaultOrchestratorConfig()
	orchestratorConfig.Mode = config.OrchestratorModeMinimal
	orchestratorConfig.MemoryPath = filepath.Join(directory, "orchestrator.db")
	server, err := orchestrator.NewWithConfig(orchestratorConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close orchestrator: %v", err)
		}
	})
	orchestratorIdentity, err := peeridentity.Open(
		filepath.Join(directory, "orchestrator.identity"),
		"orchestrator-127.0.0.1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.UseIdentity(orchestratorIdentity); err != nil {
		t.Fatal(err)
	}
	server.SetPairingVerificationRequired(false)
	mux := server.Mux()
	pairingPIN, _, active := orchestratorIdentity.ActivePairingPIN()
	if !active {
		t.Fatal("orchestrator pairing PIN is unavailable")
	}
	orchestratorFingerprint, err := peeridentity.CertificateFingerprint(orchestratorIdentity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
	httpsServer := httptest.NewUnstartedServer(handler)
	httpsServer.TLS = orchestratorIdentity.ServerTLSConfig()
	httpsServer.StartTLS()
	t.Cleanup(httpsServer.Close)

	nodeConfig := config.DefaultNodeConfig()
	nodeConfig.NodeID = "upgrade-rejoin-e2e-node"
	nodeConfig.ListenAddr = freeTCPAddress(t)
	nodeConfig.RPCRelayListenAddr = freeTCPAddress(t)
	nodeConfig.AdvertiseAddr = "https://" + nodeConfig.ListenAddr
	nodeConfig.OrchestratorAddr = httpsServer.URL
	nodeConfig.PairingPIN = pairingPIN
	nodeConfig.OrchestratorFingerprint = orchestratorFingerprint
	nodeConfig.MemoryPath = filepath.Join(directory, "node.db")
	nodeConfig.DiscoveryEnabled = false
	nodeConfig.AutoOpenBrowser = false
	nodeConfig.TrayMode = "hidden"
	nodeConfig.Engines = nil
	nodeConfig.Normalize()
	if err := nodeConfig.Validate(); err != nil {
		t.Fatal(err)
	}
	nodeConfigPath := filepath.Join(directory, "node.json")
	nodeConfigData, err := json.Marshal(nodeConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nodeConfigPath, nodeConfigData, 0600); err != nil {
		t.Fatal(err)
	}
	runtimeConfigPath := filepath.Join(directory, "runtime.json")
	args := []string{
		"--role=node",
		"--node-config=" + nodeConfigPath,
		"--runtime-config=" + runtimeConfigPath,
		"--no-discovery",
		"--no-browser",
		"--no-peer-updates",
		"--tray-mode=hidden",
	}
	address := nodeConfig.ListenAddr
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	process, err := startUpgradeE2ENode(installedBinary, directory, args)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopLocalUpgradeNode(client, address)
		waitForUpgradeNodeUnavailable(client, address, 10*time.Second)
		select {
		case <-process.done:
		default:
			_ = process.command.Process.Kill()
			select {
			case <-process.done:
			case <-time.After(5 * time.Second):
				t.Error("initial node process did not stop")
			}
		}
	})
	waitForUpgradeNodeReady(t, client, address, process, 30*time.Second)
	initialCertificate := waitForTrustedNode(t, orchestratorIdentity, nodeConfig.NodeID, process, 30*time.Second)
	initialFingerprint, err := peeridentity.CertificateFingerprint(initialCertificate)
	if err != nil {
		t.Fatal(err)
	}

	var initialPairing struct {
		NodeID      string `json:"node_id"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := getUpgradeNodeJSON(client, address, "/api/pairing/status", &initialPairing); err != nil {
		t.Fatal(err)
	}
	if initialPairing.NodeID != nodeConfig.NodeID || initialPairing.Fingerprint != initialFingerprint {
		t.Fatalf("initial node identity mismatch: %+v", initialPairing)
	}

	body, err := json.Marshal(map[string]string{"path": sourceBinary})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "http://"+address+"/api/upgrade", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	closeutil.Discard(response.Body)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("upgrade request returned %s", response.Status)
	}
	if err := waitUpgradeE2EProcessExit(process, 60*time.Second); err != nil {
		t.Fatal(err)
	}

	waitForUpgradedNode(t, client, address, "0.1.1.17", 60*time.Second)
	waitForConsumedUpgradeTicket(t, client, address, 30*time.Second)
	var upgradedPairing struct {
		NodeID      string `json:"node_id"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := getUpgradeNodeJSON(client, address, "/api/pairing/status", &upgradedPairing); err != nil {
		t.Fatal(err)
	}
	if upgradedPairing.NodeID != nodeConfig.NodeID || upgradedPairing.Fingerprint == initialFingerprint {
		t.Fatalf("upgrade did not retain the node ID and refresh its certificate: %+v", upgradedPairing)
	}
	trustedCertificate, ok := orchestratorIdentity.TrustedPeers()[nodeConfig.NodeID]
	if !ok {
		t.Fatal("orchestrator lost the upgraded node from its trust roster")
	}
	trustedFingerprint, err := peeridentity.CertificateFingerprint(trustedCertificate)
	if err != nil {
		t.Fatal(err)
	}
	if trustedFingerprint != upgradedPairing.Fingerprint {
		t.Fatalf("orchestrator trust fingerprint %q does not match upgraded node %q",
			trustedFingerprint, upgradedPairing.Fingerprint)
	}

	if !stopLocalUpgradeNode(client, address) {
		t.Fatal("upgraded node did not accept the loopback stop request")
	}
	if !waitForUpgradeNodeUnavailable(client, address, 15*time.Second) {
		t.Fatal("upgraded node remained available after the stop request")
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}

func buildUpgradeBinary(t *testing.T, root, output, version string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(
		ctx, "go", "build",
		"-ldflags", "-X=github.com/maistr0/maistr0/internal/version.Value="+version,
		"-o", output, "./cmd/maistr0",
	)
	command.Dir = root
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s binary: %v\n%s", version, err, data)
	}
}

type upgradeE2EProcess struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
	output  *upgradeE2EOutput
}

type upgradeE2EOutput struct {
	mu   sync.Mutex
	data []byte
}

func (output *upgradeE2EOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	remaining := (32 << 10) - len(output.data)
	if remaining > 0 {
		output.data = append(output.data, data[:min(len(data), remaining)]...)
	}
	return len(data), nil
}

func (output *upgradeE2EOutput) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	return string(output.data)
}

func startUpgradeE2ENode(binary, directory string, args []string) (*upgradeE2EProcess, error) {
	command := exec.Command(binary, args...)
	command.Dir = directory
	output := &upgradeE2EOutput{}
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start node process: %w", err)
	}
	process := &upgradeE2EProcess{command: command, done: make(chan struct{}), output: output}
	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()
	return process, nil
}

func waitForUpgradeNodeReady(t *testing.T, client *http.Client, address string, process *upgradeE2EProcess, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := getUpgradeNodeJSON(client, address, "/api/status", &map[string]any{}); err == nil {
			return
		} else {
			lastErr = err
		}
		select {
		case <-process.done:
			t.Fatalf("node exited before readiness: %v\n%s", process.waitErr, process.output.String())
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node did not become ready: %v\n%s", lastErr, process.output.String())
}

func waitForTrustedNode(t *testing.T, identity *peeridentity.Identity, nodeID string, process *upgradeE2EProcess, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if certificate, ok := identity.TrustedPeers()[nodeID]; ok {
			return certificate
		}
		select {
		case <-process.done:
			t.Fatalf("node exited before pairing: %v\n%s", process.waitErr, process.output.String())
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("orchestrator did not trust node %q after pairing", nodeID)
	return ""
}

func waitUpgradeE2EProcessExit(process *upgradeE2EProcess, timeout time.Duration) error {
	select {
	case <-process.done:
		if process.waitErr != nil {
			return fmt.Errorf("initial node process exited with error: %w\n%s", process.waitErr, process.output.String())
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("initial node process did not exit for upgrade\n%s", process.output.String())
	}
}

func waitForUpgradedNode(t *testing.T, client *http.Client, address, expectedVersion string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var result struct {
			Version string `json:"version"`
		}
		if err := getUpgradeNodeJSON(client, address, "/api/version", &result); err == nil &&
			result.Version == expectedVersion {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node did not restart on version %s", expectedVersion)
}

func waitForConsumedUpgradeTicket(t *testing.T, client *http.Client, address string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	var lastStatus struct {
		UpgradeStatus *struct {
			State        string `json:"state"`
			TicketStatus string `json:"ticket_status"`
		} `json:"upgrade_status"`
	}
	for time.Now().Before(deadline) {
		lastStatus = struct {
			UpgradeStatus *struct {
				State        string `json:"state"`
				TicketStatus string `json:"ticket_status"`
			} `json:"upgrade_status"`
		}{}
		lastErr = getUpgradeNodeJSON(client, address, "/api/lifecycle/status", &lastStatus)
		if lastErr == nil && lastStatus.UpgradeStatus != nil && lastStatus.UpgradeStatus.State == "completed" &&
			lastStatus.UpgradeStatus.TicketStatus == "consumed" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("upgraded node did not report a consumed rejoin ticket: status=%+v error=%v", lastStatus, lastErr)
}

func getUpgradeNodeJSON(client *http.Client, address, path string, target any) error {
	response, err := client.Get("http://" + address + path)
	if err != nil {
		return err
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %s", path, response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decode GET %s: %w", path, err)
	}
	return nil
}

func stopLocalUpgradeNode(client *http.Client, address string) bool {
	request, err := http.NewRequest(http.MethodPost, "http://"+address+"/api/lifecycle/stop", nil)
	if err != nil {
		return false
	}
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer closeutil.Discard(response.Body)
	return response.StatusCode == http.StatusAccepted
}

func waitForUpgradeNodeUnavailable(client *http.Client, address string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + address + "/api/version")
		if err != nil {
			return true
		}
		closeutil.Discard(response.Body)
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
