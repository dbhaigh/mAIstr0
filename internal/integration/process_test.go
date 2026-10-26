package integration

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/orchestrator"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestOrchestratorProcessAPI(t *testing.T) {
	address := freeTCPAddress(t)
	memoryPath := filepath.Join(t.TempDir(), "orchestrator.db")
	process := startOrchestratorProcess(t, address, memoryPath)

	localClient := &http.Client{Timeout: 3 * time.Second}
	statusResponse, err := localClient.Get("http://" + address + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		Role        string `json:"role"`
		Persistence struct {
			Available bool `json:"available"`
		} `json:"persistence"`
	}
	if err := json.NewDecoder(statusResponse.Body).Decode(&status); err != nil {
		_ = statusResponse.Body.Close()
		t.Fatal(err)
	}
	_ = statusResponse.Body.Close()
	if statusResponse.StatusCode != http.StatusOK || status.Role != "orchestrator" || !status.Persistence.Available {
		t.Fatalf("unexpected process status: code=%d status=%+v", statusResponse.StatusCode, status)
	}

	var pairing struct {
		ID  string `json:"orchestrator_id"`
		PIN string `json:"pin"`
	}
	pairingResponse, err := localClient.Get("http://" + address + "/api/pairing/status")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(pairingResponse.Body).Decode(&pairing); err != nil {
		_ = pairingResponse.Body.Close()
		t.Fatal(err)
	}
	_ = pairingResponse.Body.Close()
	if pairingResponse.StatusCode != http.StatusOK || pairing.ID == "" || pairing.PIN == "" {
		t.Fatalf("unexpected pairing response: code=%d status=%+v", pairingResponse.StatusCode, pairing)
	}

	node, err := peeridentity.Open(filepath.Join(t.TempDir(), "node.identity"), "process-test-node")
	if err != nil {
		t.Fatal(err)
	}
	joinBody, err := json.Marshal(map[string]string{
		"id": node.ID(), "role": "node", "pin": pairing.PIN, "certificate_pem": node.CertificatePEM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	bootstrapClient := &http.Client{
		Timeout:   3 * time.Second,
		Transport: node.BootstrapTransport(),
	}
	joinResponse, err := bootstrapClient.Post("https://"+address+"/api/pairing/join", "application/json", bytes.NewReader(joinBody))
	if err != nil {
		t.Fatal(err)
	}
	var joined struct {
		ID             string `json:"id"`
		Role           string `json:"role"`
		CertificatePEM string `json:"certificate_pem"`
	}
	if err := json.NewDecoder(joinResponse.Body).Decode(&joined); err != nil {
		_ = joinResponse.Body.Close()
		t.Fatal(err)
	}
	_ = joinResponse.Body.Close()
	if joinResponse.StatusCode != http.StatusOK || joined.ID != pairing.ID ||
		joined.Role != "orchestrator" || joined.CertificatePEM == "" {
		t.Fatalf("unexpected pairing join response: code=%d response=%+v", joinResponse.StatusCode, joined)
	}
	if err := node.TrustPeerDetails(joined.ID, joined.CertificatePEM, joined.Role, "https://"+address); err != nil {
		t.Fatal(err)
	}

	pairedClient := &http.Client{Timeout: 5 * time.Second, Transport: node.ClientTransport()}
	// Deliberately skip server trust here so the request reaches authorization without presenting a client certificate.
	unauthenticated := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS13,
		}},
	}
	response, err := unauthenticated.Get("https://" + address + "/api/agent/stats")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unpaired TLS request returned %d, want 403", response.StatusCode)
	}
	response, err = pairedClient.Get("https://" + address + "/api/agent/stats")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("paired TLS request returned %d, want 200", response.StatusCode)
	}

	sessionBody := strings.NewReader(`{"title":"process persistence","max_steps":4}`)
	response, err = localClient.Post("http://"+address+"/api/agent/sessions", "application/json", sessionBody)
	if err != nil {
		t.Fatal(err)
	}
	var session struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&session); err != nil {
		_ = response.Body.Close()
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusCreated || session.ID == "" {
		t.Fatalf("session creation returned %d with id %q", response.StatusCode, session.ID)
	}

	fakeWorker, workerAddress := startStreamingWorker(t, node)
	registerBody, err := json.Marshal(cluster.NodeStatus{
		ID: node.ID(), Address: "https://" + workerAddress,
		Models: []engine.Model{{Name: "process-test-model", Engine: "ollama", Tags: []string{"general"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "https://"+address+"/api/nodes/register", bytes.NewReader(registerBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err = pairedClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("register paired node returned %d", response.StatusCode)
	}

	streamBody := strings.NewReader(`{"model":"process-test-model","stream":true,"messages":[{"role":"user","content":"say hello"}]}`)
	request, err = http.NewRequest(http.MethodPost, "https://"+address+"/v1/chat/completions", streamBody)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err = pairedClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") ||
		!strings.Contains(string(stream), "process-stream-ok") || !strings.Contains(string(stream), "data: [DONE]") {
		t.Fatalf("unexpected process inference stream: status=%d content-type=%q body=%s",
			response.StatusCode, response.Header.Get("Content-Type"), stream)
	}
	_ = fakeWorker.Close()

	process.stop(t)
	process = startOrchestratorProcess(t, address, memoryPath)
	response, err = pairedClient.Get("https://" + address + "/api/agent/sessions/" + url.PathEscape(session.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(response.Body)
	var restored struct {
		ID    string `json:"id"`
		Title string `json:"title"`
	}
	if err := json.NewDecoder(response.Body).Decode(&restored); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || restored.ID != session.ID || restored.Title != "process persistence" {
		t.Fatalf("session did not survive process restart: status=%d session=%+v", response.StatusCode, restored)
	}
	process.stop(t)
}

func TestProcessHelper(t *testing.T) {
	if os.Getenv("MAISTR0_PROCESS_TEST_CHILD") != "1" {
		return
	}
	address := os.Getenv("MAISTR0_PROCESS_TEST_ADDRESS")
	memoryPath := os.Getenv("MAISTR0_PROCESS_TEST_MEMORY")
	server, err := orchestrator.NewWithMemoryAndHarness(memoryPath, "pi")
	if err != nil {
		t.Fatal(err)
	}
	server.SetDiscoveryEnabled(false)
	if err := server.SetAdvertiseAddress("https://" + address); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Run(address) }()
	select {
	case err := <-errCh:
		t.Fatalf("orchestrator server stopped: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	select {}
}

type orchestratorProcess struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
	output  *bytes.Buffer
}

func startOrchestratorProcess(t *testing.T, address, memoryPath string) *orchestratorProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	output := new(bytes.Buffer)
	command := exec.Command(executable, "-test.run=^TestProcessHelper$")
	command.Env = append(os.Environ(),
		"MAISTR0_PROCESS_TEST_CHILD=1",
		"MAISTR0_PROCESS_TEST_ADDRESS="+address,
		"MAISTR0_PROCESS_TEST_MEMORY="+memoryPath,
	)
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &orchestratorProcess{command: command, done: make(chan struct{}), output: output}
	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		select {
		case <-process.done:
			return
		default:
			_ = command.Process.Kill()
			<-process.done
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	client := &http.Client{Timeout: 250 * time.Millisecond}
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			t.Fatalf("orchestrator process exited before becoming ready: %v\n%s", process.waitErr, output.String())
		default:
		}
		response, err := client.Get("http://" + address + "/api/status")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return process
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = command.Process.Kill()
	<-process.done
	t.Fatalf("orchestrator process did not become ready at %s\n%s", address, output.String())
	return nil
}

func (p *orchestratorProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		return
	default:
	}
	if err := p.command.Process.Kill(); err != nil {
		select {
		case <-p.done:
			return
		default:
			t.Errorf("stop orchestrator process: %v", err)
		}
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("orchestrator process did not stop")
	}
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func startStreamingWorker(t *testing.T, identity *peeridentity.Identity) (*http.Server, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /generate", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"output\":\"process-stream-ok\"}\n\n")
	})
	server := &http.Server{Handler: mux, TLSConfig: identity.ServerTLSConfig()}
	go func() {
		_ = server.Serve(tls.NewListener(listener, server.TLSConfig))
	}()
	t.Cleanup(func() { _ = server.Close() })
	return server, listener.Addr().String()
}
