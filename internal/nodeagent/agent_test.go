package nodeagent

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

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

func TestPairingConnectEndpointEnrollsNode(t *testing.T) {
	orchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-pair-test")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer server.Close()

	agent := New(config.NodeConfig{
		NodeID: "node-pair-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	requestBody, _ := json.Marshal(map[string]string{"address": server.URL, "pin": "123456"})
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

func TestPairingStatusRequestsPINForUntrustedOrchestrator(t *testing.T) {
	agent := New(config.NodeConfig{
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

func TestFormerOrchestratorWorkerJoinsNewLeader(t *testing.T) {
	oldLeader, err := peeridentity.Open(filepath.Join(t.TempDir(), "old.json"), "old-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	newLeader, err := peeridentity.Open(filepath.Join(t.TempDir(), "new.json"), "new-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	agent := New(config.NodeConfig{
		NodeID: "former-orchestrator-worker", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer agent.Memory().Close()
	if err := agent.PairLocalOrchestrator(oldLeader.ID(), oldLeader.CertificatePEM(), "https://127.0.0.1:7450"); err != nil {
		t.Fatal(err)
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
	body, err := json.Marshal(map[string]string{"address": server.URL, "pin": "123456"})
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
	defer registration.Body.Close()
	if registration.StatusCode != http.StatusNoContent || !registered {
		t.Fatalf("former orchestrator worker could not register: %s", registration.Status)
	}
	if agent.getOrchestratorID() != newLeader.ID() || agent.getOrchestratorAddr() != server.URL {
		t.Fatal("worker retained its surrendered local orchestrator")
	}
}

func TestNodeControlInspectsLocalEngineWithoutSelfTLSRequest(t *testing.T) {
	agent := New(config.NodeConfig{
		NodeID: "node-local-control-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer agent.Memory().Close()
	agent.engines = []engine.Engine{engine.NewSimulated()}

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

	agent := New(config.NodeConfig{
		NodeID: "node-control-client", OrchestratorAddr: server.URL,
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer agent.Memory().Close()
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

func TestNodeModelsLocalRequestsWithoutSelfTLS(t *testing.T) {
	agent := New(config.NodeConfig{
		NodeID: "node-local-models-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer agent.Memory().Close()
	agent.engines = []engine.Engine{engine.NewSimulated()}
	agent.enginesConfigured = true

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
	agent := New(config.NodeConfig{
		NodeID: "node-models-client", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer agent.Memory().Close()
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
	agent := &Agent{engines: []engine.Engine{first, target}}
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
	agent := &Agent{engines: []engine.Engine{&pullTestEngine{name: "configured"}}}
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
	pairServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer pairServer.Close()

	agent := New(config.NodeConfig{
		NodeID: "node-invitation-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	defer agent.Memory().Close()
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

	acceptResponse := httptest.NewRecorder()
	agent.handlePairingInvitationAccept(acceptResponse, httptest.NewRequest(http.MethodPost, "/api/pairing/invitation/accept", nil))
	if acceptResponse.Code != http.StatusOK || !agent.identity.IsTrustedID(orchestrator.ID()) {
		t.Fatalf("local acceptance returned %d: %s", acceptResponse.Code, acceptResponse.Body.String())
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
	if DefaultNodeID() != DefaultNodeID() {
		t.Fatal("expected default node ID to remain stable")
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

func TestNodeStatusIncludesCopiedGPUHistory(t *testing.T) {
	agent := New(config.NodeConfig{
		NodeID: "gpu-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.GPUUsageSample{
		Timestamp: time.Unix(1, 0).UTC(), Available: true, Utilization: 41,
	}
	agent.hwMu.Lock()
	agent.gpuHistory = []hardware.GPUUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.GPUHistory) != 1 || status.GPUHistory[0] != sample {
		t.Fatalf("node status GPU history = %#v", status.GPUHistory)
	}
	if !status.TelemetryAt.Equal(sample.Timestamp) {
		t.Fatalf("node telemetry timestamp = %s, want %s", status.TelemetryAt, sample.Timestamp)
	}
	status.GPUHistory[0].Utilization = 99
	if agent.gpuHistorySnapshot()[0].Utilization != sample.Utilization {
		t.Fatal("node status exposed mutable telemetry history")
	}
}

func TestNodeStatusIncludesCopiedNPUHistory(t *testing.T) {
	agent := New(config.NodeConfig{
		NodeID: "npu-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.NPUUsageSample{
		Timestamp: time.Unix(1, 0).UTC(), Available: true, Utilization: 41,
	}
	agent.hwMu.Lock()
	agent.npuHistory = []hardware.NPUUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.NPUHistory) != 1 || status.NPUHistory[0] != sample {
		t.Fatalf("node status NPU history = %#v", status.NPUHistory)
	}
	status.NPUHistory[0].Utilization = 99
	if agent.npuHistorySnapshot()[0].Utilization != sample.Utilization {
		t.Fatal("node status exposed mutable telemetry history")
	}
}

func TestNodeStatusIncludesCopiedSystemHistory(t *testing.T) {
	agent := New(config.NodeConfig{
		NodeID: "system-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.SystemUsageSample{
		Timestamp: time.Unix(2, 0).UTC(), CPUAvailable: true, CPUUtilization: 37,
		MemoryAvailable: true, MemoryUtilization: 62,
	}
	agent.hwMu.Lock()
	agent.systemHistory = []hardware.SystemUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.SystemHistory) != 1 || status.SystemHistory[0] != sample {
		t.Fatalf("node status system history = %#v", status.SystemHistory)
	}
	status.SystemHistory[0].CPUUtilization = 99
	if agent.systemHistorySnapshot()[0].CPUUtilization != sample.CPUUtilization {
		t.Fatal("node status exposed mutable system telemetry history")
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
			agent := New(config.NodeConfig{NodeID: "system-status-test", MemoryPath: test.memory})
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
	agent := New(cfg)
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
