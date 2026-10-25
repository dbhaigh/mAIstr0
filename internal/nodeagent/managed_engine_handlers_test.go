package nodeagent

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestManagedEngineProxyFlushesHeadersAndSmallChunksBeforeCompletion(t *testing.T) {
	body, writer := io.Pipe()
	defer body.Close()
	defer writer.Close()
	finished := make(chan error, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		finished <- copyManagedEngineResponse(w, &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       body,
		})
	}))
	defer proxy.Close()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(proxy.URL)
	if err != nil {
		t.Fatalf("headers buffered until body completed: %v", err)
	}
	defer response.Body.Close()
	chunk := "data: {\"choices\":[{\"delta\":{\"content\":\"spring\"}}]}\n\n"
	go func() {
		if _, err := io.WriteString(writer, chunk); err != nil {
			_ = writer.CloseWithError(err)
		}
	}()
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != strings.TrimSuffix(chunk, "\n") {
		t.Fatalf("small SSE chunk buffered: %q %v", line, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestManagedEngineRoutesRequirePinnedCurrentOrchestrator(t *testing.T) {
	cfg := config.DefaultNodeConfig()
	cfg.NodeID = "managed-engine-node"
	cfg.MemoryPath = filepath.Join(t.TempDir(), "node.db")
	agent := mustNewTestAgent(t, cfg)
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close node agent: %v", err)
		}
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}

	oldOrchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "old-orchestrator.json"), "old-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	currentOrchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "current-orchestrator.json"), "current-orchestrator")
	if err != nil {
		t.Fatal(err)
	}
	pairedNode, err := peeridentity.Open(filepath.Join(t.TempDir(), "paired-node.json"), "paired-node")
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range []struct {
		identity *peeridentity.Identity
		role     string
	}{
		{oldOrchestrator, "orchestrator"},
		{currentOrchestrator, "orchestrator"},
		{pairedNode, "node"},
	} {
		if err := agent.identityComponent.TrustPeerDetails(peer.identity.ID(), peer.identity.CertificatePEM(), peer.role, "https://127.0.0.1:7450"); err != nil {
			t.Fatal(err)
		}
	}
	agent.setOrchestratorID(currentOrchestrator.ID())

	mux := http.NewServeMux()
	registerManagedEngineRoutes(mux, agent)
	tests := []struct {
		name        string
		certificate *x509.Certificate
		wantStatus  int
	}{
		{name: "plaintext request", wantStatus: http.StatusForbidden},
		{name: "paired worker", certificate: certificateFromPEM(t, pairedNode.CertificatePEM()), wantStatus: http.StatusForbidden},
		{name: "pinned previous orchestrator", certificate: certificateFromPEM(t, oldOrchestrator.CertificatePEM()), wantStatus: http.StatusForbidden},
		{name: "unpaired certificate", certificate: certificateFromPEM(t, mustIdentity(t, "unpaired").CertificatePEM()), wantStatus: http.StatusForbidden},
		{name: "current paired orchestrator", certificate: certificateFromPEM(t, currentOrchestrator.CertificatePEM()), wantStatus: http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/engines/managed", nil)
			if test.certificate != nil {
				request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{test.certificate}}
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("managed engine status returned %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
		})
		t.Run("installer "+test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/engines/managed/install-worker", strings.NewReader(`{"source":"invalid"}`))
			if test.certificate != nil {
				request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{test.certificate}}
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			want := test.wantStatus
			if want == http.StatusOK {
				want = http.StatusBadRequest
			}
			if response.Code != want {
				t.Fatalf("installer returned %d, want %d: %s", response.Code, want, response.Body.String())
			}
		})
		for _, route := range []struct{ method, path, body string }{
			{http.MethodGet, "/api/engines/managed/head-models?requested_context=invalid", ""},
			{http.MethodPost, "/api/engines/managed/configure-head", `{"model_id":"unknown","requested_context":-1}`},
			{http.MethodPost, "/api/engines/managed/download-model", `{"model":"unknown","requested_context":4096}`},
		} {
			t.Run(route.path+" "+test.name, func(t *testing.T) {
				request := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
				if test.certificate != nil {
					request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{test.certificate}}
				}
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, request)
				want := test.wantStatus
				if want == http.StatusOK {
					want = http.StatusBadRequest
				}
				if response.Code != want {
					t.Fatalf("head route=%d want=%d body=%s", response.Code, want, response.Body.String())
				}
			})
		}
	}

	request := httptest.NewRequest(http.MethodPost, "/api/engines/managed/missing/start", strings.NewReader(`{}`))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificateFromPEM(t, currentOrchestrator.CertificatePEM())}}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown managed engine start returned %d, want 404: %s", response.Code, response.Body.String())
	}

	profileRequest := httptest.NewRequest(
		http.MethodGet,
		"/api/engines/managed/missing/profile?requested_context=-1",
		nil,
	)
	profileRequest.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificateFromPEM(t, currentOrchestrator.CertificatePEM())},
	}
	profileResponse := httptest.NewRecorder()
	mux.ServeHTTP(profileResponse, profileRequest)
	if profileResponse.Code != http.StatusBadRequest {
		t.Fatalf("invalid model-profile context returned %d, want 400: %s", profileResponse.Code, profileResponse.Body.String())
	}

	profileRequest = httptest.NewRequest(http.MethodGet, "/api/engines/managed/missing/profile", nil)
	profileRequest.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificateFromPEM(t, currentOrchestrator.CertificatePEM())},
	}
	profileResponse = httptest.NewRecorder()
	mux.ServeHTTP(profileResponse, profileRequest)
	if profileResponse.Code != http.StatusBadRequest {
		t.Fatalf("missing model-profile context returned %d, want 400: %s", profileResponse.Code, profileResponse.Body.String())
	}

	profileRequest = httptest.NewRequest(
		http.MethodGet,
		"/api/engines/managed/missing/profile?requested_context=4096",
		nil,
	)
	profileRequest.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificateFromPEM(t, currentOrchestrator.CertificatePEM())},
	}
	profileResponse = httptest.NewRecorder()
	mux.ServeHTTP(profileResponse, profileRequest)
	if profileResponse.Code != http.StatusNotFound {
		t.Fatalf("unknown managed engine profile returned %d, want 404: %s", profileResponse.Code, profileResponse.Body.String())
	}
}

func mustIdentity(t *testing.T, id string) *peeridentity.Identity {
	t.Helper()
	identity, err := peeridentity.Open(filepath.Join(t.TempDir(), id+".json"), id)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func certificateFromPEM(t *testing.T, certificatePEM string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(certificatePEM))
	if block == nil {
		t.Fatal("identity certificate is not PEM encoded")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}
