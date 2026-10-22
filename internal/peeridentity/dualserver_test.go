package peeridentity

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestServeDualSeparatesLoopbackAndPairedTraffic(t *testing.T) {
	serverIdentity, err := Open(filepath.Join(t.TempDir(), "server.json"), "server")
	if err != nil {
		t.Fatal(err)
	}
	pairedClient, err := Open(filepath.Join(t.TempDir(), "paired.json"), "paired")
	if err != nil {
		t.Fatal(err)
	}
	unpairedClient, err := Open(filepath.Join(t.TempDir(), "unpaired.json"), "unpaired")
	if err != nil {
		t.Fatal(err)
	}
	for _, client := range []*Identity{pairedClient, unpairedClient} {
		if err := client.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
			t.Fatal(err)
		}
	}
	if err := serverIdentity.TrustPeer(pairedClient.ID(), pairedClient.CertificatePEM()); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := "https://" + listener.Addr().String()
	mutations := 0
	localHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mutations++
		}
		w.WriteHeader(http.StatusNoContent)
	})
	secureHandler := serverIdentity.RequireTrustedPeer(localHandler)
	done := make(chan error, 1)
	go func() {
		done <- ServeDualListener(listener, LoopbackOnly(localHandler), secureHandler, serverIdentity.ServerTLSConfig())
	}()

	localResponse, err := http.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	localResponse.Body.Close()
	if localResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("loopback request status = %d, want 204", localResponse.StatusCode)
	}

	pairedHTTP := &http.Client{Timeout: 3 * time.Second, Transport: pairedClient.ClientTransport()}
	pairedResponse, err := pairedHTTP.Get(address)
	if err != nil {
		t.Fatal(err)
	}
	pairedResponse.Body.Close()
	if pairedResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("paired request status = %d, want 204", pairedResponse.StatusCode)
	}
	pairedMutation, err := pairedHTTP.Post(address+"/api/tasks", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	pairedMutation.Body.Close()
	if pairedMutation.StatusCode != http.StatusNoContent || mutations != 1 {
		t.Fatalf("paired mutation status = %d and count = %d, want 204 and 1", pairedMutation.StatusCode, mutations)
	}

	unpairedHTTP := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{unpairedClient.certificate},
			InsecureSkipVerify: true,
		}},
	}
	unpairedResponse, err := unpairedHTTP.Get(address)
	if err != nil {
		t.Fatal(err)
	}
	unpairedResponse.Body.Close()
	if unpairedResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("unpaired request status = %d, want 403", unpairedResponse.StatusCode)
	}
	unpairedMutation, err := unpairedHTTP.Post(address+"/api/tasks", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	unpairedMutation.Body.Close()
	if unpairedMutation.StatusCode != http.StatusForbidden || mutations != 1 {
		t.Fatalf("unpaired mutation status = %d and count = %d, want 403 and 1", unpairedMutation.StatusCode, mutations)
	}

	_ = listener.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dual listener did not stop after its listener closed")
	}
}

func TestLoopbackOnlyRejectsRemotePlaintext(t *testing.T) {
	handler := LoopbackOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.RemoteAddr = "192.0.2.10:1234"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("remote plaintext status = %d, want 403", response.Code)
	}
}

func TestLoopbackOnlyValidatesHostAndBrowserOrigin(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		host       string
		origin     string
		referer    string
		fetchSite  string
		wantStatus int
	}{
		{name: "localhost API", method: http.MethodGet, host: "localhost:7450", wantStatus: http.StatusNoContent},
		{name: "loopback IP API", method: http.MethodPost, host: "127.0.0.1:7450", wantStatus: http.StatusNoContent},
		{name: "non-browser API", method: http.MethodPost, host: "localhost:7450", wantStatus: http.StatusNoContent},
		{name: "same-origin browser request", method: http.MethodPost, host: "localhost:7450", origin: "http://localhost:7450", fetchSite: "same-origin", wantStatus: http.StatusNoContent},
		{name: "same-origin default port", method: http.MethodPost, host: "localhost:80", origin: "http://localhost", wantStatus: http.StatusNoContent},
		{name: "top-level local navigation", method: http.MethodGet, host: "localhost:7450", fetchSite: "none", wantStatus: http.StatusNoContent},
		{name: "rebound DNS host", method: http.MethodGet, host: "attacker.example", wantStatus: http.StatusForbidden},
		{name: "lookalike localhost host", method: http.MethodGet, host: "localhost.attacker.example", wantStatus: http.StatusForbidden},
		{name: "non-loopback IP host", method: http.MethodGet, host: "192.0.2.10:7450", wantStatus: http.StatusForbidden},
		{name: "cross-origin read", method: http.MethodGet, host: "localhost:7450", origin: "http://attacker.example", wantStatus: http.StatusForbidden},
		{name: "cross-scheme read", method: http.MethodGet, host: "localhost:7450", origin: "https://localhost:7450", wantStatus: http.StatusForbidden},
		{name: "cross-site mutation", method: http.MethodPost, host: "localhost:7450", fetchSite: "cross-site", wantStatus: http.StatusForbidden},
		{name: "cross-origin referrer", method: http.MethodGet, host: "localhost:7450", referer: "http://attacker.example/page", wantStatus: http.StatusForbidden},
		{name: "unsafe no-origin navigation", method: http.MethodPost, host: "localhost:7450", fetchSite: "none", wantStatus: http.StatusForbidden},
	}
	handler := LoopbackOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "http://"+test.host+"/", nil)
			request.Host = test.host
			request.RemoteAddr = "127.0.0.1:1234"
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			if test.referer != "" {
				request.Header.Set("Referer", test.referer)
			}
			if test.fetchSite != "" {
				request.Header.Set("Sec-Fetch-Site", test.fetchSite)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("request returned %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
		})
	}
}

func TestDualServersHaveHeaderAndIdleTimeouts(t *testing.T) {
	server := newHTTPServer(http.NotFoundHandler(), nil)
	if server.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want 5s", server.ReadHeaderTimeout)
	}
	if server.IdleTimeout != 90*time.Second {
		t.Fatalf("IdleTimeout = %s, want 90s", server.IdleTimeout)
	}
}
