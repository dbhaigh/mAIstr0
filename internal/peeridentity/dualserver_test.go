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
	localHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
