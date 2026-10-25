package httpx

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewClientTimeouts(t *testing.T) {
	for _, tt := range []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{"probe", ProbeTimeout, 5 * time.Second},
		{"control", ControlTimeout, 10 * time.Second},
		{"inference", InferenceTimeout, 10 * time.Minute},
		{"download", DownloadTimeout, time.Hour},
		{"streaming", 0, 0},
		{"legacy ollama probe", 3 * time.Second, 3 * time.Second},
		{"legacy managed control", 2 * time.Minute, 2 * time.Minute},
		{"legacy supervisor probe", 750 * time.Millisecond, 750 * time.Millisecond},
		{"custom sync", 37 * time.Second, 37 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(ClientOptions{Timeout: tt.timeout})
			if client.Timeout != tt.want {
				t.Fatalf("timeout = %s, want %s", client.Timeout, tt.want)
			}
			if client.Transport != http.DefaultTransport {
				t.Fatal("client does not share the standard transport")
			}
		})
	}
}

func TestNewClientRejectsNegativeTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("negative timeout was silently accepted")
		}
	}()
	NewClient(ClientOptions{Timeout: -time.Nanosecond})
}

func TestNewClientRetainsCustomTransportAndRedirectPolicy(t *testing.T) {
	verify := func(tls.ConnectionState) error { return errors.New("pin rejected") }
	transport := &http.Transport{TLSClientConfig: &tls.Config{VerifyConnection: verify}}
	defer transport.CloseIdleConnections()
	redirectErr := errors.New("redirect rejected")
	calls := 0
	client := NewClient(ClientOptions{
		Timeout:   17 * time.Second,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			calls++
			return redirectErr
		},
	})
	if client.Transport != transport || transport.TLSClientConfig.VerifyConnection == nil {
		t.Fatal("custom transport or TLS pinning was replaced")
	}
	if err := transport.TLSClientConfig.VerifyConnection(tls.ConnectionState{}); err == nil || err.Error() != "pin rejected" {
		t.Fatalf("TLS verification policy changed: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/next", http.StatusFound)
	}))
	defer server.Close()
	response, err := client.Get(server.URL)
	if response != nil {
		response.Body.Close()
	}
	if !errors.Is(err, redirectErr) || calls != 1 {
		t.Fatalf("redirect error = %v, calls = %d", err, calls)
	}
}

func TestNewClientsShareConnectionsNotClientState(t *testing.T) {
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	first := NewClient(ClientOptions{Timeout: ProbeTimeout})
	second := NewClient(ClientOptions{Timeout: ControlTimeout})
	if first == second || first.Transport != second.Transport {
		t.Fatal("clients must be distinct and share their transport")
	}
	first.Timeout = time.Second
	if second.Timeout != ControlTimeout {
		t.Fatal("changing one client affected another")
	}
	for _, client := range []*http.Client{first, second} {
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("opened %d connections, want 1 reused connection", got)
	}
}

func TestNewClientUnlimitedStillHonorsContext(t *testing.T) {
	client := NewClient(ClientOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("request error = %v, want context cancellation", err)
	}
}
