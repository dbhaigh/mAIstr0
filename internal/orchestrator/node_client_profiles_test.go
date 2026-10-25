package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"testing"
	"time"
)

func TestDiscoveredNodeStatusContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	node := newNodeClient(nil, nil, nil)
	_, err := node.discoveredNodeStatusContext(ctx, "http://127.0.0.1:1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("discovery error = %v, want context cancellation", err)
	}
}

func TestNodeClientProfilesPreserveLegacyTimeouts(t *testing.T) {
	node := newNodeClient(nil, nil, nil)
	for _, tt := range []struct {
		name   string
		client *http.Client
		want   time.Duration
	}{
		{"default", node.client, 0},
		{"dispatch", node.dispatcher, 0},
		{"status", node.status, 0},
		{"managed control", node.managed, 2 * time.Minute},
		{"download", node.modelPull, time.Hour},
		{"sync", node.sync, 10 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.client.Timeout != tt.want {
				t.Fatalf("timeout = %s, want legacy %s", tt.client.Timeout, tt.want)
			}
			wrapped, ok := tt.client.Transport.(*requestIDTransport)
			if !ok || wrapped.next != http.DefaultTransport {
				t.Fatal("request ID transport must wrap the shared transport")
			}
		})
	}
	for _, timeout := range []time.Duration{0, 13 * time.Second} {
		node := newNodeClientWithSyncTimeout(nil, nil, nil, timeout)
		if node.sync.Timeout != timeout {
			t.Fatalf("configured sync timeout = %s, want %s", node.sync.Timeout, timeout)
		}
	}
}

func TestNodeClientPreservesInjectedClientOptions(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	redirectErr := errors.New("redirect policy")
	for _, timeout := range []time.Duration{0, 29 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			client := &http.Client{
				Timeout:   timeout,
				Transport: transport,
				Jar:       jar,
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return redirectErr
				},
			}
			node := newNodeClient(client, client, client)
			for _, clone := range []*http.Client{node.client, node.dispatcher, node.status} {
				if clone == client || clone.Timeout != timeout || clone.Jar != jar {
					t.Fatal("injected client must be cloned without changing its settings")
				}
				wrapped, ok := clone.Transport.(*requestIDTransport)
				if !ok || wrapped.next != transport {
					t.Fatal("custom transport was replaced")
				}
				if !errors.Is(clone.CheckRedirect(nil, nil), redirectErr) {
					t.Fatal("custom redirect policy was replaced")
				}
			}
			if client.Transport != transport {
				t.Fatal("injected client was mutated")
			}
		})
	}
}
