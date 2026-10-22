package orchestrator

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeClientSharesTransportAndReusesDrainedBodies(t *testing.T) {
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("response body", 512))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	transportCreations := 0
	node := newNodeClient(&http.Client{}, &http.Client{}, &http.Client{})
	node.setTransport(func() *http.Transport {
		transportCreations++
		return &http.Transport{MaxIdleConnsPerHost: 8}
	})
	if transportCreations != 1 {
		t.Fatalf("transport factory called %d times, want 1", transportCreations)
	}
	transport := node.client.Transport
	for _, client := range []*http.Client{node.dispatcher, node.status, node.modelPull, node.sync} {
		if client.Transport != transport {
			t.Fatal("node clients do not share one transport")
		}
	}
	t.Cleanup(node.client.CloseIdleConnections)

	for i := range 4 {
		client := node.client
		if i%2 != 0 {
			client = node.dispatcher
		}
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			_ = response.Body.Close()
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("server accepted %d connections, want 1 reused connection", got)
	}
}

func BenchmarkNodeClientSharedTransport(b *testing.B) {
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "transport reuse benchmark response")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	b.Cleanup(server.Close)

	transportFactory := func() *http.Transport {
		return &http.Transport{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		}
	}
	node := newNodeClient(&http.Client{}, &http.Client{}, &http.Client{})
	node.setTransport(transportFactory)
	b.Cleanup(func() {
		for _, client := range []*http.Client{node.client, node.dispatcher, node.status, node.modelPull, node.sync} {
			client.CloseIdleConnections()
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client := node.client
		if i%2 != 0 {
			client = node.dispatcher
		}
		response, err := client.Get(server.URL)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			_ = response.Body.Close()
			b.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(connections.Load()), "connections")
}
