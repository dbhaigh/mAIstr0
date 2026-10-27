package hub

import (
	"bufio"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
)

func waitForSubscribers(t *testing.T, h *Hub, want bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for h.HasSubscribers() != want {
		select {
		case <-deadline.C:
			t.Fatalf("HasSubscribers() did not become %v", want)
		case <-poll.C:
		}
	}
}

func TestHubTracksSubscribersAndDeliversEvents(t *testing.T) {
	t.Parallel()
	h := New()
	if h.HasSubscribers() {
		t.Fatal("new hub reports subscribers")
	}
	h.Broadcast([]byte(`{"ignored":true}`))

	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(response.Body)
	if got := response.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := response.Header.Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q", got)
	}
	waitForSubscribers(t, h, true)

	h.Broadcast([]byte(`{"n":1}`))
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimRight(line, "\n"); got != `data: {"n":1}` {
		t.Fatalf("event line = %q", got)
	}
	if terminator, err := reader.ReadString('\n'); err != nil || terminator != "\n" {
		t.Fatalf("SSE event terminator = %q, error = %v", terminator, err)
	}

	cancel()
	waitForSubscribers(t, h, false)
}

func TestHubGzipPreservesSSEFraming(t *testing.T) {
	t.Parallel()
	h := New()
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", "gzip")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(response.Body)
	if got := response.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	if got := response.Header.Get("Vary"); !strings.Contains(got, "Accept-Encoding") {
		t.Fatalf("Vary = %q, want Accept-Encoding", got)
	}
	waitForSubscribers(t, h, true)
	h.Broadcast([]byte(`{"n":1}`))

	compressed, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	reader := bufio.NewReader(compressed)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimRight(line, "\n"); got != `data: {"n":1}` {
		t.Fatalf("event line = %q", got)
	}
	if terminator, err := reader.ReadString('\n'); err != nil || terminator != "\n" {
		t.Fatalf("SSE event terminator = %q, error = %v", terminator, err)
	}

	cancel()
	waitForSubscribers(t, h, false)
}

func TestAcceptsGzip(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		header string
		want   bool
	}{
		{header: "gzip", want: true},
		{header: "br, gzip;q=0.4", want: true},
		{header: "gzip;q=0", want: false},
		{header: "br", want: false},
		{header: "*;q=0.5", want: true},
		{header: "*;q=1, gzip;q=0", want: false},
		{header: "GZip;Q=0.8", want: true},
		{header: "gzip;q=invalid", want: false},
	} {
		t.Run(test.header, func(t *testing.T) {
			t.Parallel()
			if got := acceptsGzip(test.header); got != test.want {
				t.Fatalf("acceptsGzip(%q) = %v, want %v", test.header, got, test.want)
			}
		})
	}
}

func BenchmarkBroadcastWithoutSubscribers(b *testing.B) {
	h := New()
	data := []byte(`{"nodes":[]}`)
	b.ReportAllocs()
	for range b.N {
		h.Broadcast(data)
	}
}
