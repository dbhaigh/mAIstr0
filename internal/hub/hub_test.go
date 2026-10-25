package hub

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func waitForSubscribers(t *testing.T, h *Hub, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for h.HasSubscribers() != want {
		if time.Now().After(deadline) {
			t.Fatalf("HasSubscribers() did not become %v", want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHubTracksSubscribersAndDeliversEvents(t *testing.T) {
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
	defer response.Body.Close()
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

func BenchmarkBroadcastWithoutSubscribers(b *testing.B) {
	h := New()
	data := []byte(`{"nodes":[]}`)
	b.ReportAllocs()
	for range b.N {
		h.Broadcast(data)
	}
}
