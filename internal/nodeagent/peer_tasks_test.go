package nodeagent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
)

type peerTasksTransport func(*http.Request) (*http.Response, error)

func (f peerTasksTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func peerTasksComponent(client *http.Client, address string, limit int64) *peerComponent {
	state := &agentState{
		cfg:   config.NodeConfig{NodeID: "self", SyncMaxBytes: limit},
		peers: []cluster.NodeStatus{{ID: "leader", Address: address, Healthy: true, Leader: true}},
	}
	telemetry := &telemetryComponent{}
	return &peerComponent{
		state: state, telemetry: telemetry,
		models:   &engineComponent{state: state, telemetry: telemetry},
		executor: newExecutorComponent(1, state),
		peerHTTP: func() *http.Client { return client },
	}
}

func TestPeerTasksBoundedRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		status int
		limit  int64
		want   string
	}{
		{name: "success", body: `[{"id":"task-1"}]`, status: http.StatusOK, limit: 17},
		{name: "exact limit", body: `[]`, status: http.StatusOK, limit: 2},
		{name: "oversized", body: `[{"id":"task-1"}]`, status: http.StatusOK, limit: 2, want: "exceed"},
		{name: "invalid JSON", body: `[`, status: http.StatusOK, limit: 10, want: "decode"},
		{name: "trailing JSON", body: `[] []`, status: http.StatusOK, limit: 10, want: "decode"},
		{name: "HTTP error", body: `[]`, status: http.StatusServiceUnavailable, limit: 10, want: "HTTP"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: peerTasksTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodGet || r.URL.Path != "/tasks" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				return &http.Response{StatusCode: tc.status, Status: http.StatusText(tc.status), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}
			peers := peerTasksComponent(client, "http://leader", tc.limit)
			tasks, err := peers.currentTasksContext(context.Background())
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error = %v, want %q", err, tc.want)
				}
			} else if err != nil {
				t.Fatal(err)
			} else if tc.name == "success" && (len(tasks) != 1 || tasks[0].ID != "task-1") {
				t.Fatalf("tasks = %+v", tasks)
			}
		})
	}
}

func TestPeerTasksCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peers := peerTasksComponent(server.Client(), server.URL, 1024)
	done := make(chan error, 1)
	go func() {
		_, err := peers.currentTasksContext(ctx)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("peer task request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer task request ignored cancellation")
	}
}
