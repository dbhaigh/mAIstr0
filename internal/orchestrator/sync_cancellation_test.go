package orchestrator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/background"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestTaskHistoryRequestsCancelInFlight(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(context.Context, *http.Client, string) error
	}{
		{"fetch", func(ctx context.Context, client *http.Client, endpoint string) error {
			_, _, _, err := fetchReplicatedTaskHistoryWithContext(ctx, client, endpoint, nil, maxReplicatedTaskHistoryBytes)
			return err
		}},
		{"post", func(ctx context.Context, client *http.Client, endpoint string) error {
			return postTaskHistoryPagesWithContext(ctx, client, endpoint, []*taskmgr.Task{{ID: "task"}}, nil, maxReplicatedTaskHistoryBytes, 0)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- test.run(ctx, server.Client(), server.URL) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("sync request did not reach peer")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("sync error = %v, want context cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("sync request did not return after cancellation")
			}
		})
	}
}

func TestTaskHistoryRecoveryJoinsRootLifecycle(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var requests atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer peer.Close()
	defer close(release)
	var group background.Group
	defer group.Stop()
	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{ID: "peer", Address: peer.URL, Healthy: true})
	syncer := newSyncerComponent(registry, taskmgr.NewManager(), peer.Client(), nil, nil, group.Go)
	syncer.recoverTaskHistory()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("recovery did not contact peer")
	}
	done := make(chan struct{})
	go func() {
		group.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("root shutdown did not cancel and join recovery")
	}
	if syncer.syncing.Load() {
		t.Fatal("recovery remained active after root shutdown")
	}
	afterStop := newSyncerComponent(registry, taskmgr.NewManager(), peer.Client(), nil, nil, group.Go)
	afterStop.recoverTaskHistory()
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want only initial recovery", got)
	}
}
