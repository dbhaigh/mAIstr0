package nodeagent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestSyncRequestsCancelInFlight(t *testing.T) {
	tasks := []*taskmgr.Task{{ID: "task"}}
	for _, test := range []struct {
		name string
		run  func(context.Context, *http.Client, string) error
	}{
		{"fetch tasks", func(ctx context.Context, client *http.Client, endpoint string) error {
			_, _, _, err := fetchTaskSnapshotWithContext(ctx, client, endpoint, nil, maxTaskSyncBodyBytes)
			return err
		}},
		{"post tasks", func(ctx context.Context, client *http.Client, endpoint string) error {
			return postTaskSnapshotPagesWithContext(ctx, client, endpoint, tasks, nil, maxTaskSyncBodyBytes, 0)
		}},
		{"fetch memory", func(ctx context.Context, client *http.Client, endpoint string) error {
			_, _, _, err := fetchMemorySyncSnapshotWithContext(ctx, client, endpoint, nil, maxMemorySyncBodyBytes)
			return err
		}},
		{"post memory", func(ctx context.Context, client *http.Client, endpoint string) error {
			_, err := postMemorySnapshotPagesWithContext(ctx, client, endpoint, [][]byte{[]byte("{}")}, nil, 0)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			cancelled := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				select {
				case <-r.Context().Done():
					close(cancelled)
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
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("peer did not observe request cancellation")
			}
		})
	}
}
