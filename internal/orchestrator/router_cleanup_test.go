package orchestrator

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// signalingRecorder reports the first body write so a test can disconnect a
// client after the router has committed a streamed response.
type signalingRecorder struct {
	*httptest.ResponseRecorder
	once  sync.Once
	wrote chan struct{}
}

func (r *signalingRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseRecorder.Write(p)
	r.once.Do(func() { close(r.wrote) })
	return n, err
}

func assertRouterResourcesReleased(t *testing.T, srv *Server, nodeID string) {
	t.Helper()
	if got := srv.dispatcher.capacityGate.Active(); got != 0 {
		t.Errorf("capacity reservations still held: %d", got)
	}
	if got := srv.metrics.routerActiveStreams.Load(); got != 0 {
		t.Errorf("router active streams = %d, want 0", got)
	}
	if got := srv.metrics.routerQueueDepth.Load(); got != 0 {
		t.Errorf("router queue depth = %d, want 0", got)
	}
	if status, ok := srv.registry.Get(nodeID); !ok || status.ActiveTasks != 0 {
		t.Errorf("node load = %+v (found=%v), want zero active tasks", status.ActiveTasks, ok)
	}
}

func TestRouterReleasesCapacityOnEveryTerminalPath(t *testing.T) {
	const firstChunk = "data: first\n\n"
	tests := []struct {
		name string
		// upstream answers one attempt; cancelled is closed when the
		// attempt context ends before the upstream finished on its own.
		upstream     func(r *http.Request, cancelled chan<- struct{}) (*http.Response, error)
		cancelClient func(rec *signalingRecorder, started <-chan struct{}) bool
		wantStatus   int
		wantBody     string
		wantUpstream bool
	}{
		{
			name: "successful stream",
			upstream: func(*http.Request, chan<- struct{}) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(firstChunk))}, nil
			},
			wantStatus: http.StatusOK, wantBody: firstChunk,
		},
		{
			name: "non-retryable upstream rejection",
			upstream: func(*http.Request, chan<- struct{}) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("bad request"))}, nil
			},
			wantStatus: http.StatusBadRequest, wantBody: "bad request",
		},
		{
			name: "client cancels before upstream headers",
			upstream: func(r *http.Request, cancelled chan<- struct{}) (*http.Response, error) {
				<-r.Context().Done()
				close(cancelled)
				return nil, r.Context().Err()
			},
			cancelClient: func(_ *signalingRecorder, started <-chan struct{}) bool {
				<-started
				return true
			},
			wantUpstream: true,
		},
		{
			name: "client disconnects after the stream is committed",
			upstream: func(r *http.Request, cancelled chan<- struct{}) (*http.Response, error) {
				body, writer := io.Pipe()
				go func() {
					_, _ = io.WriteString(writer, firstChunk)
					<-r.Context().Done()
					close(cancelled)
					_ = writer.CloseWithError(r.Context().Err())
				}()
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
			},
			cancelClient: func(rec *signalingRecorder, _ <-chan struct{}) bool {
				<-rec.wrote
				return true
			},
			wantStatus: http.StatusOK, wantUpstream: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t)
			srv.registry.Upsert(routerTestNode("node-a", "http://node-a", 100))
			started := make(chan struct{})
			cancelled := make(chan struct{})
			var startOnce sync.Once
			srv.nodeClient.dispatcher.Transport = routerRoundTripper(func(r *http.Request) (*http.Response, error) {
				startOnce.Do(func() { close(started) })
				return tt.upstream(r, cancelled)
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions",
				strings.NewReader(`{"model":"demo","stream":true}`)).WithContext(ctx)
			rec := &signalingRecorder{ResponseRecorder: httptest.NewRecorder(), wrote: make(chan struct{})}
			done := make(chan struct{})
			go func() {
				defer close(done)
				srv.Mux().ServeHTTP(rec, req)
			}()
			if tt.cancelClient != nil {
				waited := make(chan bool, 1)
				go func() { waited <- tt.cancelClient(rec, started) }()
				select {
				case <-waited:
					cancel()
				case <-time.After(5 * time.Second):
					t.Fatal("router did not reach the cancellation point")
				}
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("router handler did not return")
			}
			if tt.wantUpstream {
				select {
				case <-cancelled:
				default:
					t.Error("client cancellation did not cancel the upstream attempt")
				}
			}
			if tt.wantStatus != 0 && rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Errorf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
			assertRouterResourcesReleased(t, srv, "node-a")
		})
	}
}

func TestRouterConcurrentRequestsNeverExceedNodeCapacity(t *testing.T) {
	const (
		requests = 8
		limit    = 2
	)
	srv := newTestServer(t)
	node := routerTestNode("node-a", "http://node-a", 100)
	node.MaxConcurrentTasks, node.AvailableCapacity = limit, limit
	srv.registry.Upsert(node)

	var inFlight, peak atomic.Int32
	arrived := make(chan struct{}, requests)
	release := make(chan struct{})
	srv.nodeClient.dispatcher.Transport = routerRoundTripper(func(r *http.Request) (*http.Response, error) {
		current := inFlight.Add(1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		arrived <- struct{}{}
		defer inFlight.Add(-1)
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})

	codes := make(chan int, requests)
	for range requests {
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions", strings.NewReader(`{"model":"demo"}`))
			rec := httptest.NewRecorder()
			srv.Mux().ServeHTTP(rec, req)
			codes <- rec.Code
		}()
	}
	for range limit {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			t.Fatal("requests did not reach the upstream")
		}
	}
	if got := srv.dispatcher.capacityGate.Active(); got != limit {
		t.Errorf("reservations while saturated = %d, want %d", got, limit)
	}
	close(release)
	for range requests {
		select {
		case code := <-codes:
			if code != http.StatusOK {
				t.Errorf("request status = %d, want 200", code)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("queued requests did not complete after capacity was released")
		}
	}
	if got := peak.Load(); got > limit {
		t.Fatalf("peak upstream concurrency = %d, exceeds node limit %d", got, limit)
	}
	assertRouterResourcesReleased(t, srv, "node-a")
}
