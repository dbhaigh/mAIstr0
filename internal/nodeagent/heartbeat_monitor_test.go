package nodeagent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatMonitorRequiresConsecutiveFailuresAndRearmsAfterRecovery(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	elections := make(chan string, 2)
	recoveries := make(chan string, 1)
	monitor := newHeartbeatMonitor(
		func() string { return "https://orchestrator.example:7450" },
		func() string { return "orchestrator-a" },
		func(context.Context, string) error {
			if attempts.Add(1) == 5 {
				return nil
			}
			return errors.New("unreachable")
		},
		func(id string) { recoveries <- id },
	)
	monitor.interval = 5 * time.Millisecond
	monitor.timeout = 50 * time.Millisecond
	monitor.setElectionStarter(func(id string) { elections <- id })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go monitor.Run(ctx)

	if got := receiveString(t, elections); got != "orchestrator-a" {
		t.Fatalf("first election target = %q, want orchestrator-a", got)
	}
	if got := receiveString(t, recoveries); got != "orchestrator-a" {
		t.Fatalf("recovered orchestrator = %q, want orchestrator-a", got)
	}
	if got := receiveString(t, elections); got != "orchestrator-a" {
		t.Fatalf("second election target = %q, want orchestrator-a", got)
	}
	if got := attempts.Load(); got < 9 {
		t.Fatalf("status probes = %d, want at least 9", got)
	}
}

func TestHeartbeatMonitorElectionCallbackDoesNotBlockProbes(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	probes := make(chan struct{}, 32)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	monitor := newHeartbeatMonitor(
		func() string { return "https://orchestrator.example:7450" },
		func() string { return "orchestrator-a" },
		func(context.Context, string) error {
			attempts.Add(1)
			select {
			case probes <- struct{}{}:
			default:
			}
			return errors.New("unreachable")
		},
		nil,
	)
	monitor.interval = 5 * time.Millisecond
	monitor.timeout = 50 * time.Millisecond
	monitor.setElectionStarter(func(string) {
		started <- struct{}{}
		<-release
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go monitor.Run(ctx)
	receiveSignal(t, started)

	for {
		select {
		case <-probes:
		default:
			goto drained
		}
	}
drained:
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for range 7 {
		select {
		case <-probes:
		case <-deadline.C:
			t.Fatalf("monitor performed only %d probes while election callback was blocked", attempts.Load())
		}
	}
	select {
	case <-release:
	default:
		close(release)
	}
}

func TestStatusProbeRequestsAPIStatusAndRejectsUnhealthyResponses(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		statusCode int
		wantError  bool
	}{
		{name: "healthy", statusCode: http.StatusOK},
		{name: "unhealthy", statusCode: http.StatusServiceUnavailable, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/api/status" {
					t.Errorf("probe request = %s %s, want GET /api/status", r.Method, r.URL.Path)
				}
				w.WriteHeader(test.statusCode)
			}))
			defer server.Close()

			err := statusProbe(server.Client())(context.Background(), server.URL)
			if (err != nil) != test.wantError {
				t.Fatalf("statusProbe() error = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

func receiveString(t *testing.T, values <-chan string) string {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for monitor event")
		return ""
	}
}

func receiveSignal(t *testing.T, values <-chan struct{}) {
	t.Helper()
	select {
	case <-values:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for monitor event")
	}
}
