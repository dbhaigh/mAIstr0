package orchestrator

import (
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestRouterCircuitBreakerOpensAndAllowsOneHalfOpenProbe(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	breakers := newRouterCircuitBreakers()
	breakers.now = func() time.Time { return now }
	breakers.jitter = func(delay time.Duration) time.Duration { return delay }

	for range routerBreakerFailureThreshold {
		breakers.recordFailure("node-a", "Demo")
	}
	if breakers.eligible("node-a", "demo") {
		t.Fatal("open circuit accepted an attempt before its backoff expired")
	}
	assertRouterBreakerState(t, breakers, "open", 3)

	now = now.Add(routerBreakerBaseBackoff)
	if !breakers.eligible("node-a", "demo") {
		t.Fatal("expired open circuit did not become eligible for a probe")
	}
	if !breakers.acquire("node-a", "demo") {
		t.Fatal("half-open circuit rejected its first probe")
	}
	if breakers.acquire("node-a", "demo") {
		t.Fatal("half-open circuit admitted more than one concurrent probe")
	}
	assertRouterBreakerState(t, breakers, "half_open", 3)

	breakers.releaseProbe("node-a", "demo")
	if !breakers.acquire("node-a", "demo") {
		t.Fatal("released half-open probe did not allow a replacement probe")
	}
	breakers.recordSuccess("node-a", "demo")
	if !breakers.eligible("node-a", "demo") {
		t.Fatal("successful half-open probe did not close the circuit")
	}
}

func TestRouterCircuitBreakerExponentiallyBacksOffAfterFailedProbe(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	breakers := newRouterCircuitBreakers()
	breakers.now = func() time.Time { return now }
	breakers.jitter = func(delay time.Duration) time.Duration { return delay }

	for range routerBreakerFailureThreshold {
		breakers.recordFailure("node-a", "demo")
	}
	now = now.Add(routerBreakerBaseBackoff)
	if !breakers.acquire("node-a", "demo") {
		t.Fatal("expired circuit did not permit a probe")
	}
	breakers.recordFailure("node-a", "demo")
	assertRouterBreakerState(t, breakers, "open", routerBreakerFailureThreshold+1)
	if got := breakerOpenUntil(t, breakers, "node-a", "demo").Sub(now); got != 2*routerBreakerBaseBackoff {
		t.Fatalf("second backoff = %s, want %s", got, 2*routerBreakerBaseBackoff)
	}
}

func TestRouterCircuitBreakerUsesOnlyRecentTransientFailures(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	breakers := newRouterCircuitBreakers()
	breakers.now = func() time.Time { return now }
	breakers.jitter = func(delay time.Duration) time.Duration { return delay }

	breakers.recordFailure("node-a", "demo")
	breakers.recordFailure("node-a", "demo")
	now = now.Add(routerBreakerFailureWindow + time.Second)
	breakers.recordFailure("node-a", "demo")
	assertRouterBreakerState(t, breakers, "closed", 1)

	if isTransientRouterFailure("client_error") || !isTransientRouterFailure("upstream_5xx") {
		t.Fatal("router failure classes do not distinguish transient upstream failures")
	}
}

func assertRouterBreakerState(t *testing.T, breakers *routerCircuitBreakers, wantState string, wantFailures int) {
	t.Helper()
	snapshot := breakers.snapshot([]cluster.NodeStatus{{
		ID: "node-a", Models: []engine.Model{{Name: "demo"}},
	}})
	if len(snapshot) != 1 {
		t.Fatalf("breaker snapshot has %d entries, want 1", len(snapshot))
	}
	if snapshot[0].State != wantState || snapshot[0].RecentFailures != wantFailures {
		t.Fatalf("breaker snapshot = %+v, want state %q and %d recent failures", snapshot[0], wantState, wantFailures)
	}
}

func breakerOpenUntil(t *testing.T, breakers *routerCircuitBreakers, nodeID, model string) time.Time {
	t.Helper()
	for _, state := range breakers.snapshot([]cluster.NodeStatus{{
		ID: nodeID, Models: []engine.Model{{Name: model}},
	}}) {
		if state.OpenUntil != nil {
			return *state.OpenUntil
		}
	}
	t.Fatal("open circuit has no open-until timestamp")
	return time.Time{}
}
