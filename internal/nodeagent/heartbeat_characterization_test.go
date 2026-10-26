package nodeagent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// heartbeatStep is one monitor tick. The monitor reads the address at the
// start of each tick, so delivering step k+1 proves step k was fully handled.
type heartbeatStep struct {
	address string
	id      string
	fail    bool
	// wantElection reports that this step must start an election for id.
	wantElection bool
}

type heartbeatScript struct {
	mu         sync.Mutex
	current    heartbeatStep
	steps      chan heartbeatStep
	elections  chan string
	recoveries []string
	probes     int
}

func runHeartbeatScript(t *testing.T, failureLimit int, steps []heartbeatStep) (elections, recoveries []string, probes int) {
	t.Helper()
	script := &heartbeatScript{steps: make(chan heartbeatStep), elections: make(chan string, len(steps)+1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor := newHeartbeatMonitor(
		func() string {
			select {
			case step := <-script.steps:
				script.mu.Lock()
				script.current = step
				script.mu.Unlock()
				return step.address
			case <-ctx.Done():
				return ""
			}
		},
		func() string {
			script.mu.Lock()
			defer script.mu.Unlock()
			return script.current.id
		},
		func(context.Context, string) error {
			script.mu.Lock()
			defer script.mu.Unlock()
			script.probes++
			if script.current.fail {
				return errors.New("status probe failed")
			}
			return nil
		},
		func(id string) {
			script.mu.Lock()
			script.recoveries = append(script.recoveries, id)
			script.mu.Unlock()
		},
	)
	monitor.interval = time.Millisecond
	monitor.timeout = 50 * time.Millisecond
	monitor.failureLimit = failureLimit
	monitor.setElectionStarter(func(id string) { script.elections <- id })

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.Run(ctx)
	}()

	awaitElection := func(index int, id string) {
		select {
		case got := <-script.elections:
			if got != id {
				t.Fatalf("step %d started an election for %q, want %q", index, got, id)
			}
			elections = append(elections, got)
		case <-time.After(time.Second):
			t.Fatalf("step %d did not start an election for %q", index, id)
		}
	}
	// A trailing idle step is the sync point for the final scripted step.
	all := append(append([]heartbeatStep(nil), steps...), heartbeatStep{})
	for i, step := range all {
		select {
		case script.steps <- step:
		case <-time.After(time.Second):
			t.Fatalf("monitor did not reach step %d", i)
		}
		if i > 0 && all[i-1].wantElection {
			awaitElection(i-1, all[i-1].id)
		}
	}
	cancel()
	<-done

	// Run has returned, so every election goroutine has been spawned. Give
	// any unexpected one a brief window to surface.
	select {
	case extra := <-script.elections:
		t.Fatalf("unexpected extra election for %q after %v", extra, elections)
	case <-time.After(20 * time.Millisecond):
	}
	script.mu.Lock()
	defer script.mu.Unlock()
	return elections, script.recoveries, script.probes
}

func hbFailSteps(n int, address, id string) []heartbeatStep {
	steps := make([]heartbeatStep, n)
	for i := range steps {
		steps[i] = heartbeatStep{address: address, id: id, fail: true}
	}
	return steps
}

func hbSteps(groups ...[]heartbeatStep) []heartbeatStep {
	var out []heartbeatStep
	for _, group := range groups {
		out = append(out, group...)
	}
	return out
}

func hbTrigger(address, id string) []heartbeatStep {
	return []heartbeatStep{{address: address, id: id, fail: true, wantElection: true}}
}

func hbOK(address, id string) []heartbeatStep {
	return []heartbeatStep{{address: address, id: id}}
}

func TestHeartbeatMonitorElectionThresholdCharacterization(t *testing.T) {
	const (
		addrA = "https://leader-a:7450"
		addrB = "https://leader-b:7450"
	)
	tests := []struct {
		name           string
		limit          int
		steps          []heartbeatStep
		wantRecoveries []string
		wantProbes     int
	}{
		{
			name:       "limit minus one failures do not elect",
			limit:      4,
			steps:      hbFailSteps(3, addrA, "a"),
			wantProbes: 3,
		},
		{
			name:       "exactly limit failures elect once",
			limit:      4,
			steps:      hbSteps(hbFailSteps(3, addrA, "a"), hbTrigger(addrA, "a")),
			wantProbes: 4,
		},
		{
			name:       "failures after triggering do not re-elect",
			limit:      4,
			steps:      hbSteps(hbFailSteps(3, addrA, "a"), hbTrigger(addrA, "a"), hbFailSteps(9, addrA, "a")),
			wantProbes: 13,
		},
		{
			name:       "a success resets the consecutive count",
			limit:      4,
			steps:      hbSteps(hbFailSteps(3, addrA, "a"), hbOK(addrA, "a"), hbFailSteps(3, addrA, "a"), hbOK(addrA, "a"), hbFailSteps(3, addrA, "a")),
			wantProbes: 11,
		},
		{
			name:  "recovery re-arms with a full count",
			limit: 2,
			steps: hbSteps(
				hbFailSteps(1, addrA, "a"), hbTrigger(addrA, "a"), hbOK(addrA, "a"),
				hbFailSteps(1, addrA, "a"), hbTrigger(addrA, "a"),
			),
			wantRecoveries: []string{"a"},
			wantProbes:     5,
		},
		{
			name:       "an empty address resets without probing",
			limit:      3,
			steps:      hbSteps(hbFailSteps(2, addrA, "a"), []heartbeatStep{{}}, hbFailSteps(2, addrA, "a"), hbTrigger(addrA, "a")),
			wantProbes: 5,
		},
		{
			name:       "a coordinator ID change resets the count",
			limit:      3,
			steps:      hbSteps(hbFailSteps(2, addrA, "a"), hbFailSteps(2, addrA, "b"), hbTrigger(addrA, "b")),
			wantProbes: 5,
		},
		{
			name:       "a coordinator address change resets the count",
			limit:      3,
			steps:      hbSteps(hbFailSteps(2, addrA, "a"), hbFailSteps(2, addrB, "a"), hbTrigger(addrB, "a")),
			wantProbes: 5,
		},
		{
			name:       "trailing slashes and spaces do not reset the count",
			limit:      3,
			steps:      hbSteps(hbFailSteps(1, addrA, "a"), hbFailSteps(1, " "+addrA+"/ ", "a"), hbTrigger(addrA+"/", "a")),
			wantProbes: 3,
		},
		{
			// Current behavior: once triggered, a replacement coordinator that
			// never answers cannot start another election; the first success
			// from any coordinator reports recovery of the failed identity.
			name:  "replacement coordinator inherits the triggered state",
			limit: 2,
			steps: hbSteps(
				hbFailSteps(1, addrA, "a"), hbTrigger(addrA, "a"),
				hbFailSteps(5, addrB, "b"), hbOK(addrB, "b"),
				hbFailSteps(1, addrB, "b"), hbTrigger(addrB, "b"),
			),
			wantRecoveries: []string{"a"},
			wantProbes:     10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wantElections []string
			for _, step := range tt.steps {
				if step.wantElection {
					wantElections = append(wantElections, step.id)
				}
			}
			elections, recoveries, probes := runHeartbeatScript(t, tt.limit, tt.steps)
			if !hbEqual(elections, wantElections) {
				t.Fatalf("elections = %v, want %v", elections, wantElections)
			}
			if !hbEqual(recoveries, tt.wantRecoveries) {
				t.Fatalf("recoveries = %v, want %v", recoveries, tt.wantRecoveries)
			}
			if probes != tt.wantProbes {
				t.Fatalf("probes = %d, want %d", probes, tt.wantProbes)
			}
		})
	}
}

func TestHeartbeatMonitorWithoutStarterKeepsProbing(t *testing.T) {
	var probes int
	var mu sync.Mutex
	ticks := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor := newHeartbeatMonitor(
		func() string {
			select {
			case <-ticks:
			case <-ctx.Done():
			}
			return "https://leader:7450"
		},
		func() string { return "leader" },
		func(context.Context, string) error {
			mu.Lock()
			probes++
			mu.Unlock()
			return errors.New("down")
		},
		nil,
	)
	monitor.interval = time.Millisecond
	monitor.failureLimit = 1
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.Run(ctx)
	}()
	for i := 0; i < 4; i++ {
		select {
		case ticks <- struct{}{}:
		case <-time.After(time.Second):
			t.Fatalf("monitor stopped probing after %d ticks without an election starter", i)
		}
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if probes < 3 {
		t.Fatalf("probes = %d, want the monitor to keep probing without a starter", probes)
	}
}

func hbEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
