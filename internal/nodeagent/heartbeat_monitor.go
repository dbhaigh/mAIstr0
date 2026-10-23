package nodeagent

import (
	"context"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	heartbeatInterval       = 5 * time.Second
	heartbeatRequestTimeout = 2 * time.Second
	heartbeatFailureLimit   = 4
)

// HeartbeatMonitor checks the current orchestrator independently of node task
// scheduling and starts an election after consecutive status-probe failures.
type HeartbeatMonitor struct {
	address       func() string
	orchestrator  func() string
	ping          func(context.Context, string) error
	onRecovery    func(string)
	interval      time.Duration
	timeout       time.Duration
	failureLimit  int
	starterMu     sync.RWMutex
	startElection func(string)
}

func newHeartbeatMonitor(
	address func() string,
	orchestrator func() string,
	ping func(context.Context, string) error,
	onRecovery func(string),
) *HeartbeatMonitor {
	return &HeartbeatMonitor{
		address:      address,
		orchestrator: orchestrator,
		ping:         ping,
		onRecovery:   onRecovery,
		interval:     heartbeatInterval,
		timeout:      heartbeatRequestTimeout,
		failureLimit: heartbeatFailureLimit,
	}
}

func (m *HeartbeatMonitor) setElectionStarter(start func(string)) {
	m.starterMu.Lock()
	m.startElection = start
	m.starterMu.Unlock()
}

// Run polls until ctx is cancelled. Election startup is dispatched separately
// so a slow election cannot hold up subsequent health checks.
func (m *HeartbeatMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	failures := 0
	lastAddress, lastID := "", ""
	triggered := false
	failedID := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		address := strings.TrimRight(strings.TrimSpace(m.address()), "/")
		if address == "" {
			failures = 0
			continue
		}
		id := m.orchestrator()
		if address != lastAddress || id != lastID {
			failures = 0
			lastAddress, lastID = address, id
		}

		probeCtx, cancel := context.WithTimeout(ctx, m.timeout)
		err := m.ping(probeCtx, address)
		cancel()
		if err == nil {
			failures = 0
			if triggered {
				if m.onRecovery != nil {
					m.onRecovery(failedID)
				}
				logging.Infof("nodeagent", "nodeagent: orchestrator %s at %s responded to health checks again", failedID, address)
				triggered = false
				failedID = ""
			}
			continue
		}

		failures++
		if failures < m.failureLimit || triggered {
			continue
		}
		triggered = true
		failedID = id
		logging.Errorf("nodeagent", "nodeagent: orchestrator %s at %s failed %d consecutive status probes: %v", id, address, failures, err)

		m.starterMu.RLock()
		start := m.startElection
		m.starterMu.RUnlock()
		if start == nil {
			logging.Errorf("nodeagent", "nodeagent: no election starter is configured; orchestrator failover cannot proceed")
			continue
		}
		go start(id)
	}
}

func statusProbe(client *http.Client) func(context.Context, string) error {
	return func(ctx context.Context, address string) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, address+"/api/status", nil)
		if err != nil {
			return fmt.Errorf("create orchestrator status request: %w", err)
		}
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("request orchestrator status: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return fmt.Errorf("orchestrator status endpoint returned %s", response.Status)
		}
		return nil
	}
}
