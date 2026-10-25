package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/nodeagent"
	"github.com/maistr0/maistr0/internal/orchestrator"
)

type failoverCoordinator struct {
	agent          *nodeagent.Agent
	listener       *discovery.Listener
	orchestrator   config.OrchestratorConfig
	nodeListenAddr string

	// mu protects the election and promotion state below. It is never held
	// across network calls or server lifecycle operations.
	mu       sync.Mutex
	running  bool
	promoted *orchestrator.Server
	// retired is a stepped-down promoted server whose listener stays bound;
	// a later failover reinstates it instead of binding the port again.
	retired     *orchestrator.Server
	stopWatch   context.CancelFunc
	watchPeriod time.Duration
}

func newFailoverCoordinator(
	agent *nodeagent.Agent,
	listener *discovery.Listener,
	orchestratorConfig config.OrchestratorConfig,
	nodeListenAddr string,
) *failoverCoordinator {
	return &failoverCoordinator{
		agent:          agent,
		listener:       listener,
		orchestrator:   orchestratorConfig,
		nodeListenAddr: nodeListenAddr,
	}
}

// StartElection runs after the heartbeat monitor has detected an unavailable
// coordinator. It advertises this node during one discovery lease, selects
// only trusted cluster members, and promotes the winner with its existing
// certificate and member roster.
func (f *failoverCoordinator) StartElection(failedOrchestratorID string) {
	f.mu.Lock()
	if f.running || f.promoted != nil {
		f.mu.Unlock()
		return
	}
	f.running = true
	f.mu.Unlock()

	promoted := false
	defer func() {
		if !promoted {
			f.agent.EndElection()
		}
		f.mu.Lock()
		f.running = false
		f.mu.Unlock()
	}()

	if f.listener == nil || !f.orchestrator.DiscoveryEnabled {
		logging.Errorf("main", "failover: cannot elect a coordinator because LAN discovery is disabled")
		return
	}
	if err := f.listener.WaitReady(2 * time.Second); err != nil {
		logging.Errorf("main", "failover: cannot start election: %v", err)
		return
	}
	identity := f.agent.ClusterIdentity()
	if identity == nil {
		logging.Errorf("main", "failover: cannot start election before the node identity is initialized")
		return
	}
	if failedOrchestratorID == "" || !identity.IsTrustedID(failedOrchestratorID) {
		logging.Infof("main", "failover: refusing to elect a coordinator without a paired current-orchestrator identity")
		return
	}

	if failedOrchestratorID != "" {
		f.listener.ExcludePeer(failedOrchestratorID)
	}
	candidate := discovery.Announcement{
		Role:     "candidate",
		ID:       f.agent.ID(),
		PeerAddr: "https://" + discovery.OutboundIP() + portSuffix(f.orchestrator.ListenAddr),
		Score:    cluster.SelfScore(hardware.Detect()),
	}
	f.agent.BeginElection(candidate.PeerAddr)
	logging.Infof("main", "failover: node %s is checking candidates for %s", candidate.ID, startupElectionWindow)
	time.Sleep(startupElectionWindow)

	winner := failoverElectionWinner(candidate, f.listener.Snapshot(), failedOrchestratorID, identity.IsTrustedID)
	if winner.ID != candidate.ID {
		logging.Infof("main", "failover: trusted cluster member %s won the election; this node remains a worker", winner.ID)
		return
	}

	if err := f.promote(candidate.PeerAddr, identity, failedOrchestratorID); err != nil {
		logging.Errorf("main", "failover: could not promote elected node %s: %v", candidate.ID, err)
		return
	}
	promoted = true
	logging.Infof("main", "failover: node %s is now the cluster orchestrator", candidate.ID)
}

func failoverElectionWinner(
	candidate discovery.Announcement,
	peers []discovery.Peer,
	failedOrchestratorID string,
	isTrusted func(string) bool,
) discovery.Announcement {
	eligible := make([]discovery.Peer, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == failedOrchestratorID || peer.PeerAddr == "" {
			continue
		}
		if peer.Role != "orchestrator" && peer.Role != "candidate" {
			continue
		}
		if !isTrusted(peer.ID) {
			continue
		}
		eligible = append(eligible, peer)
	}
	return startupOrchestratorWinner(candidate, eligible, true)
}

type promotionIdentity interface {
	ID() string
	CertificatePEM() string
	TrustPeerDetails(string, string, string, string) error
	ClientTransport() *http.Transport
	IsTrustedID(string) bool
}

func (f *failoverCoordinator) promote(address string, identity promotionIdentity, replacedID string) error {
	f.mu.Lock()
	retired := f.retired
	f.mu.Unlock()
	if retired != nil {
		return f.reinstate(retired, address, identity, replacedID)
	}
	server, err := orchestrator.NewWithConfig(f.orchestrator)
	if err != nil {
		return fmt.Errorf("configure promoted orchestrator: %w", err)
	}
	if err := server.SetAdvertiseAddress(address); err != nil {
		_ = server.Close()
		return fmt.Errorf("configure promoted orchestrator address: %w", err)
	}
	if err := server.SetLocalNodeDashboard(localURL(f.nodeListenAddr)); err != nil {
		_ = server.Close()
		return fmt.Errorf("configure promoted node dashboard: %w", err)
	}
	if err := identity.TrustPeerDetails(identity.ID(), identity.CertificatePEM(), "orchestrator", address); err != nil {
		_ = server.Close()
		return fmt.Errorf("trust promoted node identity: %w", err)
	}
	if err := server.UseIdentity(f.agent.ClusterIdentity()); err != nil {
		_ = server.Close()
		return fmt.Errorf("share promoted node identity with orchestrator: %w", err)
	}
	server.SetDiscoveryEnabled(true)
	server.SetDiscovery(f.listener)

	f.mu.Lock()
	f.promoted = server
	f.mu.Unlock()
	runDone := make(chan error, 1)
	go func() {
		err := server.Run(f.orchestrator.ListenAddr)
		if err != nil {
			logging.Errorf("main", "failover: promoted orchestrator stopped: %v", err)
		} else {
			logging.Infof("main", "failover: promoted orchestrator stopped unexpectedly")
		}
		runDone <- err
		f.mu.Lock()
		if f.promoted == server {
			f.promoted = nil
			f.stopWatchLocked()
		}
		if f.retired == server {
			f.retired = nil
		}
		f.mu.Unlock()
	}()

	client := &http.Client{Timeout: 3 * time.Second, Transport: identity.ClientTransport()}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := waitForStatusEndpoint(ctx, client, localHTTPSAddress(f.orchestrator.ListenAddr), runDone); err != nil {
		return err
	}
	if err := f.agent.PromoteToOrchestrator(address); err != nil {
		return fmt.Errorf("update local coordinator state: %w", err)
	}
	f.startWatch(server, identity, replacedID)
	return nil
}

// reinstate resumes a previously retired promoted server, which still owns
// the orchestrator listener.
func (f *failoverCoordinator) reinstate(server *orchestrator.Server, address string, identity promotionIdentity, replacedID string) error {
	if err := identity.TrustPeerDetails(identity.ID(), identity.CertificatePEM(), "orchestrator", address); err != nil {
		return fmt.Errorf("trust promoted node identity: %w", err)
	}
	server.Reinstate()
	f.mu.Lock()
	if f.retired != server {
		f.mu.Unlock()
		server.Retire()
		return errors.New("retired orchestrator stopped before it could be reinstated")
	}
	f.retired = nil
	f.promoted = server
	f.mu.Unlock()
	if err := f.agent.PromoteToOrchestrator(address); err != nil {
		server.Retire()
		f.mu.Lock()
		if f.promoted == server {
			f.promoted = nil
			f.retired = server
		}
		f.mu.Unlock()
		return fmt.Errorf("update local coordinator state: %w", err)
	}
	f.startWatch(server, identity, replacedID)
	return nil
}

const defaultStepDownWatchPeriod = 5 * time.Second

// startWatch monitors a promoted server for a competing orchestrator and
// steps down when one should lead instead (see stepDownCandidates).
func (f *failoverCoordinator) startWatch(server *orchestrator.Server, identity promotionIdentity, replacedID string) {
	ctx, cancel := context.WithCancel(context.Background())
	f.mu.Lock()
	f.stopWatchLocked()
	f.stopWatch = cancel
	period := f.watchPeriod
	f.mu.Unlock()
	if period <= 0 {
		period = defaultStepDownWatchPeriod
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: identity.ClientTransport()}
	probe := func(ctx context.Context, address string) error {
		return probeStatus(ctx, client, address)
	}
	go f.watch(ctx, period, server, identity.IsTrustedID, replacedID, probe)
}

func (f *failoverCoordinator) stopWatchLocked() {
	if f.stopWatch != nil {
		f.stopWatch()
		f.stopWatch = nil
	}
}

// watch requires the same challenger on two consecutive checks so a single
// stale announcement or transient probe result cannot trigger a step-down.
func (f *failoverCoordinator) watch(
	ctx context.Context,
	period time.Duration,
	server *orchestrator.Server,
	isTrusted func(string) bool,
	replacedID string,
	probe func(context.Context, string) error,
) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	pending := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		self := server.LeadershipAnnouncement()
		target, ok := firstHealthyChallenger(ctx, stepDownCandidates(self, replacedID, f.listener.Snapshot(), isTrusted), probe)
		if !ok {
			pending = ""
			continue
		}
		if pending != target.ID {
			pending = target.ID
			continue
		}
		f.stepDown(server, target)
		return
	}
}

func firstHealthyChallenger(ctx context.Context, candidates []discovery.Peer, probe func(context.Context, string) error) (discovery.Peer, bool) {
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return discovery.Peer{}, false
		}
		if err := probe(ctx, candidate.PeerAddr); err == nil {
			return candidate, true
		}
	}
	return discovery.Peer{}, false
}

// stepDownCandidates lists trusted active orchestrators that should lead
// instead of the promoted self, best first. A challenger qualifies when it
// outranks self, is the orchestrator self replaced during failover (and leads
// at least as many members), or leads more members. Callers must still
// confirm the challenger answers as elected: an orchestrator that can see a
// higher-ranked self yields instead, so only one side steps down.
func stepDownCandidates(self discovery.Announcement, replacedID string, peers []discovery.Peer, isTrusted func(string) bool) []discovery.Peer {
	out := make([]discovery.Peer, 0, len(peers))
	for _, peer := range peers {
		if peer.Role != "orchestrator" || peer.ID == self.ID || peer.PeerAddr == "" || !isTrusted(peer.ID) {
			continue
		}
		outranks := peer.Score > self.Score || (peer.Score == self.Score && peer.ID < self.ID)
		original := replacedID != "" && peer.ID == replacedID && peer.Members >= self.Members
		if outranks || original || peer.Members > self.Members {
			out = append(out, peer)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Members != out[j].Members {
			return out[i].Members > out[j].Members
		}
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (f *failoverCoordinator) stepDown(server *orchestrator.Server, target discovery.Peer) {
	f.mu.Lock()
	if f.promoted != server {
		f.mu.Unlock()
		return
	}
	f.promoted = nil
	f.retired = server
	f.stopWatch = nil
	f.mu.Unlock()
	server.Retire()
	f.agent.DemoteFromOrchestrator(target.ID, target.PeerAddr)
	logging.Infof("main", "failover: stepping down as orchestrator; orchestrator %s at %s leads %d members and answers as elected", target.ID, target.PeerAddr, target.Members)
}

func localHTTPSAddress(listenAddr string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "https://" + listenAddr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "https://" + net.JoinHostPort(host, port)
}

// probeStatus makes one status request; any 2xx answer means the instance
// currently serves as an elected orchestrator (yielded ones answer 503).
func probeStatus(ctx context.Context, client *http.Client, address string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+"/api/status", nil)
	if err != nil {
		return fmt.Errorf("create orchestrator status request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("orchestrator returned %s", response.Status)
	}
	return nil
}

func waitForStatusEndpoint(ctx context.Context, client *http.Client, address string, runDone <-chan error) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		if err := probeStatus(ctx, client, address); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case err := <-runDone:
			if err != nil {
				return fmt.Errorf("promoted orchestrator stopped before becoming healthy: %w", err)
			}
			return errors.New("promoted orchestrator stopped before becoming healthy")
		case <-ctx.Done():
			return fmt.Errorf("promoted orchestrator did not become healthy: %w (last probe: %v)", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func (f *failoverCoordinator) Close() error {
	f.mu.Lock()
	f.stopWatchLocked()
	servers := []*orchestrator.Server{f.promoted, f.retired}
	f.promoted, f.retired = nil, nil
	f.mu.Unlock()
	var closeErr error
	for _, server := range servers {
		if server == nil {
			continue
		}
		if err := server.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}
