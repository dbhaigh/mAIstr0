package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"net"
	"net/http"
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

	mu       sync.Mutex
	running  bool
	promoted *orchestrator.Server
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

	if err := f.promote(candidate.PeerAddr, identity); err != nil {
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

func (f *failoverCoordinator) promote(address string, identity interface {
	ID() string
	CertificatePEM() string
	TrustPeerDetails(string, string, string, string) error
	ClientTransport() *http.Transport
}) error {
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
	return nil
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

func waitForStatusEndpoint(ctx context.Context, client *http.Client, address string, runDone <-chan error) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+"/api/status", nil)
		if err != nil {
			return fmt.Errorf("create promoted orchestrator status request: %w", err)
		}
		response, err := client.Do(request)
		if err == nil {
			if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
				response.Body.Close()
				return nil
			}
			lastErr = fmt.Errorf("promoted orchestrator returned %s", response.Status)
			response.Body.Close()
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
	server := f.promoted
	f.promoted = nil
	f.mu.Unlock()
	if server == nil {
		return nil
	}
	return server.Close()
}
