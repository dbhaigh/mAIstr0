package orchestrator

import (
	"log"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
)

type electionComponent struct {
	registry    *cluster.Registry
	statusProbe interface {
		discoveredNodeStatus(string) (cluster.NodeStatus, error)
	}
	identity         interface{ IsTrustedID(string) bool }
	discovery        *discovery.Listener
	selfID           string
	discoveryEnabled bool
	selfScore        float64
	leaderMu         sync.Mutex
	lastLeaderID     string
	discoveredNodes  map[string]bool
}

func newElectionComponent(
	registry *cluster.Registry,
	statusProbe interface {
		discoveredNodeStatus(string) (cluster.NodeStatus, error)
	},
	selfID string,
	selfScore float64,
	enabled bool,
) *electionComponent {
	return &electionComponent{
		registry: registry, statusProbe: statusProbe, selfID: selfID,
		selfScore: selfScore, discoveryEnabled: enabled,
		discoveredNodes: make(map[string]bool),
	}
}

func (e *electionComponent) setIdentity(identity interface{ IsTrustedID(string) bool }) {
	e.identity = identity
}

func (e *electionComponent) setDiscoveryEnabled(enabled bool) {
	e.discoveryEnabled = enabled
}

func (e *electionComponent) setDiscovery(listener *discovery.Listener) {
	e.discovery = listener
}

func (e *electionComponent) discoverySnapshot() []discovery.Peer {
	if e.discovery == nil {
		return nil
	}
	return e.discovery.Snapshot()
}

func (e *electionComponent) hasDiscovery() bool {
	return e.discovery != nil
}

func (e *electionComponent) startBeacon(listenAddr string) error {
	if !e.discoveryEnabled {
		return nil
	}
	if e.discovery == nil {
		e.discovery = discovery.Listen(e.selfID)
	}
	if err := e.discovery.WaitReady(2 * time.Second); err != nil {
		return err
	}
	selfAddr := "http://" + discovery.OutboundIP() + portSuffix(listenAddr)
	peerAddr := "https://" + discovery.OutboundIP() + portSuffix(listenAddr)
	go discovery.Beacon(func() discovery.Announcement {
		members := e.registry.Active()
		_, elected := e.registry.Leader()
		role := "candidate"
		if e.isElectedOrchestrator() {
			role = "orchestrator"
		}
		return discovery.Announcement{
			Role: role, ID: e.selfID, HTTPAddr: selfAddr, PeerAddr: peerAddr,
			Score: e.selfScore, Members: len(members), Elected: elected,
		}
	}, 5*time.Second, nil)
	return nil
}

func (e *electionComponent) isElectedOrchestrator() bool {
	if !e.discoveryEnabled || e.discovery == nil {
		return true
	}
	self := discovery.Announcement{Role: "orchestrator", ID: e.selfID, Score: e.selfScore}
	return discovery.IsElectedOrchestrator(self, e.discovery.Snapshot())
}

func (e *electionComponent) discoverySyncLoop(publish func()) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		e.syncDiscoveredPeers(publish)
	}
}

func (e *electionComponent) syncDiscoveredPeers(publish func()) {
	if !e.isElectedOrchestrator() {
		return
	}
	visibleNodes := make(map[string]bool)
	type discoveryResult struct {
		peer   discovery.Peer
		status cluster.NodeStatus
	}
	results := make(chan discoveryResult)
	var probes sync.WaitGroup
	peers := make(map[string]discovery.Peer)
	if e.discovery != nil {
		for _, peer := range e.discovery.Snapshot() {
			peers[peer.ID] = peer
		}
	}
	for _, peer := range peers {
		if peer.PeerAddr == "" || e.identity == nil || !e.identity.IsTrustedID(peer.ID) {
			continue
		}
		if peer.Role != "node" {
			continue
		}
		probes.Add(1)
		go func(peer discovery.Peer) {
			defer probes.Done()
			status, err := e.statusProbe.discoveredNodeStatus(peer.PeerAddr)
			if err != nil {
				log.Printf("orchestrator: secure status probe for paired node %s failed: %v", peer.ID, err)
				return
			}
			results <- discoveryResult{peer: peer, status: status}
		}(peer)
	}
	go func() {
		probes.Wait()
		close(results)
	}()
	for result := range results {
		visibleNodes[result.peer.ID] = true
		e.discoveredNodes[result.peer.ID] = true
		e.registry.Upsert(result.status)
	}
	for id := range e.discoveredNodes {
		if visibleNodes[id] {
			continue
		}
		if _, ok := e.registry.Get(id); ok {
			e.registry.MarkUnhealthy(id)
			log.Printf("orchestrator: node %s is offline after its LAN announcement expired", id)
		}
		delete(e.discoveredNodes, id)
	}
	publish()
}

func (e *electionComponent) recordLeader(leaderID string) {
	e.leaderMu.Lock()
	defer e.leaderMu.Unlock()
	if leaderID != e.lastLeaderID {
		if leaderID != "" {
			log.Printf("orchestrator: cluster leadership passed to %s (most capable node)", leaderID)
		}
		e.lastLeaderID = leaderID
	}
}

func (e *electionComponent) selfIDValue() string { return e.selfID }
