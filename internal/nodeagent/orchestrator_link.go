package nodeagent

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

type orchestratorCandidate struct {
	id      string
	address string
	score   float64
	role    string
}

var (
	errPairingInvitationAccepting        = errors.New("a pairing invitation is already being accepted")
	errPairingInvitationAlreadyAccepting = errors.New("pairing invitation is already being accepted")
	errPairingInvitationWaiting          = errors.New("another pairing invitation is waiting for local confirmation")
	errPairingInvitationExpired          = errors.New("pairing invitation is missing or expired")
	errPairingInvitationRejecting        = errors.New("pairing invitation is being accepted")
	errSelectedOrchestratorNotDiscovered = errors.New("selected orchestrator is no longer discovered")
)

func (c *orchestratorLink) getOrchestratorID() string {
	c.orchMu.RLock()
	defer c.orchMu.RUnlock()
	return c.orchestratorID
}

func (c *orchestratorLink) heartbeatOrchestratorID() string {
	if id := c.getOrchestratorID(); id != "" {
		return id
	}
	host := strings.TrimPrefix(strings.TrimPrefix(c.getOrchestratorAddr(), "http://"), "https://")
	if colon := strings.LastIndex(host, ":"); colon >= 0 {
		host = host[:colon]
	}
	if host == "" {
		return ""
	}
	return "orchestrator-" + host
}

func (a *Agent) getOrchestratorID() string {
	return a.orchestratorLink.getOrchestratorID()
}

func (a *Agent) setOrchestratorID(id string) {
	a.orchestratorLink.setOrchestratorID(id)
}

func (c *orchestratorLink) setOrchestratorID(id string) {
	c.orchMu.Lock()
	c.orchestratorID = id
	c.orchMu.Unlock()
}

func (c *orchestratorLink) localOrchestratorIDValue() string {
	c.orchMu.RLock()
	defer c.orchMu.RUnlock()
	return c.localOrchestratorID
}

func (c *orchestratorLink) setLocalOrchestratorID(id string) {
	c.orchMu.Lock()
	c.localOrchestratorID = id
	c.orchMu.Unlock()
}

func (c *orchestratorLink) getOrchestratorAddr() string {
	c.orchMu.RLock()
	defer c.orchMu.RUnlock()
	return c.orchestratorAddr
}

func (c *orchestratorLink) setOrchestratorAddr(addr string) {
	c.orchMu.Lock()
	c.orchestratorAddr = addr
	c.orchMu.Unlock()
}

func (c *orchestratorLink) setElectionCandidateAddr(addr string) {
	c.orchMu.Lock()
	c.electionCandidateAddr = addr
	c.orchMu.Unlock()
}

func (c *orchestratorLink) electionCandidateAddrValue() string {
	c.orchMu.RLock()
	defer c.orchMu.RUnlock()
	return c.electionCandidateAddr
}

func (c *orchestratorLink) markJoined() {
	c.joinedMu.Lock()
	c.joined = true
	c.joinedMu.Unlock()
}

func (c *orchestratorLink) isJoined() bool {
	c.joinedMu.Lock()
	defer c.joinedMu.Unlock()
	return c.joined
}

func (c *orchestratorLink) setDiscovery(listener *discovery.Listener) {
	c.discoveryListener = listener
}

// SetDiscovery attaches a shared discovery listener before Run.
func (a *Agent) SetDiscovery(listener *discovery.Listener) {
	a.orchestratorLink.setDiscovery(listener)
}

// SetElectionStarter configures the runtime failover callback. It runs away
// from the node scheduler after the orchestrator misses four status probes.
func (a *Agent) SetElectionStarter(start func(string)) {
	if start == nil {
		a.heartbeat.setElectionStarter(nil)
		return
	}
	a.heartbeat.setElectionStarter(func(failedID string) {
		if failedID != "" && a.discoveryListener != nil {
			a.discoveryListener.ExcludePeer(failedID)
		}
		start(failedID)
	})
}

// BeginElection advertises this node as a failover candidate at address.
func (a *Agent) BeginElection(address string) {
	a.orchestratorLink.setElectionCandidateAddr(address)
}

// EndElection returns this node to its ordinary worker announcement.
func (a *Agent) EndElection() {
	a.orchestratorLink.setElectionCandidateAddr("")
}

// PromoteToOrchestrator updates local coordinator state and pins this node's
// existing certificate as the local orchestrator identity.
func (a *Agent) PromoteToOrchestrator(address string) error {
	certificate, err := a.IdentityCertificate()
	if err != nil {
		return err
	}
	if err := a.identityComponent.TrustPeerDetails(a.ID(), certificate, "orchestrator", address); err != nil {
		return err
	}
	c := a.orchestratorLink
	c.orchMu.Lock()
	c.localOrchestratorID = a.ID()
	c.orchestratorID = a.ID()
	c.orchestratorAddr = address
	c.electionCandidateAddr = ""
	c.orchMu.Unlock()
	return nil
}

func (c *orchestratorLink) resolveOrchestratorAndRegister() {
	if c.getOrchestratorAddr() == "" && c.discoveryListener == nil {
		log.Printf("nodeagent: discovery disabled and no orchestrator configured; registration will retry")
	}
	c.registrationLoop()
}

func (c *orchestratorLink) registrationLoop() {
	register := func() {
		candidates := c.orchestratorCandidates()
		if len(candidates) == 0 {
			c.joinedMu.Lock()
			c.joined = false
			c.joinedMu.Unlock()
			return
		}
		body, err := json.Marshal(c.statusSnapshot())
		if err != nil {
			return
		}
		for _, candidate := range candidates {
			if !strings.HasPrefix(strings.ToLower(candidate.address), "https://") {
				log.Printf("nodeagent: refusing insecure orchestrator address %q; use https", candidate.address)
				continue
			}
			if !c.trust.IsTrustedID(candidate.id) {
				if pairErr := c.pairOrchestrator(candidate); pairErr != nil {
					log.Printf("nodeagent: pairing candidate %s failed: %v", candidate.id, pairErr)
					continue
				}
			}
			resp, postErr := c.peerClient.Post(candidate.address+"/api/nodes/register", "application/json", bytes.NewReader(body))
			if postErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				resp.Body.Close()
				if candidate.role == "orchestrator" {
					certificate, ok := c.trust.TrustedPeers()[candidate.id]
					if !ok {
						log.Printf("nodeagent: registration with %s succeeded but its certificate is missing from the trust roster", candidate.id)
						continue
					}
					if err := c.trust.TrustPeerDetails(candidate.id, certificate, "orchestrator", candidate.address); err != nil {
						log.Printf("nodeagent: registration with %s succeeded but coordinator trust could not be updated: %v", candidate.id, err)
						continue
					}
				}
				previous := c.getOrchestratorAddr()
				c.setOrchestratorAddr(candidate.address)
				c.setOrchestratorID(candidate.id)
				c.markJoined()
				if previous != candidate.address {
					log.Printf("nodeagent: election winner is %s at %s, switching orchestrator", candidate.id, candidate.address)
				}
				return
			}
			if resp != nil {
				log.Printf("nodeagent: registration candidate %s rejected or unreachable: %v (status %s)", candidate.address, postErr, resp.Status)
				resp.Body.Close()
			} else if postErr != nil {
				log.Printf("nodeagent: registration candidate %s unreachable: %v", candidate.address, postErr)
			}
		}
		c.joinedMu.Lock()
		c.joined = false
		c.joinedMu.Unlock()
		if c.discoveryListener != nil {
			log.Printf("nodeagent: retaining the last orchestrator address for health monitoring and retry")
		}
		log.Printf("nodeagent: no elected orchestrator accepted registration; discovery remains active")
	}

	time.Sleep(1500 * time.Millisecond)
	register()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		register()
	}
}

func (c *orchestratorLink) orchestratorCandidates() []orchestratorCandidate {
	byAddress := make(map[string]orchestratorCandidate)
	current := c.getOrchestratorAddr()
	if current != "" {
		host := strings.TrimPrefix(strings.TrimPrefix(current, "http://"), "https://")
		if colon := strings.LastIndex(host, ":"); colon >= 0 {
			host = host[:colon]
		}
		id := c.getOrchestratorID()
		if id == "" {
			id = "orchestrator-" + host
		}
		score := -1.0
		if id == c.localOrchestratorIDValue() {
			score = cluster.SelfScore(c.telemetry.hardwareSnapshot())
		}
		role := ""
		if c.trust.IsTrustedOrchestratorID(id) {
			role = "orchestrator"
		}
		byAddress[current] = orchestratorCandidate{id: id, address: current, score: score, role: role}
	}
	if c.discoveryListener != nil {
		for _, peer := range c.discoveryListener.Snapshot() {
			if peer.Role != "orchestrator" || peer.PeerAddr == "" {
				continue
			}
			candidate := orchestratorCandidate{
				id: peer.ID, address: peer.PeerAddr, score: peer.Score, role: peer.Role,
			}
			if existing, ok := byAddress[peer.PeerAddr]; !ok ||
				candidate.score > existing.score ||
				(candidate.score == existing.score && candidate.id < existing.id) {
				byAddress[peer.PeerAddr] = candidate
			}
		}
	}
	candidates := make([]orchestratorCandidate, 0, len(byAddress))
	for _, candidate := range byAddress {
		candidates = append(candidates, candidate)
	}
	sortOrchestratorCandidates(candidates)
	return candidates
}

func sortOrchestratorCandidates(candidates []orchestratorCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].id < candidates[j].id
	})
}

func (c *orchestratorLink) peerPollLoop() {
	poll := func() {
		addr := c.getOrchestratorAddr()
		if addr == "" {
			return
		}
		resp, err := c.peerClient.Get(addr + "/api/nodes")
		if err != nil {
			log.Printf("nodeagent: peer poll failed: %v", err)
			return
		}
		defer resp.Body.Close()
		var nodes []cluster.NodeStatus
		if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
			return
		}
		c.state.replacePeers(nodes)
		rosterResp, err := c.peerClient.Get(addr + "/api/pairing/roster")
		if err != nil {
			log.Printf("nodeagent: paired-peer roster refresh failed: %v", err)
		} else {
			var roster map[string]string
			decodeErr := json.NewDecoder(rosterResp.Body).Decode(&roster)
			_ = rosterResp.Body.Close()
			if decodeErr != nil {
				log.Printf("nodeagent: decode paired-peer roster: %v", decodeErr)
			} else {
				orchestratorID := c.getOrchestratorID()
				for id, certificate := range roster {
					if id == c.configState.configSnapshot().NodeID {
						continue
					}
					if err := c.trust.TrustPeer(id, certificate); err != nil {
						log.Printf("nodeagent: trust paired peer %q: %v", id, err)
					}
				}
				if orchestratorID != "" {
					for _, id := range c.trust.TrustedIDs() {
						if id == orchestratorID {
							continue
						}
						if _, ok := roster[id]; !ok {
							if err := c.trust.RemovePeer(id); err != nil {
								log.Printf("nodeagent: revoke stale peer %q: %v", id, err)
							}
						}
					}
				}
			}
		}
	}

	time.Sleep(1500 * time.Millisecond)
	poll()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		poll()
	}
}

func (c *orchestratorLink) sseSubscribeLoop() {
	time.Sleep(1500 * time.Millisecond)
	for {
		addr := c.getOrchestratorAddr()
		if addr == "" {
			time.Sleep(time.Second)
			continue
		}
		if err := c.subscribeOnce(addr); err != nil {
			log.Printf("nodeagent: event stream to orchestrator lost, retrying: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func (c *orchestratorLink) subscribeOnce(addr string) error {
	req, err := http.NewRequest(http.MethodGet, addr+"/api/events", nil)
	if err != nil {
		return err
	}
	resp, err := c.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		if c.getOrchestratorAddr() != addr {
			return errors.New("orchestrator changed, resubscribing")
		}
		data, ok := strings.CutPrefix(scanner.Text(), "data: ")
		if !ok {
			continue
		}
		var snap struct {
			Nodes       []cluster.NodeStatus `json:"nodes"`
			MemberCount int                  `json:"member_count"`
			Tasks       []*taskmgr.Task      `json:"tasks"`
		}
		if err := json.Unmarshal([]byte(data), &snap); err != nil {
			continue
		}
		if len(snap.Tasks) > 0 {
			if err := c.tasks.ReplaceFromSync(snap.Tasks); err != nil {
				log.Printf("nodeagent: could not apply task snapshot from orchestrator: %v", err)
			}
		}
		c.state.replacePeers(snap.Nodes)
	}
	return scanner.Err()
}

func (c *orchestratorLink) broadcastLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.publishSnapshot()
	}
}

func (c *orchestratorLink) publishSnapshot() {
	var discoveryPeers []discovery.Peer
	if c.discoveryListener != nil {
		discoveryPeers = c.discoveryListener.Snapshot()
	}
	nodes := c.state.peersSnapshot()
	snapshot := struct {
		Nodes        []cluster.NodeStatus `json:"nodes"`
		Tasks        []*taskmgr.Task      `json:"tasks"`
		Discovery    []discovery.Peer     `json:"discovery"`
		BuildVersion string               `json:"build_version"`
		MemberCount  int                  `json:"member_count"`
	}{
		Nodes: nodes, Tasks: c.currentTasks(), Discovery: discoveryPeers,
		BuildVersion: version.String(), MemberCount: len(nodes),
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	c.events.Broadcast(data)
}

func (a *Agent) getOrchestratorAddr() string {
	return a.orchestratorLink.getOrchestratorAddr()
}

func (a *Agent) setOrchestratorAddr(addr string) {
	a.orchestratorLink.setOrchestratorAddr(addr)
}

func (a *Agent) markJoined() { a.orchestratorLink.markJoined() }

func (a *Agent) isJoined() bool {
	return a.orchestratorLink.isJoined()
}

func (a *Agent) resolveOrchestratorAndRegister() {
	a.orchestratorLink.resolveOrchestratorAndRegister()
}

func (a *Agent) registrationLoop() { a.orchestratorLink.registrationLoop() }

func (a *Agent) orchestratorCandidates() []orchestratorCandidate {
	return a.orchestratorLink.orchestratorCandidates()
}

func (a *Agent) peerPollLoop() { a.orchestratorLink.peerPollLoop() }

func (a *Agent) sseSubscribeLoop() {
	a.orchestratorLink.sseSubscribeLoop()
}

func (a *Agent) subscribeOnce(addr string) error {
	return a.orchestratorLink.subscribeOnce(addr)
}

func (a *Agent) broadcastLoop() { a.orchestratorLink.broadcastLoop() }

func (a *Agent) publishSnapshot() {
	a.orchestratorLink.publishSnapshot()
}
