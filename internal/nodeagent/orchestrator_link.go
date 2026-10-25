package nodeagent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/maistr0/maistr0/internal/background"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
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

// DemoteFromOrchestrator returns a failover-promoted node to worker duty and
// points it at the orchestrator it is yielding to. Registration then joins
// that orchestrator and the node's own LAN announcement resumes.
func (a *Agent) DemoteFromOrchestrator(orchestratorID, address string) {
	c := a.orchestratorLink
	c.orchMu.Lock()
	c.localOrchestratorID = ""
	c.orchestratorID = orchestratorID
	c.orchestratorAddr = address
	c.electionCandidateAddr = ""
	c.orchMu.Unlock()
	c.joinedMu.Lock()
	c.joined = false
	c.joinedMu.Unlock()
}

func (c *orchestratorLink) resolveOrchestratorAndRegister(ctx context.Context) {
	if c.getOrchestratorAddr() == "" && c.discoveryListener == nil {
		logging.Infof("nodeagent", "nodeagent: discovery disabled and no orchestrator configured; registration will retry")
	}
	c.registrationLoop(ctx)
}

func (c *orchestratorLink) registrationLoop(ctx context.Context) {
	register := func(ctx context.Context) {
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
				logging.Infof("nodeagent", "nodeagent: refusing insecure orchestrator address %q; use https", candidate.address)
				continue
			}
			if !c.trust.IsTrustedID(candidate.id) {
				if pairErr := c.pairOrchestratorContext(ctx, candidate); pairErr != nil {
					logging.Errorf("nodeagent", "nodeagent: pairing candidate %s failed: %v", candidate.id, pairErr)
					continue
				}
			}
			request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, candidate.address+"/api/nodes/register", bytes.NewReader(body))
			if requestErr != nil {
				logging.Errorf("nodeagent", "nodeagent: create registration request: %v", requestErr)
				continue
			}
			request.Header.Set("Content-Type", "application/json")
			resp, postErr := c.peerClient.Do(request)
			if postErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				resp.Body.Close()
				if candidate.role == "orchestrator" {
					certificate, ok := c.trust.TrustedPeers()[candidate.id]
					if !ok {
						logging.Errorf("nodeagent", "nodeagent: registration with %s succeeded but its certificate is missing from the trust roster", candidate.id)
						continue
					}
					if err := c.trust.TrustPeerDetails(candidate.id, certificate, "orchestrator", candidate.address); err != nil {
						logging.Errorf("nodeagent", "nodeagent: registration with %s succeeded but coordinator trust could not be updated: %v", candidate.id, err)
						continue
					}
				}
				previous := c.getOrchestratorAddr()
				c.setOrchestratorAddr(candidate.address)
				c.setOrchestratorID(candidate.id)
				c.markJoined()
				if previous != candidate.address {
					logging.Infof("nodeagent", "nodeagent: election winner is %s at %s, switching orchestrator", candidate.id, candidate.address)
				}
				return
			}
			if resp != nil {
				logging.Errorf("nodeagent", "nodeagent: registration candidate %s rejected or unreachable: %v (status %s)", candidate.address, postErr, resp.Status)
				resp.Body.Close()
			} else if postErr != nil {
				logging.Errorf("nodeagent", "nodeagent: registration candidate %s unreachable: %v", candidate.address, postErr)
			}
		}
		c.joinedMu.Lock()
		c.joined = false
		c.joinedMu.Unlock()
		if c.discoveryListener != nil {
			logging.Infof("nodeagent", "nodeagent: retaining the last orchestrator address for health monitoring and retry")
		}
		logging.Infof("nodeagent", "nodeagent: no elected orchestrator accepted registration; discovery remains active")
	}

	if !background.Wait(ctx, 1500*time.Millisecond) {
		return
	}
	register(ctx)
	background.RunEvery(ctx, 5*time.Second, register)
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

func (c *orchestratorLink) peerPollLoop(ctx context.Context) {
	poll := func(ctx context.Context) {
		addr := c.getOrchestratorAddr()
		if addr == "" {
			return
		}
		resp, err := getWithContext(ctx, c.peerClient, addr+"/api/nodes")
		if err != nil {
			logging.Errorf("nodeagent", "nodeagent: peer poll failed: %v", err)
			return
		}
		defer resp.Body.Close()
		var nodes []cluster.NodeStatus
		if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
			return
		}
		c.state.replacePeers(nodes)
		rosterResp, err := getWithContext(ctx, c.peerClient, addr+"/api/pairing/roster")
		if err != nil {
			logging.Errorf("nodeagent", "nodeagent: paired-peer roster refresh failed: %v", err)
		} else {
			var roster map[string]string
			decodeErr := json.NewDecoder(rosterResp.Body).Decode(&roster)
			_ = rosterResp.Body.Close()
			if decodeErr != nil {
				logging.Errorf("nodeagent", "nodeagent: decode paired-peer roster: %v", decodeErr)
			} else {
				orchestratorID := c.getOrchestratorID()
				for id, certificate := range roster {
					if id == c.configState.configSnapshot().NodeID {
						continue
					}
					if err := c.trust.TrustPeer(id, certificate); err != nil {
						logging.Errorf("nodeagent", "nodeagent: trust paired peer %q: %v", id, err)
					}
				}
				if orchestratorID != "" {
					for _, id := range c.trust.TrustedIDs() {
						if id == orchestratorID {
							continue
						}
						if _, ok := roster[id]; !ok {
							if err := c.trust.RemovePeer(id); err != nil {
								logging.Errorf("nodeagent", "nodeagent: revoke stale peer %q: %v", id, err)
							}
						}
					}
				}
			}
		}
	}

	if !background.Wait(ctx, 1500*time.Millisecond) {
		return
	}
	poll(ctx)
	background.RunEvery(ctx, 10*time.Second, poll)
}

func (c *orchestratorLink) sseSubscribeLoop(ctx context.Context) {
	if !background.Wait(ctx, 1500*time.Millisecond) {
		return
	}
	for ctx.Err() == nil {
		addr := c.getOrchestratorAddr()
		if addr == "" {
			if !background.Wait(ctx, time.Second) {
				return
			}
			continue
		}
		if err := c.subscribeOnceContext(ctx, addr); err != nil && ctx.Err() == nil {
			logging.Errorf("nodeagent", "nodeagent: event stream to orchestrator lost, retrying: %v", err)
		}
		if !background.Wait(ctx, 2*time.Second) {
			return
		}
	}
}

func (c *orchestratorLink) subscribeOnce(addr string) error {
	return c.subscribeOnceContext(context.Background(), addr)
}

func (c *orchestratorLink) subscribeOnceContext(ctx context.Context, addr string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/api/events", nil)
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
				logging.Errorf("nodeagent", "nodeagent: could not apply task snapshot from orchestrator: %v", err)
			}
		}
		c.state.replacePeers(snap.Nodes)
	}
	return scanner.Err()
}

func (c *orchestratorLink) broadcastLoop(ctx context.Context) {
	c.snapshotPublisher().Run(ctx)
}

func (c *orchestratorLink) publishSnapshot() {
	// currentTasks may fetch the full task list from a peer, so skip the
	// snapshot entirely when no dashboard is listening.
	if !c.events.HasSubscribers() {
		return
	}
	c.snapshotPublisher().Request()
}

func (c *orchestratorLink) snapshotPublisher() *hub.Publisher {
	c.publisherOnce.Do(func() {
		c.publisher = hub.NewPublisher(c.events, c.buildSnapshot)
	})
	return c.publisher
}

func (c *orchestratorLink) serveEvents(w http.ResponseWriter, r *http.Request) {
	c.events.ServeHTTP(w, r)
}

func (c *orchestratorLink) buildSnapshot(ctx context.Context) ([]byte, error) {
	var discoveryPeers []discovery.Peer
	if c.discoveryListener != nil {
		discoveryPeers = c.discoveryListener.Snapshot()
	}
	nodes := c.state.peersSnapshot()
	tasks, err := c.currentTasks(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		logging.Errorf("nodeagent", "nodeagent: fetch tasks for event snapshot: %v", err)
	}
	snapshot := struct {
		Nodes        []cluster.NodeStatus `json:"nodes"`
		Tasks        []*taskmgr.Task      `json:"tasks"`
		Discovery    []discovery.Peer     `json:"discovery"`
		BuildVersion string               `json:"build_version"`
		MemberCount  int                  `json:"member_count"`
	}{
		Nodes: nodes, Tasks: tasks, Discovery: discoveryPeers,
		BuildVersion: version.String(), MemberCount: len(nodes),
	}
	return json.Marshal(snapshot)
}

func getWithContext(ctx context.Context, client *http.Client, address string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(request)
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
	a.orchestratorLink.resolveOrchestratorAndRegister(a.orchestratorLink.background.Context())
}

func (a *Agent) registrationLoop() {
	a.orchestratorLink.registrationLoop(a.orchestratorLink.background.Context())
}

func (a *Agent) orchestratorCandidates() []orchestratorCandidate {
	return a.orchestratorLink.orchestratorCandidates()
}

func (a *Agent) peerPollLoop() {
	a.orchestratorLink.peerPollLoop(a.orchestratorLink.background.Context())
}

func (a *Agent) sseSubscribeLoop() {
	a.orchestratorLink.sseSubscribeLoop(a.orchestratorLink.background.Context())
}

func (a *Agent) subscribeOnce(addr string) error {
	return a.orchestratorLink.subscribeOnce(addr)
}

func (a *Agent) broadcastLoop() {
	a.orchestratorLink.broadcastLoop(a.orchestratorLink.background.Context())
}

func (a *Agent) publishSnapshot() {
	a.orchestratorLink.publishSnapshot()
}
