package orchestrator

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/discovery"
)

type roleSwitchDiscovery struct {
	peers []discovery.Peer
}

func (d *roleSwitchDiscovery) Snapshot() []discovery.Peer    { return d.peers }
func (d *roleSwitchDiscovery) WaitReady(time.Duration) error { return nil }

type roleSwitchTrust bool

func (t roleSwitchTrust) IsTrustedID(string) bool { return bool(t) }

type roleSwitchStatusProbe struct {
	status    cluster.NodeStatus
	addresses []string
	err       error
}

func (p *roleSwitchStatusProbe) discoveredNodeStatus(address string) (cluster.NodeStatus, error) {
	p.addresses = append(p.addresses, address)
	return p.status, p.err
}

func TestRegisteredWorkerStaysHealthyAcrossDiscoveryRoleChanges(t *testing.T) {
	registry := cluster.NewRegistry()
	worker := cluster.NodeStatus{ID: "worker", Address: "https://worker:7451"}
	registry.Upsert(worker)
	probe := &roleSwitchStatusProbe{status: worker}
	election := newElectionComponent(registry, probe, "leader", 1, false)
	listener := &roleSwitchDiscovery{}
	election.discovery = listener
	election.setIdentity(roleSwitchTrust(true))
	for _, role := range []string{"node", "candidate", "node", "orchestrator", "candidate", "node"} {
		listener.peers = []discovery.Peer{{Announcement: discovery.Announcement{
			ID: worker.ID, Role: role, PeerAddr: "https://worker:7450", OrchestratorID: "leader",
		}}}
		if role == "node" {
			listener.peers[0].PeerAddr = worker.Address
		}
		election.syncDiscoveredPeers(func() {})
		current, _ := registry.Get(worker.ID)
		if !current.Healthy {
			t.Fatalf("worker marked offline for role %s", role)
		}
		if got := probe.addresses[len(probe.addresses)-1]; got != worker.Address {
			t.Fatalf("probed coordinator endpoint for role %s: %s", role, got)
		}
	}
	if len(probe.addresses) != 6 {
		t.Fatalf("status probes=%v", probe.addresses)
	}
	listener.peers = nil
	election.syncDiscoveredPeers(func() {})
	current, _ := registry.Get(worker.ID)
	if current.Healthy {
		t.Fatal("expired worker remained healthy")
	}
}

func TestCoordinatorRoleWorkerProbePreservesTrustAndFailureChecks(t *testing.T) {
	for _, test := range []struct {
		name                string
		trusted, registered bool
		role, orchestrator  string
		probeErr            error
		wantProbes          int
	}{
		{"unreachable worker", true, true, "candidate", "", errors.New("connection refused"), 1},
		{"unregistered candidate", true, false, "candidate", "", nil, 0},
		{"untrusted candidate", false, true, "candidate", "", nil, 0},
		{"worker changed coordinator", true, true, "node", "other-leader", nil, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := cluster.NewRegistry()
			worker := cluster.NodeStatus{ID: "worker", Address: "https://worker:7451"}
			if test.registered {
				registry.Upsert(worker)
			}
			probe := &roleSwitchStatusProbe{status: worker, err: test.probeErr}
			election := newElectionComponent(registry, probe, "leader", 1, false)
			election.discovery = &roleSwitchDiscovery{peers: []discovery.Peer{{Announcement: discovery.Announcement{
				ID: worker.ID, Role: test.role, PeerAddr: "https://worker:7450", OrchestratorID: test.orchestrator,
			}}}}
			election.setIdentity(roleSwitchTrust(test.trusted))
			election.discoveredNodes[worker.ID] = true
			election.syncDiscoveredPeers(func() {})
			current, _ := registry.Get(worker.ID)
			if current.Healthy || len(probe.addresses) != test.wantProbes {
				t.Fatalf("healthy=%v probes=%v", current.Healthy, probe.addresses)
			}
		})
	}
}

func TestShouldProbeDiscoveredNode(t *testing.T) {
	tests := []struct {
		name string
		peer discovery.Peer
		want bool
	}{
		{
			name: "current coordinator",
			peer: discovery.Peer{Announcement: discovery.Announcement{
				Role: "node", OrchestratorID: "leader-a",
			}},
			want: true,
		},
		{
			name: "legacy announcement without coordinator",
			peer: discovery.Peer{Announcement: discovery.Announcement{
				Role: "node",
			}},
			want: true,
		},
		{
			name: "different coordinator",
			peer: discovery.Peer{Announcement: discovery.Announcement{
				Role: "node", OrchestratorID: "leader-b",
			}},
			want: false,
		},
		{
			name: "not a node",
			peer: discovery.Peer{Announcement: discovery.Announcement{
				Role: "orchestrator", OrchestratorID: "leader-a",
			}},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldProbeDiscoveredNode(test.peer, "leader-a"); got != test.want {
				t.Fatalf("shouldProbeDiscoveredNode() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDiscoveredNodeStatus403ExplainsMutualTrust(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer peer.Close()

	client := newNodeClient(nil, nil, &http.Client{})
	_, err := client.discoveredNodeStatus(peer.URL)
	if err == nil || !strings.Contains(err.Error(), "verify mutual pairing") {
		t.Fatalf("403 status error = %v, want mutual-pairing guidance", err)
	}
}
