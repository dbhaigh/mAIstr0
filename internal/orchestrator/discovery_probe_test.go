package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/discovery"
)

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
