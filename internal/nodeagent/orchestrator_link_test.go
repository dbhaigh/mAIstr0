package nodeagent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
)

func TestOrchestratorEventStreamIgnoresHeartbeatAndParsesCRLFFrames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: heartbeat\r\ndata: ping\r\n\r\ndata: {\"nodes\":[{\"id\":\"next-node\"}]}\r\n\r\n"))
	}))
	defer server.Close()

	state := &agentState{peers: []cluster.NodeStatus{{ID: "existing-node"}}}
	link := &orchestratorLink{
		streamClient: server.Client(),
		state:        state,
	}
	link.setOrchestratorAddr(server.URL)

	if err := link.subscribeOnceContext(context.Background(), server.URL); err != nil {
		t.Fatalf("subscribeOnceContext() error = %v", err)
	}
	if got := state.peersSnapshot(); len(got) != 1 || got[0].ID != "next-node" {
		t.Fatalf("peer snapshot after heartbeat = %#v, want next-node", got)
	}
}
