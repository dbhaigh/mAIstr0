package nodeagent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/memory"
)

func newTestAgent(t *testing.T, id string) *Agent {
	t.Helper()
	a := New(config.NodeConfig{
		NodeID:        id,
		ListenAddr:    ":0",
		AdvertiseAddr: "http://127.0.0.1:0",
		MemoryPath:    filepath.Join(t.TempDir(), id+".db"),
	})
	a.engines = []engine.Engine{engine.NewSimulated()}
	t.Cleanup(func() {
		if a.mem != nil {
			_ = a.mem.Close()
		}
	})
	return a
}

func TestPeerMessageAnswersWithLocalLLM(t *testing.T) {
	a := newTestAgent(t, "node-b")

	body := `{"from_node":"node-a","from_model":"coder","dialogue_id":"p2p-1","topic":"caching","content":"I propose an LRU cache. Do you agree?"}`
	rec := httptest.NewRecorder()
	a.handlePeerMessage(rec, httptest.NewRequest("POST", "/peer/message", strings.NewReader(body)))

	if rec.Code != 200 {
		t.Fatalf("peer message returned %d: %s", rec.Code, rec.Body.String())
	}
	var resp peerMessageResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.NodeID != "node-b" || resp.Reply == "" {
		t.Fatalf("unexpected peer reply: %+v", resp)
	}
	if resp.DialogueID != "p2p-1" {
		t.Errorf("expected dialogue continuity, got %q", resp.DialogueID)
	}

	// The peer's framing must reach the model so it knows who it is talking to.
	a.dialoguesMu.RLock()
	dlg := a.dialogues["p2p-1"]
	a.dialoguesMu.RUnlock()
	if dlg == nil {
		t.Fatal("expected dialogue to be stored locally")
	}
	joined := ""
	for _, m := range dlg.Messages {
		joined += m.Role + ":" + m.Content + "\n"
	}
	if !strings.Contains(joined, "node-a") {
		t.Errorf("peer identity missing from dialogue context:\n%s", joined)
	}
}

func TestNodeToNodeConversation(t *testing.T) {
	// node-b serves the real peer endpoint; node-a drives the conversation.
	nodeB := newTestAgent(t, "node-b")
	peerServer := httptest.NewServer(http.HandlerFunc(nodeB.handlePeerMessage))
	defer peerServer.Close()

	nodeA := newTestAgent(t, "node-a")
	nodeA.mu.Lock()
	nodeA.peers = []cluster.NodeStatus{
		{ID: "node-b", Address: peerServer.URL, Healthy: true, Hardware: hardware.Info{CPUCores: 4}},
	}
	nodeA.mu.Unlock()

	res, err := nodeA.ConversePeer(peerConverseRequest{
		TargetNodeID: "node-b",
		Topic:        "cache eviction strategy",
		Rounds:       2,
	})
	if err != nil {
		t.Fatalf("peer conversation failed: %v", err)
	}
	if res.Rounds != 2 || len(res.Exchanges) != 2 {
		t.Fatalf("expected 2 exchange rounds, got %d", len(res.Exchanges))
	}
	for i, ex := range res.Exchanges {
		if ex.LocalMessage == "" || ex.PeerReply == "" {
			t.Errorf("round %d had an empty side: %+v", i+1, ex)
		}
		if ex.LocalNode != "node-a" || ex.PeerNode != "node-b" {
			t.Errorf("round %d had wrong participants: %+v", i+1, ex)
		}
	}
	if res.Conclusion == "" {
		t.Error("expected a conclusion summarising the peer conversation")
	}

	// Both sides must have persisted the exchange to their memory stores.
	stored, err := nodeA.mem.Dialogue(res.DialogueID)
	if err != nil {
		t.Fatalf("dialogue not persisted: %v", err)
	}
	if len(stored.Turns) < 4 {
		t.Errorf("expected at least 4 persisted turns, got %d", len(stored.Turns))
	}
	if len(stored.Participants) != 2 {
		t.Errorf("expected both nodes recorded as participants, got %v", stored.Participants)
	}
}

func TestPeerConversationRejectsSelfTalk(t *testing.T) {
	a := newTestAgent(t, "node-a")
	if _, err := a.ConversePeer(peerConverseRequest{TargetNodeID: "node-a", Topic: "x"}); err == nil {
		t.Error("expected a node to refuse conversing with itself")
	}
}

func TestNodeRecordsExperienceToMemory(t *testing.T) {
	a := newTestAgent(t, "node-mem")

	body := `{"model":"simulated-general","prompt":"summarise the raft protocol","subtask_id":"s1","task_type":"summarize"}`
	rec := httptest.NewRecorder()
	a.handleExecute(rec, httptest.NewRequest("POST", "/execute", strings.NewReader(body)))
	if rec.Code != 200 {
		t.Fatalf("execute returned %d: %s", rec.Code, rec.Body.String())
	}

	recalled := a.mem.Recall(memory.SearchFilter{Query: "raft protocol", Limit: 5})
	if len(recalled) == 0 {
		t.Fatal("expected the executed subtask to be remembered")
	}
	if recalled[0].NodeID != "node-mem" || !recalled[0].Success {
		t.Errorf("unexpected recorded experience: %+v", recalled[0])
	}
}
