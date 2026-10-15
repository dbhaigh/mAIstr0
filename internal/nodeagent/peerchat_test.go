package nodeagent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func newTestAgent(t *testing.T, id string) *Agent {
	t.Helper()
	a := mustNewTestAgent(t, config.NodeConfig{
		NodeID:        id,
		ListenAddr:    ":0",
		AdvertiseAddr: "https://127.0.0.1:0",
		MemoryPath:    filepath.Join(t.TempDir(), id+".db"),
	})
	a.engines = []engine.Engine{engine.NewSimulated()}
	a.detectedEngines = nil
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

func TestStandaloneLeaderReplicatesMemoryAndLearnsPeerExperience(t *testing.T) {
	leader := newTestAgent(t, "node-a")
	peer := newTestAgent(t, "node-b")
	if _, err := leader.mem.PutFact(memory.Fact{Key: "leader-note", Value: "from node-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.mem.PutFact(memory.Fact{Key: "peer-note", Value: "from node-b"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := peer.mem.Record(memory.Experience{
			Kind: "generate", TaskType: "code", NodeID: "node-b", Model: "coder",
			Success: true, DurationMs: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}

	peerMux := http.NewServeMux()
	peerMux.HandleFunc("GET /memory/sync", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(30 * time.Millisecond)
		writeJSON(w, http.StatusOK, peer.mem.Snapshot())
	})
	peerMux.HandleFunc("POST /memory/sync", func(w http.ResponseWriter, r *http.Request) {
		var snapshot memory.Snapshot
		if err := json.NewDecoder(r.Body).Decode(&snapshot); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := peer.mem.MergeSnapshot(snapshot); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	peerServer := httptest.NewServer(peerMux)
	defer peerServer.Close()

	leader.mu.Lock()
	leader.peers = []cluster.NodeStatus{
		{ID: "node-a", Healthy: true, Leader: true},
		{ID: "node-b", Address: peerServer.URL, Healthy: true},
	}
	leader.mu.Unlock()
	leader.peerClient.Timeout = 10 * time.Millisecond

	leader.syncPeerMemory()
	leader.syncPeerMemory()

	if bias := leader.mem.LearnedBias("node-b", "coder", "code"); bias <= 0 {
		t.Fatalf("leader did not learn from peer experience, bias = %.2f", bias)
	}
	if got := len(leader.mem.Experiences(10)); got != 3 {
		t.Fatalf("repeated sync duplicated peer experiences: got %d, want 3", got)
	}
	if _, err := leader.mem.GetFact("cluster", "peer-note"); err != nil {
		t.Fatalf("peer fact was not replicated to leader: %v", err)
	}
	if _, err := peer.mem.GetFact("cluster", "leader-note"); err != nil {
		t.Fatalf("leader fact was not replicated to peer: %v", err)
	}
	if got := len(peer.mem.Experiences(10)); got != 3 {
		t.Fatalf("repeated sync duplicated experiences on peer: got %d, want 3", got)
	}
}

func TestSyncOrchestratorMemoryUsesLongTimeoutClient(t *testing.T) {
	a := newTestAgent(t, "node-a")
	a.peerClient.Timeout = 10 * time.Millisecond

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/memory/sync", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(30 * time.Millisecond)
		watermarks := map[string]uint64{}
		if err := memory.SetSyncResponseHeaders(w, watermarks); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, memory.Snapshot{
			SyncVersion: memory.SyncProtocolVersion,
			Watermarks:  watermarks,
		})
	})
	mux.HandleFunc("POST /api/memory/sync", func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	if err := a.syncOrchestratorMemory(server.URL); err != nil {
		t.Fatalf("memory sync failed with a delayed response: %v", err)
	}
}

func TestPeerMemoryReplicaRequiresClusterMemberAndCurrentLeader(t *testing.T) {
	a := newTestAgent(t, "node-a")
	a.mu.Lock()
	a.peers = []cluster.NodeStatus{
		{ID: "node-a", Healthy: true, Leader: true},
		{ID: "node-b", Healthy: true},
	}
	a.mu.Unlock()

	requestWithPeer := func(method, peerID string) *http.Request {
		request := httptest.NewRequest(method, "/memory/sync", strings.NewReader(`{}`))
		request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{
			Subject: pkix.Name{CommonName: peerID},
		}}}
		return request
	}

	unauthorized := httptest.NewRecorder()
	a.handlePeerMemorySyncGet(unauthorized, httptest.NewRequest(http.MethodGet, "/memory/sync", nil))
	if unauthorized.Code != http.StatusForbidden {
		t.Fatalf("unauthenticated memory read returned %d, want forbidden", unauthorized.Code)
	}

	memberRead := httptest.NewRecorder()
	a.handlePeerMemorySyncGet(memberRead, requestWithPeer(http.MethodGet, "node-b"))
	if memberRead.Code != http.StatusOK {
		t.Fatalf("paired member memory read returned %d: %s", memberRead.Code, memberRead.Body.String())
	}

	nonLeaderWrite := httptest.NewRecorder()
	a.handlePeerMemorySyncPost(nonLeaderWrite, requestWithPeer(http.MethodPost, "node-b"))
	if nonLeaderWrite.Code != http.StatusForbidden {
		t.Fatalf("replica from non-leader returned %d, want forbidden", nonLeaderWrite.Code)
	}

	leaderWrite := httptest.NewRecorder()
	a.handlePeerMemorySyncPost(leaderWrite, requestWithPeer(http.MethodPost, "node-a"))
	if leaderWrite.Code != http.StatusNoContent {
		t.Fatalf("replica from current leader returned %d: %s", leaderWrite.Code, leaderWrite.Body.String())
	}
}

func TestPeerMemorySyncGetNegotiatesIncrementalSnapshot(t *testing.T) {
	a := newTestAgent(t, "node-a")
	for range 3 {
		if _, err := a.mem.Record(memory.Experience{Description: "record"}); err != nil {
			t.Fatal(err)
		}
	}
	a.mu.Lock()
	a.peers = []cluster.NodeStatus{
		{ID: "node-a", Healthy: true, Leader: true},
		{ID: "node-b", Healthy: true},
	}
	a.mu.Unlock()

	full := a.mem.Snapshot()
	origin := full.Experiences[0].Origin
	request := httptest.NewRequest(http.MethodGet, "/memory/sync", nil)
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{
		Subject: pkix.Name{CommonName: "node-b"},
	}}}
	if err := memory.SetSyncRequestHeaders(request, map[string]uint64{origin: 2}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	a.handlePeerMemorySyncGet(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("incremental peer sync returned %d: %s", response.Code, response.Body.String())
	}
	var incremental memory.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&incremental); err != nil {
		t.Fatal(err)
	}
	if incremental.SyncVersion != memory.SyncProtocolVersion || len(incremental.Experiences) != 1 {
		t.Fatalf("incremental snapshot = version %d, experiences %d", incremental.SyncVersion, len(incremental.Experiences))
	}
}

func TestTaskListReturnsIncrementalSnapshot(t *testing.T) {
	a := newTestAgent(t, "node-a")
	task := a.tasks.Create("task history", nil)
	request := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	if err := memory.SetSyncRequestHeaders(request, map[string]uint64{"task:node-a": task.Sequence - 1}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	a.handleTasksList(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("task list returned %d: %s", response.Code, response.Body.String())
	}
	version, watermarks, err := memory.ReadSyncHeaders(response.Header())
	if err != nil {
		t.Fatal(err)
	}
	if version != memory.SyncProtocolVersion || watermarks["task:node-a"] != task.Sequence {
		t.Fatalf("task sync response headers = version %d, watermarks %v", version, watermarks)
	}
	var tasks []taskmgr.Task
	if err := json.NewDecoder(response.Body).Decode(&tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].ID != task.ID {
		t.Fatalf("incremental task list = %+v, want task %s", tasks, task.ID)
	}
}

type blockingEngine struct {
	started chan struct{}
}

func (b *blockingEngine) Name() string    { return "blocking" }
func (b *blockingEngine) Available() bool { return true }
func (b *blockingEngine) ListModels() ([]engine.Model, error) {
	return []engine.Model{{Name: "blocking"}}, nil
}
func (b *blockingEngine) Generate(model, prompt string) (string, error) {
	return "", nil
}
func (b *blockingEngine) Chat(model string, messages []engine.ChatMessage) (string, error) {
	return "", nil
}
func (b *blockingEngine) ChatWithContext(ctx context.Context, model string, messages []engine.ChatMessage) (string, error) {
	close(b.started)
	<-ctx.Done()
	return "", ctx.Err()
}

func TestDialogueInterruption(t *testing.T) {
	a := newTestAgent(t, "node-interrupt")
	eng := &blockingEngine{started: make(chan struct{})}
	a.engines = []engine.Engine{eng}

	errCh := make(chan error, 1)
	go func() {
		_, err := a.dialogueTurn("dlg-int", "blocking", "", "user", "hello", "dialogue_turn")
		errCh <- err
	}()

	<-eng.started

	interrupted := a.InterruptDialogue("dlg-int")
	if !interrupted {
		t.Fatal("expected InterruptDialogue to return true")
	}

	err := <-errCh
	if err == nil {
		t.Fatal("expected dialogue turn to return error on interruption")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/dialogues/dlg-int/interrupt", nil)
	req.SetPathValue("id", "dlg-int")
	a.handleDialogueInterrupt(rec, req)
	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
