package nodeagent

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestPairingConnectEndpointEnrollsNode(t *testing.T) {
	orchestrator, err := peeridentity.Open(filepath.Join(t.TempDir(), "orchestrator.json"), "orchestrator-pair-test")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pairing/join" {
			t.Errorf("pairing path = %q", r.URL.Path)
		}
		var request nodePairingRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Role != "node" || request.PIN != "123456" || request.ID != "node-pair-test" || request.CertificatePEM == "" {
			t.Errorf("unexpected pairing request: %#v", request)
		}
		_ = json.NewEncoder(w).Encode(nodePairingResponse{
			ID: orchestrator.ID(), Role: "orchestrator", CertificatePEM: orchestrator.CertificatePEM(),
		})
	}))
	defer server.Close()

	agent := New(config.NodeConfig{
		NodeID: "node-pair-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	requestBody, _ := json.Marshal(map[string]string{"address": server.URL, "pin": "123456"})
	request := httptest.NewRequest(http.MethodPost, "/api/pairing/connect", bytes.NewReader(requestBody))
	response := httptest.NewRecorder()
	agent.handlePairingConnect(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("pairing returned %d: %s", response.Code, response.Body.String())
	}
	if !agent.identity.IsTrustedID(orchestrator.ID()) || agent.getOrchestratorAddr() != server.URL {
		t.Fatal("node did not trust and select the paired orchestrator")
	}
	status := httptest.NewRecorder()
	agent.handlePairingStatus(status, httptest.NewRequest(http.MethodGet, "/api/pairing/status", nil))
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"trusted":true`) {
		t.Fatalf("pairing status returned %d: %s", status.Code, status.Body.String())
	}
}

func TestDefaultNodeIDStable(t *testing.T) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node"
	}
	if got := DefaultNodeID(); got != hostname {
		t.Fatalf("expected stable hostname ID %q, got %q", hostname, got)
	}
	if DefaultNodeID() != DefaultNodeID() {
		t.Fatal("expected default node ID to remain stable")
	}
}

func TestAppendGPUUsageSampleKeepsBoundedRecentHistory(t *testing.T) {
	var history []hardware.GPUUsageSample
	for i := 0; i < gpuHistoryLimit+3; i++ {
		history = appendGPUUsageSample(history, hardware.GPUUsageSample{
			Timestamp: time.Unix(int64(i), 0), Available: true, Utilization: i,
		})
	}
	if len(history) != gpuHistoryLimit {
		t.Fatalf("history length = %d, want %d", len(history), gpuHistoryLimit)
	}
	if got := history[0].Utilization; got != 3 {
		t.Fatalf("oldest retained utilization = %d, want 3", got)
	}
	if got := history[len(history)-1].Utilization; got != gpuHistoryLimit+2 {
		t.Fatalf("newest retained utilization = %d, want %d", got, gpuHistoryLimit+2)
	}
}

func TestAppendSystemUsageSampleKeepsBoundedRecentHistory(t *testing.T) {
	history := make([]hardware.SystemUsageSample, systemHistoryLimit)
	for index := range history {
		history[index] = hardware.SystemUsageSample{CPUUtilization: index}
	}
	history = appendSystemUsageSample(history, hardware.SystemUsageSample{CPUUtilization: 100})
	if len(history) != systemHistoryLimit {
		t.Fatalf("history length = %d, want %d", len(history), systemHistoryLimit)
	}
	if history[0].CPUUtilization != 1 || history[len(history)-1].CPUUtilization != 100 {
		t.Fatalf("history endpoints = (%d, %d), want (1, 100)", history[0].CPUUtilization, history[len(history)-1].CPUUtilization)
	}
}

func TestNodeStatusIncludesCopiedGPUHistory(t *testing.T) {
	agent := New(config.NodeConfig{
		NodeID: "gpu-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.GPUUsageSample{
		Timestamp: time.Unix(1, 0).UTC(), Available: true, Utilization: 41,
	}
	agent.hwMu.Lock()
	agent.gpuHistory = []hardware.GPUUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.GPUHistory) != 1 || status.GPUHistory[0] != sample {
		t.Fatalf("node status GPU history = %#v", status.GPUHistory)
	}
	status.GPUHistory[0].Utilization = 99
	if agent.gpuHistorySnapshot()[0].Utilization != sample.Utilization {
		t.Fatal("node status exposed mutable telemetry history")
	}
}

func TestNodeStatusIncludesCopiedSystemHistory(t *testing.T) {
	agent := New(config.NodeConfig{
		NodeID: "system-history-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if agent.Memory() != nil {
		t.Cleanup(func() { _ = agent.Memory().Close() })
	}
	sample := hardware.SystemUsageSample{
		Timestamp: time.Unix(2, 0).UTC(), CPUAvailable: true, CPUUtilization: 37,
		MemoryAvailable: true, MemoryUtilization: 62,
	}
	agent.hwMu.Lock()
	agent.systemHistory = []hardware.SystemUsageSample{sample}
	agent.hwMu.Unlock()

	status := agent.status()
	if len(status.SystemHistory) != 1 || status.SystemHistory[0] != sample {
		t.Fatalf("node status system history = %#v", status.SystemHistory)
	}
	status.SystemHistory[0].CPUUtilization = 99
	if agent.systemHistorySnapshot()[0].CPUUtilization != sample.CPUUtilization {
		t.Fatal("node status exposed mutable system telemetry history")
	}
}

func TestNodeChatAndDialogueEndpoints(t *testing.T) {
	cfg := config.NodeConfig{
		NodeID:     "node-chat-test",
		ListenAddr: ":0",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}
	agent := New(cfg)
	t.Cleanup(func() { _ = agent.Memory().Close() })
	// Ensure simulated engine is present for deterministic unit testing
	agent.engines = append([]engine.Engine{engine.NewSimulated()}, agent.engines...)

	// 1. Test POST /chat (stateless multi-turn array)
	chatReqBody, _ := json.Marshal(chatRequest{
		Model: "simulated-general",
		Messages: []engine.ChatMessage{
			{Role: "user", Content: "Hello model on node"},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/chat", bytes.NewReader(chatReqBody))
	rec := httptest.NewRecorder()
	agent.handleChat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /chat returned %d", rec.Code)
	}
	var chatResp chatResponse
	if err := json.NewDecoder(rec.Body).Decode(&chatResp); err != nil {
		t.Fatalf("decode chatResp failed: %v", err)
	}
	if !strings.Contains(chatResp.Message.Content, "simulated") {
		t.Errorf("unexpected chat reply: %q", chatResp.Message.Content)
	}

	// 2. Test POST /dialogues (start a conversation)
	createReqBody, _ := json.Marshal(createDialogueRequest{
		ID:     "test-dlg-1",
		Model:  "simulated-general",
		System: "You are a helpful coding assistant",
	})
	req = httptest.NewRequest(http.MethodPost, "/dialogues", bytes.NewReader(createReqBody))
	rec = httptest.NewRecorder()
	agent.handleDialogueCreate(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /dialogues returned %d", rec.Code)
	}
	var dlg Dialogue
	if err := json.NewDecoder(rec.Body).Decode(&dlg); err != nil {
		t.Fatalf("decode dialogue failed: %v", err)
	}
	if dlg.ID != "test-dlg-1" || len(dlg.Messages) != 1 {
		t.Errorf("unexpected created dialogue: %+v", dlg)
	}

	// 3. Test POST /dialogues/{id}/messages (Turn 1)
	msgReqBody1, _ := json.Marshal(dialogueMessageRequest{
		Content: "What is concurrency in Go?",
	})
	req = httptest.NewRequest(http.MethodPost, "/dialogues/test-dlg-1/messages", bytes.NewReader(msgReqBody1))
	req.SetPathValue("id", "test-dlg-1")
	rec = httptest.NewRecorder()
	agent.handleDialogueMessage(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST turn 1 returned %d", rec.Code)
	}
	var msgResp1 dialogueMessageResponse
	if err := json.NewDecoder(rec.Body).Decode(&msgResp1); err != nil {
		t.Fatalf("decode turn 1 response failed: %v", err)
	}
	if len(msgResp1.Messages) != 3 { // system + user + assistant
		t.Errorf("expected 3 messages after turn 1, got %d", len(msgResp1.Messages))
	}

	// 4. Test POST /dialogues/{id}/messages (Turn 2 - Follow-up in ongoing conversation)
	msgReqBody2, _ := json.Marshal(dialogueMessageRequest{
		Content: "Can you provide an example with goroutines and channels?",
	})
	req = httptest.NewRequest(http.MethodPost, "/dialogues/test-dlg-1/messages", bytes.NewReader(msgReqBody2))
	req.SetPathValue("id", "test-dlg-1")
	rec = httptest.NewRecorder()
	agent.handleDialogueMessage(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST turn 2 returned %d", rec.Code)
	}
	var msgResp2 dialogueMessageResponse
	if err := json.NewDecoder(rec.Body).Decode(&msgResp2); err != nil {
		t.Fatalf("decode turn 2 response failed: %v", err)
	}
	if len(msgResp2.Messages) != 5 { // system + user1 + asst1 + user2 + asst2
		t.Errorf("expected 5 messages after turn 2, got %d", len(msgResp2.Messages))
	}

	// 5. Test GET /dialogues/{id}
	req = httptest.NewRequest(http.MethodGet, "/dialogues/test-dlg-1", nil)
	req.SetPathValue("id", "test-dlg-1")
	rec = httptest.NewRecorder()
	agent.handleDialogueGet(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET dialogue returned %d", rec.Code)
	}
	var fetchedDlg Dialogue
	if err := json.NewDecoder(rec.Body).Decode(&fetchedDlg); err != nil {
		t.Fatalf("decode fetched dialogue failed: %v", err)
	}
	if len(fetchedDlg.Messages) != 5 {
		t.Errorf("expected 5 messages in fetched dialogue, got %d", len(fetchedDlg.Messages))
	}
}
