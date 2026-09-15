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

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

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
