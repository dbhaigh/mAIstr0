package nodeagent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
)

// dialogueTurn advances a locally-hosted conversation by one turn: it
// appends the incoming utterance, runs the node's LLM over the accumulated
// history, stores the reply, and records the exchange in memory.
func (a *Agent) dialogueTurn(id, model, system, role, content, kind string) (dialogueMessageResponse, error) {
	if role == "" {
		role = "user"
	}
	if kind == "" {
		kind = "dialogue_turn"
	}

	a.dialoguesMu.Lock()
	dlg, ok := a.dialogues[id]
	if !ok {
		resolved := model
		if resolved == "" {
			resolved = a.defaultModelName()
		}
		if resolved == "" {
			if enabled := a.enabledModels(); len(enabled) > 0 {
				resolved = enabled[0].Name
			}
		}
		dlg = &Dialogue{
			ID:        id,
			Model:     resolved,
			System:    system,
			Messages:  make([]engine.ChatMessage, 0),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if system != "" {
			dlg.Messages = append(dlg.Messages, engine.ChatMessage{Role: "system", Content: system})
		}
		a.dialogues[id] = dlg
	}
	if model != "" {
		dlg.Model = model
	}
	dlg.Messages = append(dlg.Messages, engine.ChatMessage{Role: role, Content: content})
	dlg.UpdatedAt = time.Now()
	modelName := dlg.Model
	history := append([]engine.ChatMessage(nil), dlg.Messages...)
	a.dialoguesMu.Unlock()

	// Prior experience with this topic is injected as extra system context
	// so each node answers with what the cluster already learned.
	if brief := a.memoryBrief(content); brief != "" {
		history = append([]engine.ChatMessage{{Role: "system", Content: brief}}, history...)
	}

	eng := a.engineFor(modelName)
	if eng == nil {
		return dialogueMessageResponse{DialogueID: id, Model: modelName},
			fmt.Errorf("model not found on node %s: %s", a.cfg.NodeID, modelName)
	}

	a.mu.Lock()
	a.activeTasks++
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.activeTasks--
		a.mu.Unlock()
	}()

	start := time.Now()
	reply, err := eng.Chat(modelName, history)
	if err != nil {
		reply, err = eng.Generate(modelName, flattenChatML(history))
	}
	duration := time.Since(start).Milliseconds()

	a.remember(memory.Experience{
		Kind:        kind,
		Description: truncateText(content, 400),
		Prompt:      content,
		Output:      reply,
		Model:       modelName,
		Success:     err == nil,
		Error:       errText(err),
		DurationMs:  duration,
		Tags:        []string{"dialogue", id},
	})

	if err != nil {
		return dialogueMessageResponse{DialogueID: id, Model: modelName, DurationMs: duration}, err
	}

	a.dialoguesMu.Lock()
	dlg.Messages = append(dlg.Messages, engine.ChatMessage{Role: "assistant", Content: reply})
	dlg.UpdatedAt = time.Now()
	all := append([]engine.ChatMessage(nil), dlg.Messages...)
	a.dialoguesMu.Unlock()

	if a.mem != nil {
		_ = a.mem.AppendDialogueTurn(id, []string{"node:" + a.cfg.NodeID}, "", memory.Turn{
			Speaker: role, Content: content, At: time.Now(),
		})
		_ = a.mem.AppendDialogueTurn(id, []string{"node:" + a.cfg.NodeID}, "", memory.Turn{
			Speaker: "node:" + a.cfg.NodeID, NodeID: a.cfg.NodeID, Model: modelName,
			Content: reply, DurationMs: duration, At: time.Now(),
		})
	}

	return dialogueMessageResponse{
		DialogueID: id,
		Model:      modelName,
		Turn:       len(all) / 2,
		Reply:      reply,
		Messages:   all,
		DurationMs: duration,
	}, nil
}

func flattenChatML(msgs []engine.ChatMessage) string {
	var sb strings.Builder
	for _, m := range msgs {
		sb.WriteString(fmt.Sprintf("<|im_start|>%s\n%s<|im_end|>\n", m.Role, m.Content))
	}
	sb.WriteString("<|im_start|>assistant\n")
	return sb.String()
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func truncateText(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// memoryBrief renders relevant prior experience for injection into a prompt.
func (a *Agent) memoryBrief(query string) string {
	if a.mem == nil {
		return ""
	}
	return a.mem.ContextBrief(query, "", 4)
}

// experience exposes this node's learned history to the scheduler, or nil
// when memory is unavailable.
func (a *Agent) experience() scheduler.Experience {
	if a.mem == nil {
		return nil
	}
	return a.mem
}

// --- Node-to-node LLM conversation ---

// peerMessageRequest is one utterance arriving from another node's LLM.
type peerMessageRequest struct {
	FromNode   string `json:"from_node"`
	FromModel  string `json:"from_model,omitempty"`
	DialogueID string `json:"dialogue_id"`
	Topic      string `json:"topic,omitempty"`
	Content    string `json:"content"`
	Model      string `json:"model,omitempty"`
}

type peerMessageResponse struct {
	NodeID     string `json:"node_id"`
	Model      string `json:"model"`
	DialogueID string `json:"dialogue_id"`
	Reply      string `json:"reply"`
	Turn       int    `json:"turn"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

// handlePeerMessage answers a message sent by a peer node's LLM. The local
// model sees who it is talking to, so the two LLMs hold a real dialogue
// rather than exchanging isolated one-shot prompts.
func (a *Agent) handlePeerMessage(w http.ResponseWriter, r *http.Request) {
	var req peerMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, peerMessageResponse{Error: err.Error()})
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		writeJSON(w, http.StatusBadRequest, peerMessageResponse{Error: "content is required"})
		return
	}
	if req.DialogueID == "" {
		req.DialogueID = fmt.Sprintf("peer-%s-%d", req.FromNode, time.Now().UnixNano()%1000000)
	}

	system := fmt.Sprintf(
		"You are the LLM %s running on cluster node %q in the mAIstr0 cluster. "+
			"You are in a direct peer conversation with the LLM on node %q. "+
			"Answer its messages substantively and concisely, challenge weak reasoning, and build on its ideas.",
		valueOr(req.Model, a.defaultModelName()), a.cfg.NodeID, valueOr(req.FromNode, "unknown"))
	if req.Topic != "" {
		system += "\nConversation topic: " + req.Topic
	}

	speaker := "peer node " + valueOr(req.FromNode, "unknown")
	if req.FromModel != "" {
		speaker += " (" + req.FromModel + ")"
	}
	framed := fmt.Sprintf("Message from %s:\n%s", speaker, req.Content)

	res, err := a.dialogueTurn(req.DialogueID, req.Model, system, "user", framed, "peer_dialogue")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, peerMessageResponse{
			NodeID: a.cfg.NodeID, Model: res.Model, DialogueID: req.DialogueID, Error: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, peerMessageResponse{
		NodeID:     a.cfg.NodeID,
		Model:      res.Model,
		DialogueID: res.DialogueID,
		Reply:      res.Reply,
		Turn:       res.Turn,
		DurationMs: res.DurationMs,
	})
}

func valueOr(v, fallback string) string {
	if strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

type peerConverseRequest struct {
	TargetNodeID string `json:"target_node_id"`
	TargetModel  string `json:"target_model,omitempty"`
	LocalModel   string `json:"local_model,omitempty"`
	Topic        string `json:"topic"`
	Opening      string `json:"opening,omitempty"`
	Rounds       int    `json:"rounds,omitempty"`
	DialogueID   string `json:"dialogue_id,omitempty"`
}

// PeerExchange is one round of an LLM-to-LLM conversation.
type PeerExchange struct {
	Round        int    `json:"round"`
	LocalNode    string `json:"local_node"`
	LocalModel   string `json:"local_model"`
	LocalMessage string `json:"local_message"`
	PeerNode     string `json:"peer_node"`
	PeerModel    string `json:"peer_model"`
	PeerReply    string `json:"peer_reply"`
	DurationMs   int64  `json:"duration_ms"`
}

type peerConverseResponse struct {
	DialogueID  string         `json:"dialogue_id"`
	Topic       string         `json:"topic"`
	LocalNode   string         `json:"local_node"`
	PeerNode    string         `json:"peer_node"`
	Rounds      int            `json:"rounds"`
	Exchanges   []PeerExchange `json:"exchanges"`
	Conclusion  string         `json:"conclusion"`
	TotalTimeMs int64          `json:"total_time_ms"`
	Error       string         `json:"error,omitempty"`
}

// handlePeerConverse drives a real back-and-forth between this node's LLM
// and a peer node's LLM: the local model speaks, the remote model answers,
// and the local model then reacts to that answer for the next round.
func (a *Agent) handlePeerConverse(w http.ResponseWriter, r *http.Request) {
	var req peerConverseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, peerConverseResponse{Error: err.Error()})
		return
	}
	res, err := a.ConversePeer(req)
	if err != nil {
		res.Error = err.Error()
		writeJSON(w, http.StatusBadGateway, res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ConversePeer runs an LLM-to-LLM conversation between this node and a peer.
func (a *Agent) ConversePeer(req peerConverseRequest) (peerConverseResponse, error) {
	if strings.TrimSpace(req.TargetNodeID) == "" {
		return peerConverseResponse{}, errors.New("target_node_id is required")
	}
	if strings.TrimSpace(req.Topic) == "" {
		return peerConverseResponse{}, errors.New("topic is required")
	}
	peerAddr, ok := a.peerAddress(req.TargetNodeID)
	if !ok || peerAddr == "" {
		return peerConverseResponse{}, fmt.Errorf("peer node %s is not reachable from %s", req.TargetNodeID, a.cfg.NodeID)
	}
	if req.TargetNodeID == a.cfg.NodeID {
		return peerConverseResponse{}, errors.New("a node cannot hold a peer conversation with itself")
	}

	rounds := req.Rounds
	if rounds <= 0 {
		rounds = 2
	}
	if rounds > 5 {
		rounds = 5
	}
	dialogueID := req.DialogueID
	if dialogueID == "" {
		dialogueID = fmt.Sprintf("p2p-%s-%s-%d", a.cfg.NodeID, req.TargetNodeID, time.Now().UnixNano()%1000000)
	}

	localSystem := fmt.Sprintf(
		"You are the LLM running on cluster node %q. You are talking directly with the LLM on node %q about: %s. "+
			"Speak to it as a peer engineer: make concrete proposals, ask pointed questions, and refine the shared answer each round. "+
			"Keep each message under 200 words.",
		a.cfg.NodeID, req.TargetNodeID, req.Topic)

	opening := req.Opening
	if opening == "" {
		opening = "Open the discussion on \"" + req.Topic + "\": state your initial position and the single most important question you want the other node to answer."
	}

	started := time.Now()
	result := peerConverseResponse{
		DialogueID: dialogueID,
		Topic:      req.Topic,
		LocalNode:  a.cfg.NodeID,
		PeerNode:   req.TargetNodeID,
	}

	localPrompt := opening
	for round := 1; round <= rounds; round++ {
		roundStart := time.Now()

		localTurn, err := a.dialogueTurn(dialogueID, req.LocalModel, localSystem, "user", localPrompt, "peer_dialogue")
		if err != nil {
			return result, fmt.Errorf("local node %s failed in round %d: %w", a.cfg.NodeID, round, err)
		}

		peerReply, peerModel, err := a.sendPeerMessage(peerAddr, peerMessageRequest{
			FromNode:   a.cfg.NodeID,
			FromModel:  localTurn.Model,
			DialogueID: dialogueID,
			Topic:      req.Topic,
			Content:    localTurn.Reply,
			Model:      req.TargetModel,
		})
		if err != nil {
			return result, fmt.Errorf("peer node %s failed in round %d: %w", req.TargetNodeID, round, err)
		}

		result.Exchanges = append(result.Exchanges, PeerExchange{
			Round:        round,
			LocalNode:    a.cfg.NodeID,
			LocalModel:   localTurn.Model,
			LocalMessage: localTurn.Reply,
			PeerNode:     req.TargetNodeID,
			PeerModel:    peerModel,
			PeerReply:    peerReply,
			DurationMs:   time.Since(roundStart).Milliseconds(),
		})

		if a.mem != nil {
			participants := []string{"node:" + a.cfg.NodeID, "node:" + req.TargetNodeID}
			_ = a.mem.AppendDialogueTurn(dialogueID, participants, req.Topic, memory.Turn{
				Speaker: "node:" + a.cfg.NodeID, NodeID: a.cfg.NodeID, Model: localTurn.Model,
				Content: localTurn.Reply, DurationMs: localTurn.DurationMs, At: time.Now(),
			})
			_ = a.mem.AppendDialogueTurn(dialogueID, participants, req.Topic, memory.Turn{
				Speaker: "node:" + req.TargetNodeID, NodeID: req.TargetNodeID, Model: peerModel,
				Content: peerReply, At: time.Now(),
			})
		}

		localPrompt = fmt.Sprintf(
			"Node %q replied:\n%s\n\nRespond to it: agree where it is right, correct what is wrong, and push the answer forward.",
			req.TargetNodeID, peerReply)
	}

	// Final round asks the local model to distil what the two LLMs agreed on.
	summary, err := a.dialogueTurn(dialogueID, req.LocalModel, localSystem, "user",
		"The peer conversation is complete. Summarise the conclusion you and node "+req.TargetNodeID+
			" reached, listing the agreed decisions and any open disagreements.", "peer_dialogue")
	if err == nil {
		result.Conclusion = summary.Reply
	}

	result.Rounds = len(result.Exchanges)
	result.TotalTimeMs = time.Since(started).Milliseconds()

	a.remember(memory.Experience{
		Kind:        "peer_conversation",
		TaskType:    "collaboration",
		Description: fmt.Sprintf("Peer conversation with %s about %s", req.TargetNodeID, req.Topic),
		Output:      result.Conclusion,
		Model:       req.LocalModel,
		Success:     result.Rounds > 0,
		DurationMs:  result.TotalTimeMs,
		Tags:        []string{"peer", req.TargetNodeID, dialogueID},
	})

	return result, nil
}

func (a *Agent) sendPeerMessage(peerAddr string, msg peerMessageRequest) (reply string, model string, err error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return "", "", err
	}
	resp, err := a.dispatchClient.Post(peerAddr+"/peer/message", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	var out peerMessageResponse
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	if readErr != nil {
		return "", "", readErr
	}
	if err := json.Unmarshal(data, &out); err != nil {
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", "", fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
		}
		return "", "", err
	}
	if out.Error != "" {
		return "", out.Model, errors.New(out.Error)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", out.Model, fmt.Errorf("peer returned %s", resp.Status)
	}
	return out.Reply, out.Model, nil
}

// --- Memory HTTP surface ---

func (a *Agent) handleMemoryExperiences(w http.ResponseWriter, r *http.Request) {
	if a.mem == nil {
		writeJSON(w, http.StatusOK, []memory.Experience{})
		return
	}
	if q := r.URL.Query().Get("q"); q != "" {
		writeJSON(w, http.StatusOK, a.mem.Recall(memory.SearchFilter{
			Query:    q,
			TaskType: r.URL.Query().Get("task_type"),
			Limit:    queryInt(r, "limit", 20),
		}))
		return
	}
	writeJSON(w, http.StatusOK, a.mem.Experiences(queryInt(r, "limit", 50)))
}

func (a *Agent) handleMemoryInsights(w http.ResponseWriter, r *http.Request) {
	if a.mem == nil {
		writeJSON(w, http.StatusOK, memory.Insights{})
		return
	}
	writeJSON(w, http.StatusOK, a.mem.Insights())
}

func (a *Agent) handleMemoryFacts(w http.ResponseWriter, r *http.Request) {
	if a.mem == nil {
		writeJSON(w, http.StatusOK, []memory.Fact{})
		return
	}
	if r.Method == http.MethodPost {
		var f memory.Fact
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.Source = "node:" + a.cfg.NodeID
		saved, err := a.mem.PutFact(f)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, saved)
		return
	}
	writeJSON(w, http.StatusOK, a.mem.Facts(r.URL.Query().Get("scope")))
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n := 0
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return def
	}
	return n
}

// syncMemoryToOrchestrator periodically pushes this node's learned
// performance aggregates upstream so the orchestrator's routing decisions
// benefit from every node's local experience.
func (a *Agent) syncMemoryToOrchestrator() {
	if a.mem == nil {
		return
	}
	sync := func() {
		addr := a.getOrchestratorAddr()
		if addr == "" {
			return
		}
		snapshot := a.mem.Snapshot()
		body, err := json.Marshal(map[string]any{"node_id": a.cfg.NodeID, "snapshot": snapshot})
		if err != nil {
			return
		}
		resp, err := a.peerClient.Post(addr+"/api/memory/sync", "application/json", bytes.NewReader(body))
		if err != nil {
			return
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			log.Printf("nodeagent: memory sync rejected by orchestrator: %s", resp.Status)
			return
		}
		resp, err = a.peerClient.Get(addr + "/api/memory/sync")
		if err != nil {
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return
		}
		var merged memory.Snapshot
		if json.NewDecoder(resp.Body).Decode(&merged) == nil {
			if err := a.mem.MergeSnapshot(merged); err != nil {
				log.Printf("nodeagent: memory merge failed: %v", err)
			}
		}
	}
	sync()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		sync()
	}
}
