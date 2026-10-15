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
		if err := a.syncOrchestratorMemory(addr); err != nil {
			log.Printf("FILE_ONLY: nodeagent: memory sync with orchestrator %s failed: %v", addr, err)
		}
	}
	sync()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		sync()
	}
}

func (a *Agent) syncOrchestratorMemory(addr string) error {
	if a.mem == nil {
		return fmt.Errorf("memory store unavailable")
	}
	peerID := a.getOrchestratorID()
	if peerID == "" {
		peerID = addr
	}

	fetchStarted := time.Now()
	log.Printf("FILE_ONLY: nodeagent: memory sync direction=orchestrator->node event=receive_start node_id=%s peer=%s body_bytes=unknown",
		a.cfg.NodeID, addr)
	remoteSnapshot, bodyBytes, err := fetchMemorySyncSnapshot(
		a.memorySyncClient,
		strings.TrimRight(addr, "/")+"/api/memory/sync",
		memory.FilterMemoryWatermarks(a.mem.PeerWatermarks(peerID)),
	)
	if err != nil {
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=orchestrator->node event=receive_failed node_id=%s peer=%s body_bytes=0 elapsed=%s stage=request error=%v",
			a.cfg.NodeID, addr, time.Since(fetchStarted), err)
		return fmt.Errorf("fetch merged snapshot: %w", err)
	}
	log.Printf("FILE_ONLY: nodeagent: memory sync direction=orchestrator->node event=receive_complete node_id=%s peer=%s status=%d body_bytes=%d records=%d elapsed=%s",
		a.cfg.NodeID, addr, http.StatusOK, bodyBytes, memorySyncRecordCount(remoteSnapshot), time.Since(fetchStarted))
	mergeStarted := time.Now()
	if err := a.mem.MergeSnapshotFrom(remoteSnapshot, peerID); err != nil {
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=orchestrator->node event=merge_failed node_id=%s peer=%s body_bytes=%d elapsed=%s error=%v",
			a.cfg.NodeID, addr, bodyBytes, time.Since(mergeStarted), err)
		return fmt.Errorf("merge orchestrator snapshot: %w", err)
	}
	log.Printf("FILE_ONLY: nodeagent: memory sync direction=orchestrator->node event=merge_complete node_id=%s peer=%s body_bytes=%d records=%d elapsed=%s",
		a.cfg.NodeID, addr, bodyBytes, memorySyncRecordCount(remoteSnapshot), time.Since(mergeStarted))

	snapshot := memory.SnapshotForPeer(a.mem, remoteSnapshot.SyncVersion, remoteSnapshot.Watermarks)
	bodies, err := marshalMemorySnapshotPages(snapshot, maxMemorySyncBodyBytes, func(page memory.Snapshot) ([]byte, error) {
		return json.Marshal(map[string]any{
			"node_id":      a.cfg.NodeID,
			"sync_version": memory.SyncProtocolVersion,
			"snapshot":     page,
		})
	})
	if err != nil {
		return fmt.Errorf("encode local snapshot: %w", err)
	}
	postStarted := time.Now()
	var totalBodyBytes int64
	for _, body := range bodies {
		totalBodyBytes += int64(len(body))
	}
	log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->orchestrator event=send_start node_id=%s peer=%s body_bytes=%d pages=%d records=%d",
		a.cfg.NodeID, addr, totalBodyBytes, len(bodies), memorySyncRecordCount(snapshot))
	sentBytes, err := postMemorySnapshotPages(
		a.memorySyncClient,
		strings.TrimRight(addr, "/")+"/api/memory/sync",
		bodies,
		a.mem.Watermarks(),
	)
	if err != nil {
		log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->orchestrator event=send_failed node_id=%s peer=%s body_bytes=%d elapsed=%s error=%v",
			a.cfg.NodeID, addr, sentBytes, time.Since(postStarted), err)
		return fmt.Errorf("send local snapshot: %w", err)
	}
	log.Printf("FILE_ONLY: nodeagent: memory sync direction=node->orchestrator event=send_complete node_id=%s peer=%s status=%d body_bytes=%d elapsed=%s",
		a.cfg.NodeID, addr, http.StatusOK, sentBytes, time.Since(postStarted))
	if memory.SupportsIncrementalSync(remoteSnapshot.SyncVersion) {
		if err := a.mem.SetPeerWatermarks(peerID, snapshot.Watermarks); err != nil {
			return fmt.Errorf("persist orchestrator watermarks: %w", err)
		}
	}
	return nil
}
