package deepseek

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"io"
	"net/http"
	"strings"
	"time"
)

func (h *Harness) pickBestNodeForQuery(taskType, modelPref string, nodes []cluster.NodeStatus) (*cluster.NodeStatus, string, float64) {
	simulatedLoad := make(map[string]int, len(nodes))
	for _, n := range nodes {
		simulatedLoad[n.ID] = n.ActiveTasks
	}
	st := scheduler.Subtask{
		ID:          "query",
		Description: "llm_query",
		TaskType:    taskType,
	}
	return h.pickBestNodeForSubtask(st, modelPref, nodes, simulatedLoad)
}

// pickBestNodeForSubtask scores nodes considering model match, hardware score, and load penalty.
func (h *Harness) pickBestNodeForSubtask(st scheduler.Subtask, modelPref string, nodes []cluster.NodeStatus, load map[string]int) (*cluster.NodeStatus, string, float64) {
	var best *cluster.NodeStatus
	bestModel := ""
	bestScore := -99999.0

	for i := range nodes {
		n := &nodes[i]
		if !n.Healthy || len(n.Models) == 0 {
			continue
		}
		model, matchScore := matchModel(st, modelPref, n.Models, n.DefaultModel)
		if model == "" {
			continue
		}
		hwScore := n.Hardware.Score
		loadPenalty := float64(load[n.ID]) * 8.0
		acceleratorPenalty := cluster.GPUUtilizationPenalty(n.Hardware) + cluster.NPUUtilizationPenalty(n.Hardware)

		// Past results shift routing: pairings that historically succeeded
		// here get promoted, ones that failed or ran slow get demoted.
		learned := 0.0
		if h.mem != nil {
			learned = h.mem.LearnedBias(n.ID, model, st.TaskType)
		}

		totalScore := matchScore*10.0 + hwScore*0.3 - loadPenalty - acceleratorPenalty + learned
		if totalScore > bestScore {
			bestScore = totalScore
			best = n
			bestModel = model
		}
	}
	return best, bestModel, bestScore
}

func matchModel(st scheduler.Subtask, modelPref string, models []engine.Model, defaultModel string) (string, float64) {
	bestName := ""
	bestScore := -1.0

	for _, m := range models {
		score := 0.0
		if modelPref != "" && strings.EqualFold(m.Name, modelPref) {
			score += 15.0
		}
		if m.Name == defaultModel {
			score += 1.0
		}
		for _, tag := range m.Tags {
			if tag == st.TaskType {
				score += 3.0
			} else if tag == "general" {
				score += 0.5
			} else if tag == "fast" {
				score += 0.25
			}
		}
		if score > bestScore {
			bestScore = score
			bestName = m.Name
		}
	}
	if bestName == "" && len(models) > 0 {
		return models[0].Name, 0.1
	}
	return bestName, bestScore
}

// dispatchToNode sends an execution payload to a cluster node's /execute endpoint.
func (h *Harness) dispatchToNode(ctx context.Context, address, model, prompt string) (string, error) {
	// If the prompt is formatted as ChatML, try /chat or /execute
	body, err := json.Marshal(map[string]string{
		"model":  model,
		"prompt": prompt,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/execute", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.dispatcher.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return "", fmt.Errorf("node returned %s: %s", resp.Status, string(errBody))
	}

	var out struct {
		Output string `json:"output"`
		Error  string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Error != "" {
		return "", errors.New(out.Error)
	}
	return out.Output, nil
}

// DispatchChatToNode sends structured multi-turn messages to a node's /chat endpoint.
func (h *Harness) DispatchChatToNode(ctx context.Context, address, model string, messages []engine.ChatMessage) (string, error) {
	body, err := json.Marshal(map[string]any{
		"model":    model,
		"messages": messages,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/chat", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.dispatcher.Do(req)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		var out struct {
			Message engine.ChatMessage `json:"message"`
			Error   string             `json:"error,omitempty"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&out); decodeErr == nil {
			if out.Error != "" {
				return "", errors.New(out.Error)
			}
			return out.Message.Content, nil
		}
	}
	if resp != nil {
		resp.Body.Close()
	}

	// Fallback to formatted prompt on /execute
	var sb strings.Builder
	for _, m := range messages {
		sb.WriteString(fmt.Sprintf("<|im_start|>%s\n%s<|im_end|>\n", m.Role, m.Content))
	}
	sb.WriteString("<|im_start|>assistant\n")
	return h.dispatchToNode(ctx, address, model, sb.String())
}

// NodeDialogueResponse holds the outcome of a conversational turn with a node.
type NodeDialogueResponse struct {
	DialogueID string               `json:"dialogue_id"`
	NodeID     string               `json:"node_id"`
	Model      string               `json:"model"`
	Turn       int                  `json:"turn"`
	Reply      string               `json:"reply"`
	Messages   []engine.ChatMessage `json:"messages"`
	DurationMs int64                `json:"duration_ms"`
	Error      string               `json:"error,omitempty"`
}

// SendNodeDialogueMessage sends a message in a dialogue with a node's LLM, continuing the conversation.
func (h *Harness) SendNodeDialogueMessage(ctx context.Context, node cluster.NodeStatus, dialogueID, model, content, role string) (NodeDialogueResponse, error) {
	if dialogueID == "" {
		dialogueID = fmt.Sprintf("dlg-%s-%d", node.ID, time.Now().UnixNano()%1000000)
	}
	if role == "" {
		role = "user"
	}

	body, err := json.Marshal(map[string]string{
		"role":    role,
		"content": content,
	})
	if err != nil {
		return NodeDialogueResponse{}, err
	}

	url := fmt.Sprintf("%s/dialogues/%s/messages", node.Address, dialogueID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return NodeDialogueResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	h.registry.IncrementLoad(node.ID, 1)
	defer h.registry.IncrementLoad(node.ID, -1)

	resp, err := h.dispatcher.Do(req)
	if err != nil {
		return NodeDialogueResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var rawMap map[string]any
		if decodeErr := json.NewDecoder(resp.Body).Decode(&rawMap); decodeErr == nil {
			reply := ""
			if r, ok := rawMap["reply"].(string); ok && r != "" {
				reply = r
			} else if o, ok := rawMap["output"].(string); ok && o != "" {
				reply = o
			}
			errStr := ""
			if e, ok := rawMap["error"].(string); ok {
				errStr = e
			}
			if errStr != "" {
				return NodeDialogueResponse{}, errors.New(errStr)
			}
			turn := 1
			if t, ok := rawMap["turn"].(float64); ok && t > 0 {
				turn = int(t)
			}
			did := dialogueID
			if d, ok := rawMap["dialogue_id"].(string); ok && d != "" {
				did = d
			}
			mod := model
			if m, ok := rawMap["model"].(string); ok && m != "" {
				mod = m
			}
			return NodeDialogueResponse{
				DialogueID: did,
				NodeID:     node.ID,
				Model:      mod,
				Turn:       turn,
				Reply:      reply,
				Messages: []engine.ChatMessage{
					{Role: role, Content: content},
					{Role: "assistant", Content: reply},
				},
			}, nil
		}
	}

	// Fallback to direct chat if node dialogue endpoint failed
	start := time.Now()
	reply, chatErr := h.DispatchChatToNode(ctx, node.Address, model, []engine.ChatMessage{
		{Role: role, Content: content},
	})
	elapsed := time.Since(start).Milliseconds()
	if chatErr != nil {
		return NodeDialogueResponse{}, chatErr
	}

	return NodeDialogueResponse{
		DialogueID: dialogueID,
		NodeID:     node.ID,
		Model:      model,
		Turn:       1,
		Reply:      reply,
		Messages: []engine.ChatMessage{
			{Role: role, Content: content},
			{Role: "assistant", Content: reply},
		},
		DurationMs: elapsed,
	}, nil
}

func generateID(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}
