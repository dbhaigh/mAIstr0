package deepseek

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/memory"
)

// --- Tool: recall_memory ---

type RecallMemoryTool struct{}

func (t *RecallMemoryTool) Def() ToolDef {
	return ToolDef{
		Name: "recall_memory",
		Description: "Search the cluster's persistent memory database for what happened in the past: previous tasks, " +
			"which node and model handled them, whether they succeeded, how long they took, and the answers produced. " +
			"Call this before re-solving anything to reuse prior work instead of repeating it.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{
					"type":        "string",
					"description": "Keywords describing what to remember (e.g. 'go worker pool benchmark').",
				},
				"task_type": map[string]any{
					"type":        "string",
					"description": "Optional task category filter (code, math, reasoning, summarize, general).",
				},
				"node_id": map[string]any{
					"type":        "string",
					"description": "Optional filter to experiences on one node.",
				},
				"success_only": map[string]any{
					"type":        "boolean",
					"description": "When true, only recall runs that succeeded.",
				},
				"limit": map[string]any{
					"type":        "number",
					"description": "Maximum memories to return (default 8).",
				},
			},
			"required": []string{"query"},
		},
	}
}

func (t *RecallMemoryTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	if h.mem == nil {
		return nil, "", "", "", errors.New("cluster memory is not available")
	}
	query, _ := args["query"].(string)
	taskType, _ := args["task_type"].(string)
	nodeID, _ := args["node_id"].(string)
	successOnly, _ := args["success_only"].(bool)

	limit := 8
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	found := h.mem.Recall(memory.SearchFilter{
		Query:       query,
		TaskType:    taskType,
		NodeID:      nodeID,
		SuccessOnly: successOnly,
		Limit:       limit,
	})

	type recalled struct {
		When       string `json:"when"`
		What       string `json:"what"`
		NodeID     string `json:"node_id,omitempty"`
		Model      string `json:"model,omitempty"`
		Succeeded  bool   `json:"succeeded"`
		DurationMs int64  `json:"duration_ms"`
		Result     string `json:"result,omitempty"`
		Error      string `json:"error,omitempty"`
	}

	out := make([]recalled, 0, len(found))
	for _, e := range found {
		out = append(out, recalled{
			When:       e.CreatedAt.Format(time.RFC3339),
			What:       e.Description,
			NodeID:     e.NodeID,
			Model:      e.Model,
			Succeeded:  e.Success,
			DurationMs: e.DurationMs,
			Result:     clip(e.Output, 1200),
			Error:      e.Error,
		})
	}

	return map[string]any{
		"query":     query,
		"recalled":  len(out),
		"memories":  out,
		"reminders": h.mem.Recommend(taskType, 3),
	}, "", "", "cluster-memory", nil
}

func clip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// --- Tool: remember_fact ---

type RememberFactTool struct{}

func (t *RememberFactTool) Def() ToolDef {
	return ToolDef{
		Name: "remember_fact",
		Description: "Durably save a lesson, preference, or conclusion to the cluster memory database so it survives " +
			"this session and is recalled automatically in future ones. Use it whenever you discover something " +
			"non-obvious about the cluster, the user, or how a task should be done.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"key": map[string]any{
					"type":        "string",
					"description": "Short identifier for the fact (e.g. 'preferred-code-model').",
				},
				"value": map[string]any{
					"type":        "string",
					"description": "The knowledge to remember.",
				},
				"scope": map[string]any{
					"type":        "string",
					"description": "Optional scope: 'cluster' (default), 'node:<id>', or 'session:<id>'.",
				},
			},
			"required": []string{"key", "value"},
		},
	}
}

func (t *RememberFactTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	if h.mem == nil {
		return nil, "", "", "", errors.New("cluster memory is not available")
	}
	key, _ := args["key"].(string)
	value, _ := args["value"].(string)
	scope, _ := args["scope"].(string)
	if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
		return nil, "", "", "", errors.New("key and value are required")
	}

	saved, err := h.mem.PutFact(memory.Fact{
		Key:    key,
		Value:  value,
		Scope:  scope,
		Source: "deepseek-agent:" + s.ID,
	})
	if err != nil {
		return nil, "", "", "", err
	}

	return map[string]any{
		"status": "remembered",
		"key":    saved.Key,
		"scope":  saved.Scope,
		"value":  saved.Value,
	}, "", "", "cluster-memory", nil
}

// --- Tool: memory_insights ---

type MemoryInsightsTool struct{}

func (t *MemoryInsightsTool) Def() ToolDef {
	return ToolDef{
		Name: "memory_insights",
		Description: "Report what the cluster has learned from its accumulated history: overall success rate, which " +
			"node+model pairings perform best and worst per task type, and the routing lessons currently being applied.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"task_type": map[string]any{
					"type":        "string",
					"description": "Optional task category to focus the recommendations on.",
				},
			},
		},
	}
}

func (t *MemoryInsightsTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	if h.mem == nil {
		return nil, "", "", "", errors.New("cluster memory is not available")
	}
	taskType, _ := args["task_type"].(string)
	insights := h.mem.Insights()
	return map[string]any{
		"experiences_recorded": insights.TotalExperiences,
		"success_rate":         insights.SuccessRate,
		"avg_duration_ms":      insights.AvgDurationMs,
		"tracked_pairings":     insights.TrackedPairings,
		"facts_stored":         insights.Facts,
		"dialogues_stored":     insights.Dialogues,
		"task_type_breakdown":  insights.TaskTypes,
		"lessons_learned":      insights.Lessons,
		"best_pairings":        h.mem.Recommend(taskType, 5),
		"database":             insights.DatabasePath,
	}, "", "", "cluster-memory", nil
}

// --- Tool: node_to_node_conversation ---

type NodeToNodeConversationTool struct{}

func (t *NodeToNodeConversationTool) Def() ToolDef {
	return ToolDef{
		Name: "node_to_node_conversation",
		Description: "Put two worker node LLMs into direct conversation with each other for several rounds so they can " +
			"debate, design, cross-check, or peer-review a topic without the orchestrator mediating every turn. " +
			"Returns the full transcript plus the conclusion the two models reached.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"node_a": map[string]any{
					"type":        "string",
					"description": "Node ID whose LLM opens the conversation.",
				},
				"node_b": map[string]any{
					"type":        "string",
					"description": "Node ID whose LLM responds.",
				},
				"topic": map[string]any{
					"type":        "string",
					"description": "What the two node LLMs should discuss or decide.",
				},
				"opening": map[string]any{
					"type":        "string",
					"description": "Optional opening instruction for the first node's LLM.",
				},
				"rounds": map[string]any{
					"type":        "number",
					"description": "Number of back-and-forth rounds (1 to 5, default 2).",
				},
			},
			"required": []string{"node_a", "node_b", "topic"},
		},
	}
}

func (t *NodeToNodeConversationTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeA, _ := args["node_a"].(string)
	nodeB, _ := args["node_b"].(string)
	topic, _ := args["topic"].(string)
	opening, _ := args["opening"].(string)

	if strings.TrimSpace(nodeA) == "" || strings.TrimSpace(nodeB) == "" {
		return nil, "", "", "", errors.New("node_a and node_b are required")
	}
	if nodeA == nodeB {
		return nil, "", "", "", errors.New("node_a and node_b must be different nodes")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, "", "", "", errors.New("topic is required")
	}

	rounds := 2
	if r, ok := args["rounds"].(float64); ok && r > 0 {
		rounds = int(r)
	}

	a, ok := h.registry.Get(nodeA)
	if !ok || !a.Healthy {
		return nil, nodeA, "", "", fmt.Errorf("node %s not found or offline", nodeA)
	}
	if b, ok := h.registry.Get(nodeB); !ok || !b.Healthy {
		return nil, nodeB, "", "", fmt.Errorf("node %s not found or offline", nodeB)
	}

	payload, err := json.Marshal(map[string]any{
		"target_node_id": nodeB,
		"topic":          topic,
		"opening":        opening,
		"rounds":         rounds,
	})
	if err != nil {
		return nil, "", "", "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Address+"/peer/converse", strings.NewReader(string(payload)))
	if err != nil {
		return nil, "", "", "", err
	}
	req.Header.Set("Content-Type", "application/json")

	h.registry.IncrementLoad(nodeA, 1)
	h.registry.IncrementLoad(nodeB, 1)
	defer h.registry.IncrementLoad(nodeA, -1)
	defer h.registry.IncrementLoad(nodeB, -1)

	start := time.Now()
	resp, err := h.dispatcher.Do(req)
	if err != nil {
		return nil, nodeA, a.Address, "", fmt.Errorf("node %s could not reach node %s: %w", nodeA, nodeB, err)
	}
	defer resp.Body.Close()

	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nodeA, a.Address, "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := out["error"].(string)
		if msg == "" {
			msg = resp.Status
		}
		return nil, nodeA, a.Address, "", errors.New(msg)
	}

	if h.mem != nil {
		conclusion, _ := out["conclusion"].(string)
		h.remember(memory.Experience{
			Kind:        "node_to_node",
			TaskType:    "collaboration",
			Description: fmt.Sprintf("%s and %s discussed: %s", nodeA, nodeB, topic),
			Output:      conclusion,
			NodeID:      nodeA,
			SessionID:   s.ID,
			Success:     conclusion != "",
			DurationMs:  time.Since(start).Milliseconds(),
			Tags:        []string{"peer", nodeA, nodeB},
		})
	}

	return out, nodeA, a.Address, "peer-to-peer", nil
}
