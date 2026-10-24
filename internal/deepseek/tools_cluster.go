package deepseek

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/engine"
	"net/http"
	"strings"
	"time"
)

type ClusterStatusTool struct{}

func (t *ClusterStatusTool) Def() ToolDef {
	return toolDefinition[clusterStatusArgs](
		"cluster_status",
		"Inspect the current cluster status: all registered compute nodes, hardware capabilities (CPUs, RAM, GPUs), loaded models, active task workloads, health, and elected cluster leader.",
	)
}

func (t *ClusterStatusTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodes := h.registry.Active()
	type NodeSummary struct {
		ID           string         `json:"id"`
		Address      string         `json:"address"`
		Hardware     string         `json:"hardware"`
		CPU          int            `json:"cpu_cores"`
		RAMGB        float64        `json:"ram_gb"`
		GPU          string         `json:"gpu,omitempty"`
		FastScore    float64        `json:"fast_score"`
		ActiveTasks  int            `json:"active_tasks"`
		Healthy      bool           `json:"healthy"`
		Leader       bool           `json:"leader"`
		DefaultModel string         `json:"default_model,omitempty"`
		Models       []engine.Model `json:"models"`
	}

	summaries := make([]NodeSummary, 0, len(nodes))
	totalCPUs := 0
	var totalRAM float64

	for _, n := range nodes {
		gpuDesc := ""
		if n.Hardware.HasGPU {
			gpuDesc = n.Hardware.GPUVendor
			if gpuDesc == "" {
				gpuDesc = "enabled"
			}
		}
		totalCPUs += n.Hardware.CPUCores
		ramGB := float64(n.Hardware.TotalRAMMB) / 1024.0
		totalRAM += ramGB

		summaries = append(summaries, NodeSummary{
			ID:           n.ID,
			Address:      n.Address,
			Hardware:     fmt.Sprintf("%s/%s", n.Hardware.OS, n.Hardware.Arch),
			CPU:          n.Hardware.CPUCores,
			RAMGB:        ramGB,
			GPU:          gpuDesc,
			FastScore:    n.FastScore,
			ActiveTasks:  n.ActiveTasks,
			Healthy:      n.Healthy,
			Leader:       n.Leader,
			DefaultModel: n.DefaultModel,
			Models:       n.Models,
		})
	}

	return map[string]any{
		"total_nodes": len(nodes),
		"total_cpus":  totalCPUs,
		"total_ram":   fmt.Sprintf("%.1f GB", totalRAM),
		"nodes":       summaries,
	}, "", "", "", nil
}

// --- Tool: node_converse ---

type NodeConverseTool struct{}

func (t *NodeConverseTool) Def() ToolDef {
	return toolDefinition[nodeConverseArgs](
		"node_converse",
		"Hold an interactive multi-turn conversation with an LLM on a specific worker node. Maintains full dialogue context across turns using dialogue_id so you can send instructions, receive answers, ask follow-up questions, request revisions, and converse naturally with the node's model.",
	)
}

func (t *NodeConverseTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeID, _ := args["node_id"].(string)
	if strings.TrimSpace(nodeID) == "" {
		return nil, "", "", "", errors.New("node_id is required")
	}
	message, _ := args["message"].(string)
	if strings.TrimSpace(message) == "" {
		return nil, "", "", "", errors.New("message is required")
	}
	dialogueID, _ := args["dialogue_id"].(string)
	model, _ := args["model"].(string)
	systemPrompt, _ := args["system_prompt"].(string)

	node, ok := h.registry.Get(nodeID)
	if !ok || !node.Healthy {
		return nil, nodeID, "", model, fmt.Errorf("node %s not found or unhealthy", nodeID)
	}

	if model == "" {
		if node.DefaultModel != "" {
			model = node.DefaultModel
		} else if len(node.Models) > 0 {
			model = node.Models[0].Name
		} else {
			return nil, node.ID, node.Address, "", fmt.Errorf("node %s has no models available", nodeID)
		}
	}

	if dialogueID == "" {
		dialogueID = fmt.Sprintf("dlg-%s-%d", node.ID, time.Now().UnixNano()%1000000)
	}

	payloadMsg := message
	if systemPrompt != "" {
		payloadMsg = fmt.Sprintf("[Instruction/Persona: %s]\n\n%s", systemPrompt, message)
	}

	resp, err := h.SendNodeDialogueMessage(ctx, node, dialogueID, model, payloadMsg, "user")
	if err != nil {
		return nil, node.ID, node.Address, model, fmt.Errorf("conversation with node %s failed: %w", node.ID, err)
	}

	return map[string]any{
		"dialogue_id":             resp.DialogueID,
		"node_id":                 node.ID,
		"node_address":            node.Address,
		"model":                   resp.Model,
		"turn":                    resp.Turn,
		"node_reply":              resp.Reply,
		"conversation_transcript": resp.Messages,
		"duration_ms":             resp.DurationMs,
	}, node.ID, node.Address, model, nil
}

// --- Tool: node_collaborate ---

type NodeCollaborateTool struct{}

func (t *NodeCollaborateTool) Def() ToolDef {
	return toolDefinition[nodeCollaborateArgs](
		"node_collaborate",
		"Conduct an interactive multi-round conversation between the orchestrator coordinator and a worker node LLM. Iteratively proposes, critiques, refines, and perfects a complex task across multiple turns until finished.",
	)
}

func (t *NodeCollaborateTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeID, _ := args["node_id"].(string)
	task, _ := args["task"].(string)
	model, _ := args["model"].(string)
	goal, _ := args["collaboration_goal"].(string)

	rounds := 2
	if rFloat, ok := args["rounds"].(float64); ok && rFloat > 0 {
		rounds = int(rFloat)
		if rounds > 4 {
			rounds = 4
		}
	}

	node, ok := h.registry.Get(nodeID)
	if !ok || !node.Healthy {
		return nil, nodeID, "", model, fmt.Errorf("node %s not found or offline", nodeID)
	}

	if model == "" {
		if node.DefaultModel != "" {
			model = node.DefaultModel
		} else if len(node.Models) > 0 {
			model = node.Models[0].Name
		} else {
			return nil, node.ID, node.Address, "", fmt.Errorf("node %s has no models available", nodeID)
		}
	}

	dialogueID := fmt.Sprintf("collab-%s-%d", node.ID, time.Now().UnixNano()%1000000)

	type DialogueExchange struct {
		Round        int    `json:"round"`
		Orchestrator string `json:"orchestrator_prompt"`
		NodeReply    string `json:"node_reply"`
		DurationMs   int64  `json:"duration_ms"`
	}

	var exchanges []DialogueExchange
	var lastReply string

	// Round 1: Initial Task Assignment
	prompt1 := fmt.Sprintf("You are an expert worker node (%s) in the mAIstr0 cluster. Please provide your best initial solution for the following task:\n\n%s", node.ID, task)
	if goal != "" {
		prompt1 += "\n\nGoal/Criteria: " + goal
	}

	start1 := time.Now()
	resp1, err := h.SendNodeDialogueMessage(ctx, node, dialogueID, model, prompt1, "user")
	if err != nil {
		return nil, node.ID, node.Address, model, err
	}
	lastReply = resp1.Reply
	exchanges = append(exchanges, DialogueExchange{
		Round:        1,
		Orchestrator: prompt1,
		NodeReply:    resp1.Reply,
		DurationMs:   time.Since(start1).Milliseconds(),
	})

	// Subsequent Interactive Rounds: Review, critique, refine
	for r := 2; r <= rounds; r++ {
		critiquePrompt := "Please review your previous solution critically. Refine it for optimal performance, address potential edge cases, polish the formatting, and provide the final improved version."
		if goal != "" {
			critiquePrompt = fmt.Sprintf("Please review and refine your previous solution against the goal: %s. Polish and deliver the finalized solution.", goal)
		}

		startR := time.Now()
		respR, err := h.SendNodeDialogueMessage(ctx, node, dialogueID, model, critiquePrompt, "user")
		if err != nil {
			break
		}
		lastReply = respR.Reply
		exchanges = append(exchanges, DialogueExchange{
			Round:        r,
			Orchestrator: critiquePrompt,
			NodeReply:    respR.Reply,
			DurationMs:   time.Since(startR).Milliseconds(),
		})
	}

	return map[string]any{
		"dialogue_id":    dialogueID,
		"node_id":        node.ID,
		"model":          model,
		"total_rounds":   len(exchanges),
		"final_solution": lastReply,
		"dialogue_flow":  exchanges,
	}, node.ID, node.Address, model, nil
}

// --- Tool: cluster_converse ---

type ClusterConverseTool struct{}

func (t *ClusterConverseTool) Def() ToolDef {
	return toolDefinition[clusterConverseArgs](
		"cluster_converse",
		"Start or continue a multi-turn conversation with the best suited, least-loaded node in the cluster. Routes work automatically across nodes while preserving conversation history.",
	)
}

func (t *ClusterConverseTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	message, _ := args["message"].(string)
	if strings.TrimSpace(message) == "" {
		return nil, "", "", "", errors.New("message is required")
	}
	dialogueID, _ := args["dialogue_id"].(string)
	taskType, _ := args["task_type"].(string)
	if taskType == "" {
		taskType = "general"
	}
	modelPref, _ := args["model_preference"].(string)

	nodes := h.registry.Active()
	if len(nodes) == 0 {
		return nil, "", "", "", errors.New("no healthy nodes available in the cluster")
	}

	bestNode, chosenModel, _ := h.pickBestNodeForQuery(taskType, modelPref, nodes)
	if bestNode == nil {
		return nil, "", "", "", errors.New("no suitable node found")
	}

	if dialogueID == "" {
		dialogueID = fmt.Sprintf("dlg-%s-%d", bestNode.ID, time.Now().UnixNano()%1000000)
	}

	resp, err := h.SendNodeDialogueMessage(ctx, *bestNode, dialogueID, chosenModel, message, "user")
	if err != nil {
		return nil, bestNode.ID, bestNode.Address, chosenModel, err
	}

	return map[string]any{
		"dialogue_id":             resp.DialogueID,
		"node_id":                 bestNode.ID,
		"node_address":            bestNode.Address,
		"model":                   resp.Model,
		"turn":                    resp.Turn,
		"node_reply":              resp.Reply,
		"conversation_transcript": resp.Messages,
		"duration_ms":             resp.DurationMs,
	}, bestNode.ID, bestNode.Address, chosenModel, nil
}

// --- Tool: list_node_dialogues ---

type ListNodeDialoguesTool struct{}

func (t *ListNodeDialoguesTool) Def() ToolDef {
	return toolDefinition[listNodeDialoguesArgs](
		"list_node_dialogues",
		"List all ongoing or recent conversations with models across cluster nodes.",
	)
}

func (t *ListNodeDialoguesTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeIDFilter, _ := args["node_id"].(string)
	nodes := h.registry.Active()

	type NodeDialogueItem struct {
		NodeID   string `json:"node_id"`
		Endpoint string `json:"endpoint"`
	}

	var results []NodeDialogueItem
	for _, n := range nodes {
		if nodeIDFilter != "" && n.ID != nodeIDFilter {
			continue
		}
		results = append(results, NodeDialogueItem{
			NodeID:   n.ID,
			Endpoint: fmt.Sprintf("%s/dialogues", n.Address),
		})
	}

	return map[string]any{
		"nodes_checked": len(results),
		"details":       results,
	}, "", "", "", nil
}

// --- Tool: get_node_dialogue ---

type GetNodeDialogueTool struct{}

func (t *GetNodeDialogueTool) Def() ToolDef {
	return toolDefinition[getNodeDialogueArgs](
		"get_node_dialogue",
		"Fetch the complete conversation transcript of a specific dialogue from a node.",
	)
}

func (t *GetNodeDialogueTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeID, _ := args["node_id"].(string)
	dialogueID, _ := args["dialogue_id"].(string)

	if nodeID == "" || dialogueID == "" {
		return nil, "", "", "", errors.New("node_id and dialogue_id are required")
	}

	node, ok := h.registry.Get(nodeID)
	if !ok || !node.Healthy {
		return nil, nodeID, "", "", fmt.Errorf("node %s not found or offline", nodeID)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/dialogues/%s", node.Address, dialogueID))
	if err != nil {
		return nil, node.ID, node.Address, "", err
	}
	defer resp.Body.Close()

	var dlg map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&dlg); err != nil {
		return nil, node.ID, node.Address, "", err
	}

	return dlg, node.ID, node.Address, "", nil
}

// --- Tool: list_node_models ---

type ListNodeModelsTool struct{}

func (t *ListNodeModelsTool) Def() ToolDef {
	return toolDefinition[listNodeModelsArgs](
		"list_node_models",
		"Discover all LLM models currently loaded or available on each node in the cluster, including their tags (code, chat, reasoning, math, vision), engine, size, and node health.",
	)
}

func (t *ListNodeModelsTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeIDFilter, _ := args["node_id"].(string)
	nodes := h.registry.Active()

	type NodeModelEntry struct {
		NodeID       string   `json:"node_id"`
		NodeAddress  string   `json:"node_address"`
		ModelName    string   `json:"model_name"`
		Engine       string   `json:"engine"`
		Tags         []string `json:"tags"`
		SizeGB       float64  `json:"size_gb,omitempty"`
		DefaultModel bool     `json:"default_model"`
		NodeHealthy  bool     `json:"node_healthy"`
	}

	var results []NodeModelEntry
	for _, n := range nodes {
		if nodeIDFilter != "" && n.ID != nodeIDFilter {
			continue
		}
		for _, m := range n.Models {
			results = append(results, NodeModelEntry{
				NodeID:       n.ID,
				NodeAddress:  n.Address,
				ModelName:    m.Name,
				Engine:       m.Engine,
				Tags:         m.Tags,
				SizeGB:       m.SizeGB,
				DefaultModel: m.Name == n.DefaultModel,
				NodeHealthy:  n.Healthy,
			})
		}
	}

	return map[string]any{
		"total_models": len(results),
		"models":       results,
	}, "", "", "", nil
}

// --- Tool: node_llm_query ---
