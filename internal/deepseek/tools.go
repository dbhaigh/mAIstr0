package deepseek

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
)

// ToolDef provides the JSON schema metadata for a tool in the DeepSeek harness.
type ToolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// Tool represents an executable tool callable by the DeepSeek agent harness.
type Tool interface {
	Def() ToolDef
	Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (output any, nodeID string, nodeAddr string, model string, err error)
}

// DefaultTools returns the suite of distributed cluster tools registered in the harness.
func DefaultTools() []Tool {
	registrations := defaultToolRegistrations()
	tools := make([]Tool, 0, len(registrations))
	for _, registration := range registrations {
		tools = append(tools, registration.tool)
	}
	return tools
}

func defaultToolRegistrations() []toolRegistration {
	return []toolRegistration{
		typedToolRegistration[clusterStatusArgs](&ClusterStatusTool{}),
		typedToolRegistration[listNodeModelsArgs](&ListNodeModelsTool{}),
		typedToolRegistration[nodeConverseArgs](&NodeConverseTool{}),
		typedToolRegistration[nodeCollaborateArgs](&NodeCollaborateTool{}),
		typedToolRegistration[clusterConverseArgs](&ClusterConverseTool{}),
		typedToolRegistration[nodeLLMQueryArgs](&NodeLLMQueryTool{}),
		typedToolRegistration[clusterLLMQueryArgs](&ClusterLLMQueryTool{}),
		typedToolRegistration[clusterParallelDispatchArgs](&ClusterParallelDispatchTool{}),
		typedToolRegistration[createSubAgentArgs](&CreateSubAgentTool{}),
		typedToolRegistration[workflowArgs](&ExecuteWorkflowTool{}),
		typedToolRegistration[testNodeModelArgs](&TestNodeModelTool{}),
		typedToolRegistration[listNodeDialoguesArgs](&ListNodeDialoguesTool{}),
		typedToolRegistration[getNodeDialogueArgs](&GetNodeDialogueTool{}),
		typedToolRegistration[evalExpressionArgs](&EvalExpressionTool{}),
		typedToolRegistration[fetchWebArgs](&FetchWebTool{}),
		typedToolRegistration[sessionMemoryArgs](&SessionMemoryTool{}),
	}
}

// ExecuteWorkflowTool sends multi-step work through mAIstr0's validated,
// persistent task engine instead of coordinating dispatch in chat history.
type ExecuteWorkflowTool struct{}

func (t *ExecuteWorkflowTool) Def() ToolDef {
	return toolDefinition[workflowArgs](
		"execute_workflow",
		"Execute a multi-step task using mAIstr0's persistent cluster workflow engine. For complex work, provide an explicit dependency-aware task plan; if omitted, the coordinator creates and validates a structured plan. Set require_review when a separate reviewer must approve the results. Returns only after the workflow completes.",
	)
}

func (t *ExecuteWorkflowTool) Execute(ctx context.Context, h *Harness, _ *Session, args map[string]any) (any, string, string, string, error) {
	description, _ := args["description"].(string)
	if strings.TrimSpace(description) == "" {
		return nil, "", "", "", errors.New("workflow description is required")
	}
	if h.workflowExecutor == nil {
		return nil, "", "", "", errors.New("durable workflow execution is not configured")
	}
	var subtasks []scheduler.Subtask
	if rawGraph, exists := args["graph"]; exists {
		encoded, err := json.Marshal(rawGraph)
		if err == nil {
			var g scheduler.TaskGraph
			if err := json.Unmarshal(encoded, &g); err == nil {
				if sorted, err := g.TopologicalSort(); err == nil && len(sorted) > 0 {
					subtasks = sorted
				}
			}
		}
	}
	if len(subtasks) == 0 {
		if raw, exists := args["subtasks"]; exists {
			encoded, err := json.Marshal(raw)
			if err != nil {
				return nil, "", "", "", fmt.Errorf("encode workflow plan: %w", err)
			}
			if err := json.Unmarshal(encoded, &subtasks); err != nil {
				return nil, "", "", "", fmt.Errorf("decode workflow plan: %w", err)
			}
		}
	}
	requireReview, _ := args["require_review"].(bool)
	result, err := h.workflowExecutor(ctx, description, subtasks, requireReview)
	if err != nil {
		return nil, "", "", "", err
	}
	return map[string]any{"result": result}, "", "", "", nil
}

// --- Tool: cluster_status ---

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

type NodeLLMQueryTool struct{}

func (t *NodeLLMQueryTool) Def() ToolDef {
	return toolDefinition[nodeLLMQueryArgs](
		"node_llm_query",
		"Directly invoke an LLM model on a specific compute node by node_id. Allows targeted generation requests to a particular node's LLM engine.",
	)
}

func (t *NodeLLMQueryTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeID, _ := args["node_id"].(string)
	if strings.TrimSpace(nodeID) == "" {
		return nil, "", "", "", errors.New("node_id is required")
	}
	prompt, _ := args["prompt"].(string)
	if strings.TrimSpace(prompt) == "" {
		return nil, "", "", "", errors.New("prompt is required")
	}
	model, _ := args["model"].(string)
	system, _ := args["system"].(string)

	node, ok := h.registry.Get(nodeID)
	if !ok || !node.Healthy {
		return nil, nodeID, "", model, fmt.Errorf("node %s not found or unhealthy", nodeID)
	}

	// Resolve model if not explicitly specified
	if model == "" {
		if node.DefaultModel != "" {
			model = node.DefaultModel
		} else if len(node.Models) > 0 {
			model = node.Models[0].Name
		} else {
			return nil, node.ID, node.Address, "", fmt.Errorf("node %s has no models available", nodeID)
		}
	}

	fullPrompt := prompt
	if system != "" {
		fullPrompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", system, prompt)
	}

	h.registry.IncrementLoad(node.ID, 1)
	defer h.registry.IncrementLoad(node.ID, -1)

	start := time.Now()
	output, err := h.dispatchToNode(ctx, node.Address, model, fullPrompt)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		return nil, node.ID, node.Address, model, fmt.Errorf("node %s execution failed: %w", node.ID, err)
	}

	return map[string]any{
		"node_id":      node.ID,
		"node_address": node.Address,
		"model":        model,
		"duration_ms":  duration,
		"output":       output,
	}, node.ID, node.Address, model, nil
}

// --- Tool: test_node_model ---

type TestNodeModelTool struct{}

func (t *TestNodeModelTool) Def() ToolDef {
	return toolDefinition[testNodeModelArgs](
		"test_node_model",
		"Perform a health check / ping generation test against a specific model on a specific node to verify connectivity and measure latency.",
	)
}

func (t *TestNodeModelTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	nodeID, _ := args["node_id"].(string)
	model, _ := args["model"].(string)
	prompt, _ := args["prompt"].(string)

	if nodeID == "" || model == "" {
		return nil, "", "", "", errors.New("node_id and model are required")
	}
	if prompt == "" {
		prompt = "Respond with OK in 3 words."
	}

	node, ok := h.registry.Get(nodeID)
	if !ok || !node.Healthy {
		return nil, nodeID, "", model, fmt.Errorf("node %s not found or offline", nodeID)
	}

	h.registry.IncrementLoad(node.ID, 1)
	defer h.registry.IncrementLoad(node.ID, -1)

	start := time.Now()
	out, err := h.dispatchToNode(ctx, node.Address, model, prompt)
	elapsed := time.Since(start).Milliseconds()

	if err != nil {
		return map[string]any{
			"node_id":     node.ID,
			"model":       model,
			"healthy":     false,
			"error":       err.Error(),
			"duration_ms": elapsed,
		}, node.ID, node.Address, model, nil
	}

	return map[string]any{
		"node_id":     node.ID,
		"model":       model,
		"healthy":     true,
		"duration_ms": elapsed,
		"output":      out,
	}, node.ID, node.Address, model, nil
}

// --- Tool: cluster_llm_query ---

type ClusterLLMQueryTool struct{}

func (t *ClusterLLMQueryTool) Def() ToolDef {
	return toolDefinition[clusterLLMQueryArgs](
		"cluster_llm_query",
		"Dispatch an LLM generation query to the most capable and least-loaded node in the cluster. Distributes workload across nodes based on matching model tags, hardware capacity, and current active task load.",
	)
}

func (t *ClusterLLMQueryTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	prompt, _ := args["prompt"].(string)
	if strings.TrimSpace(prompt) == "" {
		return nil, "", "", "", errors.New("prompt is required")
	}
	taskType, _ := args["task_type"].(string)
	if taskType == "" {
		taskType = "general"
	}
	modelPref, _ := args["model_preference"].(string)

	nodes := h.registry.Active()
	if len(nodes) == 0 {
		return nil, "", "", "", errors.New("no healthy nodes available in the cluster")
	}

	// Select best node factoring in load penalties to spread queries evenly
	bestNode, chosenModel, score := h.pickBestNodeForQuery(taskType, modelPref, nodes)
	if bestNode == nil {
		return nil, "", "", "", errors.New("no suitable node found with available models")
	}

	// Track in-flight load in real time
	h.registry.IncrementLoad(bestNode.ID, 1)
	defer h.registry.IncrementLoad(bestNode.ID, -1)

	output, err := h.dispatchToNode(ctx, bestNode.Address, chosenModel, prompt)
	if err != nil {
		return nil, bestNode.ID, bestNode.Address, chosenModel, fmt.Errorf("node %s execution failed: %w", bestNode.ID, err)
	}

	return map[string]any{
		"node_id":      bestNode.ID,
		"node_address": bestNode.Address,
		"model":        chosenModel,
		"score":        score,
		"output":       output,
	}, bestNode.ID, bestNode.Address, chosenModel, nil
}

// --- Tool: cluster_parallel_dispatch ---

type ClusterParallelDispatchTool struct{}

func (t *ClusterParallelDispatchTool) Def() ToolDef {
	return toolDefinition[clusterParallelDispatchArgs](
		"cluster_parallel_dispatch",
		"Concurrently distribute multiple subtasks across different nodes in the cluster. Spreads the parallel workload evenly across available hardware, executing simultaneously and returning all aggregated results.",
	)
}

type parallelResult struct {
	SubtaskID   string `json:"subtask_id"`
	Description string `json:"description"`
	NodeID      string `json:"node_id"`
	NodeAddress string `json:"node_address"`
	Model       string `json:"model"`
	Output      string `json:"output,omitempty"`
	Error       string `json:"error,omitempty"`
	DurationMs  int64  `json:"duration_ms"`
}

func (t *ClusterParallelDispatchTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	rawSubtasks, ok := args["subtasks"].([]any)
	if !ok || len(rawSubtasks) == 0 {
		return nil, "", "", "", errors.New("subtasks array is required and must not be empty")
	}

	type subtaskItem struct {
		id          string
		description string
		taskType    string
		modelPref   string
	}

	items := make([]subtaskItem, 0, len(rawSubtasks))
	for i, raw := range rawSubtasks {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		desc, _ := m["description"].(string)
		if strings.TrimSpace(desc) == "" {
			continue
		}
		tt, _ := m["task_type"].(string)
		if tt == "" {
			tt = "general"
		}
		mp, _ := m["model_preference"].(string)
		items = append(items, subtaskItem{
			id:          fmt.Sprintf("sub-%d", i+1),
			description: desc,
			taskType:    tt,
			modelPref:   mp,
		})
	}

	if len(items) == 0 {
		return nil, "", "", "", errors.New("no valid subtask descriptions provided")
	}

	nodes := h.registry.Active()
	if len(nodes) == 0 {
		return nil, "", "", "", errors.New("no healthy nodes available in the cluster")
	}

	// Balance subtasks across nodes using simulated load accumulation
	simulatedLoad := make(map[string]int, len(nodes))
	for _, n := range nodes {
		simulatedLoad[n.ID] = n.ActiveTasks
	}

	type assignment struct {
		item  subtaskItem
		node  cluster.NodeStatus
		model string
	}

	assignments := make([]assignment, 0, len(items))
	for _, it := range items {
		st := scheduler.Subtask{
			ID:          it.id,
			Description: it.description,
			TaskType:    it.taskType,
		}
		bestNode, bestModel, _ := h.pickBestNodeForSubtask(st, it.modelPref, nodes, simulatedLoad)
		if bestNode == nil {
			continue
		}
		simulatedLoad[bestNode.ID]++
		assignments = append(assignments, assignment{
			item:  it,
			node:  *bestNode,
			model: bestModel,
		})
	}

	if len(assignments) == 0 {
		return nil, "", "", "", errors.New("could not assign any subtasks to available nodes")
	}

	// Concurrently dispatch to assigned nodes
	results := make([]parallelResult, len(assignments))
	var wg sync.WaitGroup
	var nodesUsed = make(map[string]bool)
	// nodesUsedMu protects the result set shared by dispatch goroutines; it is
	// released before network dispatch and is not nested with component locks.
	var nodesUsedMu sync.Mutex

	for idx, asg := range assignments {
		wg.Add(1)
		go func(i int, a assignment) {
			defer wg.Done()
			h.registry.IncrementLoad(a.node.ID, 1)
			defer h.registry.IncrementLoad(a.node.ID, -1)

			nodesUsedMu.Lock()
			nodesUsed[a.node.ID] = true
			nodesUsedMu.Unlock()

			start := time.Now()
			out, err := h.dispatchToNode(ctx, a.node.Address, a.model, a.item.description)
			elapsed := time.Since(start).Milliseconds()

			res := parallelResult{
				SubtaskID:   a.item.id,
				Description: a.item.description,
				NodeID:      a.node.ID,
				NodeAddress: a.node.Address,
				Model:       a.model,
				DurationMs:  elapsed,
			}
			if err != nil {
				res.Error = err.Error()
			} else {
				res.Output = out
			}
			results[i] = res
		}(idx, asg)
	}

	wg.Wait()

	nodeList := make([]string, 0, len(nodesUsed))
	for nid := range nodesUsed {
		nodeList = append(nodeList, nid)
	}
	sort.Strings(nodeList)

	return map[string]any{
		"total_subtasks": len(items),
		"nodes_engaged":  nodeList,
		"results":        results,
	}, fmt.Sprintf("%d nodes", len(nodeList)), "", "multi-model", nil
}

// --- Tool: eval_expression ---

type EvalExpressionTool struct{}

func (t *EvalExpressionTool) Def() ToolDef {
	return toolDefinition[evalExpressionArgs](
		"eval_expression",
		"Perform direct, deterministic arithmetic calculations, math functions (sqrt, pow, sin, cos, round, abs), or data transformations safely.",
	)
}

func (t *EvalExpressionTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	expr, _ := args["expression"].(string)
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, "", "", "", errors.New("expression is required")
	}

	result, err := evaluateMathExpression(expr)
	if err != nil {
		return nil, "", "", "", err
	}
	return map[string]any{
		"expression": expr,
		"result":     result,
	}, "", "", "builtin-math", nil
}

func evaluateMathExpression(expr string) (any, error) {
	clean := strings.ToLower(strings.TrimSpace(expr))

	// Handle functional helper patterns
	if strings.HasPrefix(clean, "sqrt(") && strings.HasSuffix(clean, ")") {
		inner := strings.TrimSuffix(strings.TrimPrefix(clean, "sqrt("), ")")
		val, err := strconv.ParseFloat(strings.TrimSpace(inner), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid sqrt argument: %v", err)
		}
		return math.Sqrt(val), nil
	}

	if strings.HasPrefix(clean, "sum(") && strings.HasSuffix(clean, ")") {
		inner := strings.TrimSuffix(strings.TrimPrefix(clean, "sum("), ")")
		parts := strings.Split(inner, ",")
		var total float64
		for _, p := range parts {
			val, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid sum term %q: %v", p, err)
			}
			total += val
		}
		return total, nil
	}

	if strings.HasPrefix(clean, "mean(") && strings.HasSuffix(clean, ")") || strings.HasPrefix(clean, "avg(") && strings.HasSuffix(clean, ")") {
		prefix := "mean("
		if strings.HasPrefix(clean, "avg(") {
			prefix = "avg("
		}
		inner := strings.TrimSuffix(strings.TrimPrefix(clean, prefix), ")")
		parts := strings.Split(inner, ",")
		if len(parts) == 0 {
			return 0, nil
		}
		var total float64
		for _, p := range parts {
			val, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid term %q: %v", p, err)
			}
			total += val
		}
		return total / float64(len(parts)), nil
	}

	// Simple 2-operand or single operand arithmetic parser
	// Tokens: +, -, *, /, %, ^
	operators := []string{"+", "-", "*", "/", "%", "^"}
	for _, op := range operators {
		idx := strings.Index(clean, op)
		if idx > 0 && idx < len(clean)-1 {
			leftStr := strings.TrimSpace(clean[:idx])
			rightStr := strings.TrimSpace(clean[idx+1:])
			left, err1 := strconv.ParseFloat(leftStr, 64)
			right, err2 := strconv.ParseFloat(rightStr, 64)
			if err1 == nil && err2 == nil {
				switch op {
				case "+":
					return left + right, nil
				case "-":
					return left - right, nil
				case "*":
					return left * right, nil
				case "/":
					if right == 0 {
						return nil, errors.New("division by zero")
					}
					return left / right, nil
				case "%":
					return math.Mod(left, right), nil
				case "^":
					return math.Pow(left, right), nil
				}
			}
		}
	}

	// Try direct numeric parse
	if val, err := strconv.ParseFloat(clean, 64); err == nil {
		return val, nil
	}

	return nil, fmt.Errorf("could not parse expression %q", expr)
}

// --- Tool: fetch_web ---

type FetchWebTool struct{}

func (t *FetchWebTool) Def() ToolDef {
	return toolDefinition[fetchWebArgs](
		"fetch_web",
		"Perform an HTTP GET request to fetch text, documentation, or JSON data from a URL.",
	)
}

func (t *FetchWebTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	urlStr, _ := args["url"].(string)
	if urlStr == "" {
		return nil, "", "", "", errors.New("url is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, "", "", "", err
	}
	req.Header.Set("User-Agent", "mAIstr0-DeepSeek/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", "", "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	if err != nil {
		return nil, "", "", "", err
	}

	return map[string]any{
		"url":         urlStr,
		"status_code": resp.StatusCode,
		"content":     string(body),
	}, "", "", "builtin-http", nil
}

// --- Tool: session_memory ---

type SessionMemoryTool struct{}

func (t *SessionMemoryTool) Def() ToolDef {
	return toolDefinition[sessionMemoryArgs](
		"session_memory",
		"Persist or recall working notes, findings, and context across turns in the interactive session scratchpad.",
	)
}

func (t *SessionMemoryTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	action, _ := args["action"].(string)
	key, _ := args["key"].(string)
	val, _ := args["value"].(string)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Memory == nil {
		s.Memory = make(map[string]string)
	}

	switch action {
	case "set":
		if key == "" {
			return nil, "", "", "", errors.New("key is required for set")
		}
		s.Memory[key] = val
		return map[string]string{"status": "saved", "key": key}, "", "", "memory", nil
	case "get":
		if key == "" {
			return nil, "", "", "", errors.New("key is required for get")
		}
		v, ok := s.Memory[key]
		if !ok {
			return map[string]any{"found": false, "key": key}, "", "", "memory", nil
		}
		return map[string]any{"found": true, "key": key, "value": v}, "", "", "memory", nil
	case "list":
		return s.Memory, "", "", "memory", nil
	case "delete":
		delete(s.Memory, key)
		return map[string]string{"status": "deleted", "key": key}, "", "", "memory", nil
	default:
		return nil, "", "", "", fmt.Errorf("unknown memory action: %s", action)
	}
}

// --- Tool: create_subagent ---

type CreateSubAgentTool struct{}

func (t *CreateSubAgentTool) Def() ToolDef {
	return toolDefinition[createSubAgentArgs](
		"create_subagent",
		"Create a specialized virtual worker (sub-agent) at runtime from a 4-tuple specification: instruction/persona, required tools, required/preferred model capabilities, and context. Dynamically selects the best compute node and model, registers the virtual worker profile for scheduling, and optionally executes an initial task.",
	)
}

func (t *CreateSubAgentTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	name, _ := args["name"].(string)
	instructions, _ := args["instructions"].(string)
	role, _ := args["role"].(string)
	contextStr, _ := args["context"].(string)
	taskPrompt, _ := args["task"].(string)

	var reqTools []string
	if raw, ok := args["required_tools"].([]any); ok {
		for _, v := range raw {
			if str, ok := v.(string); ok {
				reqTools = append(reqTools, str)
			}
		}
	}
	var reqCaps []string
	if raw, ok := args["required_capabilities"].([]any); ok {
		for _, v := range raw {
			if str, ok := v.(string); ok {
				reqCaps = append(reqCaps, str)
			}
		}
	}
	var prefCaps []string
	if raw, ok := args["preferred_capabilities"].([]any); ok {
		for _, v := range raw {
			if str, ok := v.(string); ok {
				prefCaps = append(prefCaps, str)
			}
		}
	}

	nodes := h.registry.Active()
	factory := scheduler.NewSubAgentFactory()
	worker, err := factory.Create(scheduler.SubAgentSpec{
		Name:                  name,
		Role:                  role,
		Instruction:           instructions,
		RequiredTools:         reqTools,
		RequiredCapabilities:  reqCaps,
		PreferredCapabilities: prefCaps,
		Context:               contextStr,
	}, nodes, nil)
	if err != nil {
		return nil, "", "", "", fmt.Errorf("create sub-agent: %w", err)
	}

	result := map[string]any{
		"id":             worker.ID,
		"name":           worker.Spec.Name,
		"role":           worker.Spec.Role,
		"node_id":        worker.NodeID,
		"node_address":   worker.Address,
		"model":          worker.Model,
		"score":          worker.Score,
		"required_tools": worker.Spec.RequiredTools,
		"instruction":    worker.Spec.Instruction,
	}

	if strings.TrimSpace(taskPrompt) != "" {
		formattedPrompt := worker.FormatPrompt(taskPrompt, "")
		output, err := h.dispatchToNode(ctx, worker.Address, worker.Model, formattedPrompt)
		if err != nil {
			result["execution_error"] = err.Error()
		} else {
			result["output"] = output
		}
	}

	return result, worker.NodeID, worker.Address, worker.Model, nil
}
