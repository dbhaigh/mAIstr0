package deepseek

import (
	"context"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler"
	"sort"
	"strings"
	"sync"
	"time"
)

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
