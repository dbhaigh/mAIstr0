package deepseek

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/scheduler"
	"strings"
)

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
