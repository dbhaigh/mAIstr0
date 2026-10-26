package deepseek

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"strings"
)

func (h *Harness) SendMessage(ctx context.Context, sessionID string, userText string, streamChan chan<- StreamEvent) (*Message, error) {
	s, ok := h.GetSession(sessionID)
	if !ok {
		return nil, errors.New("session not found: " + sessionID)
	}
	var agentEvents chan agent.StreamEvent
	var relayDone chan struct{}
	if streamChan != nil {
		agentEvents = make(chan agent.StreamEvent, cap(streamChan))
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			for event := range agentEvents {
				select {
				case streamChan <- FromAgentStreamEvent(event):
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	message, err := h.loop.Run(ctx, deepSeekConversation{session: s}, userText, agentEvents)
	if agentEvents != nil {
		close(agentEvents)
		<-relayDone
	}
	if message == nil {
		return nil, err
	}
	converted := FromAgentMessage(*message)
	return &converted, err
}

func toolTaskType(tc ToolCall) string {
	if tc.Arguments == nil {
		return ""
	}
	if v, ok := tc.Arguments["task_type"].(string); ok {
		return v
	}
	return ""
}

func describeToolCall(tc ToolCall) string {
	for _, key := range []string{"message", "prompt", "task", "topic", "expression", "query"} {
		if v, ok := tc.Arguments[key].(string); ok && v != "" {
			return tc.Name + ": " + v
		}
	}
	return tc.Name
}

func renderToolOutput(out any) string {
	if out == nil {
		return ""
	}
	if s, ok := out.(string); ok {
		return s
	}
	buf, err := json.Marshal(out)
	if err != nil {
		return fmt.Sprintf("%v", out)
	}
	return string(buf)
}

// lastUserMessage returns the most recent user turn, used as the recall key.
func lastUserMessage(s *Session) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.Messages) - 1; i >= 0; i-- {
		if s.Messages[i].Role == RoleUser {
			return s.Messages[i].Content
		}
	}
	return ""
}

// selectCoordinator chooses the optimal node and model to orchestrate reasoning.
func (h *Harness) selectCoordinator(s *Session) (*cluster.NodeStatus, string, error) {
	nodes := h.registry.Active()
	if len(nodes) == 0 {
		return nil, "", errors.New("no active nodes in cluster")
	}

	// The elected leader is the coordinator role. A requested model may tune
	// which model runs there, but must never move coordinator work to a worker
	// node that does not hold that role.
	for _, n := range nodes {
		if n.Leader && len(n.Models) > 0 {
			if s.Config.CoordinatorModel != "" {
				for _, m := range n.Models {
					if m.Name == s.Config.CoordinatorModel {
						return &n, m.Name, nil
					}
				}
			}
			model := pickBestCoordinatorModel(n.Models, n.DefaultModel)
			if model != "" {
				return &n, model, nil
			}
		}
	}

	// Fallback for a single-node cluster before leadership is established.
	var bestNode *cluster.NodeStatus
	bestScore := -1.0
	bestModel := ""

	for i := range nodes {
		n := &nodes[i]
		if len(n.Models) == 0 {
			continue
		}
		m := pickBestCoordinatorModel(n.Models, n.DefaultModel)
		if m == "" {
			continue
		}
		score := n.FastScore - float64(n.ActiveTasks)*8.0 -
			cluster.GPUUtilizationPenalty(n.Hardware) - cluster.NPUUtilizationPenalty(n.Hardware)
		if score > bestScore {
			bestScore = score
			bestNode = n
			bestModel = m
		}
	}

	if bestNode != nil && bestModel != "" {
		return bestNode, bestModel, nil
	}

	// 4. Default to first node's first model or simulated
	firstNode := &nodes[0]
	if len(firstNode.Models) > 0 {
		return firstNode, firstNode.Models[0].Name, nil
	}

	return firstNode, "simulated-general", nil
}

func pickBestCoordinatorModel(models []engine.Model, defaultModel string) string {
	bestName := ""
	bestScore := -1.0
	for _, m := range models {
		score := 0.0
		if m.Name == defaultModel {
			score += 5.0
		}
		for _, tag := range m.Tags {
			switch tag {
			case "reasoning":
				score += 10.0
			case "chat":
				score += 6.0
			case "general":
				score += 4.0
			case "code":
				score += 3.0
			case "fast":
				score += 2.0
			}
		}
		if score > bestScore {
			bestScore = score
			bestName = m.Name
		}
	}
	return bestName
}

func (h *Harness) buildPromptWithTools(s *Session, tools []ToolDef) string {
	var sb strings.Builder

	// System prompt
	sb.WriteString("<|im_start|>system\n")
	if s.Config.SystemPrompt != "" {
		sb.WriteString(s.Config.SystemPrompt + "\n\n")
	} else if h.style == "deepseek" {
		sb.WriteString("You are DeepSeek, an intelligent reasoning coordinator running on the mAIstr0 multi-node LLM cluster orchestrator.\n")
		sb.WriteString("Use deliberate reasoning, and use the available tools whenever cluster work is required.\n\n")
	} else {
		sb.WriteString("You are DeepSeek, an intelligent interactive AI coordinator running on the mAIstr0 multi-node LLM orchestrator.\n")
		sb.WriteString("You have access to distributed worker nodes across the cluster to execute LLM queries, evaluate math, and fan out parallel subtasks.\n\n")
	}

	// Tools schema
	toolsJSON, _ := json.MarshalIndent(tools, "", "  ")
	sb.WriteString("You have access to the following tools:\n<tools>\n")
	sb.Write(toolsJSON)
	sb.WriteString("\n</tools>\n\n")

	if h.style == "deepseek" {
		sb.WriteString("Reason inside <think>...</think> when useful. When you need to execute a tool, output ONLY this tool call block:\n")
	} else {
		sb.WriteString("When you need to execute a tool, output ONLY the tool call block in this format:\n")
	}
	sb.WriteString("<tool_call>\n{\"name\": \"tool_name\", \"arguments\": {\"arg_name\": \"value\"}}\n</tool_call>\n\n")
	sb.WriteString("Guidelines:\n")
	sb.WriteString("- To check cluster status and compute resources, call `cluster_status`.\n")
	sb.WriteString("- To discover models hosted on nodes, call `list_node_models`.\n")
	sb.WriteString("- To hold an interactive multi-turn conversation with a specific node's LLM, call `node_converse` with node_id, message, and dialogue_id.\n")
	sb.WriteString("- To collaboratively solve and iteratively refine a complex task with a worker node's LLM across multiple rounds, call `node_collaborate`.\n")
	sb.WriteString("- To converse with the least-loaded matching node in the cluster, call `cluster_converse`.\n")
	sb.WriteString("- To directly query a node's LLM once, call `node_llm_query` or `cluster_llm_query`.\n")
	sb.WriteString("- To fan out parallel tasks simultaneously across multiple nodes, call `cluster_parallel_dispatch`.\n")
	sb.WriteString("- For multi-step work that needs durable progress, dependencies, validation, or review, call `execute_workflow` instead of coordinating worker calls manually.\n")
	sb.WriteString("- To perform arithmetic or math formulas, call `eval_expression`.\n")
	sb.WriteString("- To have two worker node LLMs debate or design something together, call `node_to_node_conversation`.\n")
	sb.WriteString("- To look up what the cluster learned from past runs, call `recall_memory` or `memory_insights`.\n")
	sb.WriteString("- To durably save something worth remembering next session, call `remember_fact`.\n")
	sb.WriteString("- If no tool call is needed, provide your direct response to the user.\n")

	// Recalled experience is injected so the agent starts each step already
	// knowing what worked, what failed, and which node handled it best.
	if h.mem != nil {
		if brief := h.mem.ContextBrief(lastUserMessage(s), "", 5); brief != "" {
			sb.WriteString("\n")
			sb.WriteString(brief)
			sb.WriteString("Use this recalled experience: repeat what worked, avoid what failed, and do not re-derive answers you already have.\n")
		}
	}

	sb.WriteString("<|im_end|>\n")

	// Message history
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, msg := range s.Messages {
		switch msg.Role {
		case RoleUser:
			fmt.Fprintf(&sb, "<|im_start|>user\n%s<|im_end|>\n", msg.Content)
		case RoleAssistant:
			sb.WriteString("<|im_start|>assistant\n")
			if msg.Thought != "" {
				sb.WriteString(msg.Thought + "\n")
			}
			if len(msg.ToolCalls) > 0 {
				for _, tc := range msg.ToolCalls {
					callJSON, _ := json.Marshal(map[string]any{
						"name":      tc.Name,
						"arguments": tc.Arguments,
					})
					fmt.Fprintf(&sb, "<tool_call>\n%s\n</tool_call>\n", string(callJSON))
				}
			} else if msg.Content != "" {
				sb.WriteString(msg.Content + "\n")
			}
			sb.WriteString("<|im_end|>\n")
		case RoleTool:
			if msg.ToolResponse != nil {
				respJSON, _ := json.Marshal(map[string]any{
					"name":   msg.ToolResponse.Name,
					"output": msg.ToolResponse.Output,
					"error":  msg.ToolResponse.Error,
					"node":   msg.ToolResponse.NodeID,
				})
				fmt.Fprintf(&sb, "<tool_response>\n%s\n</tool_response>\n", string(respJSON))
			}
		}
	}

	sb.WriteString("<|im_start|>assistant\n")
	return sb.String()
}

// pickBestNodeForQuery picks the least-loaded matching node in the cluster.
