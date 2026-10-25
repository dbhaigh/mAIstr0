package deepseek

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
)

type deepSeekRequest struct {
	node   *cluster.NodeStatus
	model  string
	prompt string
}

type coordinatorSelectionError struct {
	err error
}

func (e coordinatorSelectionError) Error() string { return e.err.Error() }

func (e coordinatorSelectionError) Unwrap() error { return e.err }

func (e coordinatorSelectionError) StreamMessage() string {
	return "failed to select coordinator LLM: " + e.err.Error()
}

type deepSeekConversation struct {
	session *Session
}

func (c deepSeekConversation) SessionID() string { return c.session.ID }

func (c deepSeekConversation) BeginTurn(userText, messageID string, at time.Time) error {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	if c.session.Active {
		return errors.New("session is currently processing another request")
	}
	c.session.Active = true
	c.session.UpdatedAt = at
	c.session.Messages = append(c.session.Messages, Message{
		ID: messageID, Role: RoleUser, Content: userText, Timestamp: at,
	})
	return nil
}

func (c deepSeekConversation) Snapshot() agent.Session {
	return *ToAgentSession(c.session)
}

func (c deepSeekConversation) AppendMessage(message agent.Message, at time.Time) {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	c.session.Messages = append(c.session.Messages, FromAgentMessage(message))
	c.session.UpdatedAt = at
}

func (c deepSeekConversation) Finish(at time.Time) bool {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	wasActive := c.session.Active
	c.session.Active = false
	c.session.UpdatedAt = at
	return wasActive
}

func (c deepSeekConversation) RecordInterrupted(message agent.Message, at time.Time) *agent.Message {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()
	for i := len(c.session.Messages) - 1; i >= 0; i-- {
		stored := &c.session.Messages[i]
		if stored.Role == RoleAssistant && strings.Contains(stored.Content, "interrupted") {
			copy := ToAgentMessage(*stored)
			return &copy
		}
	}
	c.session.Messages = append(c.session.Messages, FromAgentMessage(message))
	c.session.UpdatedAt = at
	copy := message
	return &copy
}

func (h *Harness) Name() string { return "orchestrator" }

func (h *Harness) NewMessageID() string { return generateID("msg_") }

func (h *Harness) MaxSteps(session agent.Session, _ int) int {
	if session.Config.MaxSteps > 0 {
		return session.Config.MaxSteps
	}
	return 8
}

func (h *Harness) FormatMessages(session agent.Session, tools []agent.ToolDef) (deepSeekRequest, error) {
	current := sessionFromAgent(session)
	node, model, err := h.selectCoordinator(current)
	if err != nil {
		return deepSeekRequest{}, coordinatorSelectionError{err: err}
	}
	deepSeekTools := make([]ToolDef, 0, len(tools))
	for _, tool := range tools {
		deepSeekTools = append(deepSeekTools, ToolDef(tool))
	}
	return deepSeekRequest{
		node: node, model: model,
		prompt: h.buildPromptWithTools(current, deepSeekTools),
	}, nil
}

func (h *Harness) CallModel(ctx context.Context, request deepSeekRequest, _ func(string)) (*agent.ModelTurn, error) {
	started := time.Now()
	h.registry.IncrementLoad(request.node.ID, 1)
	rawOutput, err := h.dispatchToNode(ctx, request.node.Address, request.model, request.prompt)
	h.registry.IncrementLoad(request.node.ID, -1)
	duration := time.Since(started).Milliseconds()
	if err != nil {
		return nil, fmt.Errorf("coordinator error from %s (%s): %w", request.node.ID, request.model, err)
	}

	parsed := ParseResponse(rawOutput)
	turn := &agent.ModelTurn{
		Assistant: agent.Message{
			Role: agent.RoleAssistant, Content: parsed.Content, RawOutput: rawOutput,
			Thought: parsed.Thought, NodeID: request.node.ID, Model: request.model,
			Coordinator: "orchestrator", Timestamp: time.Now(), DurationMs: duration,
		},
	}
	for _, call := range parsed.ToolCalls {
		turn.ToolCalls = append(turn.ToolCalls, agent.ToolCall{
			ID: call.ID, Name: call.Name, Arguments: call.Arguments, RawArgs: call.RawArgs,
		})
	}
	turn.Events = append(turn.Events, agent.StreamEvent{
		Type: agent.EventNodeOutput, NodeID: request.node.ID, NodeAddr: request.node.Address,
		Model: request.model, Output: rawOutput, Coordinator: "orchestrator",
	})
	if parsed.Thought != "" {
		turn.Events = append(turn.Events, agent.StreamEvent{Type: agent.EventThought, Content: parsed.Thought})
	}
	return turn, nil
}

func (h *Harness) ShouldStop(turn *agent.ModelTurn) bool {
	return turn == nil || len(turn.ToolCalls) == 0
}

func (h *Harness) InterruptedMessage(_ string, duration time.Duration) agent.Message {
	return agent.Message{
		ID: generateID("msg_"), Role: agent.RoleAssistant,
		Content: "⚠️ Session interrupted by user.", Coordinator: "orchestrator",
		Timestamp: time.Now(), DurationMs: duration.Milliseconds(),
	}
}

func (h *Harness) MaxStepsMessage(_ string) agent.Message {
	return agent.Message{
		ID: generateID("msg_"), Role: agent.RoleAssistant,
		Content:   "Reached maximum reasoning steps without a final conclusion. You may ask me to continue or narrow the task.",
		Timestamp: time.Now(),
	}
}

func (h *Harness) TurnCompleted(sessionID, userText string, final agent.Message, duration time.Duration) {
	h.remember(memory.Experience{
		Kind: "agent_session", Description: userText, Output: final.Content,
		NodeID: final.NodeID, Model: final.Model, SessionID: sessionID,
		Success: final.Content != "", DurationMs: duration.Milliseconds(),
		Tags: []string{"agent", "session"},
	})
}

func (h *Harness) executeLoopTool(ctx context.Context, sessionID, name string, args map[string]any) (agent.ToolResult, error) {
	response, err := h.ExecuteTool(ctx, sessionID, name, args)
	if response == nil {
		return agent.ToolResult{}, err
	}
	return agent.ToolResult{
		Output: response.Output, NodeID: response.NodeID, NodeAddr: response.NodeAddr,
		Model: response.Model, DurationMs: response.DurationMs,
	}, err
}

func (h *Harness) newAgentLoop() *agent.AgentLoop[deepSeekRequest] {
	listTools := func() []agent.ToolDef {
		definitions := h.ListTools()
		out := make([]agent.ToolDef, 0, len(definitions))
		for _, definition := range definitions {
			out = append(out, agent.ToolDef(definition))
		}
		return out
	}
	return agent.NewAgentLoop(h, h, listTools, h.executeLoopTool)
}

func sessionFromAgent(session agent.Session) *Session {
	out := &Session{
		ID: session.ID, Title: session.Title, CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt,
		Memory: make(map[string]string, len(session.Memory)), Config: SessionConfig(session.Config),
		Active: session.Active, Messages: make([]Message, 0, len(session.Messages)),
	}
	for key, value := range session.Memory {
		out.Memory[key] = value
	}
	for _, message := range session.Messages {
		out.Messages = append(out.Messages, FromAgentMessage(message))
	}
	return out
}
