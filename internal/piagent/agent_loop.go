package piagent

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

type piLoopHarness struct {
	backend *Backend
}

func (h *piLoopHarness) Name() string { return h.backend.provider.Name() }

func (h *piLoopHarness) NewMessageID() string { return newMessageID() }

func (h *piLoopHarness) MaxSteps(session agent.Session, toolCount int) int {
	if session.Config.MaxSteps > 0 {
		return session.Config.MaxSteps
	}
	steps := toolCount + 1
	if steps < 8 {
		return 8
	}
	return steps
}

func (h *piLoopHarness) FormatMessages(session agent.Session, tools []agent.ToolDef) (CompletionRequest, error) {
	return CompletionRequest{
		Model: session.Config.CoordinatorModel, Temperature: session.Config.Temperature,
		Messages: h.backend.providerMessages(&session), Tools: tools,
	}, nil
}

func (h *piLoopHarness) CallModel(ctx context.Context, request CompletionRequest, onDelta func(string)) (*agent.ModelTurn, error) {
	completion, err := h.backend.provider.Complete(ctx, request, onDelta)
	if err != nil || completion == nil {
		return nil, err
	}
	assistant := agent.Message{
		Role: agent.RoleAssistant, Content: completion.Content, Model: request.Model,
		Coordinator: h.Name(), Timestamp: time.Now(),
	}
	if assistant.Model == "" {
		if provider, ok := h.backend.provider.(interface{ Model() string }); ok {
			assistant.Model = provider.Model()
		}
	}
	calls := make([]agent.ToolCall, 0, len(completion.ToolCalls))
	for _, call := range completion.ToolCalls {
		calls = append(calls, agent.ToolCall{
			ID: call.ID, Name: call.Function.Name, RawArgs: call.Function.Arguments,
		})
	}
	return &agent.ModelTurn{Assistant: assistant, ToolCalls: calls}, nil
}

func (h *piLoopHarness) ShouldStop(turn *agent.ModelTurn) bool {
	return turn == nil || len(turn.ToolCalls) == 0
}

func (h *piLoopHarness) InterruptedMessage(_ string, _ time.Duration) agent.Message {
	return agent.Message{
		ID: newMessageID(), Role: agent.RoleAssistant,
		Content: "⚠️ Session interrupted by user.", Coordinator: h.Name(), Timestamp: time.Now(),
	}
}

func (h *piLoopHarness) MaxStepsMessage(_ string) agent.Message {
	return agent.Message{
		ID: newMessageID(), Role: agent.RoleAssistant,
		Content:     "Reached the maximum number of agent steps without a final response.",
		Coordinator: h.Name(), Timestamp: time.Now(),
	}
}

type piConversation struct {
	state *piSession
}

func (c piConversation) SessionID() string { return c.state.session.ID }

func (c piConversation) BeginTurn(userText, messageID string, at time.Time) error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.session.Active {
		return errors.New("session is currently processing another request")
	}
	c.state.session.Active = true
	c.state.session.UpdatedAt = at
	c.state.session.Messages = append(c.state.session.Messages, agent.Message{
		ID: messageID, Role: agent.RoleUser, Content: userText, Timestamp: at,
	})
	return nil
}

func (c piConversation) Snapshot() agent.Session {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	return *cloneSession(c.state.session)
}

func (c piConversation) AppendMessage(message agent.Message, at time.Time) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.session.Messages = append(c.state.session.Messages, message)
	c.state.session.UpdatedAt = at
}

func (c piConversation) Finish(at time.Time) bool {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	wasActive := c.state.session.Active
	c.state.session.Active = false
	c.state.session.UpdatedAt = at
	return wasActive
}

func (c piConversation) RecordInterrupted(message agent.Message, at time.Time) *agent.Message {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	for i := len(c.state.session.Messages) - 1; i >= 0; i-- {
		stored := &c.state.session.Messages[i]
		if stored.Role == agent.RoleAssistant && strings.Contains(stored.Content, "interrupted") {
			copy := *stored
			return &copy
		}
	}
	c.state.session.Messages = append(c.state.session.Messages, message)
	c.state.session.UpdatedAt = at
	copy := message
	return &copy
}

func (b *Backend) newAgentLoop() *agent.AgentLoop[CompletionRequest] {
	harness := &piLoopHarness{backend: b}
	return agent.NewAgentLoop(
		harness,
		harness,
		func() []agent.ToolDef {
			if b.toolHost == nil {
				return nil
			}
			return b.toolHost.ListTools()
		},
		func(ctx context.Context, sessionID, name string, args map[string]any) (agent.ToolResult, error) {
			if b.toolHost == nil {
				return agent.ToolResult{}, errors.New("tool backend is not configured")
			}
			return b.toolHost.ExecuteTool(ctx, sessionID, name, args)
		},
	)
}
