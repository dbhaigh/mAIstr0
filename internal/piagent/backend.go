package piagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

type Backend struct {
	provider Provider
	toolHost agent.Backend

	mu       sync.RWMutex
	sessions map[string]*piSession
}

type piSession struct {
	session *agent.Session
	mu      sync.Mutex
}

func New(provider Provider, toolHost agent.Backend) *Backend {
	if provider == nil {
		provider = NewOpenAICompatibleProvider(OpenAICompatibleConfig{})
	}
	return &Backend{
		provider: provider,
		toolHost: toolHost,
		sessions: make(map[string]*piSession),
	}
}

func (b *Backend) Name() string { return "pi" }

func (b *Backend) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	if title == "" {
		title = "Pi Session " + time.Now().Format("Jan 02 15:04")
	}
	toolSession, err := b.toolHost.CreateSession(title, cfg)
	if err != nil {
		return nil, err
	}
	state := &piSession{session: &agent.Session{
		ID: toolSession.ID, Title: title, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		Messages: []agent.Message{}, Memory: map[string]string{}, Config: cfg,
	}}
	b.mu.Lock()
	b.sessions[state.session.ID] = state
	b.mu.Unlock()
	return cloneSession(state.session), nil
}

func (b *Backend) GetSession(id string) (*agent.Session, bool) {
	b.mu.RLock()
	state, ok := b.sessions[id]
	b.mu.RUnlock()
	if !ok {
		return nil, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return cloneSession(state.session), true
}

func (b *Backend) ListSessions() []*agent.Session {
	b.mu.RLock()
	states := make([]*piSession, 0, len(b.sessions))
	for _, state := range b.sessions {
		states = append(states, state)
	}
	b.mu.RUnlock()
	out := make([]*agent.Session, 0, len(states))
	for _, state := range states {
		state.mu.Lock()
		out = append(out, cloneSession(state.session))
		state.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out
}

func (b *Backend) DeleteSession(id string) bool {
	b.mu.Lock()
	_, ok := b.sessions[id]
	if ok {
		delete(b.sessions, id)
	}
	b.mu.Unlock()
	if !ok {
		return false
	}
	b.toolHost.DeleteSession(id)
	return true
}

func (b *Backend) ListTools() []agent.ToolDef { return b.toolHost.ListTools() }

func (b *Backend) ListClusterModels() []agent.ModelInfo { return b.toolHost.ListClusterModels() }

func (b *Backend) Stats() agent.AgentStats {
	b.mu.RLock()
	states := make([]*piSession, 0, len(b.sessions))
	for _, state := range b.sessions {
		states = append(states, state)
	}
	b.mu.RUnlock()
	hostStats := b.toolHost.Stats()
	stats := agent.AgentStats{TotalSessions: len(states), DispatchedLoad: hostStats.DispatchedLoad}
	for _, state := range states {
		state.mu.Lock()
		if state.session.Active {
			stats.ActiveSessions++
		}
		stats.TotalMessages += len(state.session.Messages)
		for _, msg := range state.session.Messages {
			stats.TotalToolCalls += len(msg.ToolCalls)
		}
		state.mu.Unlock()
	}
	return stats
}

func (b *Backend) ExecuteTool(ctx context.Context, sessionID, name string, args map[string]any) (agent.ToolResult, error) {
	return b.toolHost.ExecuteTool(ctx, sessionID, name, args)
}

func (b *Backend) SendMessage(ctx context.Context, sessionID, userText string, streamChan chan<- agent.StreamEvent) (*agent.Message, error) {
	b.mu.RLock()
	state, ok := b.sessions[sessionID]
	b.mu.RUnlock()
	if !ok {
		return nil, errors.New("session not found: " + sessionID)
	}
	state.mu.Lock()
	if state.session.Active {
		state.mu.Unlock()
		return nil, errors.New("session is currently processing another request")
	}
	state.session.Active = true
	state.session.UpdatedAt = time.Now()
	state.session.Messages = append(state.session.Messages, agent.Message{
		ID: newMessageID(), Role: agent.RoleUser, Content: userText, Timestamp: time.Now(),
	})
	state.mu.Unlock()
	defer func() {
		state.mu.Lock()
		state.session.Active = false
		state.session.UpdatedAt = time.Now()
		state.mu.Unlock()
	}()

	emit := func(event agent.StreamEvent) {
		event.SessionID = sessionID
		if streamChan == nil {
			return
		}
		select {
		case streamChan <- event:
		case <-ctx.Done():
		}
	}
	maxSteps := state.session.Config.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 8
	}
	tools := b.toolHost.ListTools()
	var final *agent.Message
	for step := 1; step <= maxSteps; step++ {
		if err := ctx.Err(); err != nil {
			emit(agent.StreamEvent{Type: agent.EventError, Error: "request context cancelled"})
			return nil, err
		}
		emit(agent.StreamEvent{Type: agent.EventStepStarted, Step: step})
		state.mu.Lock()
		messages := b.providerMessages(state.session)
		sessionConfig := state.session.Config
		state.mu.Unlock()
		completion, err := b.provider.Complete(ctx, CompletionRequest{
			Model: sessionConfig.CoordinatorModel, Temperature: sessionConfig.Temperature,
			Messages: messages, Tools: tools,
		}, func(delta string) {
			emit(agent.StreamEvent{Type: agent.EventTextDelta, Step: step, Content: delta})
		})
		if err != nil {
			emit(agent.StreamEvent{Type: agent.EventError, Error: err.Error()})
			return nil, err
		}
		if completion == nil {
			err = errors.New("model provider returned an empty completion")
			emit(agent.StreamEvent{Type: agent.EventError, Error: err.Error()})
			return nil, err
		}

		assistant := &agent.Message{
			ID: newMessageID(), Role: agent.RoleAssistant, Content: completion.Content,
			Model: sessionConfig.CoordinatorModel, Coordinator: b.provider.Name(), Timestamp: time.Now(),
		}
		if assistant.Model == "" {
			if provider, ok := b.provider.(interface{ Model() string }); ok {
				assistant.Model = provider.Model()
			}
		}
		for _, call := range completion.ToolCalls {
			args, _ := parseToolArguments(call.Function.Arguments)
			callID := call.ID
			if callID == "" {
				callID = newMessageID()
			}
			assistant.ToolCalls = append(assistant.ToolCalls, agent.ToolCall{
				ID: callID, Name: call.Function.Name, Arguments: args, RawArgs: call.Function.Arguments,
			})
		}
		state.mu.Lock()
		state.session.Messages = append(state.session.Messages, *assistant)
		state.session.UpdatedAt = time.Now()
		state.mu.Unlock()

		if len(completion.ToolCalls) == 0 {
			final = assistant
			emit(agent.StreamEvent{Type: agent.EventFinalMessage, Step: step, Message: final, Content: final.Content})
			break
		}

		for i, call := range completion.ToolCalls {
			parsedArgs, parseErr := parseToolArguments(call.Function.Arguments)
			toolCall := assistant.ToolCalls[i]
			emit(agent.StreamEvent{Type: agent.EventToolCall, Step: step, ToolCall: &toolCall})
			start := time.Now()
			response := agent.ToolResponse{ToolCallID: toolCall.ID, Name: call.Function.Name}
			if parseErr != nil {
				response.Error = fmt.Sprintf("invalid tool arguments: %v", parseErr)
			} else {
				result, execErr := b.toolHost.ExecuteTool(ctx, sessionID, call.Function.Name, parsedArgs)
				response.Output = result.Output
				response.NodeID, response.NodeAddr, response.Model = result.NodeID, result.NodeAddr, result.Model
				response.DurationMs = result.DurationMs
				if execErr != nil {
					response.Error = execErr.Error()
				}
			}
			if response.DurationMs == 0 {
				response.DurationMs = time.Since(start).Milliseconds()
			}
			state.mu.Lock()
			state.session.Messages = append(state.session.Messages, agent.Message{
				ID: newMessageID(), Role: agent.RoleTool, ToolResponse: &response,
				NodeID: response.NodeID, Model: response.Model, Timestamp: time.Now(),
				DurationMs: response.DurationMs,
			})
			state.session.UpdatedAt = time.Now()
			state.mu.Unlock()
			emit(agent.StreamEvent{Type: agent.EventToolResponse, Step: step, ToolResp: &response})
		}
	}
	if final == nil {
		final = &agent.Message{
			ID: newMessageID(), Role: agent.RoleAssistant,
			Content:     "Reached the maximum number of agent steps without a final response.",
			Coordinator: b.provider.Name(), Timestamp: time.Now(),
		}
		state.mu.Lock()
		state.session.Messages = append(state.session.Messages, *final)
		state.mu.Unlock()
		emit(agent.StreamEvent{Type: agent.EventFinalMessage, Message: final, Content: final.Content})
	}
	emit(agent.StreamEvent{Type: agent.EventDone})
	return final, nil
}

func (b *Backend) providerMessages(session *agent.Session) []ChatMessage {
	messages := make([]ChatMessage, 0, len(session.Messages)+1)
	systemPrompt := session.Config.SystemPrompt
	if systemPrompt == "" {
		systemPrompt = "You are the mAIstr0 cluster orchestration agent. Use the available cluster tools to inspect nodes, select models, and coordinate work. Do not claim a tool action succeeded unless its result confirms it."
	}
	messages = append(messages, ChatMessage{Role: "system", Content: systemPrompt})
	for _, message := range session.Messages {
		switch message.Role {
		case agent.RoleUser:
			messages = append(messages, ChatMessage{Role: "user", Content: message.Content})
		case agent.RoleAssistant:
			apiMessage := ChatMessage{Role: "assistant", Content: message.Content}
			for _, call := range message.ToolCalls {
				apiMessage.ToolCalls = append(apiMessage.ToolCalls, ChatToolCall{
					ID: call.ID, Type: "function", Function: ChatToolFunction{
						Name: call.Name, Arguments: call.RawArgs,
					},
				})
			}
			messages = append(messages, apiMessage)
		case agent.RoleTool:
			if message.ToolResponse == nil {
				continue
			}
			output, err := json.Marshal(message.ToolResponse.Output)
			if err != nil {
				output = []byte(fmt.Sprint(message.ToolResponse.Output))
			}
			content := string(output)
			if message.ToolResponse.Error != "" {
				content = "Tool error: " + message.ToolResponse.Error + "\n" + content
			}
			messages = append(messages, ChatMessage{
				Role: "tool", ToolCallID: message.ToolResponse.ToolCallID, Content: content,
			})
		}
	}
	return messages
}

func parseToolArguments(raw string) (map[string]any, error) {
	args := make(map[string]any)
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, err
	}
	if args == nil {
		return nil, errors.New("tool arguments must be a JSON object")
	}
	return args, nil
}

func (b *Backend) Close() error {
	b.mu.Lock()
	states := make([]*piSession, 0, len(b.sessions))
	for id, state := range b.sessions {
		states = append(states, state)
		delete(b.sessions, id)
	}
	b.mu.Unlock()
	for _, state := range states {
		b.toolHost.DeleteSession(state.session.ID)
	}
	return nil
}

func cloneSession(s *agent.Session) *agent.Session {
	out := *s
	out.Messages = append([]agent.Message(nil), s.Messages...)
	out.Memory = make(map[string]string, len(s.Memory))
	for key, value := range s.Memory {
		out.Memory[key] = value
	}
	return &out
}

func newMessageID() string {
	return fmt.Sprintf("msg_%d", time.Now().UnixNano())
}

var _ agent.Backend = (*Backend)(nil)
