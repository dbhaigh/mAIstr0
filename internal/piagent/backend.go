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
	loop     *agent.AgentLoop[CompletionRequest]

	// mu protects the session map; release it before taking a piSession.mu or
	// calling toolHost methods, so the locks are never nested.
	mu       sync.RWMutex
	sessions map[string]*piSession
}

type piSession struct {
	session *agent.Session
	// mu protects the session value; Backend.mu is released before acquiring it.
	mu sync.Mutex
}

func New(provider Provider, toolHost agent.Backend) *Backend {
	if provider == nil {
		provider = unavailableProvider{}
	}
	backend := &Backend{
		provider: provider,
		toolHost: toolHost,
		sessions: make(map[string]*piSession),
	}
	backend.loop = backend.newAgentLoop()
	return backend
}

type unavailableProvider struct{}

func (unavailableProvider) Name() string { return "unconfigured" }

func (unavailableProvider) Complete(context.Context, CompletionRequest, func(string)) (*Completion, error) {
	return nil, errors.New("Pi requires an explicitly configured local cluster provider")
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

func (b *Backend) RestoreSession(session *agent.Session) error {
	if session == nil || session.ID == "" {
		return errors.New("agent session and ID are required")
	}
	b.mu.RLock()
	_, exists := b.sessions[session.ID]
	b.mu.RUnlock()
	if exists {
		return fmt.Errorf("session %q already exists", session.ID)
	}
	if _, exists := b.toolHost.GetSession(session.ID); !exists {
		restorer, ok := b.toolHost.(agent.SessionRestorer)
		if !ok {
			return errors.New("tool backend does not support session restoration")
		}
		if err := restorer.RestoreSession(session); err != nil {
			return fmt.Errorf("restore tool session: %w", err)
		}
	}
	restored := cloneSession(session)
	restored.Active = false
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.sessions[restored.ID]; exists {
		return fmt.Errorf("session %q already exists", restored.ID)
	}
	b.sessions[restored.ID] = &piSession{session: restored}
	return nil
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
	b.InterruptSession(id)
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

func (b *Backend) InterruptSession(id string) bool {
	interrupted := b.loop.InterruptSession(id)
	if b.toolHost != nil {
		b.toolHost.InterruptSession(id)
	}
	return interrupted
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
	state, exists := b.sessions[sessionID]
	b.mu.RUnlock()
	if !exists {
		return nil, errors.New("session not found: " + sessionID)
	}
	return b.loop.Run(ctx, piConversation{state: state}, userText, streamChan)
}

func (b *Backend) providerMessages(session *agent.Session) []ChatMessage {
	messages := make([]ChatMessage, 0, len(session.Messages)+1)
	systemPrompt := session.Config.SystemPrompt
	if systemPrompt == "" {
		systemPrompt = "You are the mAIstr0 cluster orchestration agent. Use the available cluster tools to inspect nodes and answer direct questions. For multi-step work that needs dependencies, durable progress, validation, or review, use execute_workflow rather than coordinating worker calls manually. Do not claim a tool action succeeded unless its result confirms it."
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
