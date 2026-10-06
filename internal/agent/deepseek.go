package agent

import (
	"context"
	"net/http"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/deepseek"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
)

type DeepSeek struct {
	harness *deepseek.Harness
}

func NewDeepSeek(registry *cluster.Registry, dispatcher *http.Client, store *memory.Store, events *hub.Hub) *DeepSeek {
	h := deepseek.New(registry, dispatcher)
	h.SetMemory(store)
	h.SetEventsHub(events)
	return &DeepSeek{harness: h}
}

func (d *DeepSeek) Name() string { return "deepseek" }

func (d *DeepSeek) CreateSession(title string, cfg SessionConfig) (*Session, error) {
	return fromDeepSeekSession(d.harness.CreateSession(title, deepseek.SessionConfig{
		CoordinatorModel: cfg.CoordinatorModel,
		MaxSteps:         cfg.MaxSteps,
		Temperature:      cfg.Temperature,
		SystemPrompt:     cfg.SystemPrompt,
	})), nil
}

func (d *DeepSeek) GetSession(id string) (*Session, bool) {
	s, ok := d.harness.GetSession(id)
	if !ok {
		return nil, false
	}
	return fromDeepSeekSession(s), true
}

func (d *DeepSeek) ListSessions() []*Session {
	sessions := d.harness.ListSessions()
	out := make([]*Session, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, fromDeepSeekSession(s))
	}
	return out
}

func (d *DeepSeek) DeleteSession(id string) bool { return d.harness.DeleteSession(id) }

func (d *DeepSeek) SendMessage(ctx context.Context, id, text string, events chan<- StreamEvent) (*Message, error) {
	var deepseekEvents chan deepseek.StreamEvent
	var relayDone chan struct{}
	if events != nil {
		deepseekEvents = make(chan deepseek.StreamEvent, cap(events))
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			for event := range deepseekEvents {
				converted := fromDeepSeekEvent(event)
				select {
				case events <- converted:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	msg, err := d.harness.SendMessage(ctx, id, text, deepseekEvents)
	if deepseekEvents != nil {
		close(deepseekEvents)
		<-relayDone
	}
	if err != nil {
		return nil, err
	}
	return fromDeepSeekMessage(msg), nil
}

func (d *DeepSeek) ListTools() []ToolDef {
	defs := d.harness.ListTools()
	out := make([]ToolDef, 0, len(defs))
	for _, def := range defs {
		out = append(out, ToolDef(def))
	}
	return out
}

func (d *DeepSeek) ListClusterModels() []ModelInfo {
	models := d.harness.ListClusterModels()
	out := make([]ModelInfo, 0, len(models))
	for _, model := range models {
		out = append(out, ModelInfo(model))
	}
	return out
}

func (d *DeepSeek) Stats() AgentStats { return AgentStats(d.harness.Stats()) }

func (d *DeepSeek) ExecuteTool(ctx context.Context, sessionID, name string, args map[string]any) (ToolResult, error) {
	result, err := d.harness.ExecuteTool(ctx, sessionID, name, args)
	if err != nil {
		return ToolResult{}, err
	}
	return ToolResult{
		Output: result.Output, NodeID: result.NodeID, NodeAddr: result.NodeAddr,
		Model: result.Model, DurationMs: result.DurationMs,
	}, nil
}

func (d *DeepSeek) Close() error { return nil }

func fromDeepSeekSession(s *deepseek.Session) *Session {
	if s == nil {
		return nil
	}
	source := s.Snapshot()
	out := &Session{
		ID: source.ID, Title: source.Title, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt,
		Memory: make(map[string]string, len(source.Memory)),
		Config: SessionConfig{
			CoordinatorModel: source.Config.CoordinatorModel, MaxSteps: source.Config.MaxSteps,
			Temperature: source.Config.Temperature, SystemPrompt: source.Config.SystemPrompt,
		},
		Active: source.Active,
	}
	out.Messages = make([]Message, 0, len(source.Messages))
	for _, msg := range source.Messages {
		out.Messages = append(out.Messages, *fromDeepSeekMessage(&msg))
	}
	for k, v := range source.Memory {
		out.Memory[k] = v
	}
	return out
}

func fromDeepSeekMessage(msg *deepseek.Message) *Message {
	if msg == nil {
		return nil
	}
	out := &Message{
		ID: msg.ID, Role: MessageRole(msg.Role), Content: msg.Content, RawOutput: msg.RawOutput,
		Thought: msg.Thought, NodeID: msg.NodeID, Model: msg.Model, Coordinator: msg.Coordinator,
		Timestamp: msg.Timestamp, DurationMs: msg.DurationMs,
	}
	for _, call := range msg.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, ToolCall(call))
	}
	if msg.ToolResponse != nil {
		resp := ToolResponse(*msg.ToolResponse)
		out.ToolResponse = &resp
	}
	return out
}

func fromDeepSeekEvent(event deepseek.StreamEvent) StreamEvent {
	out := StreamEvent{
		Type: StreamEventType(event.Type), SessionID: event.SessionID, Step: event.Step,
		NodeID: event.NodeID, NodeAddr: event.NodeAddr, Model: event.Model,
		Output: event.Output, Coordinator: event.Coordinator, Content: event.Content, Error: event.Error,
	}
	if event.Message != nil {
		out.Message = fromDeepSeekMessage(event.Message)
	}
	if event.ToolCall != nil {
		call := ToolCall(*event.ToolCall)
		out.ToolCall = &call
	}
	if event.ToolResp != nil {
		resp := ToolResponse(*event.ToolResp)
		out.ToolResp = &resp
	}
	return out
}
