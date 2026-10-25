package deepseekadapter

import (
	"context"
	"errors"
	"net/http"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/deepseek"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
)

type DeepSeek struct {
	harness *deepseek.Harness
}

func NewDeepSeek(registry *cluster.Registry, dispatcher *http.Client, store memory.Store, events *hub.Hub) *DeepSeek {
	h := deepseek.New(registry, dispatcher)
	h.SetMemory(store)
	h.SetEventsHub(events)
	return &DeepSeek{harness: h}
}

func (d *DeepSeek) Name() string { return "deepseek" }

// SetWorkflowExecutor connects the shared agent tools to the orchestrator's
// durable task engine.
func (d *DeepSeek) SetWorkflowExecutor(executor func(context.Context, string, []scheduler.Subtask, bool) (string, error)) {
	d.harness.SetWorkflowExecutor(executor)
}

func (d *DeepSeek) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	return fromDeepSeekSession(d.harness.CreateSession(title, deepseek.SessionConfig{
		CoordinatorModel: cfg.CoordinatorModel,
		MaxSteps:         cfg.MaxSteps,
		Temperature:      cfg.Temperature,
		SystemPrompt:     cfg.SystemPrompt,
	})), nil
}

func (d *DeepSeek) GetSession(id string) (*agent.Session, bool) {
	s, ok := d.harness.GetSession(id)
	if !ok {
		return nil, false
	}
	return fromDeepSeekSession(s), true
}

func (d *DeepSeek) ListSessions() []*agent.Session {
	sessions := d.harness.ListSessions()
	out := make([]*agent.Session, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, fromDeepSeekSession(s))
	}
	return out
}

func (d *DeepSeek) DeleteSession(id string) bool { return d.harness.DeleteSession(id) }

func (d *DeepSeek) InterruptSession(id string) bool { return d.harness.InterruptSession(id) }

func (d *DeepSeek) RestoreSession(session *agent.Session) error {
	if session == nil || session.ID == "" {
		return errors.New("agent session and ID are required")
	}
	return d.harness.RestoreSession(deepseek.ToSessionSnapshot(session))
}

func (d *DeepSeek) SendMessage(ctx context.Context, id, text string, events chan<- agent.StreamEvent) (*agent.Message, error) {
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

func (d *DeepSeek) ListTools() []agent.ToolDef {
	defs := d.harness.ListTools()
	out := make([]agent.ToolDef, 0, len(defs))
	for _, def := range defs {
		out = append(out, agent.ToolDef(def))
	}
	return out
}

func (d *DeepSeek) ListClusterModels() []agent.ModelInfo {
	models := d.harness.ListClusterModels()
	out := make([]agent.ModelInfo, 0, len(models))
	for _, model := range models {
		out = append(out, agent.ModelInfo(model))
	}
	return out
}

func (d *DeepSeek) Stats() agent.AgentStats { return agent.AgentStats(d.harness.Stats()) }

func (d *DeepSeek) ExecuteTool(ctx context.Context, sessionID, name string, args map[string]any) (agent.ToolResult, error) {
	result, err := d.harness.ExecuteTool(ctx, sessionID, name, args)
	if err != nil {
		return agent.ToolResult{}, err
	}
	return agent.ToolResult{
		Output: result.Output, NodeID: result.NodeID, NodeAddr: result.NodeAddr,
		Model: result.Model, DurationMs: result.DurationMs,
	}, nil
}

func (d *DeepSeek) Close() error { return nil }

func fromDeepSeekSession(s *deepseek.Session) *agent.Session {
	return deepseek.ToAgentSession(s)
}

func fromDeepSeekMessage(msg *deepseek.Message) *agent.Message {
	if msg == nil {
		return nil
	}
	out := deepseek.ToAgentMessage(*msg)
	return &out
}

func fromDeepSeekEvent(event deepseek.StreamEvent) agent.StreamEvent {
	return deepseek.ToAgentStreamEvent(event)
}
