package deepseek

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"net/http"
	"sort"
	"sync"
	"time"
)

type Harness struct {
	registry   *cluster.Registry
	dispatcher *http.Client
	tools      *ToolRegistry
	loop       *agent.AgentLoop[deepSeekRequest]

	sessions map[string]*Session
	// sessionsMu protects the session map. Stats may nest session.mu beneath
	// this lock; session code must not acquire sessionsMu while holding session.mu.
	sessionsMu sync.RWMutex

	events           *hub.Hub
	mem              memory.Store
	style            string
	workflowExecutor func(context.Context, string, []scheduler.Subtask, bool) (string, error)
}

// SetWorkflowExecutor installs the shared durable workflow execution path.
func (h *Harness) SetWorkflowExecutor(executor func(context.Context, string, []scheduler.Subtask, bool) (string, error)) {
	h.workflowExecutor = executor
}

func New(registry *cluster.Registry, dispatcher *http.Client) *Harness {
	return newWithStyle(registry, dispatcher, "deepseek")
}

func newWithStyle(registry *cluster.Registry, dispatcher *http.Client, style string) *Harness {
	if dispatcher == nil {
		dispatcher = httpx.NewClient(httpx.ClientOptions{Timeout: httpx.InferenceTimeout})
	}
	h := &Harness{
		registry:   registry,
		dispatcher: dispatcher,
		tools:      NewToolRegistry(),
		sessions:   make(map[string]*Session),
		events:     hub.New(),
		style:      style,
	}

	for _, registration := range defaultToolRegistrations() {
		h.tools.register(registration.tool, registration.argumentType)
	}
	h.loop = h.newAgentLoop()
	return h
}

// RegisterTool adds a tool to the harness catalog.
func (h *Harness) RegisterTool(t Tool) {
	h.tools.Register(t)
}

// SetEventsHub attaches an SSE hub to broadcast agent events cluster-wide.
func (h *Harness) SetEventsHub(events *hub.Hub) {
	h.events = events
}

// SetMemory attaches the persistent cluster memory the harness recalls from
// and learns into. Without it the harness still works, just statelessly.
func (h *Harness) SetMemory(store memory.Store) {
	h.mem = store
	if store != nil {
		registerTypedTool[recallMemoryArgs](h.tools, &RecallMemoryTool{})
		registerTypedTool[rememberFactArgs](h.tools, &RememberFactTool{})
		registerTypedTool[memoryInsightsArgs](h.tools, &MemoryInsightsTool{})
		registerTypedTool[nodeToNodeConversationArgs](h.tools, &NodeToNodeConversationTool{})
	}
}

// Memory returns the attached memory store (may be nil).
func (h *Harness) Memory() memory.Store { return h.mem }

// remember persists one agent action so future sessions can recall it and
// the scheduler can learn which node/model pairings actually work.
func (h *Harness) remember(e memory.Experience) {
	if h.mem == nil {
		return
	}
	if _, err := h.mem.Record(e); err != nil {
		logging.Errorf("deepseek", "deepseek: memory write failed: %v", err)
	}
}

// ListTools returns definitions for all available tools in the harness.
func (h *Harness) ListTools() []ToolDef {
	return h.tools.List()
}

// ExecuteTool runs a registered tool outside the harness-owned agent loop.
// It is used by other agent backends that share mAIstr0's cluster tools.
func (h *Harness) ExecuteTool(ctx context.Context, sessionID, name string, args map[string]any) (*ToolResponse, error) {
	s, ok := h.GetSession(sessionID)
	if !ok {
		return nil, errors.New("session not found: " + sessionID)
	}
	s.toolMu.Lock()
	defer s.toolMu.Unlock()
	tool, ok := h.tools.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
	start := time.Now()
	output, nodeID, nodeAddr, model, err := tool.Execute(ctx, h, s, args)
	response := &ToolResponse{
		ToolCallID: generateID("call_"), Name: name, Output: output,
		NodeID: nodeID, NodeAddr: nodeAddr, Model: model,
		DurationMs: time.Since(start).Milliseconds(),
	}
	if err != nil {
		response.Error = err.Error()
	}
	argsJSON, _ := json.Marshal(args)
	call := ToolCall{Name: name, Arguments: args, RawArgs: string(argsJSON)}
	h.remember(memory.Experience{
		Kind: "tool_call", TaskType: toolTaskType(call), Description: describeToolCall(call), Prompt: string(argsJSON),
		Output: renderToolOutput(response.Output), NodeID: nodeID, Model: model,
		Tool: name, SessionID: sessionID, Success: err == nil, DurationMs: response.DurationMs,
		Error: response.Error, Tags: []string{"agent", name},
	})
	return response, err
}

// ModelInfo describes an LLM model available on a specific cluster node.
type ModelInfo struct {
	NodeID        string   `json:"node_id"`
	NodeAddr      string   `json:"node_address"`
	Name          string   `json:"name"`
	Engine        string   `json:"engine"`
	Tags          []string `json:"tags"`
	SizeGB        float64  `json:"size_gb,omitempty"`
	ContextLength int      `json:"context_length,omitempty"`
	Members       []string `json:"members,omitempty"`
	Default       bool     `json:"default"`
	Healthy       bool     `json:"healthy"`
}

// ListClusterModels returns every available model across all healthy nodes.
func (h *Harness) ListClusterModels() []ModelInfo {
	nodes := h.registry.Active()
	var models []ModelInfo
	for _, n := range nodes {
		for _, m := range n.Models {
			models = append(models, ModelInfo{
				NodeID:        n.ID,
				NodeAddr:      n.Address,
				Name:          m.Name,
				Engine:        m.Engine,
				Tags:          m.Tags,
				SizeGB:        m.SizeGB,
				ContextLength: m.ContextLength,
				Default:       m.Name == n.DefaultModel,
				Healthy:       n.Healthy,
			})
		}
	}
	return models
}

// CreateSession initializes a new interactive conversation session.
func (h *Harness) CreateSession(title string, cfg SessionConfig) *Session {
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()

	id := generateID("ses_")
	if title == "" {
		title = "Interactive Session " + time.Now().Format("Jan 02 15:04")
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 8
	}

	s := &Session{
		ID:        id,
		Title:     title,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Messages:  make([]Message, 0),
		Memory:    make(map[string]string),
		Config:    cfg,
		Active:    false,
	}
	h.sessions[id] = s
	return s
}

// RestoreSession adds a saved session to the harness without replacing an
// existing session with the same ID.
func (h *Harness) RestoreSession(snapshot SessionSnapshot) error {
	if snapshot.ID == "" {
		return errors.New("session ID is required")
	}
	if snapshot.Config.MaxSteps <= 0 {
		snapshot.Config.MaxSteps = 8
	}
	if snapshot.Memory == nil {
		snapshot.Memory = make(map[string]string)
	}
	if snapshot.Messages == nil {
		snapshot.Messages = make([]Message, 0)
	}
	snapshot.Messages = append([]Message(nil), snapshot.Messages...)
	memory := make(map[string]string, len(snapshot.Memory))
	for key, value := range snapshot.Memory {
		memory[key] = value
	}
	session := &Session{
		ID: snapshot.ID, Title: snapshot.Title, CreatedAt: snapshot.CreatedAt,
		UpdatedAt: snapshot.UpdatedAt, Messages: snapshot.Messages, Memory: memory,
		Config: snapshot.Config, Active: false,
	}

	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	if _, exists := h.sessions[session.ID]; exists {
		return fmt.Errorf("session %q already exists", session.ID)
	}
	h.sessions[session.ID] = session
	return nil
}

// GetSession fetches a session by its unique ID.
func (h *Harness) GetSession(id string) (*Session, bool) {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	s, ok := h.sessions[id]
	return s, ok
}

// ListSessions returns all active and past sessions ordered by last update.
func (h *Harness) ListSessions() []*Session {
	h.sessionsMu.RLock()
	defer h.sessionsMu.RUnlock()
	list := make([]*Session, 0, len(h.sessions))
	for _, s := range h.sessions {
		list = append(list, s)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].UpdatedAt.After(list[j].UpdatedAt)
	})
	return list
}

// DeleteSession removes a session from memory.
func (h *Harness) DeleteSession(id string) bool {
	h.InterruptSession(id)
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	if _, ok := h.sessions[id]; ok {
		delete(h.sessions, id)
		return true
	}
	return false
}

// InterruptSession aborts in-flight reasoning and tools for a session.
func (h *Harness) InterruptSession(sessionID string) bool {
	return h.loop.InterruptSession(sessionID)
}

// Stats returns runtime statistics for the interactive agent harness.
func (h *Harness) Stats() AgentStats {
	h.sessionsMu.RLock()
	totalSessions := len(h.sessions)
	activeSessions := 0
	totalMessages := 0
	totalToolCalls := 0
	for _, s := range h.sessions {
		s.mu.RLock()
		if s.Active {
			activeSessions++
		}
		totalMessages += len(s.Messages)
		for _, m := range s.Messages {
			totalToolCalls += len(m.ToolCalls)
		}
		s.mu.RUnlock()
	}
	h.sessionsMu.RUnlock()

	nodes := h.registry.Active()
	loadMap := make(map[string]int, len(nodes))
	for _, n := range nodes {
		loadMap[n.ID] = n.ActiveTasks
	}

	return AgentStats{
		TotalSessions:  totalSessions,
		ActiveSessions: activeSessions,
		TotalMessages:  totalMessages,
		TotalToolCalls: totalToolCalls,
		DispatchedLoad: loadMap,
	}
}

// SendMessage runs the full interactive DeepSeek agent loop for an incoming user message.
