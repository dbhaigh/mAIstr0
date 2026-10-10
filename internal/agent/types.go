package agent

import (
	"context"
	"time"
)

type MessageRole string

const (
	RoleSystem    MessageRole = "system"
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleTool      MessageRole = "tool"
)

type ToolCall struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
	RawArgs   string         `json:"raw_args,omitempty"`
}

type ToolResponse struct {
	ToolCallID string `json:"tool_call_id"`
	Name       string `json:"name"`
	Output     any    `json:"output"`
	Error      string `json:"error,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	NodeAddr   string `json:"node_addr,omitempty"`
	Model      string `json:"model,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`
}

type Message struct {
	ID           string        `json:"id"`
	Role         MessageRole   `json:"role"`
	Content      string        `json:"content"`
	RawOutput    string        `json:"raw_output,omitempty"`
	Thought      string        `json:"thought,omitempty"`
	ToolCalls    []ToolCall    `json:"tool_calls,omitempty"`
	ToolResponse *ToolResponse `json:"tool_response,omitempty"`
	NodeID       string        `json:"node_id,omitempty"`
	Model        string        `json:"model,omitempty"`
	Coordinator  string        `json:"coordinator,omitempty"`
	Timestamp    time.Time     `json:"timestamp"`
	DurationMs   int64         `json:"duration_ms,omitempty"`
}

type SessionConfig struct {
	CoordinatorModel string  `json:"coordinator_model,omitempty"`
	MaxSteps         int     `json:"max_steps,omitempty"`
	Temperature      float64 `json:"temperature,omitempty"`
	SystemPrompt     string  `json:"system_prompt,omitempty"`
}

type Session struct {
	ID        string            `json:"id"`
	Title     string            `json:"title"`
	Harness   string            `json:"harness,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Messages  []Message         `json:"messages"`
	Memory    map[string]string `json:"memory"`
	Config    SessionConfig     `json:"config"`
	Active    bool              `json:"active"`
}

type ToolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

type ModelInfo struct {
	NodeID   string   `json:"node_id"`
	NodeAddr string   `json:"node_address"`
	Name     string   `json:"name"`
	Engine   string   `json:"engine"`
	Tags     []string `json:"tags"`
	SizeGB   float64  `json:"size_gb,omitempty"`
	Default  bool     `json:"default"`
	Healthy  bool     `json:"healthy"`
}

type AgentStats struct {
	TotalSessions  int            `json:"total_sessions"`
	ActiveSessions int            `json:"active_sessions"`
	TotalMessages  int            `json:"total_messages"`
	TotalToolCalls int            `json:"total_tool_calls"`
	DispatchedLoad map[string]int `json:"dispatched_load"`
}

type StreamEventType string

const (
	EventStepStarted  StreamEventType = "step_started"
	EventThought      StreamEventType = "thought"
	EventNodeOutput   StreamEventType = "node_output"
	EventToolCall     StreamEventType = "tool_call"
	EventToolResponse StreamEventType = "tool_response"
	EventFinalMessage StreamEventType = "final_message"
	EventError        StreamEventType = "error"
	EventDone         StreamEventType = "done"
	EventTextDelta    StreamEventType = "text_delta"
)

type StreamEvent struct {
	Type        StreamEventType `json:"type"`
	SessionID   string          `json:"session_id"`
	Step        int             `json:"step,omitempty"`
	Message     *Message        `json:"message,omitempty"`
	ToolCall    *ToolCall       `json:"tool_call,omitempty"`
	ToolResp    *ToolResponse   `json:"tool_resp,omitempty"`
	NodeID      string          `json:"node_id,omitempty"`
	NodeAddr    string          `json:"node_addr,omitempty"`
	Model       string          `json:"model,omitempty"`
	Output      string          `json:"output,omitempty"`
	Coordinator string          `json:"coordinator,omitempty"`
	Content     string          `json:"content,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type ToolResult struct {
	Output     any
	NodeID     string
	NodeAddr   string
	Model      string
	DurationMs int64
}

type Backend interface {
	Name() string
	CreateSession(title string, cfg SessionConfig) (*Session, error)
	GetSession(id string) (*Session, bool)
	ListSessions() []*Session
	DeleteSession(id string) bool
	SendMessage(ctx context.Context, sessionID, userText string, streamChan chan<- StreamEvent) (*Message, error)
	ListTools() []ToolDef
	ListClusterModels() []ModelInfo
	Stats() AgentStats
	ExecuteTool(ctx context.Context, sessionID, name string, args map[string]any) (ToolResult, error)
	Close() error
}

type SessionRestorer interface {
	RestoreSession(session *Session) error
}
