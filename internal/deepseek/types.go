package deepseek

import (
	"sync"
	"time"
)

// MessageRole represents the author of a message in a conversation.
type MessageRole string

const (
	RoleSystem    MessageRole = "system"
	RoleUser      MessageRole = "user"
	RoleAssistant MessageRole = "assistant"
	RoleTool      MessageRole = "tool"
)

// ToolCall represents a structured tool invocation requested by the agent.
type ToolCall struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
	RawArgs   string         `json:"raw_args,omitempty"`
}

// ToolResponse holds the execution outcome of a tool call.
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

// Message is a single entry in the interactive session history.
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

// SessionConfig tunes the agent harness behavior for a session.
type SessionConfig struct {
	CoordinatorModel string  `json:"coordinator_model,omitempty"`
	MaxSteps         int     `json:"max_steps,omitempty"`
	Temperature      float64 `json:"temperature,omitempty"`
	SystemPrompt     string  `json:"system_prompt,omitempty"`
}

// Session represents an interactive multi-turn conversation with memory and history.
type Session struct {
	ID        string            `json:"id"`
	Title     string            `json:"title"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Messages  []Message         `json:"messages"`
	Memory    map[string]string `json:"memory"`
	Config    SessionConfig     `json:"config"`
	Active    bool              `json:"active"`

	mu sync.RWMutex
}

// AgentStats summarizes interactive harness activity across the cluster.
type AgentStats struct {
	TotalSessions  int            `json:"total_sessions"`
	ActiveSessions int            `json:"active_sessions"`
	TotalMessages  int            `json:"total_messages"`
	TotalToolCalls int            `json:"total_tool_calls"`
	DispatchedLoad map[string]int `json:"dispatched_load"`
}

// StreamEventType defines the stage of real-time agent execution.
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
)

// StreamEvent is sent over SSE streams during interactive execution.
type StreamEvent struct {
	Type        StreamEventType `json:"type"`
	SessionID   string          `json:"session_id"`
	Step        int             `json:"step,omitempty"`
	Message     *Message        `json:"message,omitempty"`
	ToolCall    *ToolCall       `json:"tool_call,omitempty"`
	ToolResp    *ToolResponse   `json:"tool_response,omitempty"`
	NodeID      string          `json:"node_id,omitempty"`
	NodeAddr    string          `json:"node_addr,omitempty"`
	Model       string          `json:"model,omitempty"`
	Output      string          `json:"output,omitempty"`
	Coordinator string          `json:"coordinator,omitempty"`
	Content     string          `json:"content,omitempty"`
	Error       string          `json:"error,omitempty"`
}
