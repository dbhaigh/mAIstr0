package deepseek

import (
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

// MessageRole represents the author of a message in a conversation.
type MessageRole = agent.MessageRole

const (
	RoleSystem    = agent.RoleSystem
	RoleUser      = agent.RoleUser
	RoleAssistant = agent.RoleAssistant
	RoleTool      = agent.RoleTool
)

// ToolCall represents a structured tool invocation requested by the agent.
type ToolCall = agent.ToolCall

// ToolResponse holds the execution outcome of a tool call.
type ToolResponse = agent.ToolResponse

// Message is a single entry in the interactive session history.
type Message = agent.Message

// SessionConfig tunes the agent harness behavior for a session.
type SessionConfig = agent.SessionConfig

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

	// mu protects mutable session fields. When both locks are needed,
	// toolMu is acquired first; session methods never acquire toolMu from mu.
	mu sync.RWMutex
	// toolMu serializes tool execution and snapshots against session mutation.
	toolMu sync.Mutex
}

type SessionSnapshot struct {
	ID        string            `json:"id"`
	Title     string            `json:"title"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Messages  []Message         `json:"messages"`
	Memory    map[string]string `json:"memory"`
	Config    SessionConfig     `json:"config"`
	Active    bool              `json:"active"`
}

func (s *Session) Snapshot() SessionSnapshot {
	s.toolMu.Lock()
	defer s.toolMu.Unlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := SessionSnapshot{
		ID: s.ID, Title: s.Title, CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
		Memory: make(map[string]string, len(s.Memory)), Config: s.Config, Active: s.Active,
		Messages: append([]Message(nil), s.Messages...),
	}
	for key, value := range s.Memory {
		snapshot.Memory[key] = value
	}
	return snapshot
}

// AgentStats summarizes interactive harness activity across the cluster.
type AgentStats = agent.AgentStats

// StreamEventType defines the stage of real-time agent execution.
type StreamEventType = agent.StreamEventType

const (
	EventStepStarted  = agent.EventStepStarted
	EventThought      = agent.EventThought
	EventNodeOutput   = agent.EventNodeOutput
	EventToolCall     = agent.EventToolCall
	EventToolResponse = agent.EventToolResponse
	EventFinalMessage = agent.EventFinalMessage
	EventError        = agent.EventError
	EventDone         = agent.EventDone
	EventTextDelta    = agent.EventTextDelta
	EventInterrupted  = agent.EventInterrupted
)

// StreamEvent is sent over SSE streams during interactive execution.
type StreamEvent = agent.StreamEvent
