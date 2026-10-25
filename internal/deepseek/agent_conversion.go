package deepseek

import "github.com/maistr0/maistr0/internal/agent"

func ToAgentSession(session *Session) *agent.Session {
	if session == nil {
		return nil
	}
	source := session.Snapshot()
	out := &agent.Session{
		ID: source.ID, Title: source.Title, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt,
		Config: agent.SessionConfig(source.Config), Active: source.Active,
		Memory:   make(map[string]string, len(source.Memory)),
		Messages: make([]agent.Message, 0, len(source.Messages)),
	}
	for key, value := range source.Memory {
		out.Memory[key] = value
	}
	for _, message := range source.Messages {
		out.Messages = append(out.Messages, ToAgentMessage(message))
	}
	return out
}

func ToSessionSnapshot(session *agent.Session) SessionSnapshot {
	if session == nil {
		return SessionSnapshot{}
	}
	out := SessionSnapshot{
		ID: session.ID, Title: session.Title, CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt,
		Config: SessionConfig(session.Config), Active: false,
		Memory:   make(map[string]string, len(session.Memory)),
		Messages: make([]Message, 0, len(session.Messages)),
	}
	for key, value := range session.Memory {
		out.Memory[key] = value
	}
	for _, message := range session.Messages {
		out.Messages = append(out.Messages, FromAgentMessage(message))
	}
	return out
}

func ToAgentMessage(message Message) agent.Message {
	out := agent.Message{
		ID: message.ID, Role: agent.MessageRole(message.Role), Content: message.Content,
		RawOutput: message.RawOutput, Thought: message.Thought, NodeID: message.NodeID,
		Model: message.Model, Coordinator: message.Coordinator,
		Timestamp: message.Timestamp, DurationMs: message.DurationMs,
	}
	for _, call := range message.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, agent.ToolCall(call))
	}
	if message.ToolResponse != nil {
		response := agent.ToolResponse(*message.ToolResponse)
		out.ToolResponse = &response
	}
	return out
}

func FromAgentMessage(message agent.Message) Message {
	out := Message{
		ID: message.ID, Role: MessageRole(message.Role), Content: message.Content,
		RawOutput: message.RawOutput, Thought: message.Thought, NodeID: message.NodeID,
		Model: message.Model, Coordinator: message.Coordinator,
		Timestamp: message.Timestamp, DurationMs: message.DurationMs,
	}
	for _, call := range message.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, ToolCall(call))
	}
	if message.ToolResponse != nil {
		response := ToolResponse(*message.ToolResponse)
		out.ToolResponse = &response
	}
	return out
}

func ToAgentStreamEvent(event StreamEvent) agent.StreamEvent {
	out := agent.StreamEvent{
		Type: agent.StreamEventType(event.Type), SessionID: event.SessionID, Step: event.Step,
		NodeID: event.NodeID, NodeAddr: event.NodeAddr, Model: event.Model,
		Output: event.Output, Coordinator: event.Coordinator, Content: event.Content, Error: event.Error,
	}
	if event.Message != nil {
		message := ToAgentMessage(*event.Message)
		out.Message = &message
	}
	if event.ToolCall != nil {
		call := agent.ToolCall(*event.ToolCall)
		out.ToolCall = &call
	}
	if event.ToolResp != nil {
		response := agent.ToolResponse(*event.ToolResp)
		out.ToolResp = &response
	}
	return out
}

func FromAgentStreamEvent(event agent.StreamEvent) StreamEvent {
	out := StreamEvent{
		Type: StreamEventType(event.Type), SessionID: event.SessionID, Step: event.Step,
		NodeID: event.NodeID, NodeAddr: event.NodeAddr, Model: event.Model,
		Output: event.Output, Coordinator: event.Coordinator, Content: event.Content, Error: event.Error,
	}
	if event.Message != nil {
		message := FromAgentMessage(*event.Message)
		out.Message = &message
	}
	if event.ToolCall != nil {
		call := ToolCall(*event.ToolCall)
		out.ToolCall = &call
	}
	if event.ToolResp != nil {
		response := ToolResponse(*event.ToolResp)
		out.ToolResp = &response
	}
	return out
}
