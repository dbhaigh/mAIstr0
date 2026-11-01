package deepseek

import "github.com/maistr0/maistr0/internal/agent"

func ToAgentSession(session *Session) *agent.Session {
	if session == nil {
		return nil
	}
	source := session.Snapshot()
	out := &agent.Session{
		ID: source.ID, Title: source.Title, CreatedAt: source.CreatedAt, UpdatedAt: source.UpdatedAt,
		Config: source.Config, Active: source.Active,
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
		Config: session.Config, Active: false,
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
	return agent.CloneMessage(message)
}

func FromAgentMessage(message agent.Message) Message {
	return agent.CloneMessage(message)
}

func ToAgentStreamEvent(event StreamEvent) agent.StreamEvent {
	return agent.CloneStreamEvent(event)
}

func FromAgentStreamEvent(event agent.StreamEvent) StreamEvent {
	return agent.CloneStreamEvent(event)
}
