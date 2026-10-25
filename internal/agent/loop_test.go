package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type loopTestRequest struct{}

type loopTestHarness struct{}

func (loopTestHarness) Name() string { return "test" }

func (loopTestHarness) NewMessageID() string { return "generated" }

func (loopTestHarness) MaxSteps(Session, int) int { return 2 }

func (loopTestHarness) FormatMessages(Session, []ToolDef) (loopTestRequest, error) {
	return loopTestRequest{}, nil
}

func (loopTestHarness) CallModel(context.Context, loopTestRequest, func(string)) (*ModelTurn, error) {
	return &ModelTurn{
		Assistant: Message{Role: RoleAssistant, Content: "calling a tool"},
		ToolCalls: []ToolCall{{ID: "call-1", Name: "wait", RawArgs: `{}`}},
	}, nil
}

func (loopTestHarness) ShouldStop(turn *ModelTurn) bool { return len(turn.ToolCalls) == 0 }

func (loopTestHarness) InterruptedMessage(string, time.Duration) Message {
	return Message{Role: RoleAssistant, Content: "Session interrupted."}
}

func (loopTestHarness) MaxStepsMessage(string) Message {
	return Message{Role: RoleAssistant, Content: "maximum steps"}
}

type loopTestConversation struct {
	mu      sync.Mutex
	session Session
	active  bool
}

func newLoopTestConversation() *loopTestConversation {
	return &loopTestConversation{session: Session{ID: "session-1"}}
}

func (c *loopTestConversation) SessionID() string { return c.session.ID }

func (c *loopTestConversation) BeginTurn(text, id string, at time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active {
		return errors.New("session is currently processing another request")
	}
	c.active = true
	c.session.Active = true
	c.session.Messages = append(c.session.Messages, Message{
		ID: id, Role: RoleUser, Content: text, Timestamp: at,
	})
	return nil
}

func (c *loopTestConversation) Snapshot() Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy := c.session
	copy.Messages = append([]Message(nil), c.session.Messages...)
	return copy
}

func (c *loopTestConversation) AppendMessage(message Message, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session.Messages = append(c.session.Messages, message)
	c.session.UpdatedAt = at
}

func (c *loopTestConversation) Finish(at time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	wasActive := c.active
	c.active = false
	c.session.Active = false
	c.session.UpdatedAt = at
	return wasActive
}

func (c *loopTestConversation) RecordInterrupted(message Message, at time.Time) *Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session.Messages = append(c.session.Messages, message)
	c.session.UpdatedAt = at
	copy := message
	return &copy
}

func TestAgentLoopInterruptCancelsToolAndRetainsConversation(t *testing.T) {
	conversation := newLoopTestConversation()
	started := make(chan struct{})
	events := make(chan StreamEvent, 8)
	loop := NewAgentLoop(
		loopTestHarness{},
		loopTestHarness{},
		func() []ToolDef { return []ToolDef{{Name: "wait"}} },
		func(ctx context.Context, _, _ string, _ map[string]any) (ToolResult, error) {
			close(started)
			<-ctx.Done()
			return ToolResult{}, ctx.Err()
		},
	)
	result := make(chan error, 1)
	go func() {
		_, err := loop.Run(context.Background(), conversation, "keep this turn", events)
		result <- err
	}()

	<-started
	if !loop.InterruptSession(conversation.SessionID()) {
		t.Fatal("InterruptSession returned false for an active turn")
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context cancellation", err)
	}

	session := conversation.Snapshot()
	if session.Active {
		t.Fatal("session remained active after interruption")
	}
	if len(session.Messages) != 3 ||
		session.Messages[0].Role != RoleUser ||
		session.Messages[1].Role != RoleAssistant ||
		len(session.Messages[1].ToolCalls) != 1 ||
		session.Messages[2].Role != RoleAssistant ||
		session.Messages[2].Content != "Session interrupted." {
		t.Fatalf("interrupted conversation history = %#v", session.Messages)
	}

	close(events)
	foundInterrupted := false
	for event := range events {
		foundInterrupted = foundInterrupted || event.Type == EventInterrupted
	}
	if !foundInterrupted {
		t.Fatal("missing interrupted stream event")
	}
}
