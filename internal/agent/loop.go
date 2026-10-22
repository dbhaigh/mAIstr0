package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Conversation interface {
	SessionID() string
	BeginTurn(userText, messageID string, at time.Time) error
	Snapshot() Session
	AppendMessage(message Message, at time.Time)
	Finish(at time.Time) bool
	RecordInterrupted(message Message, at time.Time) *Message
}

type LoopHarness[Request any] interface {
	FormatMessages(session Session, tools []ToolDef) (Request, error)
	CallModel(ctx context.Context, request Request, onDelta func(string)) (*ModelTurn, error)
	ShouldStop(turn *ModelTurn) bool
}

type LoopPolicy interface {
	NewMessageID() string
	MaxSteps(session Session, toolCount int) int
	InterruptedMessage(sessionID string, duration time.Duration) Message
	MaxStepsMessage(sessionID string) Message
}

type ModelTurn struct {
	Assistant Message
	ToolCalls []ToolCall
	Events    []StreamEvent
}

type ToolExecutor func(context.Context, string, string, map[string]any) (ToolResult, error)

type CompletionObserver interface {
	TurnCompleted(sessionID, userText string, final Message, duration time.Duration)
}

type streamMessageError interface {
	error
	StreamMessage() string
}

type activeTurn struct {
	conversation Conversation
	cancel       context.CancelFunc
	token        *struct{}
}

type AgentLoop[Request any] struct {
	harness   LoopHarness[Request]
	policy    LoopPolicy
	listTools func() []ToolDef
	execute   ToolExecutor

	mu     sync.Mutex
	active map[string]activeTurn
}

func NewAgentLoop[Request any](harness LoopHarness[Request], policy LoopPolicy, listTools func() []ToolDef, execute ToolExecutor) *AgentLoop[Request] {
	return &AgentLoop[Request]{
		harness: harness, policy: policy, listTools: listTools, execute: execute,
		active: make(map[string]activeTurn),
	}
}

func (l *AgentLoop[Request]) Run(ctx context.Context, conversation Conversation, userText string, streamChan chan<- StreamEvent) (*Message, error) {
	if conversation == nil {
		return nil, errors.New("conversation is required")
	}
	if l.harness == nil || l.policy == nil || l.listTools == nil || l.execute == nil {
		return nil, errors.New("agent loop dependencies are required")
	}

	sessionID := conversation.SessionID()
	turnCtx, turnCancel := context.WithCancel(ctx)
	l.mu.Lock()
	if _, exists := l.active[sessionID]; exists {
		l.mu.Unlock()
		turnCancel()
		return nil, errors.New("session is currently processing another request")
	}
	if err := conversation.BeginTurn(userText, l.policy.NewMessageID(), time.Now()); err != nil {
		l.mu.Unlock()
		turnCancel()
		return nil, err
	}
	turn := activeTurn{conversation: conversation, cancel: turnCancel, token: &struct{}{}}
	l.active[sessionID] = turn
	l.mu.Unlock()

	startedAt := time.Now()
	defer func() {
		l.mu.Lock()
		if current, exists := l.active[sessionID]; exists && current.token == turn.token {
			delete(l.active, sessionID)
		}
		l.mu.Unlock()
		turnCancel()
		conversation.Finish(time.Now())
	}()

	emit := func(event StreamEvent) {
		event.SessionID = sessionID
		if streamChan == nil {
			return
		}
		select {
		case streamChan <- event:
		default:
			timer := time.NewTimer(50 * time.Millisecond)
			defer timer.Stop()
			select {
			case streamChan <- event:
			case <-timer.C:
			}
		}
	}

	recordInterrupted := func(step int) (*Message, error) {
		emit(StreamEvent{Type: EventInterrupted, Step: step, Content: "Session interrupted."})
		message := l.policy.InterruptedMessage(sessionID, time.Since(startedAt))
		if message.ID == "" {
			message.ID = l.policy.NewMessageID()
		}
		if message.Timestamp.IsZero() {
			message.Timestamp = time.Now()
		}
		return conversation.RecordInterrupted(message, time.Now()), turnCtx.Err()
	}

	tools := l.listTools()
	maxSteps := l.policy.MaxSteps(conversation.Snapshot(), len(tools))
	if maxSteps <= 0 {
		maxSteps = 8
	}

	var final *Message
	for step := 1; step <= maxSteps; step++ {
		if err := turnCtx.Err(); err != nil {
			return recordInterrupted(step)
		}
		emit(StreamEvent{Type: EventStepStarted, Step: step})

		request, err := l.harness.FormatMessages(conversation.Snapshot(), tools)
		if err != nil {
			if turnCtx.Err() != nil {
				return recordInterrupted(step)
			}
			emit(StreamEvent{Type: EventError, Error: streamErrorMessage(err)})
			return nil, err
		}
		result, err := l.harness.CallModel(turnCtx, request, func(delta string) {
			emit(StreamEvent{Type: EventTextDelta, Step: step, Content: delta})
		})
		if err != nil {
			if turnCtx.Err() != nil {
				return recordInterrupted(step)
			}
			emit(StreamEvent{Type: EventError, Error: streamErrorMessage(err)})
			return nil, err
		}
		if result == nil {
			err = errors.New("model provider returned an empty completion")
			emit(StreamEvent{Type: EventError, Error: err.Error()})
			return nil, err
		}
		for _, event := range result.Events {
			if event.Step == 0 {
				event.Step = step
			}
			emit(event)
		}

		calls := result.ToolCalls
		if len(calls) == 0 {
			calls = result.Assistant.ToolCalls
		}
		calls = append([]ToolCall(nil), calls...)
		callErrors := make([]error, len(calls))
		for i := range calls {
			if calls[i].ID == "" {
				calls[i].ID = l.policy.NewMessageID()
			}
			if calls[i].RawArgs != "" {
				args, err := decodeToolArguments(calls[i].RawArgs)
				if err != nil {
					callErrors[i] = err
				} else {
					calls[i].Arguments = args
				}
			} else if calls[i].Arguments == nil {
				calls[i].Arguments = map[string]any{}
			}
		}
		result.ToolCalls = calls

		assistant := result.Assistant
		if assistant.ID == "" {
			assistant.ID = l.policy.NewMessageID()
		}
		assistant.Role = RoleAssistant
		assistant.Timestamp = nonzeroTime(assistant.Timestamp, time.Now())
		if l.harness.ShouldStop(result) {
			assistant.ToolCalls = nil
			conversation.AppendMessage(assistant, time.Now())
			final = &assistant
			emit(StreamEvent{Type: EventFinalMessage, Step: step, Message: final, Content: final.Content})
			break
		}

		assistant.ToolCalls = calls
		conversation.AppendMessage(assistant, time.Now())
		for i, call := range calls {
			if err := turnCtx.Err(); err != nil {
				return recordInterrupted(step)
			}
			emit(StreamEvent{Type: EventToolCall, Step: step, ToolCall: &call})
			toolStarted := time.Now()
			response := ToolResponse{ToolCallID: call.ID, Name: call.Name}
			var result ToolResult
			var executeErr error
			if callErrors[i] != nil {
				executeErr = fmt.Errorf("invalid tool arguments: %w", callErrors[i])
			} else {
				result, executeErr = l.execute(turnCtx, sessionID, call.Name, call.Arguments)
			}
			if turnCtx.Err() != nil {
				return recordInterrupted(step)
			}
			response.Output = result.Output
			response.NodeID, response.NodeAddr, response.Model = result.NodeID, result.NodeAddr, result.Model
			response.DurationMs = result.DurationMs
			if executeErr != nil {
				response.Error = executeErr.Error()
			}
			if response.DurationMs == 0 {
				response.DurationMs = time.Since(toolStarted).Milliseconds()
			}
			toolMessage := Message{
				ID: l.policy.NewMessageID(), Role: RoleTool, ToolResponse: &response,
				NodeID: response.NodeID, Model: response.Model,
				Timestamp: time.Now(), DurationMs: response.DurationMs,
			}
			conversation.AppendMessage(toolMessage, time.Now())
			emit(StreamEvent{Type: EventToolResponse, Step: step, ToolResp: &response})
		}
	}

	if final == nil {
		message := l.policy.MaxStepsMessage(sessionID)
		message.ID = nonempty(message.ID, l.policy.NewMessageID())
		message.Role = RoleAssistant
		message.Timestamp = nonzeroTime(message.Timestamp, time.Now())
		conversation.AppendMessage(message, time.Now())
		final = &message
		emit(StreamEvent{Type: EventFinalMessage, Message: final, Content: final.Content})
	}
	if observer, ok := l.harness.(CompletionObserver); ok {
		observer.TurnCompleted(sessionID, userText, *final, time.Since(startedAt))
	}
	emit(StreamEvent{Type: EventDone})
	return final, nil
}

func (l *AgentLoop[Request]) InterruptSession(sessionID string) bool {
	l.mu.Lock()
	turn, ok := l.active[sessionID]
	l.mu.Unlock()
	if !ok {
		return false
	}
	turn.cancel()
	wasActive := turn.conversation.Finish(time.Now())
	return wasActive || ok
}

func decodeToolArguments(raw string) (map[string]any, error) {
	var args map[string]any
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, err
	}
	if args == nil {
		return nil, errors.New("tool arguments must be a JSON object")
	}
	return args, nil
}

func nonzeroTime(value, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}

func nonempty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func streamErrorMessage(err error) string {
	var formatted streamMessageError
	if errors.As(err, &formatted) {
		return formatted.StreamMessage()
	}
	return err.Error()
}
