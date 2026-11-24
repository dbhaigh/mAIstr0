package openhands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/maistr0/maistr0/internal/agent"
)

var errSequenceGap = errors.New("OpenHands session stream has a sequence gap")

type socketFrame struct {
	Type       string          `json:"type"`
	FromSeq    *int64          `json:"from_seq"`
	ThroughSeq *int64          `json:"through_seq"`
	Seq        *int64          `json:"seq"`
	Event      json.RawMessage `json:"event"`
	Content    string          `json:"content"`
	Kind       string          `json:"kind"`
}

type nativeEvent struct {
	ID         string          `json:"id"`
	Kind       string          `json:"kind"`
	Timestamp  string          `json:"timestamp"`
	LLMMessage json.RawMessage `json:"llm_message"`
}

type nativeMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type textPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type socketRead struct {
	messageType websocket.MessageType
	data        []byte
	err         error
}

func (b *Backend) connectSocket(
	ctx context.Context,
	mapping sessionMapping,
) (*websocket.Conn, socketFrame, error) {
	target, err := b.socketURL(mapping.ConversationID, mapping.LastSeq)
	if err != nil {
		return nil, socketFrame{}, err
	}
	header := make(http.Header)
	header.Set("X-Session-API-Key", b.apiKey)
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(dialCtx, target, &websocket.DialOptions{
		HTTPClient: b.client,
		HTTPHeader: header,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, socketFrame{}, ctx.Err()
		}
		if response != nil {
			if response.Body != nil {
				_ = response.Body.Close()
			}
			if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
				return nil, socketFrame{}, ErrAuthentication
			}
		}
		return nil, socketFrame{}, ErrServerUnavailable
	}
	conn.SetReadLimit(maximumSocketFrame)
	auth, err := json.Marshal(map[string]string{
		"type": "auth", "session_api_key": b.apiKey,
	})
	if err != nil {
		_ = conn.CloseNow()
		return nil, socketFrame{}, ErrProtocol
	}
	writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
	err = conn.Write(writeCtx, websocket.MessageText, auth)
	writeCancel()
	if err != nil {
		_ = conn.CloseNow()
		return nil, socketFrame{}, ErrServerUnavailable
	}
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	messageType, data, err := conn.Read(readCtx)
	readCancel()
	if err != nil || messageType != websocket.MessageText {
		_ = conn.CloseNow()
		return nil, socketFrame{}, ErrServerUnavailable
	}
	var syncFrame socketFrame
	if json.Unmarshal(data, &syncFrame) != nil || syncFrame.Type != "sync" ||
		syncFrame.FromSeq == nil || *syncFrame.FromSeq != mapping.LastSeq ||
		syncFrame.ThroughSeq != nil && *syncFrame.ThroughSeq < mapping.LastSeq {
		_ = conn.CloseNow()
		return nil, socketFrame{}, ErrProtocol
	}
	return conn, syncFrame, nil
}

func (b *Backend) replayToSync(
	ctx context.Context,
	state *sessionState,
	conn *websocket.Conn,
	syncFrame socketFrame,
) error {
	if syncFrame.ThroughSeq == nil {
		return nil
	}
	state.mu.Lock()
	lastSeq := state.mapping.LastSeq
	state.mu.Unlock()
	if lastSeq >= *syncFrame.ThroughSeq {
		return nil
	}
	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for lastSeq < *syncFrame.ThroughSeq {
		messageType, data, err := conn.Read(readCtx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrServerUnavailable
		}
		if messageType != websocket.MessageText {
			return ErrProtocol
		}
		var frame socketFrame
		if json.Unmarshal(data, &frame) != nil || frame.Type == "" {
			return ErrProtocol
		}
		if err := b.applySocketFrame(ctx, state, frame, nil); err != nil {
			return err
		}
		state.mu.Lock()
		lastSeq = state.mapping.LastSeq
		state.mu.Unlock()
	}
	return nil
}

func (b *Backend) readSocket(ctx context.Context, conn *websocket.Conn) <-chan socketRead {
	frames := make(chan socketRead, 1)
	go func() {
		for {
			messageType, data, err := conn.Read(ctx)
			select {
			case frames <- socketRead{messageType: messageType, data: data, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return frames
}

func (b *Backend) applySocketFrame(
	ctx context.Context,
	state *sessionState,
	frame socketFrame,
	streamChan chan<- agent.StreamEvent,
) error {
	switch frame.Type {
	case "durable":
		if frame.Seq == nil || *frame.Seq < 0 || len(frame.Event) == 0 {
			return ErrProtocol
		}
		var event nativeEvent
		if json.Unmarshal(frame.Event, &event) != nil || event.ID == "" || event.Kind == "" {
			return ErrProtocol
		}
		var message *agent.Message
		if event.Kind == "MessageEvent" {
			converted, err := convertMessageEvent(event)
			if err != nil {
				return err
			}
			message = converted
		}

		state.persistMu.Lock()
		state.mu.Lock()
		mapping := state.mapping
		if *frame.Seq <= mapping.LastSeq {
			state.mu.Unlock()
			state.persistMu.Unlock()
			return nil
		}
		if *frame.Seq != mapping.LastSeq+1 {
			state.mu.Unlock()
			state.persistMu.Unlock()
			return errSequenceGap
		}
		if message != nil {
			if message.Role == agent.RoleUser && mapping.Pending {
				mapping.PendingSeen = true
			}
			appendNativeMessage(state.session, *message)
		}
		mapping.LastSeq = *frame.Seq
		state.mu.Unlock()
		if err := saveMapping(b.mappingDir, mapping); err != nil {
			state.persistMu.Unlock()
			return err
		}
		state.mu.Lock()
		state.mapping = mapping
		state.mu.Unlock()
		state.persistMu.Unlock()
		return nil
	case "transient", "item_started", "item_aborted":
		if frame.Type == "transient" && len(frame.Event) == 0 {
			return ErrProtocol
		}
		return nil
	case "delta":
		if frame.Kind != "text" || frame.Content == "" || streamChan == nil {
			return nil
		}
		state.mu.Lock()
		sessionID := state.session.ID
		state.mu.Unlock()
		return emit(ctx, streamChan, agent.StreamEvent{
			Type: agent.EventTextDelta, SessionID: sessionID, Content: frame.Content,
		})
	case "error":
		return ErrProtocol
	case "sync":
		return ErrProtocol
	default:
		return nil
	}
}

func convertMessageEvent(event nativeEvent) (*agent.Message, error) {
	var native nativeMessage
	if len(event.LLMMessage) == 0 || json.Unmarshal(event.LLMMessage, &native) != nil {
		return nil, ErrProtocol
	}
	role := agent.MessageRole(native.Role)
	if role != agent.RoleUser && role != agent.RoleAssistant {
		return nil, nil
	}
	content, err := parseTextContent(native.Content)
	if err != nil {
		return nil, ErrProtocol
	}
	if content == "" {
		return nil, nil
	}
	timestamp, err := time.Parse(time.RFC3339Nano, event.Timestamp)
	if err != nil {
		timestamp = time.Now()
	}
	return &agent.Message{
		ID: event.ID, Role: role, Content: content, Timestamp: timestamp,
	}, nil
}

func parseTextContent(value json.RawMessage) (string, error) {
	if len(value) == 0 || string(value) == "null" {
		return "", nil
	}
	var text string
	if json.Unmarshal(value, &text) == nil {
		return text, nil
	}
	var parts []textPart
	if json.Unmarshal(value, &parts) != nil {
		return "", ErrProtocol
	}
	var content strings.Builder
	for _, part := range parts {
		if part.Type == "text" {
			content.WriteString(part.Text)
		}
	}
	return content.String(), nil
}

func appendNativeMessage(session *agent.Session, message agent.Message) {
	for index := range session.Messages {
		if session.Messages[index].ID == message.ID {
			return
		}
	}
	if len(session.Messages) > 0 {
		last := &session.Messages[len(session.Messages)-1]
		if strings.HasPrefix(last.ID, "openhands-local-") &&
			last.Role == message.Role && last.Content == message.Content {
			*last = message
			session.UpdatedAt = message.Timestamp
			return
		}
	}
	session.Messages = append(session.Messages, message)
	session.UpdatedAt = message.Timestamp
}

func appendLocalMessage(state *sessionState, role agent.MessageRole, content, id string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.session.Messages = append(state.session.Messages, agent.Message{
		ID: id, Role: role, Content: content, Timestamp: time.Now(),
	})
	state.session.UpdatedAt = time.Now()
}

func syntheticMessageID(prefix string, sequence int64) string {
	return fmt.Sprintf("openhands-local-%s-%d", prefix, sequence)
}

func emit(ctx context.Context, streamChan chan<- agent.StreamEvent, event agent.StreamEvent) error {
	select {
	case streamChan <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Backend) SendMessage(
	ctx context.Context,
	sessionID string,
	userText string,
	streamChan chan<- agent.StreamEvent,
) (*agent.Message, error) {
	if ctx == nil || strings.TrimSpace(userText) == "" {
		return nil, ErrInvalidRequest
	}
	if !b.beginOperation() {
		return nil, ErrServerUnavailable
	}
	defer b.operations.Done()
	state, exists := b.getState(sessionID)
	if !exists {
		return nil, ErrConversationUnavailable
	}
	turnCtx, cancel := context.WithCancel(ctx)
	state.mu.Lock()
	if state.deleted {
		state.mu.Unlock()
		cancel()
		return nil, ErrConversationUnavailable
	}
	if state.active {
		state.mu.Unlock()
		cancel()
		return nil, ErrSessionBusy
	}
	state.active = true
	state.cancel = cancel
	state.session.Active = true
	state.mu.Unlock()
	defer func() {
		cancel()
		state.mu.Lock()
		state.active = false
		state.cancel = nil
		state.session.Active = state.uncertain
		state.session.UpdatedAt = time.Now()
		state.mu.Unlock()
	}()

	select {
	case b.turnGate <- struct{}{}:
		defer func() { <-b.turnGate }()
	case <-turnCtx.Done():
		return nil, turnCtx.Err()
	}

	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	if mapping.Pending {
		previous, err := b.waitForRun(turnCtx, state, nil, nil)
		if err != nil {
			return nil, err
		}
		if previous.status != "finished" && previous.status != "paused" {
			return nil, ErrOutcomeUnknown
		}
	}

	state.mu.Lock()
	mapping = state.mapping
	state.mu.Unlock()
	verifyCtx, verifyCancel := context.WithTimeout(turnCtx, 10*time.Second)
	_, err := b.getConversation(verifyCtx, mapping.ConversationID)
	verifyCancel()
	if err != nil {
		return nil, err
	}
	conn, syncFrame, err := b.connectSocket(turnCtx, mapping)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.CloseNow() }()
	if err := b.replayToSync(turnCtx, state, conn, syncFrame); err != nil {
		return nil, err
	}
	state.mu.Lock()
	mapping = state.mapping
	state.mu.Unlock()
	if err := b.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = true
		mapping.PendingSeen = false
		mapping.PendingStartSeq = mapping.LastSeq
		mapping.PendingSince = time.Now()
	}); err != nil {
		return nil, err
	}
	message, err := json.Marshal(map[string]any{
		"role": "user",
		"content": []map[string]string{
			{"type": "text", "text": userText},
		},
	})
	if err != nil {
		return nil, ErrProtocol
	}
	if err := conn.Write(turnCtx, websocket.MessageText, message); err != nil {
		b.markUncertain(state)
		return nil, ErrOutcomeUnknown
	}
	appendLocalMessage(state, agent.RoleUser, userText, syntheticMessageID("user", mapping.LastSeq))
	result, err := b.waitForRun(turnCtx, state, conn, streamChan)
	if err != nil {
		return nil, err
	}
	switch result.status {
	case "finished":
		return result.message, nil
	case "paused":
		return b.interruptedMessage(), context.Canceled
	default:
		return nil, ErrOutcomeUnknown
	}
}

func (b *Backend) waitForRun(
	ctx context.Context,
	state *sessionState,
	conn *websocket.Conn,
	streamChan chan<- agent.StreamEvent,
) (terminalResult, error) {
	turnTimer := time.NewTimer(b.turnTimeout)
	defer turnTimer.Stop()
	pollTicker := time.NewTicker(b.pollInterval)
	defer pollTicker.Stop()
	var frames <-chan socketRead
	if conn != nil {
		frames = b.readSocket(ctx, conn)
	}
	disconnectedAt := time.Time{}
	nextReconnect := time.Now()
	reconnectDelay := 100 * time.Millisecond
	idleSince := time.Time{}
	for {
		select {
		case <-ctx.Done():
			if conn != nil {
				_ = conn.CloseNow()
			}
			result, err := b.interruptAndConfirm(state)
			if err != nil {
				b.markUncertain(state)
				return terminalResult{}, ErrCancellationUnconfirmed
			}
			switch result.status {
			case "finished":
				return result, nil
			case "paused":
				return result, context.Canceled
			case "error", "stuck":
				return result, ErrTurnFailed
			default:
				b.markUncertain(state)
				return terminalResult{}, ErrOutcomeUnknown
			}
		case <-turnTimer.C:
			if conn != nil {
				_ = conn.CloseNow()
			}
			result, err := b.interruptAndConfirm(state)
			if err != nil {
				b.markUncertain(state)
				return terminalResult{}, ErrCancellationUnconfirmed
			}
			if result.status == "finished" {
				return result, nil
			}
			if result.status == "paused" {
				return result, ErrTurnTimeout
			}
			b.markUncertain(state)
			return terminalResult{}, ErrOutcomeUnknown
		case read := <-frames:
			if read.err != nil {
				if conn != nil {
					_ = conn.CloseNow()
				}
				conn, frames = nil, nil
				if disconnectedAt.IsZero() {
					disconnectedAt = time.Now()
				}
				nextReconnect = time.Now().Add(reconnectDelay)
				if reconnectDelay < 2*time.Second {
					reconnectDelay *= 2
				}
				continue
			}
			if read.messageType != websocket.MessageText {
				if conn != nil {
					_ = conn.CloseNow()
				}
				b.markUncertain(state)
				return terminalResult{}, ErrProtocol
			}
			var frame socketFrame
			if json.Unmarshal(read.data, &frame) != nil || frame.Type == "" {
				if conn != nil {
					_ = conn.CloseNow()
				}
				b.markUncertain(state)
				return terminalResult{}, ErrProtocol
			}
			err := b.applySocketFrame(ctx, state, frame, streamChan)
			if errors.Is(err, errSequenceGap) {
				if conn != nil {
					_ = conn.CloseNow()
				}
				conn, frames = nil, nil
				if disconnectedAt.IsZero() {
					disconnectedAt = time.Now()
				}
				nextReconnect = time.Now()
				continue
			}
			if err != nil {
				if conn != nil {
					_ = conn.CloseNow()
				}
				if ctx.Err() != nil {
					continue
				}
				b.markUncertain(state)
				return terminalResult{}, err
			}
		case <-pollTicker.C:
			statusCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			info, err := b.pollConversation(statusCtx, currentConversationID(state))
			cancel()
			if err != nil {
				if disconnectedAt.IsZero() {
					disconnectedAt = time.Now()
				}
				if time.Since(disconnectedAt) >= b.disconnectWait {
					b.markUncertain(state)
					return terminalResult{}, ErrOutcomeUnknown
				}
				continue
			}
			disconnectedAt = time.Time{}
			switch info.ExecutionStatus {
			case "finished":
				if conn != nil {
					_ = conn.CloseNow()
				}
				return b.finishSuccess(ctx, state, streamChan)
			case "paused":
				if conn != nil {
					_ = conn.CloseNow()
				}
				if err := b.clearPending(state); err != nil {
					b.markUncertain(state)
					return terminalResult{}, err
				}
				return terminalResult{status: "paused"}, nil
			case "error", "stuck":
				if conn != nil {
					_ = conn.CloseNow()
				}
				if err := b.clearPending(state); err != nil {
					b.markUncertain(state)
					return terminalResult{}, err
				}
				return terminalResult{status: info.ExecutionStatus}, ErrTurnFailed
			case "waiting_for_confirmation":
				if conn != nil {
					_ = conn.CloseNow()
				}
				if _, err := b.interruptAndConfirm(state); err != nil {
					b.markUncertain(state)
					return terminalResult{}, ErrCancellationUnconfirmed
				}
				return terminalResult{}, ErrApprovalUnsupported
			case "running":
				idleSince = time.Time{}
			case "idle":
				if idleSince.IsZero() {
					idleSince = time.Now()
				}
				if time.Since(idleSince) >= b.disconnectWait {
					b.markUncertain(state)
					return terminalResult{}, ErrOutcomeUnknown
				}
			default:
				b.markUncertain(state)
				return terminalResult{}, ErrOutcomeUnknown
			}
			if conn == nil && time.Now().After(nextReconnect) {
				state.mu.Lock()
				mapping := state.mapping
				state.mu.Unlock()
				socket, syncFrame, connectErr := b.connectSocket(ctx, mapping)
				if connectErr != nil {
					nextReconnect = time.Now().Add(reconnectDelay)
					if reconnectDelay < 2*time.Second {
						reconnectDelay *= 2
					}
					if !errors.Is(connectErr, ErrServerUnavailable) &&
						!errors.Is(connectErr, ErrAuthentication) {
						b.markUncertain(state)
						return terminalResult{}, connectErr
					}
					if time.Since(disconnectedAt) >= b.disconnectWait {
						b.markUncertain(state)
						return terminalResult{}, ErrOutcomeUnknown
					}
					continue
				}
				if err := b.replayToSync(ctx, state, socket, syncFrame); err != nil {
					_ = socket.CloseNow()
					if errors.Is(err, ErrProtocol) {
						b.markUncertain(state)
						return terminalResult{}, err
					}
					nextReconnect = time.Now().Add(reconnectDelay)
					continue
				}
				conn = socket
				frames = b.readSocket(ctx, conn)
				disconnectedAt = time.Time{}
				reconnectDelay = 100 * time.Millisecond
			}
		}
	}
}

func currentConversationID(state *sessionState) string {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.mapping.ConversationID
}

func (b *Backend) finishSuccess(
	ctx context.Context,
	state *sessionState,
	streamChan chan<- agent.StreamEvent,
) (terminalResult, error) {
	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	response, err := b.finalResponse(ctx, mapping.ConversationID)
	if err != nil {
		b.markUncertain(state)
		return terminalResult{}, ErrOutcomeUnknown
	}
	if response == "" {
		b.markUncertain(state)
		return terminalResult{}, ErrProtocol
	}
	message := appendFinalMessage(state, response, mapping.PendingStartSeq)
	if err := b.clearPending(state); err != nil {
		b.markUncertain(state)
		return terminalResult{}, err
	}
	if streamChan != nil {
		if err := emit(ctx, streamChan, agent.StreamEvent{
			Type: agent.EventFinalMessage, SessionID: messageSessionID(state),
			Message: message, Content: message.Content,
		}); err != nil {
			return terminalResult{}, err
		}
		if err := emit(ctx, streamChan, agent.StreamEvent{
			Type: agent.EventDone, SessionID: messageSessionID(state),
		}); err != nil {
			return terminalResult{}, err
		}
	}
	return terminalResult{status: "finished", message: message}, nil
}

func appendFinalMessage(state *sessionState, content string, startSeq int64) *agent.Message {
	state.mu.Lock()
	defer state.mu.Unlock()
	for index := len(state.session.Messages) - 1; index >= 0; index-- {
		message := &state.session.Messages[index]
		if message.Role == agent.RoleUser {
			break
		}
		if message.Role == agent.RoleAssistant {
			if message.Content == content {
				copy := agent.CloneMessage(*message)
				return &copy
			}
			break
		}
	}
	message := agent.Message{
		ID: syntheticMessageID("final", startSeq), Role: agent.RoleAssistant,
		Content: content, Timestamp: time.Now(),
	}
	state.session.Messages = append(state.session.Messages, message)
	state.session.UpdatedAt = message.Timestamp
	copy := agent.CloneMessage(message)
	return &copy
}

func messageSessionID(state *sessionState) string {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.session.ID
}

func (b *Backend) clearPending(state *sessionState) error {
	if err := b.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = false
		mapping.PendingSeen = false
		mapping.PendingSince = time.Time{}
	}); err != nil {
		return err
	}
	state.mu.Lock()
	state.uncertain = false
	if !state.active {
		state.session.Active = false
	}
	state.mu.Unlock()
	return nil
}

func (b *Backend) markUncertain(state *sessionState) {
	state.mu.Lock()
	state.uncertain = true
	state.session.Active = true
	state.mu.Unlock()
}

func (b *Backend) interruptAndConfirm(state *sessionState) (terminalResult, error) {
	state.cancelMu.Lock()
	defer state.cancelMu.Unlock()
	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	if !mapping.Pending {
		return terminalResult{status: "paused"}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.cancelWait)
	defer cancel()
	_ = b.request(
		ctx, http.MethodPost,
		"/api/conversations/"+mapping.ConversationID+"/interrupt",
		nil, nil, nil,
	)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		statusCtx, statusCancel := context.WithTimeout(ctx, 5*time.Second)
		info, err := b.pollConversation(statusCtx, mapping.ConversationID)
		statusCancel()
		if err == nil {
			switch info.ExecutionStatus {
			case "paused":
				if err := b.clearPending(state); err != nil {
					return terminalResult{}, ErrCancellationUnconfirmed
				}
				return terminalResult{status: "paused"}, nil
			case "finished":
				result, err := b.finishSuccess(ctx, state, nil)
				if err != nil {
					return terminalResult{}, err
				}
				return result, nil
			case "error", "stuck":
				if err := b.clearPending(state); err != nil {
					return terminalResult{}, ErrCancellationUnconfirmed
				}
				return terminalResult{status: info.ExecutionStatus}, nil
			}
		} else if errors.Is(err, ErrAuthentication) {
			return terminalResult{}, ErrCancellationUnconfirmed
		}
		select {
		case <-ctx.Done():
			b.markUncertain(state)
			return terminalResult{}, ErrCancellationUnconfirmed
		case <-ticker.C:
		}
	}
}

func (b *Backend) interruptedMessage() *agent.Message {
	return &agent.Message{
		ID: newLocalMessageID(), Role: agent.RoleAssistant,
		Content: "Turn interrupted.", Timestamp: time.Now(),
	}
}
