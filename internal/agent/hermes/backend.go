package hermes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

const (
	Name                = "hermes"
	defaultTurnTimeout  = 30 * time.Minute
	defaultPollInterval = 250 * time.Millisecond
	cancellationWait    = 30 * time.Second
	serverLossWait      = 30 * time.Second
)

var (
	ErrInvalidConfiguration     = errors.New("Hermes backend configuration is invalid")
	ErrInvalidRequest           = errors.New("Hermes request is invalid")
	ErrUnsupportedVersion       = errors.New("Hermes API Server must report version 0.23.3")
	ErrUnsupportedCapabilities  = errors.New("Hermes API Server does not provide the required durable Runs API")
	ErrAuthentication           = errors.New("Hermes API Server authentication failed")
	ErrServerUnavailable        = errors.New("Hermes API Server is unavailable")
	ErrServerRequest            = errors.New("Hermes API Server rejected the request")
	ErrSessionUnavailable       = errors.New("Hermes session is unavailable")
	ErrSessionBusy              = errors.New("Hermes session already has an active turn")
	ErrSessionMapping           = errors.New("Hermes session mapping is unavailable")
	ErrUnsupportedConfiguration = errors.New("Hermes backend does not support the requested session configuration")
	ErrUnsupportedOperation     = errors.New("Hermes backend does not support this operation")
	ErrProtocol                 = errors.New("Hermes API Server returned an invalid response")
	ErrTurnFailed               = errors.New("Hermes reported a failed run")
	ErrTurnTimeout              = errors.New("Hermes run exceeded its runtime limit")
	ErrOutcomeUnknown           = errors.New("Hermes run outcome is unknown; gateway-owned work may still be active")
	ErrCancellationUnconfirmed  = errors.New("Hermes did not confirm that the run stopped")
	ErrApprovalUnsupported      = errors.New("Hermes requested an approval that mAIstr0 cannot provide")
)

type Config struct {
	ServerURL  string
	APIKey     string
	SessionDir string
}

type Backend struct {
	client     *http.Client
	baseURL    string
	apiKey     string
	mappingDir string
	version    string

	// mu protects the session map, closed state, and operation admission.
	// Per-session persistence uses persistMu before state.mu; neither is held over network I/O.
	mu          sync.RWMutex
	sessions    map[string]*sessionState
	closed      bool
	fullyClosed bool
	operations  sync.WaitGroup
	closeMu     sync.Mutex

	turnTimeout    time.Duration
	pollInterval   time.Duration
	cancelWait     time.Duration
	serverLossWait time.Duration
}

type sessionState struct {
	// persistMu serializes sidecar writes; stopMu serializes remote stop/reconciliation.
	// mu protects the session, mapping, and active-turn fields.
	persistMu sync.Mutex
	stopMu    sync.Mutex
	mu        sync.Mutex

	session     *agent.Session
	mapping     sessionMapping
	active      bool
	deleted     bool
	cancel      context.CancelFunc
	done        chan struct{}
	lastOutcome string
}

type runOutcome struct {
	status  string
	message *agent.Message
}

func New(config Config) (*Backend, error) {
	baseURL, err := validateServerURL(config.ServerURL, config.APIKey)
	if err != nil {
		return nil, err
	}
	mappingDir, err := ensureMappingDirectory(config.SessionDir)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 10 * time.Second
	backend := &Backend{
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		baseURL: baseURL, apiKey: config.APIKey, mappingDir: mappingDir,
		sessions:    make(map[string]*sessionState),
		turnTimeout: defaultTurnTimeout, pollInterval: defaultPollInterval,
		cancelWait: cancellationWait, serverLossWait: serverLossWait,
	}
	startupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	backend.version, err = backend.verifyServer(startupCtx)
	if err == nil {
		err = backend.verifyCapabilities(startupCtx)
	}
	if err != nil {
		backend.client.CloseIdleConnections()
		backend.apiKey = ""
		return nil, err
	}
	return backend, nil
}

func NewFromEnvironment() (*Backend, bool, error) {
	names := []string{
		"MAISTR0_HERMES_SERVER_URL",
		"MAISTR0_HERMES_API_KEY",
		"MAISTR0_HERMES_SESSION_DIR",
	}
	values := make(map[string]string, len(names))
	enabled := false
	for _, name := range names {
		value, exists := os.LookupEnv(name)
		if exists {
			enabled = true
			values[name] = strings.TrimSpace(value)
		}
	}
	if !enabled {
		return nil, false, nil
	}
	for _, name := range names {
		if values[name] == "" {
			return nil, true, fmt.Errorf("Hermes opt-in requires %s", name)
		}
	}
	backend, err := New(Config{
		ServerURL:  values["MAISTR0_HERMES_SERVER_URL"],
		APIKey:     values["MAISTR0_HERMES_API_KEY"],
		SessionDir: values["MAISTR0_HERMES_SESSION_DIR"],
	})
	return backend, true, err
}

func (b *Backend) Name() string    { return Name }
func (b *Backend) Version() string { return b.version }

func (b *Backend) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	if cfg.MaxSteps != 0 || cfg.Temperature != 0 {
		return nil, ErrUnsupportedConfiguration
	}
	if !b.beginOperation() {
		return nil, ErrServerUnavailable
	}
	defer b.operations.Done()
	if title == "" {
		title = "Hermes Agent Session " + time.Now().Format("Jan 02 15:04")
	}
	id, err := newSessionID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	session := &agent.Session{
		ID: id, Title: title, Harness: Name, CreatedAt: now, UpdatedAt: now,
		Messages: []agent.Message{}, Memory: map[string]string{}, Config: cfg,
	}
	mapping := sessionMapping{Version: mappingVersion, SessionID: id, LastSeq: -1}
	if err := saveMapping(b.mappingDir, mapping); err != nil {
		return nil, err
	}
	state := &sessionState{session: session, mapping: mapping}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = removeMapping(b.mappingDir, id)
		return nil, ErrServerUnavailable
	}
	b.sessions[id] = state
	b.mu.Unlock()
	return cloneSession(session), nil
}

func (b *Backend) RestoreSession(session *agent.Session) error {
	if session == nil || !validSessionID(session.ID) {
		return ErrSessionMapping
	}
	mapping, err := loadMapping(b.mappingDir, session.ID)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrServerUnavailable
	}
	if _, exists := b.sessions[session.ID]; exists {
		return errors.New("Hermes session already exists")
	}
	restored := cloneSession(session)
	restored.Harness = Name
	restored.Active = mapping.Pending
	b.sessions[restored.ID] = &sessionState{session: restored, mapping: mapping}
	return nil
}

func (b *Backend) GetSession(id string) (*agent.Session, bool) {
	state, exists := b.getState(id)
	if !exists {
		return nil, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleted {
		return nil, false
	}
	return cloneSession(state.session), true
}

func (b *Backend) ListSessions() []*agent.Session {
	b.mu.RLock()
	states := make([]*sessionState, 0, len(b.sessions))
	for _, state := range b.sessions {
		states = append(states, state)
	}
	b.mu.RUnlock()
	sessions := make([]*agent.Session, 0, len(states))
	for _, state := range states {
		state.mu.Lock()
		if !state.deleted {
			sessions = append(sessions, cloneSession(state.session))
		}
		state.mu.Unlock()
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	return sessions
}

func (b *Backend) DeleteSession(id string) bool {
	if !b.beginOperation() {
		return false
	}
	defer b.operations.Done()
	state, exists := b.getState(id)
	if !exists {
		return false
	}
	state.mu.Lock()
	if state.deleted || state.active {
		state.mu.Unlock()
		return false
	}
	mapping := state.mapping
	state.mu.Unlock()
	if mapping.Pending {
		outcome, err := b.stopAndWait(state)
		if err != nil || outcome.status == "interrupted" || outcome.status == "unknown" {
			return false
		}
		if err := b.clearPending(state); err != nil {
			return false
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.deleteRemoteSession(ctx, id); err != nil {
		return false
	}
	state.persistMu.Lock()
	defer state.persistMu.Unlock()
	if err := removeMapping(b.mappingDir, id); err != nil {
		return false
	}
	state.mu.Lock()
	state.deleted = true
	state.session.Active = false
	state.mu.Unlock()
	b.mu.Lock()
	if b.sessions[id] == state {
		delete(b.sessions, id)
	}
	b.mu.Unlock()
	return true
}

func (b *Backend) InterruptSession(id string) bool {
	if !b.beginOperation() {
		return false
	}
	defer b.operations.Done()
	state, exists := b.getState(id)
	if !exists {
		return false
	}
	state.mu.Lock()
	if state.deleted || !state.mapping.Pending {
		state.mu.Unlock()
		return false
	}
	if state.cancel != nil {
		state.cancel()
	}
	done := state.done
	state.mu.Unlock()
	if done != nil {
		<-done
		state.mu.Lock()
		confirmed := state.lastOutcome == "cancelled"
		state.mu.Unlock()
		return confirmed
	}
	outcome, err := b.stopAndWait(state)
	if err != nil {
		return false
	}
	switch outcome.status {
	case "cancelled":
		return b.clearPending(state) == nil
	case "completed", "failed":
		_ = b.clearPending(state)
		return false
	default:
		return false
	}
}

func (b *Backend) ListTools() []agent.ToolDef           { return []agent.ToolDef{} }
func (b *Backend) ListClusterModels() []agent.ModelInfo { return []agent.ModelInfo{} }

func (b *Backend) Stats() agent.AgentStats {
	b.mu.RLock()
	states := make([]*sessionState, 0, len(b.sessions))
	for _, state := range b.sessions {
		states = append(states, state)
	}
	b.mu.RUnlock()
	stats := agent.AgentStats{TotalSessions: len(states), DispatchedLoad: map[string]int{}}
	for _, state := range states {
		state.mu.Lock()
		if !state.deleted {
			if state.active || state.mapping.Pending {
				stats.ActiveSessions++
			}
			stats.TotalMessages += len(state.session.Messages)
		}
		state.mu.Unlock()
	}
	return stats
}

func (b *Backend) ExecuteTool(context.Context, string, string, map[string]any) (agent.ToolResult, error) {
	return agent.ToolResult{}, ErrUnsupportedOperation
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
		return nil, ErrSessionUnavailable
	}
	turnCtx, cancel := context.WithTimeout(ctx, b.turnTimeout)
	state.mu.Lock()
	if state.deleted {
		state.mu.Unlock()
		cancel()
		return nil, ErrSessionUnavailable
	}
	if state.active {
		state.mu.Unlock()
		cancel()
		return nil, ErrSessionBusy
	}
	state.active = true
	state.cancel = cancel
	state.done = make(chan struct{})
	state.lastOutcome = ""
	state.session.Active = true
	state.mu.Unlock()
	defer func() {
		cancel()
		state.mu.Lock()
		state.active = false
		state.cancel = nil
		close(state.done)
		state.done = nil
		state.session.Active = state.mapping.Pending
		state.session.UpdatedAt = time.Now()
		state.mu.Unlock()
	}()

	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	if mapping.Pending {
		if mapping.RunID == "" {
			return nil, ErrOutcomeUnknown
		}
		_, err := b.watchRun(turnCtx, state, nil, false, false)
		if err != nil {
			state.mu.Lock()
			stillPending := state.mapping.Pending
			state.mu.Unlock()
			if stillPending || errors.Is(err, ErrOutcomeUnknown) ||
				errors.Is(err, ErrCancellationUnconfirmed) || errors.Is(err, ErrApprovalUnsupported) {
				return nil, err
			}
			if turnCtx.Err() != nil {
				return nil, turnCtx.Err()
			}
		}
	}
	if err := turnCtx.Err(); err != nil {
		return nil, err
	}

	key, err := newIdempotencyKey()
	if err != nil {
		return nil, err
	}
	if err := b.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = true
		mapping.RunID = ""
		mapping.IdempotencyKey = key
		mapping.LastSeq = -1
	}); err != nil {
		return nil, err
	}
	body := map[string]any{
		"input": userText, "session_id": sessionID,
	}
	state.mu.Lock()
	config := state.session.Config
	state.mu.Unlock()
	if config.SystemPrompt != "" {
		body["instructions"] = config.SystemPrompt
	}
	if config.CoordinatorModel != "" {
		body["model"] = config.CoordinatorModel
	}
	submission, err := b.submitRun(turnCtx, key, body)
	if err != nil {
		if errors.Is(err, ErrOutcomeUnknown) || turnCtx.Err() != nil ||
			errors.Is(err, ErrServerUnavailable) {
			return nil, ErrOutcomeUnknown
		}
		if clearErr := b.clearPending(state); clearErr != nil {
			return nil, errors.Join(err, clearErr)
		}
		return nil, err
	}
	if err := b.updateMapping(state, func(mapping *sessionMapping) {
		mapping.RunID = submission.RunID
	}); err != nil {
		state.mu.Lock()
		state.mapping.RunID = submission.RunID
		state.mu.Unlock()
		return nil, err
	}
	appendLocalMessage(state, agent.RoleUser, userText, localMessageID("user", submission.RunID))
	outcome, err := b.watchRun(turnCtx, state, streamChan, true, true)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, ErrTurnTimeout
		}
		return nil, err
	}
	if outcome.status != "completed" || outcome.message == nil {
		return nil, ErrProtocol
	}
	return outcome.message, nil
}

func (b *Backend) watchRun(
	ctx context.Context,
	state *sessionState,
	streamChan chan<- agent.StreamEvent,
	appendResult bool,
	stopOnCancel bool,
) (runOutcome, error) {
	state.mu.Lock()
	mapping := state.mapping
	sessionID := state.session.ID
	state.mu.Unlock()
	if !mapping.Pending || !validRunID(mapping.RunID) {
		return runOutcome{}, ErrOutcomeUnknown
	}
	deadline := time.Now().Add(b.serverLossWait)
	for {
		if ctx.Err() != nil {
			if b.isClosing() {
				return runOutcome{}, ErrOutcomeUnknown
			}
			if !stopOnCancel {
				return runOutcome{}, ErrOutcomeUnknown
			}
			outcome, err := b.stopAndWait(state)
			if err != nil {
				return runOutcome{}, ErrCancellationUnconfirmed
			}
			return b.settleRun(ctx, state, outcome, streamChan, appendResult)
		}
		statusCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		status, err := b.getRun(statusCtx, mapping.RunID)
		cancel()
		if err != nil {
			if errors.Is(err, ErrAuthentication) || errors.Is(err, ErrProtocol) {
				return runOutcome{}, err
			}
			if time.Now().After(deadline) {
				return runOutcome{}, ErrOutcomeUnknown
			}
			_ = waitContext(ctx, b.pollInterval)
			continue
		}
		if status.RunID != mapping.RunID || status.SessionID != sessionID || status.Status == "" {
			return runOutcome{}, ErrProtocol
		}
		switch status.Status {
		case "completed", "failed", "cancelled", "interrupted", "unknown":
			return b.settleRun(ctx, state, runOutcome{status: status.Status, message: assistantMessage(status)}, streamChan, appendResult)
		case "waiting_for_approval":
			return runOutcome{status: status.Status}, ErrApprovalUnsupported
		case "queued", "running", "stopping":
		default:
			return runOutcome{}, ErrProtocol
		}

		state.mu.Lock()
		lastSeq := state.mapping.LastSeq
		state.mu.Unlock()
		eventCtx, eventCancel := context.WithCancel(ctx)
		err = b.readEvents(eventCtx, mapping.RunID, lastSeq, func(event runEvent, seq int64) error {
			if err := b.applyEvent(ctx, sessionID, event, seq, streamChan, state); err != nil {
				return err
			}
			if event.Event == "approval.request" {
				return ErrApprovalUnsupported
			}
			return nil
		})
		eventCancel()
		_ = b.persistMapping(state)
		if ctx.Err() != nil {
			continue
		}
		if errors.Is(err, ErrProtocol) || errors.Is(err, ErrApprovalUnsupported) {
			return runOutcome{}, err
		}
		if err != nil {
			if time.Now().After(deadline) {
				return runOutcome{}, ErrOutcomeUnknown
			}
		}
		if waitContext(ctx, b.pollInterval) != nil {
			continue
		}
	}
}

func (b *Backend) settleRun(
	ctx context.Context,
	state *sessionState,
	outcome runOutcome,
	streamChan chan<- agent.StreamEvent,
	appendResult bool,
) (runOutcome, error) {
	switch outcome.status {
	case "completed":
		if outcome.message == nil || strings.TrimSpace(outcome.message.Content) == "" {
			return runOutcome{}, ErrProtocol
		}
		state.mu.Lock()
		state.lastOutcome = outcome.status
		state.mu.Unlock()
		if err := b.clearPending(state); err != nil {
			return runOutcome{}, err
		}
		if appendResult {
			appendLocalMessage(state, agent.RoleAssistant, outcome.message.Content, outcome.message.ID)
			sessionID := sessionIDOf(state)
			if err := emit(ctx, streamChan, agent.StreamEvent{
				Type: agent.EventFinalMessage, SessionID: sessionID,
				Message: agentMessageCopy(outcome.message),
			}); err != nil {
				return runOutcome{}, err
			}
			if err := emit(ctx, streamChan, agent.StreamEvent{Type: agent.EventDone, SessionID: sessionID}); err != nil {
				return runOutcome{}, err
			}
		}
		return outcome, nil
	case "failed":
		state.mu.Lock()
		state.lastOutcome = outcome.status
		state.mu.Unlock()
		if err := b.clearPending(state); err != nil {
			return runOutcome{}, err
		}
		if appendResult {
			_ = emit(ctx, streamChan, agent.StreamEvent{
				Type: agent.EventError, SessionID: sessionIDOf(state), Error: ErrTurnFailed.Error(),
			})
			_ = emit(ctx, streamChan, agent.StreamEvent{Type: agent.EventDone, SessionID: sessionIDOf(state)})
		}
		return outcome, ErrTurnFailed
	case "cancelled":
		state.mu.Lock()
		state.lastOutcome = outcome.status
		state.mu.Unlock()
		if err := b.clearPending(state); err != nil {
			return runOutcome{}, err
		}
		if appendResult {
			_ = emit(ctx, streamChan, agent.StreamEvent{
				Type: agent.EventInterrupted, SessionID: sessionIDOf(state),
			})
			_ = emit(ctx, streamChan, agent.StreamEvent{
				Type: agent.EventDone, SessionID: sessionIDOf(state),
			})
		}
		return outcome, context.Canceled
	case "interrupted", "unknown":
		return outcome, ErrOutcomeUnknown
	default:
		return runOutcome{}, ErrProtocol
	}
}

func (b *Backend) applyEvent(
	ctx context.Context,
	sessionID string,
	event runEvent,
	seq int64,
	streamChan chan<- agent.StreamEvent,
	state *sessionState,
) error {
	if seq < -1 {
		return ErrProtocol
	}
	state.mu.Lock()
	if seq >= 0 && seq <= state.mapping.LastSeq {
		state.mu.Unlock()
		return nil
	}
	if seq >= 0 {
		state.mapping.LastSeq = seq
	}
	state.mu.Unlock()

	switch event.Event {
	case "message.delta":
		text := event.Delta
		if text == "" && event.Text != "" {
			text = event.Text
		}
		if text == "" {
			return nil
		}
		return emit(ctx, streamChan, agent.StreamEvent{
			Type: agent.EventTextDelta, SessionID: sessionID, Content: text,
		})
	case "tool.started":
		if event.Tool == "" {
			return ErrProtocol
		}
		return emit(ctx, streamChan, agent.StreamEvent{
			Type: agent.EventToolCall, SessionID: sessionID,
			ToolCall: &agent.ToolCall{
				ID: event.ToolCallID, Name: event.Tool, Arguments: map[string]any{},
			},
		})
	case "tool.completed":
		if event.Tool == "" {
			return ErrProtocol
		}
		toolResponse := &agent.ToolResponse{
			ToolCallID: event.ToolCallID, Name: event.Tool, Output: "",
		}
		var isError bool
		if json.Unmarshal(event.Error, &isError) == nil && isError {
			toolResponse.Error = "tool failed"
		}
		return emit(ctx, streamChan, agent.StreamEvent{
			Type: agent.EventToolResponse, SessionID: sessionID, ToolResp: toolResponse,
		})
	case "approval.request":
		return ErrApprovalUnsupported
	case "message.interim", "reasoning.available", "subagent.start", "subagent.complete",
		"run.queued", "run.started", "run.completed", "run.failed", "run.cancelled", "run.interrupted":
		return nil
	default:
		return nil
	}
}

func (b *Backend) stopAndWait(state *sessionState) (runOutcome, error) {
	state.stopMu.Lock()
	defer state.stopMu.Unlock()
	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	if !mapping.Pending || !validRunID(mapping.RunID) {
		return runOutcome{}, ErrOutcomeUnknown
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, stopErr := b.stopRun(ctx, mapping.RunID)
	cancel()
	if stopErr != nil && !errors.Is(stopErr, ErrOutcomeUnknown) {
		return runOutcome{}, stopErr
	}
	deadline := time.Now().Add(b.cancelWait)
	for {
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 5*time.Second)
		status, err := b.getRun(pollCtx, mapping.RunID)
		pollCancel()
		if err == nil {
			if status.RunID != mapping.RunID || status.SessionID != mapping.SessionID || status.Status == "" {
				return runOutcome{}, ErrProtocol
			}
			switch status.Status {
			case "cancelled", "completed", "failed", "interrupted", "unknown":
				return runOutcome{status: status.Status, message: assistantMessage(status)}, nil
			case "queued", "running", "stopping", "waiting_for_approval":
			default:
				return runOutcome{}, ErrProtocol
			}
		} else if !errors.Is(err, ErrServerUnavailable) && !errors.Is(err, ErrOutcomeUnknown) {
			return runOutcome{}, err
		}
		if time.Now().After(deadline) {
			return runOutcome{}, ErrCancellationUnconfirmed
		}
		if err := waitContext(context.Background(), b.pollInterval); err != nil {
			return runOutcome{}, err
		}
	}
}

func (b *Backend) clearPending(state *sessionState) error {
	return b.updateMapping(state, func(mapping *sessionMapping) {
		mapping.Pending = false
		mapping.RunID = ""
		mapping.IdempotencyKey = ""
		mapping.LastSeq = -1
	})
}

func (b *Backend) updateMapping(state *sessionState, update func(*sessionMapping)) error {
	state.persistMu.Lock()
	defer state.persistMu.Unlock()
	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	update(&mapping)
	if err := saveMapping(b.mappingDir, mapping); err != nil {
		return err
	}
	state.mu.Lock()
	state.mapping = mapping
	state.mu.Unlock()
	return nil
}

func (b *Backend) persistMapping(state *sessionState) error {
	state.persistMu.Lock()
	defer state.persistMu.Unlock()
	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	return saveMapping(b.mappingDir, mapping)
}

func (b *Backend) getState(id string) (*sessionState, bool) {
	b.mu.RLock()
	state, exists := b.sessions[id]
	b.mu.RUnlock()
	return state, exists
}

func (b *Backend) beginOperation() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.operations.Add(1)
	return true
}

func (b *Backend) isClosing() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

func (b *Backend) Close() error {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	b.mu.Lock()
	if b.fullyClosed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	states := make([]*sessionState, 0, len(b.sessions))
	for _, state := range b.sessions {
		states = append(states, state)
	}
	b.mu.Unlock()
	for _, state := range states {
		state.mu.Lock()
		if state.cancel != nil {
			state.cancel()
		}
		state.mu.Unlock()
	}
	b.operations.Wait()
	b.client.CloseIdleConnections()
	b.apiKey = ""
	b.mu.Lock()
	b.fullyClosed = true
	b.mu.Unlock()
	return nil
}

func assistantMessage(status runStatus) *agent.Message {
	return &agent.Message{
		ID: localMessageID("assistant", status.RunID), Role: agent.RoleAssistant,
		Content: status.Output, Timestamp: time.Now(),
	}
}

func localMessageID(role, runID string) string {
	digest := sha256.Sum256([]byte(role + "\x00" + runID))
	return "hermes-local-" + hex.EncodeToString(digest[:8])
}

func appendLocalMessage(state *sessionState, role agent.MessageRole, content, id string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	for index := range state.session.Messages {
		if state.session.Messages[index].ID == id {
			state.session.Messages[index].Content = content
			state.session.Messages[index].Timestamp = time.Now()
			state.session.UpdatedAt = state.session.Messages[index].Timestamp
			return
		}
	}
	now := time.Now()
	state.session.Messages = append(state.session.Messages, agent.Message{
		ID: id, Role: role, Content: content, Timestamp: now,
	})
	state.session.UpdatedAt = now
}

func sessionIDOf(state *sessionState) string {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.session.ID
}

func cloneSession(session *agent.Session) *agent.Session {
	if session == nil {
		return nil
	}
	cloned := *session
	cloned.Messages = append([]agent.Message(nil), session.Messages...)
	for index := range cloned.Messages {
		cloned.Messages[index] = agent.CloneMessage(cloned.Messages[index])
	}
	if session.Memory != nil {
		cloned.Memory = make(map[string]string, len(session.Memory))
		for key, value := range session.Memory {
			cloned.Memory[key] = value
		}
	}
	return &cloned
}

func agentMessageCopy(message *agent.Message) *agent.Message {
	if message == nil {
		return nil
	}
	cloned := *message
	return &cloned
}

func emit(ctx context.Context, streamChan chan<- agent.StreamEvent, event agent.StreamEvent) error {
	if streamChan == nil {
		return nil
	}
	select {
	case streamChan <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ agent.Backend = (*Backend)(nil)
var _ agent.SessionRestorer = (*Backend)(nil)
