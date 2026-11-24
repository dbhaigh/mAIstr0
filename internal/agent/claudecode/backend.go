package claudecode

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/execution"
)

const (
	Name           = "claude-code"
	MinimumVersion = "2.1.286"
	MaximumVersion = "2.2.0"
)

var (
	ErrUnsupportedConfiguration = errors.New("Claude Code backend does not support the requested session configuration")
	ErrUnsupportedOperation     = errors.New("Claude Code backend does not support this operation")
	ErrSessionBusy              = errors.New("session is currently processing another request")
	ErrProcessFailed            = errors.New("Claude Code process failed")
	ErrTurnFailed               = errors.New("Claude Code reported a failed turn")
	ErrProtocol                 = errors.New("Claude Code returned an invalid stream")
)

type Config struct {
	Executable string
	Workspace  string
	APIKey     string
}

type Backend struct {
	runner *execution.Runner
	// mu protects the session map. Release it before acquiring sessionState.mu.
	mu       sync.RWMutex
	sessions map[string]*sessionState
}

type sessionState struct {
	// mu protects the session value, active turn, cancellation function, and CLI resume state.
	mu      sync.Mutex
	session *agent.Session
	active  bool
	cancel  context.CancelFunc
	started bool
}

type streamMessage struct {
	Type      string          `json:"type"`
	Subtype   string          `json:"subtype"`
	SessionID string          `json:"session_id"`
	IsError   bool            `json:"is_error"`
	Result    string          `json:"result"`
	Event     json.RawMessage `json:"event"`
}

type streamEvent struct {
	Delta struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"delta"`
}

func New(config Config) (*Backend, error) {
	if strings.TrimSpace(config.APIKey) == "" {
		return nil, errors.New("Claude Code backend requires ANTHROPIC_API_KEY")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return nil, errors.New("Claude Code user data directory is unavailable")
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = config.Workspace
	runnerConfig.Executable = config.Executable
	runnerConfig.MaxRuntime = 10 * time.Minute
	runnerConfig.MaxConcurrency = 4
	runnerConfig.Environment = map[string]string{"ANTHROPIC_API_KEY": config.APIKey}
	if runtime.GOOS == "windows" {
		runnerConfig.Environment["USERPROFILE"] = home
	} else {
		runnerConfig.Environment["HOME"] = home
	}
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		return nil, err
	}
	return newBackendWithRunner(runner)
}

func newBackendWithRunner(runner *execution.Runner) (*Backend, error) {
	versionCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var versionOutput string
	err := runner.RunLines(versionCtx, []string{"--version"}, nil, func(line []byte) error {
		versionOutput = strings.TrimSpace(string(line))
		return nil
	})
	if err != nil {
		_ = runner.Close()
		return nil, errors.New("Claude Code version check failed")
	}
	if err := ValidateVersion(versionOutput); err != nil {
		_ = runner.Close()
		return nil, err
	}
	return &Backend{runner: runner, sessions: make(map[string]*sessionState)}, nil
}

func NewFromEnvironment() (*Backend, bool, error) {
	executable, hasExecutable := os.LookupEnv("MAISTR0_CLAUDE_CODE_EXECUTABLE")
	workspace, hasWorkspace := os.LookupEnv("MAISTR0_CLAUDE_CODE_WORKSPACE")
	if !hasExecutable && !hasWorkspace {
		return nil, false, nil
	}
	if strings.TrimSpace(executable) == "" || strings.TrimSpace(workspace) == "" {
		return nil, true, errors.New("Claude Code opt-in requires both MAISTR0_CLAUDE_CODE_EXECUTABLE and MAISTR0_CLAUDE_CODE_WORKSPACE")
	}
	backend, err := New(Config{
		Executable: executable,
		Workspace:  workspace,
		APIKey:     os.Getenv("ANTHROPIC_API_KEY"),
	})
	return backend, true, err
}

func ValidateVersion(output string) error {
	fields := strings.Fields(output)
	if len(fields) == 0 {
		return errors.New("Claude Code version output is empty")
	}
	for _, field := range fields {
		parts := strings.Split(strings.Trim(strings.TrimPrefix(field, "v"), "(),"), ".")
		if len(parts) != 3 {
			continue
		}
		major, majorErr := strconv.Atoi(parts[0])
		minor, minorErr := strconv.Atoi(parts[1])
		patch, patchErr := strconv.Atoi(parts[2])
		if majorErr == nil && minorErr == nil && patchErr == nil &&
			major == 2 && minor == 1 && patch >= 286 {
			return nil
		}
	}
	return fmt.Errorf("Claude Code version is outside the supported range %s to < %s", MinimumVersion, MaximumVersion)
}

func (b *Backend) Name() string { return Name }

func (b *Backend) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	if cfg.CoordinatorModel != "" || cfg.MaxSteps != 0 || cfg.Temperature != 0 || cfg.SystemPrompt != "" {
		return nil, ErrUnsupportedConfiguration
	}
	id, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("create Claude Code session ID: %w", err)
	}
	if title == "" {
		title = "Claude Code Session " + time.Now().Format("Jan 02 15:04")
	}
	now := time.Now()
	session := &agent.Session{
		ID: id, Title: title, Harness: Name, CreatedAt: now, UpdatedAt: now,
		Messages: []agent.Message{}, Memory: map[string]string{}, Config: cfg,
	}
	state := &sessionState{session: session}
	b.mu.Lock()
	b.sessions[id] = state
	b.mu.Unlock()
	return cloneSession(session), nil
}

func (b *Backend) RestoreSession(session *agent.Session) error {
	if session == nil || !validSessionID(session.ID) {
		return errors.New("agent session and ID are required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.sessions[session.ID]; exists {
		return errors.New("Claude Code session already exists")
	}
	restored := cloneSession(session)
	restored.Harness = Name
	restored.Active = false
	b.sessions[restored.ID] = &sessionState{session: restored, started: true}
	return nil
}

func (b *Backend) GetSession(id string) (*agent.Session, bool) {
	b.mu.RLock()
	state, exists := b.sessions[id]
	b.mu.RUnlock()
	if !exists {
		return nil, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
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
		sessions = append(sessions, cloneSession(state.session))
		state.mu.Unlock()
	}
	return sessions
}

func (b *Backend) DeleteSession(id string) bool {
	b.mu.RLock()
	state, exists := b.sessions[id]
	b.mu.RUnlock()
	if !exists {
		return false
	}
	state.mu.Lock()
	if state.cancel != nil {
		state.cancel()
	}
	state.mu.Unlock()
	b.mu.Lock()
	if b.sessions[id] == state {
		delete(b.sessions, id)
		b.mu.Unlock()
		return true
	}
	b.mu.Unlock()
	return false
}

func (b *Backend) InterruptSession(id string) bool {
	b.mu.RLock()
	state, exists := b.sessions[id]
	b.mu.RUnlock()
	if !exists {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.active || state.cancel == nil {
		return false
	}
	state.cancel()
	return true
}

func (b *Backend) SendMessage(ctx context.Context, sessionID, userText string, streamChan chan<- agent.StreamEvent) (*agent.Message, error) {
	if ctx == nil || strings.TrimSpace(userText) == "" {
		return nil, errors.New("context and user message are required")
	}
	b.mu.RLock()
	state, exists := b.sessions[sessionID]
	b.mu.RUnlock()
	if !exists {
		return nil, errors.New("session not found")
	}
	turnCtx, cancel := context.WithCancel(ctx)
	state.mu.Lock()
	if state.active {
		state.mu.Unlock()
		cancel()
		return nil, ErrSessionBusy
	}
	state.active = true
	state.cancel = cancel
	state.session.Active = true
	state.session.UpdatedAt = time.Now()
	state.session.Messages = append(state.session.Messages, agent.Message{
		ID: messageID(), Role: agent.RoleUser, Content: userText, Timestamp: time.Now(),
	})
	resume := state.started
	state.mu.Unlock()

	startedAt := time.Now()
	defer func() {
		cancel()
		state.mu.Lock()
		state.active = false
		state.cancel = nil
		state.session.Active = false
		state.session.UpdatedAt = time.Now()
		state.mu.Unlock()
	}()

	args := claudeArgs(sessionID, resume)
	var finalText string
	var finalSeen bool
	var turnFailed bool
	var protocolFailed bool
	var sessionStarted bool
	err := b.runner.RunLines(turnCtx, args, []byte(userText), func(line []byte) error {
		var event streamMessage
		if json.Unmarshal(line, &event) != nil {
			protocolFailed = true
			return ErrProtocol
		}
		if event.SessionID != "" && event.SessionID != sessionID {
			protocolFailed = true
			return ErrProtocol
		}
		if event.Type == "system" && event.Subtype == "init" && event.SessionID == sessionID {
			sessionStarted = true
		}
		switch event.Type {
		case "stream_event":
			var data streamEvent
			if json.Unmarshal(event.Event, &data) != nil {
				protocolFailed = true
				return ErrProtocol
			}
			if data.Delta.Type == "text_delta" && data.Delta.Text != "" {
				return emit(turnCtx, streamChan, agent.StreamEvent{
					Type: agent.EventTextDelta, SessionID: sessionID, Content: data.Delta.Text,
				})
			}
		case "result":
			if event.SessionID != sessionID {
				protocolFailed = true
				return ErrProtocol
			}
			finalText = event.Result
			finalSeen = true
			sessionStarted = true
			turnFailed = event.IsError || event.Subtype != "success"
		}
		return nil
	})
	if sessionStarted {
		state.mu.Lock()
		state.started = true
		state.mu.Unlock()
	}
	if err != nil {
		if protocolFailed {
			return nil, ErrProtocol
		}
		if errors.Is(turnCtx.Err(), context.Canceled) || errors.Is(turnCtx.Err(), context.DeadlineExceeded) {
			message := interruptedMessage(time.Since(startedAt))
			state.mu.Lock()
			state.session.Messages = append(state.session.Messages, message)
			state.session.UpdatedAt = time.Now()
			state.mu.Unlock()
			emitInterrupted(streamChan, agent.StreamEvent{
				Type: agent.EventInterrupted, SessionID: sessionID, Content: message.Content, Message: &message,
			})
			return &message, turnCtx.Err()
		}
		return nil, ErrProcessFailed
	}
	if !finalSeen {
		return nil, ErrProtocol
	}
	if turnFailed {
		return nil, ErrTurnFailed
	}
	final := &agent.Message{
		ID: messageID(), Role: agent.RoleAssistant, Content: finalText,
		Timestamp: time.Now(), DurationMs: time.Since(startedAt).Milliseconds(),
	}
	state.mu.Lock()
	state.started = true
	state.session.Messages = append(state.session.Messages, *final)
	state.session.UpdatedAt = final.Timestamp
	state.mu.Unlock()
	if err := emit(turnCtx, streamChan, agent.StreamEvent{
		Type: agent.EventFinalMessage, SessionID: sessionID, Message: final, Content: final.Content,
	}); err != nil {
		return nil, err
	}
	if err := emit(turnCtx, streamChan, agent.StreamEvent{Type: agent.EventDone, SessionID: sessionID}); err != nil {
		return nil, err
	}
	return final, nil
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
		if state.active {
			stats.ActiveSessions++
		}
		stats.TotalMessages += len(state.session.Messages)
		state.mu.Unlock()
	}
	return stats
}

func (b *Backend) ExecuteTool(context.Context, string, string, map[string]any) (agent.ToolResult, error) {
	return agent.ToolResult{}, ErrUnsupportedOperation
}

func (b *Backend) Close() error {
	b.mu.Lock()
	states := make([]*sessionState, 0, len(b.sessions))
	for id, state := range b.sessions {
		states = append(states, state)
		delete(b.sessions, id)
	}
	b.mu.Unlock()
	for _, state := range states {
		state.mu.Lock()
		if state.cancel != nil {
			state.cancel()
		}
		state.mu.Unlock()
	}
	return b.runner.Close()
}

func claudeArgs(sessionID string, resume bool) []string {
	args := []string{
		"--print",
		"--output-format", "stream-json",
		"--verbose",
		"--include-partial-messages",
		"--bare",
		"--restricted",
		"--tools", "",
		"--disallowedTools", "mcp__*",
		"--strict-mcp-config",
		"--mcp-config", `{"mcpServers":{}}`,
		"--permission-mode", "plan",
		"--permission-prompts", "none",
		"--disable-slash-commands",
	}
	if resume {
		return append(args, "--resume", sessionID)
	}
	return append(args, "--session-id", sessionID)
}

func emit(ctx context.Context, stream chan<- agent.StreamEvent, event agent.StreamEvent) error {
	if stream == nil {
		return nil
	}
	select {
	case stream <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func emitInterrupted(stream chan<- agent.StreamEvent, event agent.StreamEvent) {
	if stream == nil {
		return
	}
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case stream <- event:
	case <-timer.C:
	}
}

func interruptedMessage(duration time.Duration) agent.Message {
	return agent.Message{
		ID: messageID(), Role: agent.RoleAssistant, Content: "Session interrupted.",
		Timestamp: time.Now(), DurationMs: duration.Milliseconds(),
	}
}

func cloneSession(session *agent.Session) *agent.Session {
	cloned := *session
	cloned.Messages = append([]agent.Message(nil), session.Messages...)
	cloned.Memory = make(map[string]string, len(session.Memory))
	for key, value := range session.Memory {
		cloned.Memory[key] = value
	}
	return &cloned
}

func newSessionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func validSessionID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	for index, char := range id {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func messageID() string {
	return fmt.Sprintf("msg_%d", time.Now().UnixNano())
}

var _ agent.Backend = (*Backend)(nil)
var _ agent.SessionRestorer = (*Backend)(nil)
