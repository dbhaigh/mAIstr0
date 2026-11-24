package geminicli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/execution"
)

const (
	Name           = "gemini-cli"
	MinimumVersion = "0.70.0"
	MaximumVersion = "0.71.0"

	mappingDirectoryName = "maistr0-sessions"
	cliHomeDirectoryName = "maistr0-cli-home"
)

var (
	ErrUnsupportedConfiguration = errors.New("Gemini CLI backend does not support the requested session configuration")
	ErrUnsupportedOperation     = errors.New("Gemini CLI backend does not support this operation")
	ErrSessionBusy              = errors.New("session is currently processing another request")
	ErrSessionUnavailable       = errors.New("Gemini CLI session cannot be resumed")
	ErrSessionMapping           = errors.New("Gemini CLI session mapping is unavailable")
	ErrProcessFailed            = errors.New("Gemini CLI process failed")
	ErrTurnFailed               = errors.New("Gemini CLI reported a failed turn")
	ErrProtocol                 = errors.New("Gemini CLI returned an invalid event stream")
	ErrPolicyUnavailable        = errors.New("Gemini CLI cannot enforce the required execution policy")
)

type Config struct {
	Executable                   string
	Workspace                    string
	Home                         string
	Sandbox                      string
	AuthMode                     string
	APIKey                       string
	VertexProject                string
	VertexLocation               string
	GoogleApplicationCredentials string
}

type Backend struct {
	runner     *execution.Runner
	workspace  string
	mappingDir string
	policyPath string
	version    string

	mu        sync.RWMutex
	sessions  map[string]*sessionState
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

type sessionState struct {
	// mu protects the session value, native session identity, active turn, and deletion state.
	mu       sync.Mutex
	session  *agent.Session
	active   bool
	started  bool
	deleted  bool
	deleting bool
	cancel   context.CancelFunc
	done     chan struct{}
}

type streamEvent struct {
	Type       string         `json:"type"`
	SessionID  string         `json:"session_id"`
	Model      string         `json:"model"`
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	Delta      *bool          `json:"delta"`
	ToolName   string         `json:"tool_name"`
	ToolID     string         `json:"tool_id"`
	Parameters map[string]any `json:"parameters"`
	Status     string         `json:"status"`
	Output     string         `json:"output"`
	Severity   string         `json:"severity"`
	Error      *streamError   `json:"error"`
}

type streamError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type toolState struct {
	call      agent.ToolCall
	response  agent.ToolResponse
	completed bool
}

func New(config Config) (*Backend, error) {
	workspace, err := canonicalDirectory(config.Workspace)
	if err != nil {
		return nil, errors.New("Gemini CLI backend requires an existing absolute workspace")
	}
	home, err := canonicalDirectory(config.Home)
	if err != nil {
		return nil, errors.New("Gemini CLI backend requires an existing absolute Gemini home")
	}
	if pathWithin(workspace, home) {
		return nil, errors.New("Gemini CLI home must be outside the workspace")
	}
	sandbox, err := validateSandbox(config.Sandbox)
	if err != nil {
		return nil, err
	}
	if config.AuthMode == "vertex-service-account" {
		resolved, err := filepath.EvalSymlinks(config.GoogleApplicationCredentials)
		if err != nil || !filepath.IsAbs(config.GoogleApplicationCredentials) || pathWithin(workspace, resolved) {
			return nil, errors.New("Vertex service-account credentials must be an absolute file outside the workspace")
		}
		config.GoogleApplicationCredentials = resolved
	}
	authEnvironment, err := authenticationEnvironment(config)
	if err != nil {
		return nil, err
	}
	cliHome, err := ensurePrivateDirectory(filepath.Join(home, cliHomeDirectoryName))
	if err != nil {
		return nil, errors.New("Gemini CLI home could not be isolated")
	}
	if err := rejectCachedCredentials(cliHome); err != nil {
		return nil, err
	}
	mappingDir, err := ensurePrivateDirectory(filepath.Join(home, mappingDirectoryName))
	if err != nil {
		return nil, ErrSessionMapping
	}
	policyPath, err := writeSystemPolicy(cliHome, sandbox)
	if err != nil {
		return nil, errors.New("Gemini CLI system policy could not be created")
	}

	homeVariable := "HOME"
	if runtime.GOOS == "windows" {
		homeVariable = "USERPROFILE"
	}
	environment := map[string]string{
		"GEMINI_CLI_SYSTEM_SETTINGS_PATH": policyPath,
		"GEMINI_SANDBOX":                  sandbox,
		"PATH":                            os.Getenv("PATH"),
		homeVariable:                      cliHome,
	}
	for name, value := range authEnvironment {
		environment[name] = value
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = workspace
	runnerConfig.Executable = config.Executable
	runnerConfig.MaxRuntime = 10 * time.Minute
	runnerConfig.MaxInputBytes = 8 << 20
	runnerConfig.MaxOutputBytes = 8 << 20
	runnerConfig.MaxConcurrency = 4
	runnerConfig.Environment = environment
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		_ = os.Remove(policyPath)
		return nil, err
	}
	backend, err := newBackendWithRunner(runner, workspace, mappingDir, policyPath)
	if err != nil {
		_ = runner.Close()
		_ = os.Remove(policyPath)
		return nil, err
	}
	return backend, nil
}

func newBackendWithRunner(runner *execution.Runner, workspace, mappingDir, policyPath string) (*Backend, error) {
	backend := &Backend{
		runner: runner, workspace: workspace, mappingDir: mappingDir, policyPath: policyPath,
		sessions: make(map[string]*sessionState), closeDone: make(chan struct{}),
	}
	versionCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var versionOutput string
	err := runner.RunLines(versionCtx, []string{"--version"}, nil, func(line []byte) error {
		if versionOutput != "" {
			return ErrProtocol
		}
		versionOutput = strings.TrimSpace(string(line))
		return nil
	})
	if err != nil {
		return nil, errors.New("Gemini CLI version check failed")
	}
	version, err := supportedVersion(versionOutput)
	if err != nil {
		return nil, err
	}
	backend.version = version
	if err := backend.verifyPolicyFlags(); err != nil {
		return nil, err
	}
	return backend, nil
}

func (b *Backend) verifyPolicyFlags() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var help strings.Builder
	err := b.runner.RunLines(ctx, []string{"--help"}, nil, func(line []byte) error {
		help.Write(line)
		help.WriteByte('\n')
		return nil
	})
	if err != nil {
		return ErrPolicyUnavailable
	}
	for _, required := range []string{
		"--output-format", "--approval-mode", "--sandbox", "--session-id", "--resume", "--prompt",
	} {
		if !strings.Contains(help.String(), required) {
			return ErrPolicyUnavailable
		}
	}
	return nil
}

func NewFromEnvironment() (*Backend, bool, error) {
	names := []string{
		"MAISTR0_GEMINI_EXECUTABLE", "MAISTR0_GEMINI_WORKSPACE", "MAISTR0_GEMINI_HOME",
		"MAISTR0_GEMINI_SANDBOX", "MAISTR0_GEMINI_AUTH_MODE",
	}
	enabled := false
	for _, name := range names {
		if _, exists := os.LookupEnv(name); exists {
			enabled = true
			break
		}
	}
	if !enabled {
		return nil, false, nil
	}
	executable := strings.TrimSpace(os.Getenv("MAISTR0_GEMINI_EXECUTABLE"))
	workspace := strings.TrimSpace(os.Getenv("MAISTR0_GEMINI_WORKSPACE"))
	home := strings.TrimSpace(os.Getenv("MAISTR0_GEMINI_HOME"))
	sandbox := strings.TrimSpace(os.Getenv("MAISTR0_GEMINI_SANDBOX"))
	authMode := strings.TrimSpace(os.Getenv("MAISTR0_GEMINI_AUTH_MODE"))
	if executable == "" || workspace == "" || home == "" || sandbox == "" || authMode == "" {
		return nil, true, errors.New("Gemini CLI opt-in requires MAISTR0_GEMINI_EXECUTABLE, MAISTR0_GEMINI_WORKSPACE, MAISTR0_GEMINI_HOME, MAISTR0_GEMINI_SANDBOX, and MAISTR0_GEMINI_AUTH_MODE")
	}
	config := Config{
		Executable:     executable,
		Workspace:      workspace,
		Home:           home,
		Sandbox:        sandbox,
		AuthMode:       authMode,
		VertexProject:  os.Getenv("GOOGLE_CLOUD_PROJECT"),
		VertexLocation: os.Getenv("GOOGLE_CLOUD_LOCATION"),
	}
	switch authMode {
	case "api-key":
		config.APIKey = os.Getenv("GEMINI_API_KEY")
	case "vertex-api-key":
		config.APIKey = os.Getenv("GOOGLE_API_KEY")
	case "vertex-service-account":
		config.GoogleApplicationCredentials = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	}
	backend, err := New(config)
	return backend, true, err
}

func ValidateVersion(output string) error {
	_, err := supportedVersion(output)
	return err
}

func supportedVersion(output string) (string, error) {
	var version string
	for _, field := range strings.Fields(output) {
		token := strings.Trim(strings.TrimPrefix(field, "v"), "(),")
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			continue
		}
		major, majorErr := strconv.Atoi(parts[0])
		minor, minorErr := strconv.Atoi(parts[1])
		patch, patchErr := strconv.Atoi(parts[2])
		if majorErr == nil && minorErr == nil && patchErr == nil &&
			major >= 0 && minor >= 0 && patch >= 0 {
			version = fmt.Sprintf("%d.%d.%d", major, minor, patch)
			break
		}
	}
	if version == "" {
		return "", versionError()
	}
	parts := strings.Split(version, ".")
	major, _ := strconv.Atoi(parts[0])
	minor, _ := strconv.Atoi(parts[1])
	if major != 0 || minor != 70 {
		return "", versionError()
	}
	return version, nil
}

func versionError() error {
	return fmt.Errorf("Gemini CLI version is outside the supported range %s to < %s", MinimumVersion, MaximumVersion)
}

func (b *Backend) Name() string { return Name }

func (b *Backend) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	if cfg.CoordinatorModel != "" || cfg.MaxSteps != 0 || cfg.Temperature != 0 || cfg.SystemPrompt != "" {
		return nil, ErrUnsupportedConfiguration
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
	state := &sessionState{session: session}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, execution.ErrClosed
	}
	b.sessions[id] = state
	return cloneSession(session), nil
}

func (b *Backend) RestoreSession(session *agent.Session) error {
	if session == nil || !validSessionID(session.ID) || session.Harness != Name {
		return errors.New("Gemini CLI session and ID are required")
	}
	started, err := b.hasSessionMapping(session.ID)
	if err != nil {
		return ErrSessionMapping
	}
	if !started && len(session.Messages) != 0 {
		return ErrSessionUnavailable
	}
	copy := cloneSession(session)
	state := &sessionState{session: copy, started: started}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return execution.ErrClosed
	}
	if _, exists := b.sessions[session.ID]; exists {
		return errors.New("Gemini CLI session already exists")
	}
	b.sessions[session.ID] = state
	return nil
}

func (b *Backend) GetSession(id string) (*agent.Session, bool) {
	b.mu.RLock()
	state := b.sessions[id]
	b.mu.RUnlock()
	if state == nil {
		return nil, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleted || state.deleting {
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
		if !state.deleted && !state.deleting {
			sessions = append(sessions, cloneSession(state.session))
		}
		state.mu.Unlock()
	}
	return sessions
}

func (b *Backend) DeleteSession(id string) bool {
	b.mu.Lock()
	state := b.sessions[id]
	if state == nil {
		b.mu.Unlock()
		return false
	}
	state.mu.Lock()
	if state.active || state.deleted || state.deleting {
		state.mu.Unlock()
		b.mu.Unlock()
		return false
	}
	state.deleting = true
	state.mu.Unlock()
	b.mu.Unlock()

	removeErr := b.removeSessionMapping(id)
	b.mu.Lock()
	if b.sessions[id] != state {
		b.mu.Unlock()
		return false
	}
	state.mu.Lock()
	state.deleting = false
	if removeErr != nil {
		state.mu.Unlock()
		b.mu.Unlock()
		return false
	}
	state.deleted = true
	delete(b.sessions, id)
	state.mu.Unlock()
	b.mu.Unlock()
	return true
}

func (b *Backend) SendMessage(ctx context.Context, sessionID, userText string, streamChan chan<- agent.StreamEvent) (*agent.Message, error) {
	if ctx == nil || strings.TrimSpace(userText) == "" {
		return nil, errors.New("context and user message are required")
	}
	b.mu.RLock()
	state := b.sessions[sessionID]
	b.mu.RUnlock()
	if state == nil {
		return nil, errors.New("session not found")
	}
	turnCtx, cancel := context.WithCancel(ctx)
	state.mu.Lock()
	if state.deleted || state.deleting {
		state.mu.Unlock()
		cancel()
		return nil, errors.New("session not found")
	}
	if state.active {
		state.mu.Unlock()
		cancel()
		return nil, ErrSessionBusy
	}
	state.active = true
	state.cancel = cancel
	state.done = make(chan struct{})
	state.session.Active = true
	state.session.UpdatedAt = time.Now()
	resume := state.started
	done := state.done
	state.mu.Unlock()

	startedAt := time.Now()
	defer func() {
		cancel()
		state.mu.Lock()
		state.active = false
		state.cancel = nil
		state.done = nil
		state.session.Active = false
		state.session.UpdatedAt = time.Now()
		close(done)
		state.mu.Unlock()
	}()
	if resume {
		if _, err := b.loadSessionMapping(sessionID); err != nil {
			return nil, ErrSessionUnavailable
		}
	}
	state.mu.Lock()
	if state.deleted || state.deleting {
		state.mu.Unlock()
		return nil, errors.New("session not found")
	}
	state.session.Messages = append(state.session.Messages, agent.Message{
		ID: messageID(), Role: agent.RoleUser, Content: userText, Timestamp: time.Now(),
	})
	state.mu.Unlock()

	tools := make(map[string]*toolState)
	var toolOrder []string
	var finalText strings.Builder
	var deltaText strings.Builder
	var hasDelta bool
	var model string
	var initialized, assistantSeen bool
	var terminal string
	var protocolFailed, turnFailed, mappingFailed, errorEmitted bool
	err := b.runner.RunLines(turnCtx, b.commandArgs(sessionID, resume), []byte(userText), func(line []byte) error {
		var event streamEvent
		if json.Unmarshal(line, &event) != nil || event.Type == "" || terminal != "" {
			protocolFailed = true
			return ErrProtocol
		}
		switch event.Type {
		case "init":
			if initialized || !validSessionID(event.SessionID) || event.SessionID != sessionID {
				protocolFailed = true
				return ErrProtocol
			}
			if !resume {
				if err := b.saveSessionMapping(sessionID); err != nil {
					mappingFailed = true
					return ErrSessionMapping
				}
			} else if _, err := b.loadSessionMapping(sessionID); err != nil {
				mappingFailed = true
				return ErrSessionMapping
			}
			state.mu.Lock()
			state.started = true
			state.mu.Unlock()
			initialized = true
			model = event.Model
		case "message":
			if !initialized || (event.Role != "assistant" && event.Role != "user") {
				protocolFailed = true
				return ErrProtocol
			}
			if event.Role == "user" {
				return nil
			}
			assistantSeen = true
			appendContent := event.Content
			if event.Delta != nil && *event.Delta {
				hasDelta = true
				deltaText.WriteString(event.Content)
			} else if hasDelta {
				current := deltaText.String()
				switch {
				case event.Content == current:
					appendContent = ""
				case strings.HasPrefix(event.Content, current):
					appendContent = event.Content[len(current):]
				case strings.HasPrefix(current, event.Content):
					appendContent = ""
				}
				deltaText.Reset()
				hasDelta = false
			}
			if appendContent != "" {
				finalText.WriteString(appendContent)
				if err := emit(turnCtx, streamChan, agent.StreamEvent{
					Type: agent.EventTextDelta, SessionID: sessionID, Content: appendContent, Model: model,
				}); err != nil {
					return err
				}
			}
		case "tool_use":
			if !initialized || event.ToolID == "" || event.ToolName == "" || tools[event.ToolID] != nil {
				protocolFailed = true
				return ErrProtocol
			}
			if event.Parameters == nil {
				event.Parameters = map[string]any{}
			}
			call := agent.ToolCall{ID: event.ToolID, Name: event.ToolName, Arguments: event.Parameters}
			tools[event.ToolID] = &toolState{call: call}
			toolOrder = append(toolOrder, event.ToolID)
			deltaText.Reset()
			hasDelta = false
			if err := emit(turnCtx, streamChan, agent.StreamEvent{
				Type: agent.EventToolCall, SessionID: sessionID, ToolCall: &call, Model: model,
			}); err != nil {
				return err
			}
		case "tool_result":
			if !initialized {
				protocolFailed = true
				return ErrProtocol
			}
			tool := tools[event.ToolID]
			if tool == nil || tool.completed || (event.Status != "success" && event.Status != "error") {
				protocolFailed = true
				return ErrProtocol
			}
			response := agent.ToolResponse{
				ToolCallID: event.ToolID, Name: tool.call.Name, Output: event.Output, Model: model,
			}
			if event.Status == "error" || event.Error != nil {
				response.Error = "tool execution failed"
			}
			tool.response = response
			tool.completed = true
			deltaText.Reset()
			hasDelta = false
			if err := emit(turnCtx, streamChan, agent.StreamEvent{
				Type: agent.EventToolResponse, SessionID: sessionID, ToolResp: &response, Model: model,
			}); err != nil {
				return err
			}
		case "error":
			if event.Severity != "warning" && event.Severity != "error" {
				protocolFailed = true
				return ErrProtocol
			}
			if event.Severity == "error" {
				turnFailed = true
				errorEmitted = true
				if err := emit(turnCtx, streamChan, agent.StreamEvent{
					Type: agent.EventError, SessionID: sessionID, Error: ErrTurnFailed.Error(),
				}); err != nil {
					return err
				}
			}
		case "result":
			if event.Status != "success" && event.Status != "error" {
				protocolFailed = true
				return ErrProtocol
			}
			if event.Status == "success" && (!initialized || !assistantSeen) {
				protocolFailed = true
				return ErrProtocol
			}
			if event.Status == "success" {
				for _, id := range toolOrder {
					if !tools[id].completed {
						protocolFailed = true
						return ErrProtocol
					}
				}
			}
			terminal = event.Status
			if event.Status == "error" || event.Error != nil {
				turnFailed = true
				if !errorEmitted {
					errorEmitted = true
					if err := emit(turnCtx, streamChan, agent.StreamEvent{
						Type: agent.EventError, SessionID: sessionID, Error: ErrTurnFailed.Error(),
					}); err != nil {
						return err
					}
				}
			}
		default:
			protocolFailed = true
			return ErrProtocol
		}
		return nil
	})
	if err != nil {
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
		if protocolFailed {
			return nil, ErrProtocol
		}
		if mappingFailed {
			return nil, ErrSessionMapping
		}
		if turnFailed || terminal == "error" {
			return nil, ErrTurnFailed
		}
		return nil, ErrProcessFailed
	}
	if protocolFailed {
		return nil, ErrProtocol
	}
	if mappingFailed {
		return nil, ErrSessionMapping
	}
	if turnFailed || terminal == "error" {
		return nil, ErrTurnFailed
	}
	if terminal != "success" || !initialized || !assistantSeen {
		return nil, ErrProtocol
	}
	final := &agent.Message{
		ID: messageID(), Role: agent.RoleAssistant, Content: finalText.String(),
		ToolCalls: make([]agent.ToolCall, 0, len(toolOrder)), Model: model,
		Timestamp: time.Now(), DurationMs: time.Since(startedAt).Milliseconds(),
	}
	for _, id := range toolOrder {
		final.ToolCalls = append(final.ToolCalls, tools[id].call)
	}
	state.mu.Lock()
	state.session.Messages = append(state.session.Messages, *final)
	for _, id := range toolOrder {
		tool := tools[id]
		response := tool.response
		content, _ := response.Output.(string)
		state.session.Messages = append(state.session.Messages, agent.Message{
			ID: messageID(), Role: agent.RoleTool, Content: content, ToolResponse: &response,
			Model: model, Timestamp: time.Now(),
		})
	}
	state.session.UpdatedAt = final.Timestamp
	state.mu.Unlock()
	if err := emit(turnCtx, streamChan, agent.StreamEvent{
		Type: agent.EventFinalMessage, SessionID: sessionID, Message: final, Content: final.Content, Model: model,
	}); err != nil {
		return nil, err
	}
	if err := emit(turnCtx, streamChan, agent.StreamEvent{Type: agent.EventDone, SessionID: sessionID}); err != nil {
		return nil, err
	}
	return final, nil
}

func (b *Backend) commandArgs(sessionID string, resume bool) []string {
	args := []string{
		"--output-format", "stream-json",
		"--prompt", " ",
		"--sandbox",
		"--approval-mode=plan",
	}
	if resume {
		return append(args, "--resume", sessionID)
	}
	return append(args, "--session-id", sessionID)
}

func (b *Backend) saveSessionMapping(sessionID string) error {
	if !validSessionID(sessionID) {
		return ErrSessionMapping
	}
	path := b.mappingPath(sessionID)
	if _, err := os.Lstat(path); err == nil {
		if _, loadErr := b.loadSessionMapping(sessionID); loadErr == nil {
			return nil
		}
		return ErrSessionMapping
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrSessionMapping
	}
	temp, err := os.CreateTemp(b.mappingDir, ".gemini-session-*")
	if err != nil {
		return ErrSessionMapping
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return ErrSessionMapping
	}
	if _, err := temp.WriteString(sessionID + "\n"); err != nil {
		_ = temp.Close()
		return ErrSessionMapping
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return ErrSessionMapping
	}
	if err := temp.Close(); err != nil {
		return ErrSessionMapping
	}
	if err := os.Rename(tempPath, path); err != nil {
		return ErrSessionMapping
	}
	return nil
}

func (b *Backend) loadSessionMapping(sessionID string) (string, error) {
	if !validSessionID(sessionID) {
		return "", ErrSessionMapping
	}
	info, err := os.Lstat(b.mappingPath(sessionID))
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrSessionMapping
	}
	data, err := os.ReadFile(b.mappingPath(sessionID))
	if err != nil {
		return "", ErrSessionMapping
	}
	mapped := strings.TrimSpace(string(data))
	if mapped != sessionID {
		return "", ErrSessionMapping
	}
	return mapped, nil
}

func (b *Backend) hasSessionMapping(sessionID string) (bool, error) {
	_, err := b.loadSessionMapping(sessionID)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b *Backend) removeSessionMapping(sessionID string) error {
	if !validSessionID(sessionID) {
		return ErrSessionMapping
	}
	err := os.Remove(b.mappingPath(sessionID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrSessionMapping
	}
	return nil
}

func (b *Backend) mappingPath(sessionID string) string {
	return filepath.Join(b.mappingDir, sessionID+".session")
}

func (b *Backend) InterruptSession(id string) bool {
	b.mu.RLock()
	state := b.sessions[id]
	b.mu.RUnlock()
	if state == nil {
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

func (b *Backend) ListTools() []agent.ToolDef           { return []agent.ToolDef{} }
func (b *Backend) ListClusterModels() []agent.ModelInfo { return []agent.ModelInfo{} }

func (b *Backend) Stats() agent.AgentStats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	stats := agent.AgentStats{TotalSessions: len(b.sessions), DispatchedLoad: map[string]int{}}
	for _, state := range b.sessions {
		state.mu.Lock()
		stats.TotalMessages += len(state.session.Messages)
		if state.active {
			stats.ActiveSessions++
		}
		for _, message := range state.session.Messages {
			stats.TotalToolCalls += len(message.ToolCalls)
		}
		state.mu.Unlock()
	}
	return stats
}

func (b *Backend) ExecuteTool(context.Context, string, string, map[string]any) (agent.ToolResult, error) {
	return agent.ToolResult{}, ErrUnsupportedOperation
}

func (b *Backend) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		states := make([]*sessionState, 0, len(b.sessions))
		for id, state := range b.sessions {
			states = append(states, state)
			delete(b.sessions, id)
		}
		b.mu.Unlock()
		for _, state := range states {
			state.mu.Lock()
			state.deleted = true
			if state.cancel != nil {
				state.cancel()
			}
			done := state.done
			state.mu.Unlock()
			if done != nil {
				<-done
			}
		}
		b.closeErr = b.runner.Close()
		if err := os.Remove(b.policyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			b.closeErr = errors.Join(b.closeErr, errors.New("Gemini CLI system policy cleanup failed"))
		}
		close(b.closeDone)
	})
	<-b.closeDone
	return b.closeErr
}

func authenticationEnvironment(config Config) (map[string]string, error) {
	switch config.AuthMode {
	case "api-key":
		if strings.TrimSpace(config.APIKey) == "" {
			return nil, errors.New("Gemini API-key authentication requires GEMINI_API_KEY")
		}
		return map[string]string{"GEMINI_API_KEY": config.APIKey}, nil
	case "vertex-api-key":
		if strings.TrimSpace(config.APIKey) == "" || strings.TrimSpace(config.VertexProject) == "" ||
			strings.TrimSpace(config.VertexLocation) == "" {
			return nil, errors.New("Vertex API-key authentication requires GOOGLE_API_KEY, GOOGLE_CLOUD_PROJECT, and GOOGLE_CLOUD_LOCATION")
		}
		return map[string]string{
			"GOOGLE_API_KEY":            config.APIKey,
			"GOOGLE_CLOUD_PROJECT":      config.VertexProject,
			"GOOGLE_CLOUD_LOCATION":     config.VertexLocation,
			"GOOGLE_GENAI_USE_VERTEXAI": "true",
		}, nil
	case "vertex-service-account":
		credentials := config.GoogleApplicationCredentials
		if strings.TrimSpace(config.VertexProject) == "" || strings.TrimSpace(config.VertexLocation) == "" ||
			credentials == "" || !filepath.IsAbs(credentials) {
			return nil, errors.New("Vertex service-account authentication requires an absolute GOOGLE_APPLICATION_CREDENTIALS path, GOOGLE_CLOUD_PROJECT, and GOOGLE_CLOUD_LOCATION")
		}
		info, err := os.Stat(credentials)
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.New("Vertex service-account credentials file is unavailable")
		}
		return map[string]string{
			"GOOGLE_APPLICATION_CREDENTIALS": credentials,
			"GOOGLE_CLOUD_PROJECT":           config.VertexProject,
			"GOOGLE_CLOUD_LOCATION":          config.VertexLocation,
			"GOOGLE_GENAI_USE_VERTEXAI":      "true",
		}, nil
	default:
		return nil, errors.New("Gemini CLI authentication mode is unsupported")
	}
}

func validateSandbox(sandbox string) (string, error) {
	switch sandbox {
	case "windows-native":
		if runtime.GOOS != "windows" {
			return "", ErrPolicyUnavailable
		}
	case "sandbox-exec":
		if runtime.GOOS != "darwin" {
			return "", ErrPolicyUnavailable
		}
	case "docker", "podman":
	case "runsc", "lxc":
		if runtime.GOOS != "linux" {
			return "", ErrPolicyUnavailable
		}
	default:
		return "", ErrPolicyUnavailable
	}
	return sandbox, nil
}

func writeSystemPolicy(home, sandbox string) (string, error) {
	policy := map[string]any{
		"admin": map[string]any{
			"secureModeEnabled": true,
			"extensions":        map[string]any{"enabled": false},
			"mcp":               map[string]any{"enabled": false, "config": map[string]any{}},
		},
		"extensions":  map[string]any{"disabled": []string{"*"}},
		"hooksConfig": map[string]any{"enabled": false},
		"hooks":       map[string]any{},
		"mcp":         map[string]any{"allowed": []string{}, "excluded": []string{"*"}},
		"skills":      map[string]any{"enabled": false},
		"general":     map[string]any{"defaultApprovalMode": "plan"},
		"tools": map[string]any{
			"sandbox":              sandbox,
			"sandboxAllowedPaths":  []string{},
			"sandboxNetworkAccess": true,
		},
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp(home, ".gemini-system-settings-*")
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func rejectCachedCredentials(home string) error {
	geminiHome := filepath.Join(home, ".gemini")
	if info, err := os.Lstat(geminiHome); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("Gemini CLI home contains unsupported configuration or credentials")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("Gemini CLI home contains unsupported configuration or credentials")
	}
	for _, relative := range []string{
		filepath.Join(".gemini", "settings.json"),
		filepath.Join(".gemini", "oauth_creds.json"),
		filepath.Join(".gemini", "google_accounts.json"),
	} {
		info, err := os.Lstat(filepath.Join(home, relative))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("Gemini CLI home contains unsupported configuration or credentials")
		}
		return errors.New("Gemini CLI home contains user configuration or cached credentials")
	}
	return nil
}

func ensurePrivateDirectory(path string) (string, error) {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", execution.ErrInvalidConfig
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		if err := os.Chmod(path, 0o700); err != nil {
			return "", execution.ErrInvalidConfig
		}
	}
	return filepath.Clean(path), nil
}

func canonicalDirectory(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsRune(path, '\x00') {
		return "", execution.ErrInvalidConfig
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", execution.ErrInvalidConfig
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", execution.ErrInvalidConfig
	}
	return filepath.Clean(resolved), nil
}

func pathWithin(parent, candidate string) bool {
	relative, err := filepath.Rel(parent, candidate)
	if err != nil {
		return false
	}
	if relative == "." {
		return true
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
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

var messageSequence atomic.Uint64

func messageID() string {
	return fmt.Sprintf("gemini_msg_%d_%d", time.Now().UnixNano(), messageSequence.Add(1))
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
	for index := range cloned.Messages {
		cloned.Messages[index] = agent.CloneMessage(cloned.Messages[index])
	}
	cloned.Memory = make(map[string]string, len(session.Memory))
	for key, value := range session.Memory {
		cloned.Memory[key] = value
	}
	return &cloned
}

var _ agent.Backend = (*Backend)(nil)
var _ agent.SessionRestorer = (*Backend)(nil)
