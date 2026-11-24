package codexcli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/execution"
)

const (
	Name           = "codex"
	MinimumVersion = "0.171.0"
	MaximumVersion = "0.172.0"

	mappingDirectoryName = "maistr0-sessions"
)

var (
	ErrUnsupportedConfiguration = errors.New("Codex backend does not support the requested session configuration")
	ErrUnsupportedOperation     = errors.New("Codex backend does not support this operation")
	ErrSessionBusy              = errors.New("session is currently processing another request")
	ErrSessionUnavailable       = errors.New("Codex session cannot be resumed")
	ErrSessionMapping           = errors.New("Codex session mapping is unavailable")
	ErrProcessFailed            = errors.New("Codex process failed")
	ErrTurnFailed               = errors.New("Codex reported a failed turn")
	ErrProtocol                 = errors.New("Codex returned an invalid event stream")
	ErrPolicyUnavailable        = errors.New("Codex CLI cannot enforce the required execution policy")
)

type Config struct {
	Executable string
	Workspace  string
	CodexHome  string
}

type Backend struct {
	runner     *execution.Runner
	workspace  string
	mappingDir string
	version    string
	mu         sync.RWMutex
	sessions   map[string]*sessionState
	closed     bool
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error
}

type sessionState struct {
	// mu protects the session value, native thread ID, active turn, and deletion state.
	mu       sync.Mutex
	session  *agent.Session
	threadID string
	active   bool
	started  bool
	deleted  bool
	cancel   context.CancelFunc
	done     chan struct{}
}

type streamEvent struct {
	Type     string          `json:"type"`
	ThreadID string          `json:"thread_id"`
	Item     json.RawMessage `json:"item"`
}

type streamItem struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Text string `json:"text"`
}

func New(config Config) (*Backend, error) {
	workspace, err := canonicalDirectory(config.Workspace)
	if err != nil {
		return nil, errors.New("Codex backend requires an existing absolute workspace")
	}
	codexHome, err := canonicalDirectory(config.CodexHome)
	if err != nil {
		return nil, errors.New("Codex backend requires an existing absolute CODEX_HOME")
	}
	if pathWithin(workspace, codexHome) {
		return nil, errors.New("Codex CODEX_HOME must be outside the workspace")
	}
	mappingDir, err := ensureMappingDirectory(codexHome)
	if err != nil {
		return nil, err
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return nil, errors.New("Codex user home directory is unavailable")
	}
	runnerConfig := execution.DefaultConfig()
	runnerConfig.Enabled = true
	runnerConfig.ApprovedWorkspace = workspace
	runnerConfig.Executable = config.Executable
	runnerConfig.MaxRuntime = 10 * time.Minute
	runnerConfig.MaxConcurrency = 4
	runnerConfig.Environment = map[string]string{
		"CODEX_HOME": codexHome,
		"PATH":       os.Getenv("PATH"),
	}
	if runtime.GOOS == "windows" {
		runnerConfig.Environment["USERPROFILE"] = home
	} else {
		runnerConfig.Environment["HOME"] = home
	}
	runner, err := execution.NewRunner(runnerConfig)
	if err != nil {
		return nil, err
	}
	return newBackendWithRunner(runner, workspace, mappingDir)
}

func newBackendWithRunner(runner *execution.Runner, workspace, mappingDir string) (*Backend, error) {
	backend := &Backend{
		runner: runner, workspace: workspace, mappingDir: mappingDir,
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
		_ = runner.Close()
		return nil, errors.New("Codex CLI version check failed")
	}
	version, err := supportedVersion(versionOutput)
	if err != nil {
		_ = runner.Close()
		return nil, err
	}
	backend.version = version
	if err := backend.verifyPolicyFlags(); err != nil {
		_ = runner.Close()
		return nil, err
	}
	return backend, nil
}

func (b *Backend) verifyPolicyFlags() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var help strings.Builder
	err := b.runner.RunLines(ctx, []string{"exec", "--help"}, nil, func(line []byte) error {
		help.Write(line)
		help.WriteByte('\n')
		return nil
	})
	if err != nil {
		return ErrPolicyUnavailable
	}
	for _, required := range []string{
		"--json", "--sandbox", "--ask-for-approval", "--ignore-user-config", "--strict-config", "--config",
	} {
		if !strings.Contains(help.String(), required) {
			return ErrPolicyUnavailable
		}
	}
	return nil
}

func NewFromEnvironment() (*Backend, bool, error) {
	executable, hasExecutable := os.LookupEnv("MAISTR0_CODEX_EXECUTABLE")
	workspace, hasWorkspace := os.LookupEnv("MAISTR0_CODEX_WORKSPACE")
	codexHome, hasCodexHome := os.LookupEnv("MAISTR0_CODEX_HOME")
	if !hasExecutable && !hasWorkspace && !hasCodexHome {
		return nil, false, nil
	}
	if strings.TrimSpace(executable) == "" || strings.TrimSpace(workspace) == "" ||
		strings.TrimSpace(codexHome) == "" {
		return nil, true, errors.New("Codex opt-in requires MAISTR0_CODEX_EXECUTABLE, MAISTR0_CODEX_WORKSPACE, and MAISTR0_CODEX_HOME")
	}
	backend, err := New(Config{
		Executable: executable,
		Workspace:  workspace,
		CodexHome:  codexHome,
	})
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
		if majorErr == nil && minorErr == nil && patchErr == nil {
			if version != "" {
				return "", fmt.Errorf("Codex CLI version is outside the supported range %s to < %s", MinimumVersion, MaximumVersion)
			}
			version = fmt.Sprintf("%d.%d.%d", major, minor, patch)
		}
	}
	if version == "" {
		return "", fmt.Errorf("Codex CLI version is outside the supported range %s to < %s", MinimumVersion, MaximumVersion)
	}
	parts := strings.Split(version, ".")
	major, _ := strconv.Atoi(parts[0])
	minor, _ := strconv.Atoi(parts[1])
	patch, _ := strconv.Atoi(parts[2])
	if major != 0 || minor != 171 || patch < 0 {
		return "", fmt.Errorf("Codex CLI version is outside the supported range %s to < %s", MinimumVersion, MaximumVersion)
	}
	return version, nil
}

func (b *Backend) Name() string { return Name }

func (b *Backend) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	if cfg.CoordinatorModel != "" || cfg.MaxSteps != 0 || cfg.Temperature != 0 || cfg.SystemPrompt != "" {
		return nil, ErrUnsupportedConfiguration
	}
	id, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("create Codex session ID: %w", err)
	}
	if title == "" {
		title = "Codex Session " + time.Now().Format("Jan 02 15:04")
	}
	now := time.Now()
	session := &agent.Session{
		ID: id, Title: title, Harness: Name, CreatedAt: now, UpdatedAt: now,
		Messages: []agent.Message{}, Memory: map[string]string{}, Config: cfg,
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, execution.ErrClosed
	}
	b.sessions[id] = &sessionState{session: session}
	b.mu.Unlock()
	return cloneSession(session), nil
}

func (b *Backend) RestoreSession(session *agent.Session) error {
	if session == nil || !validSessionID(session.ID) {
		return errors.New("Codex session and ID are required")
	}
	threadID, err := b.loadThreadID(session.ID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) && len(session.Messages) != 0 {
		return ErrSessionMapping
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return execution.ErrClosed
	}
	if _, exists := b.sessions[session.ID]; exists {
		return errors.New("Codex session already exists")
	}
	restored := cloneSession(session)
	restored.Harness = Name
	restored.Active = false
	b.sessions[restored.ID] = &sessionState{
		session: restored, threadID: threadID, started: len(restored.Messages) != 0,
	}
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
	if state.deleted {
		state.mu.Unlock()
		return false
	}
	state.deleted = true
	if state.cancel != nil {
		state.cancel()
	}
	done := state.done
	state.mu.Unlock()
	if done != nil {
		<-done
	}
	b.mu.Lock()
	if b.sessions[id] == state {
		delete(b.sessions, id)
	}
	b.mu.Unlock()
	if err := b.removeThreadID(id); err != nil {
		slog.Warn("Codex session mapping cleanup failed", "error", err)
	}
	return true
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
	if state.deleted || !state.active || state.cancel == nil {
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
	if state.deleted {
		state.mu.Unlock()
		cancel()
		return nil, errors.New("session not found")
	}
	if state.active {
		state.mu.Unlock()
		cancel()
		return nil, ErrSessionBusy
	}
	if state.started && state.threadID == "" {
		state.mu.Unlock()
		cancel()
		return nil, ErrSessionUnavailable
	}
	state.active = true
	state.cancel = cancel
	state.done = make(chan struct{})
	state.started = true
	state.session.Active = true
	state.session.UpdatedAt = time.Now()
	state.session.Messages = append(state.session.Messages, agent.Message{
		ID: messageID(), Role: agent.RoleUser, Content: userText, Timestamp: time.Now(),
	})
	threadID := state.threadID
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

	var finalText strings.Builder
	itemText := make(map[string]string)
	completedItems := make(map[string]bool)
	var actualThreadID string
	var finalSeen bool
	var protocolFailed bool
	var protocolReason string
	var mappingFailed bool
	var terminal string
	err := b.runner.RunLines(turnCtx, b.commandArgs(threadID), []byte(userText), func(line []byte) error {
		var event streamEvent
		if json.Unmarshal(line, &event) != nil || event.Type == "" {
			protocolFailed = true
			protocolReason = "invalid event record"
			return ErrProtocol
		}
		switch event.Type {
		case "thread.started":
			if terminal != "" || !validThreadID(event.ThreadID) ||
				actualThreadID != "" && actualThreadID != event.ThreadID ||
				threadID != "" && threadID != event.ThreadID {
				protocolFailed = true
				protocolReason = "invalid thread identity"
				return ErrProtocol
			}
			actualThreadID = event.ThreadID
			if threadID == "" {
				if err := b.saveThreadID(sessionID, actualThreadID); err != nil {
					mappingFailed = true
					return ErrSessionMapping
				}
				state.mu.Lock()
				state.threadID = actualThreadID
				state.mu.Unlock()
			}
		case "item.updated", "item.completed":
			if terminal != "" || actualThreadID == "" || len(event.Item) == 0 {
				protocolFailed = true
				protocolReason = "item event outside an active turn"
				return ErrProtocol
			}
			var item streamItem
			if json.Unmarshal(event.Item, &item) != nil || item.ID == "" || item.Type == "" {
				protocolFailed = true
				protocolReason = "invalid item payload"
				return ErrProtocol
			}
			if item.Type != "agent_message" {
				return nil
			}
			if completedItems[item.ID] {
				protocolFailed = true
				protocolReason = "duplicate completed item"
				return ErrProtocol
			}
			previous := itemText[item.ID]
			if !strings.HasPrefix(item.Text, previous) {
				protocolFailed = true
				protocolReason = "agent message update was not append-only"
				return ErrProtocol
			}
			delta := item.Text[len(previous):]
			itemText[item.ID] = item.Text
			if event.Type == "item.completed" {
				completedItems[item.ID] = true
				finalSeen = true
				finalText.WriteString(item.Text)
			}
			if delta != "" {
				if err := emit(turnCtx, streamChan, agent.StreamEvent{
					Type: agent.EventTextDelta, SessionID: sessionID, Content: delta,
				}); err != nil {
					return err
				}
			}
		case "turn.completed":
			if actualThreadID == "" || terminal != "" {
				protocolFailed = true
				protocolReason = "invalid completion event"
				return ErrProtocol
			}
			terminal = "completed"
		case "turn.failed", "error":
			if terminal == "completed" {
				protocolFailed = true
				protocolReason = "failure event after completion"
				return ErrProtocol
			}
			terminal = "failed"
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
		if mappingFailed {
			return nil, ErrSessionMapping
		}
		if protocolFailed {
			return nil, fmt.Errorf("%w: %s", ErrProtocol, protocolReason)
		}
		if terminal == "failed" {
			return nil, ErrTurnFailed
		}
		return nil, ErrProcessFailed
	}
	if mappingFailed {
		return nil, ErrSessionMapping
	}
	if protocolFailed {
		return nil, fmt.Errorf("%w: %s", ErrProtocol, protocolReason)
	}
	if terminal == "failed" {
		return nil, ErrTurnFailed
	}
	if terminal != "completed" || actualThreadID == "" || !finalSeen {
		return nil, fmt.Errorf("%w: missing completed turn result", ErrProtocol)
	}
	final := &agent.Message{
		ID: messageID(), Role: agent.RoleAssistant, Content: finalText.String(),
		Timestamp: time.Now(), DurationMs: time.Since(startedAt).Milliseconds(),
	}
	state.mu.Lock()
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
		if !state.deleted {
			if state.active {
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
		close(b.closeDone)
	})
	<-b.closeDone
	return b.closeErr
}

func (b *Backend) commandArgs(threadID string) []string {
	args := []string{
		"exec",
		"--json",
		"--sandbox", "read-only",
		"--ask-for-approval", "never",
		"--ignore-user-config",
		"--strict-config",
		"--config", `approval_policy="never"`,
		"--config", `sandbox_mode="read-only"`,
		"--config", "mcp_servers={}",
		"--cd", b.workspace,
	}
	if threadID != "" {
		args = append(args, "resume", threadID, "-")
	} else {
		args = append(args, "-")
	}
	return args
}

func (b *Backend) saveThreadID(sessionID, threadID string) error {
	if !validSessionID(sessionID) || !validThreadID(threadID) {
		return ErrSessionMapping
	}
	path := b.mappingPath(sessionID)
	temp, err := os.CreateTemp(b.mappingDir, ".codex-session-*")
	if err != nil {
		return ErrSessionMapping
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return ErrSessionMapping
	}
	if _, err := temp.WriteString(threadID + "\n"); err != nil {
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
	if _, err := os.Lstat(path); err == nil {
		return ErrSessionMapping
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrSessionMapping
	}
	if err := os.Rename(tempPath, path); err != nil {
		return ErrSessionMapping
	}
	return nil
}

func (b *Backend) loadThreadID(sessionID string) (string, error) {
	if !validSessionID(sessionID) {
		return "", ErrSessionMapping
	}
	path := b.mappingPath(sessionID)
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrSessionMapping
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", ErrSessionMapping
	}
	threadID := strings.TrimSpace(string(data))
	if !validThreadID(threadID) {
		return "", ErrSessionMapping
	}
	return threadID, nil
}

func (b *Backend) removeThreadID(sessionID string) error {
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
	return filepath.Join(b.mappingDir, sessionID+".thread")
}

func ensureMappingDirectory(codexHome string) (string, error) {
	path := filepath.Join(codexHome, mappingDirectoryName)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", ErrSessionMapping
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrSessionMapping
	}
	return path, nil
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

func validThreadID(id string) bool {
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

func validSessionID(id string) bool { return validThreadID(id) }

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

func messageID() string { return fmt.Sprintf("msg_%d", time.Now().UnixNano()) }

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

var _ agent.Backend = (*Backend)(nil)
var _ agent.SessionRestorer = (*Backend)(nil)
