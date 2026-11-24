package opencode

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/execution"
)

const (
	Name                   = "opencode"
	ModeManaged            = "managed"
	ModeConnect            = "connect"
	defaultMaxTurnDuration = 30 * time.Minute
	cancelConfirmationWait = 30 * time.Second
)

var (
	ErrInvalidConfiguration     = errors.New("OpenCode backend configuration is invalid")
	ErrInvalidRequest           = errors.New("OpenCode request is invalid")
	ErrUnsupportedVersion       = errors.New("OpenCode server version is outside the supported range >=2.3.2, <2.4.0")
	ErrServerUnavailable        = errors.New("OpenCode server is unavailable")
	ErrServerRequest            = errors.New("OpenCode server rejected the request")
	ErrPolicyUnavailable        = errors.New("OpenCode server cannot enforce the configured tool policy")
	ErrSessionBusy              = errors.New("OpenCode session is currently processing another request")
	ErrSessionUnavailable       = errors.New("OpenCode session is unavailable")
	ErrSessionOwnership         = errors.New("OpenCode session does not belong to the configured workspace")
	ErrSessionMapping           = errors.New("OpenCode session mapping is unavailable")
	ErrCancellationUnconfirmed  = errors.New("OpenCode did not confirm that session execution stopped")
	ErrTurnFailed               = errors.New("OpenCode reported a failed turn")
	ErrTurnTimeout              = errors.New("OpenCode turn exceeded its runtime limit")
	ErrProtocol                 = errors.New("OpenCode returned an invalid API response")
	ErrUnsupportedOperation     = errors.New("OpenCode backend does not support this operation")
	ErrUnsupportedConfiguration = errors.New("OpenCode backend does not support the requested session configuration")
)

type Config struct {
	Mode           string
	Executable     string
	Workspace      string
	OpenCodeHome   string
	ServerURL      string
	ServerPassword string
	SessionDir     string
}

type Backend struct {
	client     *http.Client
	baseURL    string
	password   string
	workspace  string
	mappingDir string
	version    string
	owned      *execution.ServiceProcess

	// mu protects the backend session map and closed state. Never hold it over
	// server calls, disk I/O, or process shutdown.
	mu          sync.RWMutex
	sessions    map[string]*sessionState
	closed      bool
	fullyClosed bool
	operations  sync.WaitGroup
	closeMu     sync.Mutex
}

type sessionState struct {
	// mu protects session, native mapping, and turn/cancellation state. It is
	// never held while calling OpenCode or performing mapping-file I/O.
	mu sync.Mutex

	session    *agent.Session
	mapping    sessionMapping
	active     bool
	uncertain  bool
	deleted    bool
	cancel     context.CancelFunc
	done       chan struct{}
	cancelCall *cancellationCall
}

type cancellationCall struct {
	done        chan struct{}
	interrupted bool
	err         error
}

type turnResult struct {
	found   bool
	outcome string
	final   *agent.Message
}

func New(config Config) (*Backend, error) {
	workspace, err := canonicalDirectory(config.Workspace)
	if err != nil {
		return nil, errors.New("OpenCode backend requires an existing absolute workspace")
	}
	mappingDir := config.SessionDir
	var baseURL, password string
	var owned *execution.ServiceProcess

	switch config.Mode {
	case ModeManaged:
		home, err := canonicalDirectory(config.OpenCodeHome)
		if err != nil {
			return nil, errors.New("managed OpenCode mode requires an existing absolute OpenCode home")
		}
		if pathsOverlap(workspace, home) {
			return nil, errors.New("OpenCode home must not overlap the workspace")
		}
		if config.Executable == "" || config.ServerURL != "" || config.ServerPassword != "" {
			return nil, ErrInvalidConfiguration
		}
		if mappingDir == "" {
			mappingDir = filepath.Join(home, "maistr0-sessions")
		}
		mappingDir, err = ensureMappingDirectory(mappingDir)
		if err != nil {
			return nil, err
		}
		if pathsOverlap(workspace, mappingDir) {
			return nil, errors.New("OpenCode session mapping directory must not overlap the workspace")
		}
		baseURL, password, owned, err = startManagedServer(config.Executable, workspace, home)
		if err != nil {
			return nil, err
		}
	case ModeConnect:
		if config.Executable != "" || config.OpenCodeHome != "" {
			return nil, ErrInvalidConfiguration
		}
		if mappingDir == "" {
			return nil, errors.New("connected OpenCode mode requires a session mapping directory")
		}
		mappingDir, err = ensureMappingDirectory(mappingDir)
		if err != nil {
			return nil, err
		}
		if pathsOverlap(workspace, mappingDir) {
			return nil, errors.New("OpenCode session mapping directory must not overlap the workspace")
		}
		baseURL, err = validateServerURL(config.ServerURL, config.ServerPassword)
		if err != nil {
			return nil, err
		}
		password = config.ServerPassword
	default:
		return nil, ErrInvalidConfiguration
	}

	backend := &Backend{
		client: newHTTPClient(), baseURL: baseURL, password: password,
		workspace: workspace, mappingDir: mappingDir, owned: owned,
		sessions: make(map[string]*sessionState),
	}
	startupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var processStatus func() (bool, error)
	if owned != nil {
		processStatus = owned.Status
	}
	version, err := backend.waitForServer(startupCtx, processStatus)
	if err == nil {
		backend.version = version
		err = backend.verifyPolicy(startupCtx)
	}
	if err != nil {
		if owned != nil {
			_ = owned.Close()
		}
		return nil, err
	}
	return backend, nil
}

func startManagedServer(executable, workspace, home string) (string, string, *execution.ServiceProcess, error) {
	if !filepath.IsAbs(executable) || strings.ContainsRune(executable, '\x00') {
		return "", "", nil, ErrInvalidConfiguration
	}
	configDir := filepath.Join(home, ".config", "opencode")
	dataRoot := filepath.Join(home, ".local", "share")
	cacheRoot := filepath.Join(home, ".cache")
	stateRoot := filepath.Join(home, ".local", "state")
	for _, directory := range []string{configDir, dataRoot, cacheRoot, stateRoot} {
		if err := ensurePrivateDirectory(directory); err != nil {
			return "", "", nil, ErrInvalidConfiguration
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", nil, ErrServerUnavailable
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return "", "", nil, ErrServerUnavailable
	}
	token, err := randomToken()
	if err != nil {
		return "", "", nil, ErrInvalidConfiguration
	}
	restrictedConfig, err := json.Marshal(map[string]any{
		"plugins": []string{},
		"mcp":     map[string]any{"servers": map[string]any{}},
	})
	if err != nil {
		return "", "", nil, ErrInvalidConfiguration
	}
	homeEnv := map[string]string{
		"HOME":                            home,
		"USERPROFILE":                     home,
		"PATH":                            os.Getenv("PATH"),
		"XDG_CONFIG_HOME":                 filepath.Join(home, ".config"),
		"XDG_DATA_HOME":                   dataRoot,
		"XDG_CACHE_HOME":                  cacheRoot,
		"XDG_STATE_HOME":                  stateRoot,
		"OPENCODE_CONFIG_DIR":             configDir,
		"OPENCODE_CONFIG_CONTENT":         string(restrictedConfig),
		"OPENCODE_CONFIG_PROJECT_DISABLE": "1",
		"OPENCODE_DISABLE_CLAUDE_CODE":    "1",
		"OPENCODE_PASSWORD":               token,
		"OPENCODE_SERVER_PASSWORD":        token,
	}
	if runtime.GOOS != "windows" {
		delete(homeEnv, "USERPROFILE")
	}
	process, err := execution.StartServiceProcess(execution.ServiceConfig{
		Executable: executable,
		Args: []string{
			"serve", "--hostname", "127.0.0.1", "--port", strconv.Itoa(port),
		},
		WorkingDirectory: workspace,
		Environment:      homeEnv,
	})
	if err != nil {
		return "", "", nil, err
	}
	return "http://127.0.0.1:" + strconv.Itoa(port), token, process, nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidConfiguration
	}
	return nil
}

func randomToken() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func NewFromEnvironment() (*Backend, bool, error) {
	names := []string{
		"MAISTR0_OPENCODE_MODE", "MAISTR0_OPENCODE_EXECUTABLE", "MAISTR0_OPENCODE_WORKSPACE",
		"MAISTR0_OPENCODE_HOME", "MAISTR0_OPENCODE_SERVER_URL", "MAISTR0_OPENCODE_SERVER_PASSWORD",
		"MAISTR0_OPENCODE_SESSION_DIR",
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
	mode := values["MAISTR0_OPENCODE_MODE"]
	workspace := values["MAISTR0_OPENCODE_WORKSPACE"]
	config := Config{Mode: mode, Workspace: workspace, SessionDir: values["MAISTR0_OPENCODE_SESSION_DIR"]}
	switch mode {
	case ModeManaged:
		config.Executable = values["MAISTR0_OPENCODE_EXECUTABLE"]
		config.OpenCodeHome = values["MAISTR0_OPENCODE_HOME"]
		if config.Executable == "" || workspace == "" || config.OpenCodeHome == "" ||
			values["MAISTR0_OPENCODE_SERVER_URL"] != "" || values["MAISTR0_OPENCODE_SERVER_PASSWORD"] != "" {
			return nil, true, errors.New("managed OpenCode mode requires MAISTR0_OPENCODE_EXECUTABLE, MAISTR0_OPENCODE_WORKSPACE, and MAISTR0_OPENCODE_HOME only")
		}
	case ModeConnect:
		config.ServerURL = values["MAISTR0_OPENCODE_SERVER_URL"]
		config.ServerPassword = values["MAISTR0_OPENCODE_SERVER_PASSWORD"]
		if workspace == "" || config.ServerURL == "" || config.ServerPassword == "" ||
			config.SessionDir == "" || values["MAISTR0_OPENCODE_EXECUTABLE"] != "" ||
			values["MAISTR0_OPENCODE_HOME"] != "" {
			return nil, true, errors.New("connected OpenCode mode requires MAISTR0_OPENCODE_WORKSPACE, MAISTR0_OPENCODE_SERVER_URL, MAISTR0_OPENCODE_SERVER_PASSWORD, and MAISTR0_OPENCODE_SESSION_DIR only")
		}
	default:
		return nil, true, errors.New("OpenCode opt-in requires MAISTR0_OPENCODE_MODE=managed or connect")
	}
	backend, err := New(config)
	return backend, true, err
}

func (b *Backend) Name() string { return Name }

func (b *Backend) Version() string { return b.version }

func (b *Backend) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	if cfg.CoordinatorModel != "" || cfg.MaxSteps != 0 || cfg.Temperature != 0 || cfg.SystemPrompt != "" {
		return nil, ErrUnsupportedConfiguration
	}
	if !b.beginOperation() {
		return nil, execution.ErrClosed
	}
	defer b.operations.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.verifyPolicy(ctx); err != nil {
		return nil, err
	}
	if title == "" {
		title = "OpenCode Session " + time.Now().Format("Jan 02 15:04")
	}
	var response sessionEnvelope
	err := b.request(ctx, http.MethodPost, "/api/session", nil, map[string]any{
		"title":       title,
		"location":    map[string]string{"directory": b.workspace},
		"permissions": sessionPermissions(b.workspace),
	}, &response)
	if err != nil {
		return nil, err
	}
	if !validNativeSessionID(response.Data.ID) || !sameDirectory(response.Data.Location.Directory, b.workspace) {
		if validNativeSessionID(response.Data.ID) {
			_ = b.request(ctx, http.MethodDelete, "/api/session/"+response.Data.ID, nil, nil, nil)
		}
		return nil, ErrProtocol
	}
	id, err := newSessionID()
	if err != nil {
		_ = b.request(ctx, http.MethodDelete, "/api/session/"+response.Data.ID, nil, nil, nil)
		return nil, ErrSessionMapping
	}
	now := time.Now()
	session := &agent.Session{
		ID: id, Title: title, Harness: Name, CreatedAt: now, UpdatedAt: now,
		Messages: []agent.Message{}, Memory: map[string]string{}, Config: cfg,
	}
	mapping := sessionMapping{Version: mappingVersion, SessionID: id, NativeID: response.Data.ID}
	if err := saveMapping(b.mappingDir, mapping); err != nil {
		_ = b.request(ctx, http.MethodDelete, "/api/session/"+response.Data.ID, nil, nil, nil)
		return nil, err
	}
	state := &sessionState{session: session, mapping: mapping}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = b.request(ctx, http.MethodDelete, "/api/session/"+response.Data.ID, nil, nil, nil)
		_ = removeMapping(b.mappingDir, id)
		return nil, execution.ErrClosed
	}
	b.sessions[id] = state
	b.mu.Unlock()
	return cloneSession(session), nil
}

func (b *Backend) RestoreSession(session *agent.Session) error {
	if session == nil || strings.TrimSpace(session.ID) == "" {
		return ErrSessionMapping
	}
	mapping, err := loadMapping(b.mappingDir, session.ID)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return execution.ErrClosed
	}
	if _, exists := b.sessions[session.ID]; exists {
		return errors.New("OpenCode session already exists")
	}
	restored := cloneSession(session)
	restored.Harness = Name
	restored.Active = false
	b.sessions[restored.ID] = &sessionState{session: restored, mapping: mapping}
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
	if state.deleted {
		state.mu.Unlock()
		return false
	}
	mapping := state.mapping
	done := state.done
	state.mu.Unlock()
	validateCtx, validateCancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := b.validateSession(validateCtx, mapping.NativeID)
	validateCancel()
	if err != nil {
		if errors.Is(err, ErrSessionUnavailable) {
			return b.forgetSession(id, state)
		}
		return false
	}
	if err := b.interruptAndWait(mapping.NativeID); err != nil {
		return false
	}
	if done != nil {
		state.mu.Lock()
		if state.cancel != nil {
			state.cancel()
		}
		state.mu.Unlock()
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.validateSession(ctx, mapping.NativeID); err != nil {
		if errors.Is(err, ErrSessionUnavailable) {
			return b.forgetSession(id, state)
		}
		return false
	}
	err = b.request(ctx, http.MethodDelete, "/api/session/"+mapping.NativeID, nil, nil, nil)
	if err != nil && !errors.Is(err, ErrSessionUnavailable) {
		return false
	}
	return b.forgetSession(id, state)
}

func (b *Backend) forgetSession(id string, state *sessionState) bool {
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
	if state.deleted || !state.active {
		state.mu.Unlock()
		return false
	}
	state.mu.Unlock()
	interrupted, err := b.confirmCancellation(state)
	return err == nil && interrupted
}

func (b *Backend) SendMessage(
	ctx context.Context,
	sessionID string,
	userText string,
	streamChan chan<- agent.StreamEvent,
) (*agent.Message, error) {
	if ctx == nil || strings.TrimSpace(userText) == "" {
		return nil, errors.New("context and user message are required")
	}
	if streamChan != nil {
		return nil, ErrUnsupportedOperation
	}
	if !b.beginOperation() {
		return nil, execution.ErrClosed
	}
	defer b.operations.Done()
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, execution.ErrClosed
	}
	state, exists := b.sessions[sessionID]
	if !exists {
		b.mu.RUnlock()
		return nil, ErrSessionUnavailable
	}
	turnCtx, timeoutCancel := context.WithTimeout(ctx, defaultMaxTurnDuration)
	turnCtx, cancel := context.WithCancel(turnCtx)
	state.mu.Lock()
	if state.deleted {
		state.mu.Unlock()
		b.mu.RUnlock()
		timeoutCancel()
		cancel()
		return nil, ErrSessionUnavailable
	}
	if state.active {
		state.mu.Unlock()
		b.mu.RUnlock()
		timeoutCancel()
		cancel()
		return nil, ErrSessionBusy
	}
	state.active = true
	state.cancel = func() {
		timeoutCancel()
		cancel()
	}
	state.done = make(chan struct{})
	state.cancelCall = nil
	state.session.Active = true
	state.session.UpdatedAt = time.Now()
	mapping := state.mapping
	done := state.done
	state.mu.Unlock()
	b.mu.RUnlock()
	defer func() {
		state.mu.Lock()
		state.active = false
		state.cancel = nil
		state.done = nil
		state.cancelCall = nil
		state.session.Active = state.uncertain
		state.session.UpdatedAt = time.Now()
		close(done)
		state.mu.Unlock()
		timeoutCancel()
		cancel()
	}()

	if err := b.validateSession(turnCtx, mapping.NativeID); err != nil {
		state.mu.Lock()
		state.uncertain = true
		state.mu.Unlock()
		return nil, err
	}
	if err := b.waitIdle(turnCtx, mapping.NativeID); err != nil {
		if turnCtx.Err() != nil {
			return b.handleCancelledTurn(state, mapping.NativeID, turnCtx.Err())
		}
		state.mu.Lock()
		state.uncertain = true
		state.mu.Unlock()
		return nil, err
	}
	state.mu.Lock()
	state.uncertain = false
	state.session.Active = false
	mapping = state.mapping
	state.mu.Unlock()
	if _, err := b.reconcile(turnCtx, state, mapping.PendingID, true); err != nil {
		return nil, err
	}
	if err := b.verifyPolicy(turnCtx); err != nil {
		return nil, err
	}
	permissions := sessionPermissions(b.workspace)
	if err := b.request(turnCtx, http.MethodPatch, "/api/session/"+mapping.NativeID, nil,
		map[string]any{"permissions": permissions}, nil); err != nil {
		return nil, err
	}
	promptID, err := newNativeMessageID()
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	mapping = state.mapping
	mapping.PendingID = promptID
	state.mu.Unlock()
	if err := saveMapping(b.mappingDir, mapping); err != nil {
		return nil, err
	}
	state.mu.Lock()
	state.mapping = mapping
	state.mu.Unlock()
	if err := b.request(turnCtx, http.MethodPost, "/api/session/"+mapping.NativeID+"/prompt", nil,
		map[string]any{"id": promptID, "text": userText}, nil); err != nil {
		if turnCtx.Err() != nil {
			return b.handleCancelledTurn(state, mapping.NativeID, turnCtx.Err())
		}
		state.mu.Lock()
		state.uncertain = true
		state.mu.Unlock()
		return nil, err
	}
	if err := b.waitIdle(turnCtx, mapping.NativeID); err != nil {
		if turnCtx.Err() != nil {
			return b.handleCancelledTurn(state, mapping.NativeID, turnCtx.Err())
		}
		state.mu.Lock()
		state.uncertain = true
		state.mu.Unlock()
		return nil, err
	}
	state.mu.Lock()
	state.uncertain = false
	state.session.Active = false
	state.mu.Unlock()
	if turnCtx.Err() != nil {
		return b.handleCancelledTurn(state, mapping.NativeID, turnCtx.Err())
	}
	result, err := b.reconcile(turnCtx, state, promptID, false)
	if err != nil {
		if turnCtx.Err() != nil {
			return b.handleCancelledTurn(state, mapping.NativeID, turnCtx.Err())
		}
		return nil, err
	}
	switch result.outcome {
	case "failed":
		return nil, ErrTurnFailed
	case "interrupted":
		return interruptedMessage(sessionID, time.Since(state.session.UpdatedAt)), context.Canceled
	case "succeeded":
		if result.final == nil {
			return nil, ErrProtocol
		}
		return result.final, nil
	default:
		return nil, ErrProtocol
	}
}

func (b *Backend) handleCancelledTurn(state *sessionState, nativeID string, cause error) (*agent.Message, error) {
	interrupted, err := b.confirmCancellation(state)
	if err != nil {
		return nil, ErrCancellationUnconfirmed
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cancelConfirmationWait)
	defer cancel()
	state.mu.Lock()
	pendingID := state.mapping.PendingID
	state.mu.Unlock()
	result, err := b.reconcile(cleanupCtx, state, pendingID, false)
	if err != nil {
		return nil, err
	}
	if interrupted {
		return interruptedMessage(state.session.ID, 0), cause
	}
	if result.final != nil && result.outcome == "succeeded" {
		return result.final, nil
	}
	if result.outcome == "failed" {
		return nil, ErrTurnFailed
	}
	return nil, cause
}

func (b *Backend) confirmCancellation(state *sessionState) (bool, error) {
	state.mu.Lock()
	if call := state.cancelCall; call != nil {
		state.mu.Unlock()
		<-call.done
		return call.interrupted, call.err
	}
	call := &cancellationCall{done: make(chan struct{})}
	state.cancelCall = call
	nativeID := state.mapping.NativeID
	state.mu.Unlock()

	state.mu.Lock()
	if state.cancel != nil {
		state.cancel()
	}
	state.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), cancelConfirmationWait)
	query := url.Values{"resume": {"false"}}
	var response struct {
		Interrupted bool `json:"interrupted"`
	}
	err := b.request(ctx, http.MethodPost, "/api/session/"+nativeID+"/interrupt", query, nil, &response)
	interrupted := response.Interrupted
	if err == nil {
		err = b.waitIdle(ctx, nativeID)
	}
	cancel()
	state.mu.Lock()
	call.interrupted = interrupted
	call.err = err
	state.uncertain = err != nil
	close(call.done)
	state.mu.Unlock()
	return interrupted, err
}

func (b *Backend) cancelAndWait(ctx context.Context, nativeID string) (bool, error) {
	query := url.Values{"resume": {"false"}}
	var response struct {
		Interrupted bool `json:"interrupted"`
	}
	if err := b.request(ctx, http.MethodPost, "/api/session/"+nativeID+"/interrupt", query, nil, &response); err != nil {
		return false, ErrCancellationUnconfirmed
	}
	if err := b.waitIdle(ctx, nativeID); err != nil {
		return false, ErrCancellationUnconfirmed
	}
	return response.Interrupted, nil
}

func (b *Backend) interruptAndWait(nativeID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), cancelConfirmationWait)
	defer cancel()
	_, err := b.cancelAndWait(ctx, nativeID)
	return err
}

func (b *Backend) validateSession(ctx context.Context, nativeID string) error {
	var response sessionEnvelope
	if err := b.request(ctx, http.MethodGet, "/api/session/"+nativeID, nil, nil, &response); err != nil {
		return err
	}
	if response.Data.ID != nativeID || !sameDirectory(response.Data.Location.Directory, b.workspace) {
		return ErrSessionOwnership
	}
	return nil
}

func sameDirectory(first, second string) bool {
	left, leftErr := filepath.EvalSymlinks(first)
	right, rightErr := filepath.EvalSymlinks(second)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func (b *Backend) waitIdle(ctx context.Context, nativeID string) error {
	if err := b.request(ctx, http.MethodPost, "/api/experimental/session/"+nativeID+"/wait", nil, nil, nil); err != nil {
		return err
	}
	return nil
}

func (b *Backend) reconcile(
	ctx context.Context,
	state *sessionState,
	expectedMessageID string,
	commitCursor bool,
) (turnResult, error) {
	state.mu.Lock()
	mapping := state.mapping
	state.mu.Unlock()
	messages, latestID, err := b.fetchMessages(ctx, mapping.NativeID, mapping.CursorID)
	if err != nil {
		return turnResult{}, err
	}
	result := turnResult{}
	inTurn := false
	for _, message := range messages {
		if message.ID == expectedMessageID && message.Type == "user" {
			result.found = true
			inTurn = true
		} else if inTurn && message.Type == "idle" {
			result.outcome = message.Outcome
			inTurn = false
		} else if inTurn && message.Type == "assistant" {
			text := assistantText(message)
			if text != "" {
				result.final = &agent.Message{
					ID: message.ID, Role: agent.RoleAssistant, Content: text, Timestamp: time.Now(),
				}
			}
		}
		if err := appendNativeMessage(state, message); err != nil {
			return turnResult{}, err
		}
	}
	if expectedMessageID != "" && !result.found {
		if commitCursor {
			mapping.PendingID = ""
		}
	} else if expectedMessageID != "" && result.outcome == "" {
		return turnResult{}, ErrProtocol
	}
	if commitCursor {
		mapping.CursorID = latestID
		mapping.PendingID = ""
		if err := saveMapping(b.mappingDir, mapping); err != nil {
			return turnResult{}, err
		}
		state.mu.Lock()
		state.mapping = mapping
		state.mu.Unlock()
	}
	return result, nil
}

func (b *Backend) fetchMessages(
	ctx context.Context,
	nativeID string,
	cursorID string,
) ([]nativeMessage, string, error) {
	newestFirst := make([]nativeMessage, 0)
	query := url.Values{"limit": {"200"}, "order": {"desc"}}
	latestID := ""
	foundCursor := cursorID == ""
	for page := 0; page < 100; page++ {
		var response sessionMessagesEnvelope
		if err := b.request(ctx, http.MethodGet, "/api/session/"+nativeID+"/message", query, nil, &response); err != nil {
			return nil, "", err
		}
		if page == 0 && len(response.Data) > 0 {
			latestID = response.Data[0].ID
		}
		for _, message := range response.Data {
			if !validNativeMessageID(message.ID) {
				return nil, "", ErrProtocol
			}
			if cursorID != "" && message.ID == cursorID {
				foundCursor = true
				break
			}
			newestFirst = append(newestFirst, message)
		}
		if foundCursor || response.Cursor.Next == "" {
			break
		}
		query = url.Values{"limit": {"200"}, "cursor": {response.Cursor.Next}}
	}
	if !foundCursor {
		return nil, "", ErrSessionMapping
	}
	for left, right := 0, len(newestFirst)-1; left < right; left, right = left+1, right-1 {
		newestFirst[left], newestFirst[right] = newestFirst[right], newestFirst[left]
	}
	if latestID == "" {
		latestID = cursorID
	}
	return newestFirst, latestID, nil
}

func appendNativeMessage(state *sessionState, native nativeMessage) error {
	var converted *agent.Message
	switch native.Type {
	case "user":
		if native.Text != "" {
			converted = &agent.Message{ID: native.ID, Role: agent.RoleUser, Content: native.Text}
		}
	case "assistant":
		if text := assistantText(native); text != "" {
			converted = &agent.Message{ID: native.ID, Role: agent.RoleAssistant, Content: text}
		}
	}
	if converted == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, existing := range state.session.Messages {
		if existing.ID == converted.ID {
			return nil
		}
	}
	converted.Timestamp = time.Now()
	state.session.Messages = append(state.session.Messages, *converted)
	state.session.UpdatedAt = converted.Timestamp
	return nil
}

func assistantText(message nativeMessage) string {
	var text strings.Builder
	for _, content := range message.Content {
		if content.Type == "text" {
			text.WriteString(content.Text)
		}
	}
	return text.String()
}

func interruptedMessage(sessionID string, duration time.Duration) *agent.Message {
	return &agent.Message{
		ID: newLocalMessageID(), Role: agent.RoleAssistant, Content: "Turn interrupted.",
		Timestamp: time.Now(), DurationMs: duration.Milliseconds(),
	}
}

func (b *Backend) getState(id string) (*sessionState, bool) {
	b.mu.RLock()
	state, exists := b.sessions[id]
	b.mu.RUnlock()
	return state, exists
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
			if state.active || state.uncertain {
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

	var closeErr error
	for _, state := range states {
		state.mu.Lock()
		mapping := state.mapping
		active := state.active
		done := state.done
		state.mu.Unlock()
		validateCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		validateErr := b.validateSession(validateCtx, mapping.NativeID)
		cancel()
		if validateErr != nil {
			if !errors.Is(validateErr, ErrSessionUnavailable) {
				closeErr = errors.Join(closeErr, ErrCancellationUnconfirmed)
			}
			if done != nil {
				<-done
			}
			continue
		}
		var err error
		if active {
			_, err = b.confirmCancellation(state)
		} else {
			err = b.interruptAndWait(mapping.NativeID)
		}
		if err != nil {
			closeErr = errors.Join(closeErr, ErrCancellationUnconfirmed)
		}
		if done != nil {
			<-done
		}
	}
	b.operations.Wait()
	if closeErr != nil {
		return closeErr
	}
	if b.owned != nil {
		if err := b.owned.Close(); err != nil {
			return err
		}
	}
	b.client.CloseIdleConnections()
	b.password = ""
	b.mu.Lock()
	b.fullyClosed = true
	b.mu.Unlock()
	return nil
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

func newSessionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "opencode-" + hex.EncodeToString(value), nil
}

func newNativeMessageID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "msg_" + hex.EncodeToString(value), nil
}

func newLocalMessageID() string {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		return "opencode-interrupted"
	}
	return "opencode-" + hex.EncodeToString(value)
}

func cloneSession(session *agent.Session) *agent.Session {
	if session == nil {
		return nil
	}
	cloned := *session
	cloned.Messages = append([]agent.Message(nil), session.Messages...)
	for index := range cloned.Messages {
		cloned.Messages[index].ToolCalls = append([]agent.ToolCall(nil), session.Messages[index].ToolCalls...)
		if session.Messages[index].ToolResponse != nil {
			response := *session.Messages[index].ToolResponse
			cloned.Messages[index].ToolResponse = &response
		}
	}
	if session.Memory != nil {
		cloned.Memory = make(map[string]string, len(session.Memory))
		for key, value := range session.Memory {
			cloned.Memory[key] = value
		}
	}
	return &cloned
}

func validSessionID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= 256 && !strings.ContainsRune(id, '\x00')
}

var _ agent.Backend = (*Backend)(nil)
var _ agent.SessionRestorer = (*Backend)(nil)
