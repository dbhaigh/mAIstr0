package openhands

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	Name                  = "openhands"
	defaultTurnTimeout    = 30 * time.Minute
	defaultDisconnectWait = 30 * time.Second
	defaultPollInterval   = time.Second
	cancellationWait      = 30 * time.Second
)

var (
	ErrInvalidConfiguration     = errors.New("OpenHands backend configuration is invalid")
	ErrInvalidRequest           = errors.New("OpenHands request is invalid")
	ErrUnsupportedVersion       = errors.New("OpenHands Agent Server must report version 1.63.1")
	ErrAuthentication           = errors.New("OpenHands server authentication failed")
	ErrAuthenticationRequired   = errors.New("OpenHands server does not require session API-key authentication")
	ErrServerUnavailable        = errors.New("OpenHands server is unavailable")
	ErrServerRequest            = errors.New("OpenHands server rejected the request")
	ErrConversationUnavailable  = errors.New("OpenHands conversation is unavailable")
	ErrSessionOwnership         = errors.New("OpenHands conversation does not belong to the configured workspace")
	ErrSessionMapping           = errors.New("OpenHands session mapping is unavailable")
	ErrSessionBusy              = errors.New("OpenHands session is already processing a request")
	ErrUnsupportedConfiguration = errors.New("OpenHands backend does not support the requested session configuration")
	ErrUnsupportedOperation     = errors.New("OpenHands backend does not support this operation")
	ErrProtocol                 = errors.New("OpenHands server returned an invalid protocol response")
	ErrTurnFailed               = errors.New("OpenHands reported a failed turn")
	ErrTurnPaused               = errors.New("OpenHands conversation was paused")
	ErrTurnTimeout              = errors.New("OpenHands turn exceeded its runtime limit")
	ErrOutcomeUnknown           = errors.New("OpenHands turn outcome is unknown; the server may still be running")
	ErrCancellationUnconfirmed  = errors.New("OpenHands did not confirm that conversation execution stopped")
	ErrApprovalUnsupported      = errors.New("OpenHands requested approval, which mAIstr0 does not support")
)

type Config struct {
	ServerURL  string
	SessionKey string
	Workspace  string
	Model      string
	SessionDir string
}

type Backend struct {
	client     *http.Client
	baseURL    string
	socketBase string
	apiKey     string
	workspace  string
	model      string
	mappingDir string
	version    string

	// mu protects the session map, closed state, and operation admission.
	// Per-session persistence uses persistMu before state.mu; no lock is held
	// across network operations.
	mu          sync.RWMutex
	sessions    map[string]*sessionState
	closed      bool
	fullyClosed bool
	operations  sync.WaitGroup
	closeMu     sync.Mutex
	turnGate    chan struct{}

	turnTimeout    time.Duration
	disconnectWait time.Duration
	pollInterval   time.Duration
	cancelWait     time.Duration
}

type sessionState struct {
	// persistMu serializes sidecar updates. cancelMu serializes remote
	// interruption with send cancellation. state.mu protects the session,
	// mapping, and turn state; never hold it over I/O.
	persistMu sync.Mutex
	cancelMu  sync.Mutex
	mu        sync.Mutex

	session   *agent.Session
	mapping   sessionMapping
	active    bool
	uncertain bool
	deleted   bool
	cancel    context.CancelFunc
}

type terminalResult struct {
	status  string
	message *agent.Message
}

func New(config Config) (*Backend, error) {
	baseURL, socketBase, err := validateServerURL(config.ServerURL, config.SessionKey)
	if err != nil {
		return nil, err
	}
	workspace, err := validateWorkspace(config.Workspace)
	if err != nil {
		return nil, err
	}
	model := strings.TrimSpace(config.Model)
	if model == "" || len(model) > 256 || strings.ContainsAny(model, "\r\n\x00") {
		return nil, ErrInvalidConfiguration
	}
	mappingDir, err := ensureMappingDirectory(config.SessionDir)
	if err != nil {
		return nil, err
	}
	backend := &Backend{
		client: &http.Client{
			Timeout: 15 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		baseURL: baseURL, socketBase: socketBase, apiKey: config.SessionKey,
		workspace: workspace, model: model, mappingDir: mappingDir,
		sessions: make(map[string]*sessionState), turnGate: make(chan struct{}, 1),
		turnTimeout: defaultTurnTimeout, disconnectWait: defaultDisconnectWait,
		pollInterval: defaultPollInterval, cancelWait: cancellationWait,
	}
	startupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	backend.version, err = backend.serverVersion(startupCtx)
	if err == nil {
		err = backend.verifyAuthentication(startupCtx)
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
		"MAISTR0_OPENHANDS_SERVER_URL",
		"MAISTR0_OPENHANDS_SESSION_API_KEY",
		"MAISTR0_OPENHANDS_WORKSPACE",
		"MAISTR0_OPENHANDS_MODEL",
		"MAISTR0_OPENHANDS_SESSION_DIR",
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
			return nil, true, fmt.Errorf("OpenHands opt-in requires %s", name)
		}
	}
	backend, err := New(Config{
		ServerURL:  values["MAISTR0_OPENHANDS_SERVER_URL"],
		SessionKey: values["MAISTR0_OPENHANDS_SESSION_API_KEY"],
		Workspace:  values["MAISTR0_OPENHANDS_WORKSPACE"],
		Model:      values["MAISTR0_OPENHANDS_MODEL"],
		SessionDir: values["MAISTR0_OPENHANDS_SESSION_DIR"],
	})
	return backend, true, err
}

func (b *Backend) Name() string    { return Name }
func (b *Backend) Version() string { return b.version }

func (b *Backend) CreateSession(title string, cfg agent.SessionConfig) (*agent.Session, error) {
	if cfg.CoordinatorModel != "" || cfg.MaxSteps != 0 || cfg.Temperature != 0 || cfg.SystemPrompt != "" {
		return nil, ErrUnsupportedConfiguration
	}
	if !b.beginOperation() {
		return nil, ErrServerUnavailable
	}
	defer b.operations.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	info, err := b.createConversation(ctx)
	if err != nil {
		return nil, err
	}
	if title == "" {
		title = "OpenHands Session " + time.Now().Format("Jan 02 15:04")
	}
	id, err := newSessionID()
	if err != nil {
		return nil, b.cleanupConversation(ctx, info.ID, err)
	}
	now := time.Now()
	session := &agent.Session{
		ID: id, Title: title, Harness: Name, CreatedAt: now, UpdatedAt: now,
		Messages: []agent.Message{}, Memory: map[string]string{}, Config: cfg,
	}
	mapping := sessionMapping{
		Version: mappingVersion, SessionID: id, ConversationID: info.ID,
		Workspace: b.workspace, LastSeq: -1, PendingStartSeq: -1,
	}
	if err := saveMapping(b.mappingDir, mapping); err != nil {
		return nil, b.cleanupConversation(ctx, info.ID, err)
	}
	state := &sessionState{session: session, mapping: mapping}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = removeMapping(b.mappingDir, id)
		return nil, b.cleanupConversation(ctx, info.ID, ErrServerUnavailable)
	}
	b.sessions[id] = state
	b.mu.Unlock()
	return cloneSession(session), nil
}

func (b *Backend) cleanupConversation(ctx context.Context, conversationID string, cause error) error {
	if !validConversationID(conversationID) {
		return cause
	}
	err := b.request(ctx, http.MethodDelete, "/api/conversations/"+conversationID, nil, nil, nil)
	if err != nil && !errors.Is(err, ErrConversationUnavailable) {
		return errors.Join(cause, fmt.Errorf("OpenHands conversation cleanup failed: %w", err))
	}
	return cause
}

func (b *Backend) RestoreSession(session *agent.Session) error {
	if session == nil || !validSessionID(session.ID) {
		return ErrSessionMapping
	}
	mapping, err := loadMapping(b.mappingDir, session.ID)
	if err != nil {
		return err
	}
	if mapping.Workspace != b.workspace {
		return ErrSessionOwnership
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrServerUnavailable
	}
	if _, exists := b.sessions[session.ID]; exists {
		return errors.New("OpenHands session already exists")
	}
	restored := cloneSession(session)
	restored.Harness = Name
	restored.Active = mapping.Pending
	b.sessions[restored.ID] = &sessionState{
		session: restored, mapping: mapping, uncertain: mapping.Pending,
	}
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
	if state.deleted || state.active && !state.mapping.Pending {
		state.mu.Unlock()
		return false
	}
	mapping := state.mapping
	pending := mapping.Pending
	if pending && state.cancel != nil {
		state.cancel()
	}
	state.mu.Unlock()
	if pending {
		result, err := b.interruptAndConfirm(state)
		if err != nil || result.status == "" {
			return false
		}
		if !waitForSessionInactive(state, b.cancelWait) {
			return false
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	_, err := b.getConversation(ctx, mapping.ConversationID)
	if errors.Is(err, ErrConversationUnavailable) {
		cancel()
		return b.forgetSession(id, state)
	}
	if err != nil {
		cancel()
		return false
	}
	err = b.request(ctx, http.MethodDelete, "/api/conversations/"+mapping.ConversationID, nil, nil, nil)
	cancel()
	if err != nil && !errors.Is(err, ErrConversationUnavailable) {
		return false
	}
	return b.forgetSession(id, state)
}

func waitForSessionInactive(state *sessionState, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		state.mu.Lock()
		active := state.active
		state.mu.Unlock()
		if !active {
			return true
		}
		select {
		case <-timer.C:
			return false
		case <-ticker.C:
		}
	}
}

func (b *Backend) forgetSession(id string, state *sessionState) bool {
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
	state.mu.Unlock()
	result, err := b.interruptAndConfirm(state)
	return err == nil && result.status == "paused"
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
	for _, state := range states {
		state.mu.Lock()
		if state.cancel != nil {
			state.cancel()
		}
		state.mu.Unlock()
	}
	b.mu.Unlock()

	var closeErr error
	for _, state := range states {
		state.mu.Lock()
		pending := state.mapping.Pending
		state.mu.Unlock()
		if pending {
			if _, err := b.interruptAndConfirm(state); err != nil {
				closeErr = errors.Join(closeErr, ErrCancellationUnconfirmed)
			}
		}
	}
	b.operations.Wait()
	b.client.CloseIdleConnections()
	if closeErr != nil {
		return closeErr
	}
	b.apiKey = ""
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

func (b *Backend) getState(id string) (*sessionState, bool) {
	b.mu.RLock()
	state, exists := b.sessions[id]
	b.mu.RUnlock()
	return state, exists
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

func newSessionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", ErrSessionMapping
	}
	return "openhands-" + hex.EncodeToString(value), nil
}

func newLocalMessageID() string {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		return "openhands-interrupted"
	}
	return "openhands-" + hex.EncodeToString(value)
}

func validConversationID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16
}

func validSessionID(value string) bool {
	return strings.TrimSpace(value) != "" && len(value) <= 256 && !strings.ContainsRune(value, '\x00')
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

var _ agent.Backend = (*Backend)(nil)
var _ agent.SessionRestorer = (*Backend)(nil)
