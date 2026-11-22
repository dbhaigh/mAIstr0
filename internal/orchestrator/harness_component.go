package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/agent"
	"sort"
	"sync"
)

type harnessManager struct {
	// mu protects backend/session ownership maps. Keep it independent from cancelsMu; do not call backends while holding it.
	mu               sync.RWMutex
	registry         *agent.Registry
	activeHarness    string
	sessionHarnesses map[string]string
	sessionSnapshots map[string]*agent.Session
	sessionReady     map[string]bool
	sessionReasons   map[string]string
	sessionStore     SessionStore
	// sessionCancels holds one handle per in-flight turn so overlapping turns
	// on a session cannot remove each other's cancellation.
	sessionCancels map[string]map[*harnessTurnHandle]struct{}
	// cancelsMu protects active-turn cancellation handles; copy handles before invoking them.
	cancelsMu sync.Mutex
}

type harnessAPI interface {
	harnessName() string
	currentAgent() (string, agent.Backend)
	backendRegistration(string) (agent.BackendRegistration, bool)
	agentForSession(string) (string, agent.Backend, bool)
	session(string) (agentSessionView, bool)
	selectHarness(string) bool
	setSessionHarness(string, string)
	removeSessionHarness(string, string)
	listAgentSessions() []agentSessionView
	backendViews() []agentBackendView
	restoreAgentSessions() error
	persistAgentSession(*agent.Session) error
	agentStats() agent.AgentStats
	registerTurnCancel(string, bool, context.CancelFunc) func()
	interruptSession(string) bool
	deletePersistedSession(string, string) error
	close() error
}

type agentSessionView struct {
	*agent.Session
	Available         bool   `json:"available"`
	UnavailableReason string `json:"unavailable_reason,omitempty"`
}

type agentBackendView struct {
	Name         string              `json:"name"`
	Available    bool                `json:"available"`
	Capabilities *agent.Capabilities `json:"capabilities,omitempty"`
}

func newHarnessManager(registry *agent.Registry, activeHarness string, sessionStore SessionStore) *harnessManager {
	return &harnessManager{
		registry: registry, activeHarness: activeHarness,
		sessionHarnesses: make(map[string]string), sessionSnapshots: make(map[string]*agent.Session),
		sessionReady: make(map[string]bool), sessionReasons: make(map[string]string),
		sessionStore:   sessionStore,
		sessionCancels: make(map[string]map[*harnessTurnHandle]struct{}),
	}
}

func (h *harnessManager) harnessName() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.activeHarness
}

func (h *harnessManager) backend(name string) agent.Backend {
	registration, ok := h.registry.Get(name)
	if !ok {
		return nil
	}
	return registration.Backend
}

func (h *harnessManager) backendRegistration(name string) (agent.BackendRegistration, bool) {
	return h.registry.Get(name)
}

func (h *harnessManager) currentAgent() (string, agent.Backend) {
	h.mu.RLock()
	name := h.activeHarness
	h.mu.RUnlock()
	return name, h.backend(name)
}

func (h *harnessManager) agentForSession(id string) (string, agent.Backend, bool) {
	h.mu.RLock()
	name, ok := h.sessionHarnesses[id]
	ready := h.sessionReady[id]
	h.mu.RUnlock()
	if !ok || !ready {
		return "", nil, false
	}
	backend := h.backend(name)
	return name, backend, backend != nil
}

func (h *harnessManager) selectHarness(name string) bool {
	if h.backend(name) == nil {
		return false
	}
	h.mu.Lock()
	h.activeHarness = name
	h.mu.Unlock()
	return true
}

func (h *harnessManager) setSessionHarness(id, name string) {
	h.mu.Lock()
	h.sessionHarnesses[id] = name
	h.sessionReady[id] = true
	delete(h.sessionReasons, id)
	h.mu.Unlock()
}

func (h *harnessManager) removeSessionHarness(id, name string) {
	h.deleteSessionState(id, name)
}

func (h *harnessManager) session(id string) (agentSessionView, bool) {
	h.mu.RLock()
	name, exists := h.sessionHarnesses[id]
	snapshot := h.sessionSnapshots[id]
	ready := h.sessionReady[id]
	reason := h.sessionReasons[id]
	h.mu.RUnlock()
	if !exists {
		return agentSessionView{}, false
	}
	if ready {
		if backend := h.backend(name); backend != nil {
			if session, ok := backend.GetSession(id); ok {
				session.Harness = name
				return agentSessionView{Session: session, Available: true}, true
			}
		}
		if reason == "" {
			reason = "session_unavailable"
		}
	}
	if snapshot == nil {
		return agentSessionView{}, false
	}
	session := *snapshot
	session.Harness = name
	if reason == "" {
		reason = "backend_unavailable"
	}
	return agentSessionView{Session: &session, UnavailableReason: reason}, true
}

func (h *harnessManager) listAgentSessions() []agentSessionView {
	h.mu.RLock()
	ids := make([]string, 0, len(h.sessionHarnesses))
	for id := range h.sessionHarnesses {
		ids = append(ids, id)
	}
	h.mu.RUnlock()

	sessions := make([]agentSessionView, 0, len(ids))
	for _, id := range ids {
		session, ok := h.session(id)
		if !ok {
			continue
		}
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	return sessions
}

func (h *harnessManager) backendViews() []agentBackendView {
	views := make(map[string]agentBackendView)
	for _, registration := range h.registry.List() {
		capabilities := registration.Capabilities
		name := registration.Backend.Name()
		views[name] = agentBackendView{
			Name: name, Available: true, Capabilities: &capabilities,
		}
	}
	h.mu.RLock()
	views[h.activeHarness] = unavailableBackendView(h.activeHarness, views[h.activeHarness])
	for _, name := range h.sessionHarnesses {
		if _, exists := views[name]; !exists {
			views[name] = unavailableBackendView(name, agentBackendView{})
		}
	}
	h.mu.RUnlock()
	out := make([]agentBackendView, 0, len(views))
	for _, view := range views {
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func unavailableBackendView(name string, existing agentBackendView) agentBackendView {
	if existing.Name != "" {
		return existing
	}
	return agentBackendView{Name: name, Available: false}
}

func (h *harnessManager) restoreAgentSessions() error {
	if h.sessionStore == nil {
		return nil
	}
	records, err := h.sessionStore.AgentSessions()
	if err != nil {
		return err
	}
	sessions := make([]*agent.Session, 0, len(records))
	for _, record := range records {
		var session agent.Session
		if err := json.Unmarshal(record, &session); err != nil {
			return fmt.Errorf("decode persisted session: %w", err)
		}
		if session.ID == "" || session.Harness == "" {
			return errors.New("persisted agent session is missing its ID or harness")
		}
		session.Active = false
		sessions = append(sessions, &session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].Harness != sessions[j].Harness {
			return sessions[i].Harness < sessions[j].Harness
		}
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	for _, session := range sessions {
		h.mu.Lock()
		if _, exists := h.sessionHarnesses[session.ID]; exists {
			h.mu.Unlock()
			return fmt.Errorf("persisted session ID %q is duplicated", session.ID)
		}
		h.sessionHarnesses[session.ID] = session.Harness
		h.sessionSnapshots[session.ID] = session
		h.sessionReady[session.ID] = false
		h.mu.Unlock()

		registration, exists := h.registry.Get(session.Harness)
		if !exists {
			h.setSessionUnavailable(session.ID, "backend_unavailable")
			continue
		}
		restorer, ok := registration.Backend.(agent.SessionRestorer)
		if !registration.Capabilities.SessionResume || !ok {
			h.setSessionUnavailable(session.ID, "session_resume_unsupported")
			continue
		}
		if err := restorer.RestoreSession(session); err != nil {
			h.setSessionUnavailable(session.ID, "session_restore_failed")
			continue
		}
		h.mu.Lock()
		h.sessionReady[session.ID] = true
		delete(h.sessionReasons, session.ID)
		h.mu.Unlock()
	}
	return nil
}

func (h *harnessManager) setSessionUnavailable(id, reason string) {
	h.mu.Lock()
	h.sessionReady[id] = false
	h.sessionReasons[id] = reason
	h.mu.Unlock()
}

func (h *harnessManager) persistAgentSession(session *agent.Session) error {
	if session == nil || session.ID == "" || session.Harness == "" {
		return errors.New("session ID and harness are required")
	}
	encoded, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("encode agent session: %w", err)
	}
	if h.sessionStore != nil {
		if err := h.sessionStore.SaveAgentSession(session.Harness, session.ID, encoded); err != nil {
			return err
		}
	}
	var snapshot agent.Session
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return fmt.Errorf("decode agent session snapshot: %w", err)
	}
	h.mu.Lock()
	h.sessionSnapshots[session.ID] = &snapshot
	h.mu.Unlock()
	return nil
}

func (h *harnessManager) agentStats() agent.AgentStats {
	_, activeBackend := h.currentAgent()
	stats := agent.AgentStats{}
	if activeBackend != nil {
		stats = activeBackend.Stats()
	}
	stats.TotalSessions = 0
	stats.ActiveSessions = 0
	stats.TotalMessages = 0
	stats.TotalToolCalls = 0
	for _, session := range h.listAgentSessions() {
		stats.TotalSessions++
		if session.Active {
			stats.ActiveSessions++
		}
		stats.TotalMessages += len(session.Messages)
		for _, message := range session.Messages {
			stats.TotalToolCalls += len(message.ToolCalls)
		}
	}
	return stats
}

type harnessTurnHandle struct {
	cancel      context.CancelFunc
	cancellable bool
}

// registerTurnCancel records an in-flight turn and returns a cleanup that
// removes only that turn's handle and cancels its context.
func (h *harnessManager) registerTurnCancel(id string, cancellable bool, cancel context.CancelFunc) func() {
	handle := &harnessTurnHandle{cancel: cancel, cancellable: cancellable}
	h.cancelsMu.Lock()
	turns := h.sessionCancels[id]
	if turns == nil {
		turns = make(map[*harnessTurnHandle]struct{})
		h.sessionCancels[id] = turns
	}
	turns[handle] = struct{}{}
	h.cancelsMu.Unlock()
	return func() {
		h.cancelsMu.Lock()
		if turns := h.sessionCancels[id]; turns != nil {
			delete(turns, handle)
			if len(turns) == 0 {
				delete(h.sessionCancels, id)
			}
		}
		h.cancelsMu.Unlock()
		cancel()
	}
}

func (h *harnessManager) interruptSession(id string) bool {
	name, backend, exists := h.agentForSession(id)
	if !exists || backend == nil {
		return false
	}
	registration, ok := h.registry.Get(name)
	if !ok || !registration.Capabilities.Cancellation {
		return false
	}
	h.cancelsMu.Lock()
	turns := h.sessionCancels[id]
	cancels := make([]context.CancelFunc, 0, len(turns))
	for handle := range turns {
		if handle.cancellable {
			cancels = append(cancels, handle.cancel)
		}
	}
	h.cancelsMu.Unlock()
	hasCancel := len(cancels) > 0
	for _, cancel := range cancels {
		cancel()
	}

	backendInterrupted := backend.InterruptSession(id)
	return hasCancel || backendInterrupted
}

func (h *harnessManager) close() error {
	var closeErr error
	for _, registration := range h.registry.List() {
		if err := registration.Backend.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func (h *harnessManager) deleteSessionState(id, harness string) {
	h.mu.Lock()
	if h.sessionHarnesses[id] == harness {
		delete(h.sessionHarnesses, id)
		delete(h.sessionSnapshots, id)
		delete(h.sessionReady, id)
		delete(h.sessionReasons, id)
	}
	h.mu.Unlock()
}

func (h *harnessManager) deletePersistedSession(harness, id string) error {
	if h.sessionStore == nil {
		return nil
	}
	return h.sessionStore.DeleteAgentSession(harness, id)
}
