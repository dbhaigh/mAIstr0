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
	mu               sync.RWMutex
	agents           map[string]agent.Backend
	activeHarness    string
	sessionHarnesses map[string]string
	sessionStore     SessionStore
	sessionCancels   map[string]context.CancelFunc
	cancelsMu        sync.Mutex
}

func newHarnessManager(agents map[string]agent.Backend, activeHarness string, sessionStore SessionStore) *harnessManager {
	return &harnessManager{
		agents: agents, activeHarness: activeHarness,
		sessionHarnesses: make(map[string]string), sessionStore: sessionStore,
		sessionCancels: make(map[string]context.CancelFunc),
	}
}

func (h *harnessManager) registerBackend(name string, backend agent.Backend) {
	h.mu.Lock()
	h.agents[name] = backend
	h.mu.Unlock()
}

func (h *harnessManager) harnessName() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.activeHarness
}

func (h *harnessManager) backend(name string) agent.Backend {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.agents[name]
}

func (h *harnessManager) currentAgent() (string, agent.Backend) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.activeHarness, h.agents[h.activeHarness]
}

func (h *harnessManager) agentForSession(id string) (string, agent.Backend, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	name, ok := h.sessionHarnesses[id]
	if !ok {
		return "", nil, false
	}
	backend, ok := h.agents[name]
	return name, backend, ok
}

func (h *harnessManager) selectHarness(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.agents[name]; !ok {
		return false
	}
	h.activeHarness = name
	return true
}

func (h *harnessManager) setSessionHarness(id, name string) {
	h.mu.Lock()
	h.sessionHarnesses[id] = name
	h.mu.Unlock()
}

func (h *harnessManager) removeSessionHarness(id, name string) {
	h.mu.Lock()
	if h.sessionHarnesses[id] == name {
		delete(h.sessionHarnesses, id)
	}
	h.mu.Unlock()
}

func (h *harnessManager) listAgentSessions() []*agent.Session {
	h.mu.RLock()
	owners := make(map[string]string, len(h.sessionHarnesses))
	backends := make(map[string]agent.Backend, len(h.agents))
	for id, name := range h.sessionHarnesses {
		owners[id] = name
	}
	for name, backend := range h.agents {
		backends[name] = backend
	}
	h.mu.RUnlock()

	sessions := make([]*agent.Session, 0, len(owners))
	for id, name := range owners {
		session, ok := backends[name].GetSession(id)
		if !ok {
			continue
		}
		session.Harness = name
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	return sessions
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
			return sessions[i].Harness == "deepseek"
		}
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, session := range sessions {
		backend, exists := h.agents[session.Harness]
		if !exists {
			return fmt.Errorf("persisted session %q has unsupported harness %q", session.ID, session.Harness)
		}
		restorer, ok := backend.(agent.SessionRestorer)
		if !ok {
			return fmt.Errorf("agent backend %q cannot restore sessions", session.Harness)
		}
		if err := restorer.RestoreSession(session); err != nil {
			return fmt.Errorf("restore session %q for %s: %w", session.ID, session.Harness, err)
		}
		h.sessionHarnesses[session.ID] = session.Harness
	}
	return nil
}

func (h *harnessManager) persistAgentSession(session *agent.Session) error {
	if h.sessionStore == nil {
		return nil
	}
	if session == nil || session.ID == "" || session.Harness == "" {
		return errors.New("session ID and harness are required")
	}
	encoded, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("encode agent session: %w", err)
	}
	return h.sessionStore.SaveAgentSession(session.Harness, session.ID, encoded)
}

func (h *harnessManager) agentStats() agent.AgentStats {
	_, activeBackend := h.currentAgent()
	stats := activeBackend.Stats()
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

func (h *harnessManager) setTurnCancel(id string, cancel func()) {
	h.cancelsMu.Lock()
	h.sessionCancels[id] = cancel
	h.cancelsMu.Unlock()
}

func (h *harnessManager) clearTurnCancel(id string) {
	h.cancelsMu.Lock()
	delete(h.sessionCancels, id)
	h.cancelsMu.Unlock()
}

func (h *harnessManager) interruptSession(id string) bool {
	h.cancelsMu.Lock()
	cancel, hasCancel := h.sessionCancels[id]
	h.cancelsMu.Unlock()
	if hasCancel && cancel != nil {
		cancel()
	}

	_, backend, exists := h.agentForSession(id)
	backendInterrupted := false
	if exists && backend != nil {
		backendInterrupted = backend.InterruptSession(id)
	}
	return hasCancel || backendInterrupted
}

func (h *harnessManager) close() error {
	h.mu.RLock()
	backends := make([]agent.Backend, 0, len(h.agents))
	for _, backend := range h.agents {
		backends = append(backends, backend)
	}
	h.mu.RUnlock()
	var closeErr error
	for _, backend := range backends {
		if err := backend.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func (h *harnessManager) deletePersistedSession(harness, id string) error {
	if h.sessionStore == nil {
		return nil
	}
	return h.sessionStore.DeleteAgentSession(harness, id)
}
