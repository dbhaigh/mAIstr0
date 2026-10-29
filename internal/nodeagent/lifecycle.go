package nodeagent

import (
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

type lifecycleSnapshot struct {
	Draining    bool `json:"draining"`
	ActiveTasks int  `json:"active_tasks"`
	Idle        bool `json:"idle"`
}

func (a *Agent) SetShutdownHandler(shutdown func()) {
	a.lifecycleMu.Lock()
	a.shutdown = shutdown
	a.lifecycleMu.Unlock()
}

func (a *Agent) handleLifecycleStatus(w http.ResponseWriter, _ *http.Request) {
	cfg := a.configSnapshot()
	writeJSON(w, http.StatusOK, lifecycleSnapshot{
		Draining:    cfg.Draining,
		ActiveTasks: a.activeTaskCount() + a.peerComponent.tasks.ActiveCount(),
		Idle:        a.UpdateIdle(),
	})
}

func (a *Agent) handleLifecycleAuthority(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"orchestrator_id": a.currentElectedOrchestratorID()})
}

func (a *Agent) handleLifecycleDrain(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	a.cfg.Draining = true
	a.mu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]string{"state": "draining"})
}

func (a *Agent) handleLifecycleStop(w http.ResponseWriter, _ *http.Request) {
	a.lifecycleMu.RLock()
	shutdown := a.shutdown
	a.lifecycleMu.RUnlock()
	if shutdown == nil {
		writeError(w, apperr.UnavailableError(errLifecycleShutdownUnavailable))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"state": "stopping"})
	go func() {
		time.Sleep(100 * time.Millisecond)
		shutdown()
	}()
}

var errLifecycleShutdownUnavailable = lifecycleUnavailableError("node shutdown is not configured")

type lifecycleUnavailableError string

func (err lifecycleUnavailableError) Error() string { return string(err) }

func (a *Agent) lifecycleControl(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			next.ServeHTTP(w, r)
			return
		}
		a.requireManagedEngineOrchestrator(next).ServeHTTP(w, r)
	})
}

func (a *Agent) rejectDrainingWork(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && a.configSnapshot().Draining && isWorkRequest(r.URL.Path) {
			writeError(w, apperr.UnavailableError(lifecycleUnavailableError("node is draining and not accepting new work")))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isWorkRequest(path string) bool {
	switch {
	case path == "/execute", path == "/submit", path == "/generate", path == "/chat":
		return true
	case path == "/api/tasks":
		return true
	case strings.HasPrefix(path, "/api/tasks/") &&
		(strings.HasSuffix(path, "/resume") || strings.HasSuffix(path, "/approve")):
		return true
	case strings.HasPrefix(path, "/stage/"):
		return true
	case path == "/peer/message", path == "/peer/converse":
		return true
	case path == "/dialogues", strings.HasPrefix(path, "/dialogues/"):
		return true
	case strings.HasSuffix(path, "/dialogues"), strings.Contains(path, "/dialogues/"):
		return true
	default:
		return false
	}
}

func registerLifecycleRoutes(mux *http.ServeMux, agent *Agent) {
	mux.Handle("GET /api/lifecycle/status", peeridentity.LoopbackOnly(http.HandlerFunc(agent.handleLifecycleStatus)))
	mux.Handle("GET /api/lifecycle/authority", peeridentity.LoopbackOnly(http.HandlerFunc(agent.handleLifecycleAuthority)))
	mux.Handle("POST /api/lifecycle/drain", peeridentity.LoopbackOnly(agent.lifecycleControl(http.HandlerFunc(agent.handleLifecycleDrain))))
	mux.Handle("POST /api/lifecycle/stop", peeridentity.LoopbackOnly(agent.lifecycleControl(http.HandlerFunc(agent.handleLifecycleStop))))
}
