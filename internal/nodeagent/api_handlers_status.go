package nodeagent

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/memory"
)

func (a *Agent) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.status())
}

func (a *Agent) handleSystemStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"role":        "node",
		"persistence": memory.AvailabilityFor(a.mem != nil),
	})
}
