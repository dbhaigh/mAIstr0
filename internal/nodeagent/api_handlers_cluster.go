package nodeagent

import (
	"errors"
	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/discovery"
	"net/http"
	"net/url"
	"strings"
)

// handleAPIOrchestratorProxy forwards dashboard operations to the joined
// orchestrator so node-served dashboards use the same cluster APIs.
func (a *Agent) handleAPIOrchestratorProxy(w http.ResponseWriter, r *http.Request) {
	addr := a.getOrchestratorAddr()
	if addr == "" {
		writeError(w, apperr.UnavailableError(errors.New("this node has not joined an orchestrator")))
		return
	}
	a.proxy(w, r, addr, withRawQuery(r.URL.Path, r))
}

func (a *Agent) handleAPIOrchestratorRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, apperr.Invalid(errors.New("cluster view only supports GET")))
		return
	}
	a.handleAPIOrchestratorProxy(w, r)
}

// withRawQuery keeps the caller's query string (for example pagination) when
// proxying to the orchestrator.
func withRawQuery(path string, r *http.Request) string {
	if r.URL.RawQuery == "" {
		return path
	}
	return path + "?" + r.URL.RawQuery
}

func (a *Agent) handlePeersGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.Peers())
}

// handleAPINodesGet mirrors the orchestrator's GET /api/nodes so the same
// GUI works unmodified when loaded from any node: every node's peer cache
// is refreshed from the same orchestrator registry, so this list is
// identical across the whole cluster.
func (a *Agent) handleAPINodesGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.Peers())
}

func (a *Agent) handleAPIDiscovery(w http.ResponseWriter, r *http.Request) {
	if a.discoveryListener == nil {
		writeJSON(w, http.StatusOK, []discovery.Peer{})
		return
	}
	writeJSON(w, http.StatusOK, a.discoveryListener.Snapshot())
}

func (a *Agent) handleAPIDiscoveryPairingInvite(w http.ResponseWriter, r *http.Request) {
	orchestratorAddr := a.getOrchestratorAddr()
	if orchestratorAddr == "" {
		writeError(w, apperr.UnavailableError(errors.New("pairing invitations must be sent from a joined cluster member")))
		return
	}
	path := "/api/discovery/" + url.PathEscape(r.PathValue("id")) + "/pairing/invite"
	a.proxy(w, r, orchestratorAddr, path)
}

func (a *Agent) handleAPIMemorySync(w http.ResponseWriter, r *http.Request) {
	addr := a.getOrchestratorAddr()
	if addr == "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator unavailable")))
		return
	}
	a.proxy(w, r, addr, "/api/memory/sync")
}

// handleAPINodeModelsGet/Patch serve local models directly and proxy remote
// models to their owner so the same GUI works from any node.
func (a *Agent) handleAPINodeModelsGet(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("id") == a.cfg.NodeID {
		a.handleModelsAllGet(w, r)
		return
	}
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	a.proxy(w, r, addr, "/models/all")
}

func (a *Agent) handleAPINodeModelsPatch(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("id") == a.cfg.NodeID {
		a.handleModelsPatch(w, r)
		return
	}
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	a.proxy(w, r, addr, "/models")
}

func (a *Agent) handleAPINodeControl(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	if strings.Contains(r.URL.Path, "/engines/managed") {
		orchestratorAddr := a.getOrchestratorAddr()
		if orchestratorAddr == "" {
			writeError(w, apperr.UnavailableError(errors.New("orchestrator unavailable")))
			return
		}
		a.proxy(w, r, orchestratorAddr, r.URL.RequestURI())
		return
	}
	path := "/properties"
	if strings.HasSuffix(r.URL.Path, "/engines") {
		path = "/engine"
	}
	if strings.HasSuffix(r.URL.Path, "/pull") {
		path = "/models/pull"
	}
	if strings.HasSuffix(r.URL.Path, "/refresh") {
		path = "/models/refresh"
	}
	if strings.HasSuffix(r.URL.Path, "/pairing/connect") {
		path = "/api/pairing/connect"
	}
	if nodeID == a.cfg.NodeID {
		switch path {
		case "/engine":
			a.handleEngineInfo(w, r)
		case "/models/pull":
			a.handleModelPull(w, r)
		case "/models/refresh":
			a.handleModelsRefresh(w, r)
		case "/api/pairing/connect":
			a.handlePairingConnect(w, r)
		default:
			a.handleProperties(w, r)
		}
		return
	}
	if _, ok := a.peerAddress(nodeID); !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	orchestratorAddr := a.getOrchestratorAddr()
	if orchestratorAddr == "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator unavailable")))
		return
	}
	a.proxy(w, r, orchestratorAddr, r.URL.RequestURI())
}

func (a *Agent) handleAPINodeDialogue(w http.ResponseWriter, r *http.Request) {
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	prefix := "/api/nodes/" + url.PathEscape(r.PathValue("id"))
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if path == "" {
		path = "/dialogues"
	} else if strings.HasPrefix(path, "/dialogues") {
		path = strings.TrimPrefix(path, "/dialogues")
		path = "/dialogues" + path
	}
	a.proxy(w, r, addr, path)
}

func (a *Agent) handleAPIAgentProxy(w http.ResponseWriter, r *http.Request) {
	orch := a.getOrchestratorAddr()
	if orch == "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator unavailable; harness requests cannot run on a worker node")))
		return
	}
	destPath := r.URL.Path
	if r.URL.RawQuery != "" {
		destPath += "?" + r.URL.RawQuery
	}
	a.proxy(w, r, orch, destPath)
}
