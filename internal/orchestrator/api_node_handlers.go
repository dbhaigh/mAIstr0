package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/version"
)

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	status, err := httpx.DecodeJSON[cluster.NodeStatus](
		w, r, httpx.NodeStatusJSONBodyBytes,
	)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	if status.ID == "" || status.Address == "" {
		writeError(w, apperr.Invalid(errors.New("id and address are required")))
		return
	}
	if status.Version != "" {
		for _, existing := range s.registry.All() {
			if existing.Version != "" && version.Compare(status.Version, existing.Version) < 0 {
				message := "node " + status.ID + " version " + status.Version + " is older than node " + existing.ID + " version " + existing.Version
				logging.Infof("orchestrator", "orchestrator: %s", message)
				s.registry.Upsert(status)
				writeError(w, apperr.ConflictError(errors.New(message)))
				return
			}
		}
	}
	s.registry.Upsert(status)
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleEject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, apperr.Invalid(errors.New("node id is required")))
		return
	}
	if s.identity != nil {
		if err := s.identity.RemovePeer(id); err != nil {
			writeError(w, apperr.InternalError(fmt.Errorf("could not revoke node certificate: %w", err)))
			return
		}
	}
	s.registry.Remove(id)
	s.publishSnapshot()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.Active())
}

func (s *Server) handleListAllNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.All())
}

func (s *Server) handleRefreshAllNodeModels(w http.ResponseWriter, r *http.Request) {
	results := make(map[string]string)
	for _, node := range s.registry.Active() {
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, node.Address+"/models/refresh", nil)
		if err != nil {
			results[node.ID] = err.Error()
			continue
		}
		response, err := s.nodeClientPort().DoDispatch(request)
		if err != nil {
			results[node.ID] = "unreachable: " + err.Error()
			continue
		}
		_ = response.Body.Close()
		if response.StatusCode >= 300 {
			results[node.ID] = response.Status
			continue
		}
		results[node.ID] = "refreshed"
	}
	writeJSON(w, http.StatusOK, results)
}

// handleNodeModelsGet proxies to a node agent's /models/all so the GUI can
// show every discovered model (enabled or not) without the browser needing
// direct network access to each node.
func (s *Server) handleNodeModelsGet(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, n.Address+"/models/all", nil)
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	resp, err := s.nodeClientPort().Do(request)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node unreachable: %w", err)))
		return
	}
	defer closeutil.Discard(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	copyNodeResponse(w, resp.Body)
}

func copyNodeResponse(w http.ResponseWriter, body io.Reader) {
	if err := httpx.NewStreamWriter(w).Copy(body, nil); err != nil {
		logging.Errorf("orchestrator", "orchestrator: stream node response: %v", err)
	}
}

func (s *Server) handleModelCatalog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, engine.ModelCatalog(r.URL.Query().Get("engine"), r.URL.Query().Get("q")))
}

// handleNodeModelsPatch proxies a disabled-models update to the node agent.
func (s *Server) handleNodeModelsPatch(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	body, err := httpx.ReadBody(w, r, httpx.ControlJSONBodyBytes)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPatch, n.Address+"/models", bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.nodeClientPort().Do(req)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node unreachable: %w", err)))
		return
	}
	defer closeutil.Discard(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	copyNodeResponse(w, resp.Body)
}

func (s *Server) handleNodeControl(w http.ResponseWriter, r *http.Request) {
	n, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	path := "/properties"
	if strings.HasSuffix(r.URL.Path, "/engines") {
		path = "/engine"
	}
	if strings.HasSuffix(r.URL.Path, "/engines/managed") {
		path = "/api/engines/managed"
	}
	if strings.HasSuffix(r.URL.Path, "/engines/managed/install-worker") {
		path = "/api/engines/managed/install-worker"
	}
	for _, operation := range []string{"head-models", "configure-head", "download-model"} {
		if strings.HasSuffix(r.URL.Path, "/engines/managed/"+operation) {
			path = "/api/engines/managed/" + operation
		}
	}
	if engineID := r.PathValue("engine_id"); engineID != "" {
		path = "/api/engines/managed/" + url.PathEscape(engineID) + "/profile"
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
	body, err := httpx.ReadBody(w, r, httpx.ControlJSONBodyBytes)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	target := n.Address + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	for k, values := range r.Header {
		for _, value := range values {
			req.Header.Add(k, value)
		}
	}
	var resp *http.Response
	if path == "/api/engines/managed/install-worker" || path == "/api/engines/managed/download-model" {
		client, ok := s.nodeClientPort().(interface {
			DoModelPull(*http.Request) (*http.Response, error)
		})
		if !ok {
			writeError(w, apperr.UnavailableError(errors.New("worker installation client is unavailable")))
			return
		}
		resp, err = client.DoModelPull(req)
	} else {
		resp, err = s.nodeClientPort().DoDispatch(req)
	}
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node communication error: %w", err)))
		return
	}
	defer closeutil.Discard(resp.Body)
	if path == "/api/engines/managed/install-worker" && resp.StatusCode == http.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, httpx.ControlJSONBodyBytes+1))
		var result supervisor.WorkerInstallResult
		if readErr != nil || len(data) > int(httpx.ControlJSONBodyBytes) || json.Unmarshal(data, &result) != nil {
			writeError(w, apperr.UpstreamError(errors.New("invalid worker installation response")))
			return
		}
		if result.Devices != nil && !s.registry.UpdateDevices(n.ID, n.Address, result.Devices) {
			writeError(w, apperr.UnavailableError(errors.New("node changed while refreshing device memory; retry installation")))
			return
		}
		resp.Body = io.NopCloser(bytes.NewReader(data))
	}
	for k, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(k, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	copyNodeResponse(w, resp.Body)
}

// handleNodeChat proxies a multi-turn chat request directly to a node's /chat endpoint.
func (s *Server) handleNodeChat(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		writeError(w, apperr.NotFoundError(errors.New("node not found or offline")))
		return
	}
	body, err := httpx.ReadBody(w, r, httpx.InferenceJSONBodyBytes)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, n.Address+"/chat", bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.nodeClientPort().DoDispatch(req)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node execution failed: %w", err)))
		return
	}
	defer closeutil.Discard(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	copyNodeResponse(w, resp.Body)
}

// handleNodeDialoguesProxy proxies dialogue interactions directly to a node's dialogue endpoints.
func (s *Server) handleNodeDialoguesProxy(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		writeError(w, apperr.NotFoundError(errors.New("node not found or offline")))
		return
	}
	// Extract subpath under /api/nodes/{id}/dialogues
	pathPrefix := "/api/nodes/" + url.PathEscape(nodeID)
	subPath := strings.TrimPrefix(r.URL.Path, pathPrefix)
	if !strings.HasPrefix(subPath, "/") {
		subPath = "/" + subPath
	}
	destURL := n.Address + subPath
	if r.URL.RawQuery != "" {
		destURL += "?" + r.URL.RawQuery
	}

	body, err := httpx.ReadBody(w, r, httpx.InferenceJSONBodyBytes)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, destURL, bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	for k, v := range r.Header {
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}
	resp, err := s.nodeClientPort().DoDispatch(req)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node communication error: %w", err)))
		return
	}
	defer closeutil.Discard(resp.Body)
	for k, v := range resp.Header {
		for _, val := range v {
			w.Header().Add(k, val)
		}
	}
	w.WriteHeader(resp.StatusCode)
	copyNodeResponse(w, resp.Body)
}
