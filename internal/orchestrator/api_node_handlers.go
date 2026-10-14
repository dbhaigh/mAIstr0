package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/version"
)

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var status cluster.NodeStatus
	if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
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
				log.Printf("orchestrator: %s", message)
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
		response, err := s.nodeClient.dispatcher.Do(request)
		if err != nil {
			results[node.ID] = "unreachable: " + err.Error()
			continue
		}
		response.Body.Close()
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
	resp, err := s.nodeClient.client.Get(n.Address + "/models/all")
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node unreachable: %w", err)))
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	req, err := http.NewRequest(http.MethodPatch, n.Address+"/models", bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.nodeClient.client.Do(req)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node unreachable: %w", err)))
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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
	if strings.HasSuffix(r.URL.Path, "/pull") {
		path = "/models/pull"
	}
	if strings.HasSuffix(r.URL.Path, "/refresh") {
		path = "/models/refresh"
	}
	if strings.HasSuffix(r.URL.Path, "/pairing/connect") {
		path = "/api/pairing/connect"
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	req, err := http.NewRequest(r.Method, n.Address+path, bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	for k, values := range r.Header {
		for _, value := range values {
			req.Header.Add(k, value)
		}
	}
	resp, err := s.nodeClient.dispatcher.Do(req)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node communication error: %w", err)))
		return
	}
	defer resp.Body.Close()
	for k, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(k, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// handleNodeChat proxies a multi-turn chat request directly to a node's /chat endpoint.
func (s *Server) handleNodeChat(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		writeError(w, apperr.NotFoundError(errors.New("node not found or offline")))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	req, err := http.NewRequest(http.MethodPost, n.Address+"/chat", bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.nodeClient.dispatcher.Do(req)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node execution failed: %w", err)))
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	req, err := http.NewRequest(r.Method, destURL, bytes.NewReader(body))
	if err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	for k, v := range r.Header {
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}
	resp, err := s.nodeClient.dispatcher.Do(req)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node communication error: %w", err)))
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		for _, val := range v {
			w.Header().Add(k, val)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
