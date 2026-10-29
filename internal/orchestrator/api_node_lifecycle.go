package orchestrator

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/closeutil"
)

func (s *Server) handleNodeServiceStatus(w http.ResponseWriter, r *http.Request) {
	s.proxyNodeServiceRequest(w, r, "GET", "status")
}

func (s *Server) handleNodeServiceAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	switch action {
	case "start", "stop", "restart", "drain":
	default:
		writeError(w, apperr.Invalid(errors.New("action must be start, stop, restart, or drain")))
		return
	}
	s.proxyNodeServiceRequest(w, r, http.MethodPost, action)
}

func (s *Server) proxyNodeServiceRequest(w http.ResponseWriter, r *http.Request, method, operation string) {
	node, ok := s.registry.Get(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	address, err := serviceControlURL(node.ControlAddress)
	if err != nil {
		writeError(w, apperr.UnavailableError(fmt.Errorf("node service controller unavailable: %w", err)))
		return
	}
	request, err := http.NewRequestWithContext(r.Context(), method, address+"/api/service/"+operation, nil)
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("prepare node service request: %w", err)))
		return
	}
	response, err := s.nodeClientPort().Do(request)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("node service controller unreachable: %w", err)))
		return
	}
	defer closeutil.Discard(response.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.StatusCode)
	copyNodeResponse(w, response.Body)
}

func serviceControlURL(value string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("invalid or unconfigured control address")
	}
	return "https://" + parsed.Host, nil
}
