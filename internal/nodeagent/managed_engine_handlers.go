package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
)

type managedEngineStartRequest struct {
	ShardPlan *supervisor.ShardPlan `json:"shard_plan,omitempty"`
}

func (a *Agent) handleDownloadHeadModel(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSON[struct {
		Model            string `json:"model"`
		RequestedContext int    `json:"requested_context"`
	}](w, r, httpx.ControlJSONBodyBytes)
	if err != nil || request.Model == "" || request.RequestedContext < 1 || request.RequestedContext > 1<<20 {
		http.Error(w, "a recommended model ID and valid requested_context are required", http.StatusBadRequest)
		return
	}
	model, err := a.managedEngines.DownloadHeadModel(r.Context(), request.Model, request.RequestedContext)
	if err != nil {
		logging.Errorf("nodeagent", "Hugging Face model acquisition failed: %v", err)
		if errors.Is(err, supervisor.ErrNotFound) {
			http.Error(w, "unknown recommended GGUF model ID", http.StatusBadRequest)
			return
		}
		http.Error(w, "Hugging Face model acquisition failed; check node logs for metadata, download, or verification errors.", http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, model)
}

func (a *Agent) requireManagedEngineOrchestrator(next http.Handler) http.Handler {
	return a.identityComponent.requireTrustedPeer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "managed engine control requires a paired orchestrator", http.StatusForbidden)
			return
		}
		peerID := strings.TrimSpace(r.TLS.PeerCertificates[0].Subject.CommonName)
		if !a.identityComponent.IsTrustedOrchestratorID(peerID) || peerID != a.currentElectedOrchestratorID() {
			http.Error(w, "managed engine control requires the current paired orchestrator", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	}))
}

func (a *Agent) handleManagedEngineStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.managedEngines.Statuses())
}

func (a *Agent) handleManagedWorkerInstall(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSON[struct {
		Source string `json:"source"`
	}](w, r, httpx.ControlJSONBodyBytes)
	if err != nil || (request.Source != "official" && request.Source != "xdna") {
		http.Error(w, "source must be official or xdna", http.StatusBadRequest)
		return
	}
	result, err := a.managedEngines.EnsureWorker(r.Context(), request.Source)
	if err != nil {
		logging.Errorf("nodeagent", "managed worker installation failed: %v", err)
		switch {
		case errors.Is(err, supervisor.ErrWorkerBusy):
			http.Error(w, "Worker update deferred: stop all managed engines before updating.", http.StatusConflict)
		case errors.Is(err, supervisor.ErrUnsupportedPlatform):
			http.Error(w, "Automatic workers support Windows and Linux amd64 only.", http.StatusNotImplemented)
		case errors.Is(err, supervisor.ErrClosed):
			http.Error(w, "Managed engine supervisor is closed.", http.StatusServiceUnavailable)
		case errors.Is(err, context.Canceled):
			http.Error(w, "Worker installation was canceled.", http.StatusRequestTimeout)
		case errors.Is(err, context.DeadlineExceeded):
			http.Error(w, "Worker installation timed out.", http.StatusGatewayTimeout)
		default:
			http.Error(w, "Worker installation failed: no supported verified release is available, or its download/extraction failed. Check node logs.", http.StatusBadGateway)
		}
		return
	}
	a.refreshHardwareUsage()
	result.Devices = a.hardwareSnapshot().Devices
	writeJSON(w, http.StatusOK, result)
}

func (a *Agent) handleLocalHeadModels(w http.ResponseWriter, r *http.Request) {
	requestedContext, err := strconv.Atoi(r.URL.Query().Get("requested_context"))
	if err != nil || requestedContext < 1 || requestedContext > 1<<20 {
		http.Error(w, "requested_context must be an integer from 1 to 1048576", http.StatusBadRequest)
		return
	}
	models, err := a.managedEngines.LocalHeadModels(r.Context(), requestedContext)
	if err != nil {
		logging.Errorf("nodeagent", "local head model discovery failed: %v", err)
		http.Error(w, "Local model discovery failed; check node logs.", http.StatusUnprocessableEntity)
		return
	}
	writeJSON(w, http.StatusOK, models)
}

func (a *Agent) handleConfigureLocalHead(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSON[struct {
		ModelID          string `json:"model_id"`
		RequestedContext int    `json:"requested_context"`
	}](w, r, httpx.ControlJSONBodyBytes)
	if err != nil || request.ModelID == "" || request.RequestedContext < 1 || request.RequestedContext > 1<<20 {
		http.Error(w, "model_id and a positive requested_context are required", http.StatusBadRequest)
		return
	}
	status, err := a.managedEngines.ConfigureLocalHead(r.Context(), request.ModelID, request.RequestedContext)
	if err != nil {
		logging.Errorf("nodeagent", "local model head configuration failed: %v", err)
		writeManagedEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (a *Agent) handleManagedEngineModelProfile(w http.ResponseWriter, r *http.Request) {
	rawContext := r.URL.Query().Get("requested_context")
	parsedContext, err := strconv.Atoi(rawContext)
	if err != nil || parsedContext < 1 || parsedContext > 1<<20 {
		http.Error(w, "requested_context must be an integer from 1 to 1048576", http.StatusBadRequest)
		return
	}
	profile, err := a.managedEngines.ModelProfile(r.PathValue("id"), parsedContext)
	if err != nil {
		if errors.Is(err, supervisor.ErrNotFound) {
			http.Error(w, "managed engine not found", http.StatusNotFound)
			return
		}
		logging.Errorf("nodeagent", "nodeagent: managed model profile read failed for engine %s: %v", r.PathValue("id"), err)
		http.Error(w, "model profile is unavailable; verify the configured model metadata and weight files", http.StatusUnprocessableEntity)
		return
	}
	writeJSON(w, http.StatusOK, profile)
}

func (a *Agent) handleManagedEngineStart(w http.ResponseWriter, r *http.Request) {
	request, err := decodeManagedEngineStartRequest(r)
	if err != nil {
		http.Error(w, "invalid managed engine request body", http.StatusBadRequest)
		return
	}
	if err := a.managedEngines.Start(r.Context(), r.PathValue("id"), request.ShardPlan); err != nil {
		writeManagedEngineError(w, err)
		return
	}
	a.writeManagedEngineStatus(w, r.PathValue("id"))
}

func (a *Agent) handleManagedEngineStop(w http.ResponseWriter, r *http.Request) {
	if err := requireEmptyManagedEngineBody(r); err != nil {
		http.Error(w, "stop does not accept a request body", http.StatusBadRequest)
		return
	}
	if err := a.managedEngines.Stop(r.Context(), r.PathValue("id")); err != nil {
		writeManagedEngineError(w, err)
		return
	}
	a.telemetryComponent.refreshHardwareUsage()
	a.writeManagedEngineStatus(w, r.PathValue("id"))
}

func (a *Agent) handleManagedEngineProxy(w http.ResponseWriter, r *http.Request) {
	path := "/" + strings.TrimPrefix(r.PathValue("path"), "/")
	if _, ok := inferroute.Lookup(r.Method, path); !ok {
		http.NotFound(w, r)
		return
	}
	status, err := a.managedEngines.Status(r.PathValue("id"))
	if err != nil {
		writeManagedEngineError(w, err)
		return
	}
	if status.State != "ready" {
		http.Error(w, "managed engine is not ready", http.StatusServiceUnavailable)
		return
	}
	endpoint, err := url.Parse(status.Endpoint)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil {
		http.Error(w, "managed engine does not expose an HTTP endpoint", http.StatusBadGateway)
		return
	}
	upstreamURL := strings.TrimRight(status.Endpoint, "/") + path
	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}
	request, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, r.Body)
	if err != nil {
		http.Error(w, "could not prepare managed engine request", http.StatusBadGateway)
		return
	}
	request.Header = r.Header.Clone()
	stripManagedProxyHopHeaders(request.Header)
	response, err := httpx.NewClient(httpx.ClientOptions{}).Do(request)
	if err != nil {
		http.Error(w, "managed engine request failed", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	if err := copyManagedEngineResponse(w, response); err != nil {
		logging.Errorf("nodeagent", "managed engine proxy response failed: %v", err)
	}
}

func copyManagedEngineResponse(w http.ResponseWriter, response *http.Response) error {
	copyManagedProxyHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	stream := httpx.NewStreamWriter(w)
	stream.Flush()
	return stream.Copy(response.Body, nil)
}

func stripManagedProxyHopHeaders(header http.Header) {
	for _, name := range []string{
		"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"TE", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		header.Del(name)
	}
}

func copyManagedProxyHeaders(destination, source http.Header) {
	for name, values := range source {
		if strings.EqualFold(name, "Connection") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		for _, value := range values {
			destination.Add(name, value)
		}
	}
}

func (a *Agent) handleManagedEngineRestart(w http.ResponseWriter, r *http.Request) {
	request, err := decodeManagedEngineStartRequest(r)
	if err != nil {
		http.Error(w, "invalid managed engine request body", http.StatusBadRequest)
		return
	}
	if err := a.managedEngines.Restart(r.Context(), r.PathValue("id"), request.ShardPlan); err != nil {
		writeManagedEngineError(w, err)
		return
	}
	a.writeManagedEngineStatus(w, r.PathValue("id"))
}

func (a *Agent) writeManagedEngineStatus(w http.ResponseWriter, id string) {
	status, err := a.managedEngines.Status(id)
	if err != nil {
		writeManagedEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func decodeManagedEngineStartRequest(r *http.Request) (managedEngineStartRequest, error) {
	var request managedEngineStartRequest
	if r.Body == nil {
		return request, nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, (64<<10)+1))
	if err != nil {
		return managedEngineStartRequest{}, err
	}
	if len(body) > 64<<10 {
		return managedEngineStartRequest{}, errors.New("managed engine request body is too large")
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return request, nil
	}
	if trimmed := bytes.TrimSpace(body); trimmed[0] != '{' {
		return managedEngineStartRequest{}, errors.New("managed engine request must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		return managedEngineStartRequest{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return managedEngineStartRequest{}, errors.New("multiple JSON values")
		}
		return managedEngineStartRequest{}, err
	}
	return request, nil
}

func requireEmptyManagedEngineBody(r *http.Request) error {
	if r.Body == nil {
		return nil
	}
	var extra any
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected request body")
		}
		return err
	}
	return nil
}

func writeManagedEngineError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	message := "managed engine operation failed"
	switch {
	case errors.Is(err, supervisor.ErrNotFound):
		status, message = http.StatusNotFound, "managed engine not found"
	case errors.Is(err, supervisor.ErrInvalidShardPlan):
		status, message = http.StatusBadRequest, "invalid shard plan"
	case errors.Is(err, supervisor.ErrUntrustedShardNetwork):
		status, message = http.StatusBadRequest, "vLLM shard network preflight failed"
	case errors.Is(err, supervisor.ErrRelayUnavailable):
		status, message = http.StatusBadGateway, "authenticated RPC relay could not be started"
	case errors.Is(err, supervisor.ErrUnsupportedPlatform):
		status, message = http.StatusNotImplemented, "vllm managed engines are not supported on native Windows"
	case errors.Is(err, supervisor.ErrAlreadyRunning):
		status, message = http.StatusConflict, "managed engine is already running"
	case errors.Is(err, supervisor.ErrWorkerBusy):
		status, message = http.StatusConflict, "managed worker installation conflicts with active engine control"
	case errors.Is(err, supervisor.ErrStop):
		status, message = http.StatusGatewayTimeout, "managed engine did not stop before the deadline"
	case errors.Is(err, supervisor.ErrReadinessTimeout):
		status, message = http.StatusGatewayTimeout, "managed engine readiness timed out"
	case errors.Is(err, supervisor.ErrRestartLimit):
		status, message = http.StatusBadGateway, "managed engine restart limit exceeded"
	case errors.Is(err, supervisor.ErrClosed):
		status, message = http.StatusServiceUnavailable, "managed engine supervisor is closed"
	case errors.Is(err, context.Canceled):
		status, message = http.StatusRequestTimeout, "managed engine operation was canceled"
	case errors.Is(err, supervisor.ErrStart), errors.Is(err, supervisor.ErrProcessExited):
		status, message = http.StatusBadGateway, "managed engine failed to start"
	}
	writeJSON(w, status, map[string]string{"error": message})
}
