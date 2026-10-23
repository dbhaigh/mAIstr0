package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
)

type managedEngineStartRequest struct {
	ShardPlan *supervisor.ShardPlan `json:"shard_plan,omitempty"`
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
	a.writeManagedEngineStatus(w, r.PathValue("id"))
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
	case errors.Is(err, supervisor.ErrUnsupportedPlatform):
		status, message = http.StatusNotImplemented, "vllm managed engines are not supported on native Windows"
	case errors.Is(err, supervisor.ErrAlreadyRunning):
		status, message = http.StatusConflict, "managed engine is already running"
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
