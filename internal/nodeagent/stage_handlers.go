package nodeagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"mime"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
)

const (
	maxStageControlRequestBytes = 1 << 20
	maxStageForwardRequestBytes = 64 << 20
)

func (a *Agent) handleStageLoad(w http.ResponseWriter, r *http.Request) {
	assignment, err := decodeStageAssignment(w, r)
	if err != nil {
		writeStageError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.stageComponent.load(r.Context(), assignment, a.resolveStageEngine); err != nil {
		status := http.StatusBadGateway
		switch {
		case errors.Is(err, errStageAssignmentConflict):
			status = http.StatusConflict
		case errors.Is(err, errStageEngineUnsupported):
			status = http.StatusNotImplemented
		case errors.Is(err, errStageModelUnavailable):
			status = http.StatusNotFound
		case r.Context().Err() != nil:
			status = http.StatusRequestTimeout
		}
		writeStageError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model_id": assignment.ModelID, "start_layer": assignment.StartLayer,
		"end_layer": assignment.EndLayer, "loaded": true,
	})
}

func (a *Agent) handleStageUnload(w http.ResponseWriter, r *http.Request) {
	assignment, err := decodeStageAssignment(w, r)
	if err != nil {
		writeStageError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.stageComponent.unload(r.Context(), assignment); err != nil {
		status := http.StatusBadGateway
		switch {
		case errors.Is(err, errStageNotLoaded):
			status = http.StatusNotFound
		case errors.Is(err, errStageAssignmentConflict), errors.Is(err, errStageUnavailable):
			status = http.StatusConflict
		case r.Context().Err() != nil:
			status = http.StatusRequestTimeout
		}
		writeStageError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model_id": assignment.ModelID, "start_layer": assignment.StartLayer,
		"end_layer": assignment.EndLayer, "loaded": false,
	})
}

func (a *Agent) handleStageOpen(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSONStrict[engine.PipelineStageControlRequest](
		w, r, maxStageControlRequestBytes,
	)
	if err != nil {
		writeStageError(w, http.StatusBadRequest, err)
		return
	}
	if err := request.Validate(); err != nil {
		writeStageError(w, http.StatusBadRequest, err)
		return
	}
	releaseCapacity, err := a.executorComponent.beginExecution(r.Context())
	if err != nil {
		writeStageError(w, http.StatusRequestTimeout, err)
		return
	}
	if err := a.stageComponent.openPipelineRequest(
		r.Context(), request.Assignment, request.RequestID, releaseCapacity,
	); err != nil {
		releaseCapacity()
		status := http.StatusConflict
		if errors.Is(err, errStageNotLoaded) || errors.Is(err, errStageModelUnavailable) {
			status = http.StatusNotFound
		} else if errors.Is(err, errStageEngineUnsupported) {
			status = http.StatusNotImplemented
		} else if r.Context().Err() != nil {
			status = http.StatusRequestTimeout
		}
		writeStageError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"protocol_version": engine.PipelineStageProtocolVersion,
		"request_id":       request.RequestID,
		"opened":           true,
	})
}

func (a *Agent) handleStagePrefill(w http.ResponseWriter, r *http.Request) {
	a.handleStageStep(w, r, true)
}

func (a *Agent) handleStageDecode(w http.ResponseWriter, r *http.Request) {
	a.handleStageStep(w, r, false)
}

func (a *Agent) handleStageStep(w http.ResponseWriter, r *http.Request, prefill bool) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != engine.PipelineStageV2ContentType {
		writeStageError(w, http.StatusUnsupportedMediaType, errors.New("Content-Type must be "+engine.PipelineStageV2ContentType))
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxStageForwardRequestBytes+maxStageControlRequestBytes+4)
	message, err := engine.ReadStageMessage(body)
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeStageError(w, status, err)
		return
	}
	if err := message.Request.Validate(prefill); err != nil {
		writeStageError(w, http.StatusBadRequest, err)
		return
	}
	var output engine.PipelineStageOutput
	if prefill {
		output, err = a.stageComponent.prefillPipelineStage(r.Context(), message.Request)
	} else {
		output, err = a.stageComponent.decodePipelineStage(r.Context(), message.Request)
	}
	if err != nil {
		status := http.StatusBadGateway
		if strings.Contains(err.Error(), "not open") || errors.Is(err, errStageNotLoaded) {
			status = http.StatusNotFound
		} else if errors.Is(err, errStageAssignmentConflict) {
			status = http.StatusConflict
		} else if r.Context().Err() != nil {
			status = http.StatusRequestTimeout
		}
		writeStageError(w, status, err)
		return
	}
	if err := output.Validate(message.Request.Assignment.IsFinal); err != nil {
		writeStageError(w, http.StatusBadGateway, fmt.Errorf("stage engine returned invalid output: %w", err))
		return
	}
	if prefill && message.Request.Assignment.IsFinal && output.PromptTokens == 0 {
		writeStageError(w, http.StatusBadGateway, errors.New("final prefill result must include prompt_tokens"))
		return
	}
	if !prefill && output.PromptTokens != 0 {
		writeStageError(w, http.StatusBadGateway, errors.New("decode result must not include prompt_tokens"))
		return
	}
	w.Header().Set("Content-Type", engine.PipelineStageV2ContentType)
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	responseRequest := message.Request
	responseRequest.Activation = nil
	if err := engine.WriteStageMessage(w, engine.StageMessageHeader{
		Request: responseRequest, Output: &output,
	}, output.Activation); err != nil {
		logging.Errorf("nodeagent", "nodeagent: write pipeline stage v2 response failed: %v", err)
	}
}

func (a *Agent) handleStageClose(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSONStrict[engine.PipelineStageControlRequest](
		w, r, maxStageControlRequestBytes,
	)
	if err != nil {
		writeStageError(w, http.StatusBadRequest, err)
		return
	}
	if err := request.Validate(); err != nil {
		writeStageError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.stageComponent.closePipelineRequest(
		r.Context(), request.Assignment, request.RequestID,
	); err != nil {
		var status int
		switch {
		case errors.Is(err, errStageAssignmentConflict):
			status = http.StatusConflict
		case r.Context().Err() != nil:
			status = http.StatusRequestTimeout
		default:
			status = http.StatusBadGateway
		}
		writeStageError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"protocol_version": engine.PipelineStageProtocolVersion,
		"request_id":       request.RequestID,
		"closed":           true,
	})
}

func (a *Agent) handleStageForward(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/x-ndjson" {
		writeStageError(w, http.StatusUnsupportedMediaType, errors.New("Content-Type must be application/x-ndjson"))
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxStageForwardRequestBytes)
	decoder := json.NewDecoder(body)
	start, err := engine.DecodeStageForwardStart(decoder)
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeStageError(w, status, err)
		return
	}

	releaseCapacity, err := a.executorComponent.beginExecution(r.Context())
	if err != nil {
		writeStageError(w, http.StatusRequestTimeout, err)
		return
	}
	defer releaseCapacity()
	lease, err := a.stageComponent.acquire(r.Context(), *start.Assignment)
	if err != nil {
		status := http.StatusNotFound
		switch {
		case errors.Is(err, errStageAssignmentConflict), errors.Is(err, errStageUnavailable):
			status = http.StatusConflict
		case r.Context().Err() != nil:
			status = http.StatusRequestTimeout
		}
		writeStageError(w, status, err)
		return
	}
	defer lease.release()
	requestEngine, supportsRequests := lease.entry.runtime.(engine.PipelineStageRequestEngine)
	if !supportsRequests && (start.Prompt != "" || start.Assignment.NextStageAddr == "") {
		writeStageError(w, http.StatusNotImplemented, errStageEngineUnsupported)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	activations := engine.NewStageActivationStream(decoder)
	var updatedCache []byte
	if supportsRequests {
		updatedCache, err = requestEngine.ForwardStageRequest(
			r.Context(), lease.entry.assignment,
			engine.StageForwardRequest{
				Prompt: start.Prompt, Activations: activations, KVCache: start.KVCache,
				Options: engine.GenerationOptions{Temperature: start.Temperature, MaxTokens: start.MaxTokens},
			},
			func(frame engine.StageForwardFrame) error {
				expectedType := engine.StageFrameActivation
				if lease.entry.assignment.IsFinal {
					expectedType = engine.StageFrameToken
				}
				if frame.Type != expectedType {
					return fmt.Errorf("stage engine returned %q; want %q", frame.Type, expectedType)
				}
				return writeStageFrame(w, r, engine.StageForwardFrame{Type: frame.Type, Data: frame.Data})
			},
		)
	} else {
		updatedCache, err = lease.entry.runtime.ForwardStage(
			r.Context(), lease.entry.assignment, activations, start.KVCache,
			func(data []byte) error {
				return writeStageFrame(w, r, engine.StageForwardFrame{
					Type: engine.StageFrameActivation, Data: data,
				})
			},
		)
	}
	if err != nil {
		if writeErr := writeStageFrame(w, r, engine.StageForwardFrame{
			Type: engine.StageFrameError, Error: err.Error(),
		}); writeErr != nil {
			logging.Errorf("nodeagent", "nodeagent: writing stage forward error frame failed: %v", writeErr)
		}
		return
	}
	if err := writeStageFrame(w, r, engine.StageForwardFrame{
		Type: engine.StageFrameComplete, KVCache: updatedCache,
	}); err != nil {
		logging.Errorf("nodeagent", "nodeagent: writing stage completion frame failed: %v", err)
	}
}

func decodeStageAssignment(w http.ResponseWriter, r *http.Request) (engine.StageAssignment, error) {
	assignment, err := httpx.DecodeJSONStrict[engine.StageAssignment](
		w, r, maxStageControlRequestBytes,
	)
	if err != nil {
		return engine.StageAssignment{}, err
	}
	assignment.Normalize()
	if err := assignment.Validate(); err != nil {
		return engine.StageAssignment{}, err
	}
	return assignment, nil
}

func writeStageFrame(w http.ResponseWriter, r *http.Request, frame engine.StageForwardFrame) error {
	if err := httpx.NewStreamWriter(w).WriteJSONLine(frame); err != nil {
		return fmt.Errorf("write stage frame: %w", err)
	}
	if err := r.Context().Err(); err != nil {
		return err
	}
	return nil
}

func writeStageError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": strings.TrimSpace(err.Error())})
}
