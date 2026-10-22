package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/memory"
)

func (a *Agent) handleExecute(w http.ResponseWriter, r *http.Request) {
	req, err := httpx.DecodeJSON[executeRequest](
		w, r, httpx.InferenceJSONBodyBytes,
	)
	if err != nil {
		writeJSONError(w, apperr.Invalid(err), executeResponse{Error: err.Error()})
		return
	}

	eng := a.engineForModel(req.Model, req.Engine)
	if eng == nil {
		err := apperr.NotFoundError(fmt.Errorf("model not found on this node: %s", req.Model))
		writeJSONError(w, err, executeResponse{Error: err.Error()})
		return
	}

	release, err := a.executorComponent.beginExecution(r.Context())
	if err != nil {
		writeJSON(w, http.StatusRequestTimeout, executeResponse{Error: "execution interrupted while waiting for capacity"})
		return
	}
	defer release()

	start := time.Now()
	output, err := generateWithOptions(r.Context(), eng, req.Model, req.Prompt, engine.GenerationOptions{})
	duration := time.Since(start).Milliseconds()

	a.remember(memory.Experience{
		Kind:        "subtask",
		TaskType:    req.TaskType,
		Description: truncateText(req.Prompt, 400),
		Prompt:      req.Prompt,
		Output:      output,
		Model:       req.Model,
		Success:     err == nil,
		Error:       errText(err),
		DurationMs:  duration,
		Tags:        []string{"execute", req.SubtaskID},
	})

	if err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			writeJSON(w, http.StatusRequestTimeout, executeResponse{Error: "execution interrupted"})
			return
		}
		writeJSONError(w, apperr.InternalError(err), executeResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, executeResponse{Output: output})
}

func (a *Agent) handleGenerate(w http.ResponseWriter, r *http.Request) {
	req, err := httpx.DecodeJSON[generateRequest](
		w, r, httpx.InferenceJSONBodyBytes,
	)
	if err != nil {
		writeJSONError(w, apperr.Invalid(err), generateResponse{Error: err.Error()})
		return
	}
	idempotencyKey := r.Header.Get(inferroute.HeaderIdempotencyKey)
	if err := inferroute.ValidateIdempotencyKey(idempotencyKey); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateResponse{Error: err.Error()})
		return
	}
	streaming := strings.EqualFold(r.URL.Query().Get("stream"), "true")
	if idempotencyKey != "" && streaming {
		err := apperr.Invalid(errors.New("Idempotency-Key is supported only for non-streaming generate requests"))
		writeJSONError(w, err, generateResponse{Error: err.Error()})
		return
	}

	modelName := req.Model
	if modelName == "" {
		modelName = a.defaultModelName()
	}
	if modelName == "" {
		enabled := a.enabledModels()
		if len(enabled) > 0 {
			modelName = enabled[0].Name
		}
	}

	eng := a.engineFor(modelName)
	if eng == nil {
		err := apperr.NotFoundError(fmt.Errorf("model not found on this node: %s", modelName))
		writeJSONError(w, err, generateResponse{Error: err.Error()})
		return
	}

	prompt := req.Prompt
	if req.System != "" {
		prompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", req.System, req.Prompt)
	}

	options := engine.GenerationOptions{Temperature: req.Temperature, MaxTokens: req.MaxTokens}
	if streaming {
		release := a.executorComponent.beginActiveTask()
		defer release()
		a.streamGenerate(w, r, eng, modelName, prompt, options)
		return
	}

	generate := func() (generateIdempotencyResult, error) {
		release := a.executorComponent.beginActiveTask()
		defer release()
		start := time.Now()
		output, err := generateWithOptions(r.Context(), eng, modelName, prompt, options)
		duration := time.Since(start).Milliseconds()
		a.remember(memory.Experience{
			Kind:        "generate",
			Description: truncateText(req.Prompt, 400),
			Prompt:      req.Prompt,
			Output:      output,
			Model:       modelName,
			Success:     err == nil,
			Error:       errText(err),
			DurationMs:  duration,
		})
		return generateIdempotencyResult{output: output, durationMs: duration}, err
	}

	var result generateIdempotencyResult
	if idempotencyKey == "" {
		result, err = generate()
	} else {
		fingerprint, fingerprintErr := generateRequestFingerprint(modelName, prompt, options)
		if fingerprintErr != nil {
			writeJSONError(w, apperr.InternalError(fingerprintErr), generateResponse{Model: modelName, Error: fingerprintErr.Error()})
			return
		}
		result, err = a.generateIdempotency.do(r.Context(), idempotencyKey, fingerprint, generate)
	}
	if err != nil {
		switch {
		case errors.Is(err, errIdempotencyKeyConflict):
			writeJSONError(w, apperr.ConflictError(err), generateResponse{Model: modelName, Error: err.Error()})
		case errors.Is(err, errIdempotencyCacheBusy):
			writeJSONError(w, apperr.UnavailableError(err), generateResponse{Model: modelName, Error: err.Error()})
		default:
			writeJSONError(w, apperr.InternalError(err), generateResponse{Model: modelName, DurationMs: result.durationMs, Error: err.Error()})
		}
		return
	}
	writeJSON(w, http.StatusOK, generateResponse{Model: modelName, Output: result.output, DurationMs: result.durationMs})
}

func (a *Agent) handleChat(w http.ResponseWriter, r *http.Request) {
	req, err := httpx.DecodeJSON[chatRequest](
		w, r, httpx.InferenceJSONBodyBytes,
	)
	if err != nil {
		writeJSONError(w, apperr.Invalid(err), chatResponse{Error: err.Error()})
		return
	}

	modelName := req.Model
	if modelName == "" {
		modelName = a.defaultModelName()
	}
	if modelName == "" {
		enabled := a.enabledModels()
		if len(enabled) > 0 {
			modelName = enabled[0].Name
		}
	}

	eng := a.engineFor(modelName)
	if eng == nil {
		err := apperr.NotFoundError(fmt.Errorf("model not found on this node: %s", modelName))
		writeJSONError(w, err, chatResponse{Error: err.Error()})
		return
	}

	release := a.executorComponent.beginActiveTask()
	defer release()

	start := time.Now()
	reply, err := chatWithContext(r.Context(), eng, modelName, req.Messages)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			writeJSON(w, http.StatusRequestTimeout, chatResponse{Model: modelName, DurationMs: duration, Error: "chat interrupted"})
			return
		}
		// Fallback to Generate if Chat returned an error
		var sb strings.Builder
		for _, m := range req.Messages {
			sb.WriteString(fmt.Sprintf("<|im_start|>%s\n%s<|im_end|>\n", m.Role, m.Content))
		}
		sb.WriteString("<|im_start|>assistant\n")
		var genErr error
		reply, genErr = generateWithOptions(r.Context(), eng, modelName, sb.String(), engine.GenerationOptions{})
		if genErr != nil {
			writeJSONError(w, apperr.InternalError(fmt.Errorf("%s; fallback error: %w", err, genErr)),
				chatResponse{Model: modelName, DurationMs: duration, Error: err.Error() + "; fallback error: " + genErr.Error()})
			return
		}
	}

	writeJSON(w, http.StatusOK, chatResponse{
		Model: modelName,
		Message: engine.ChatMessage{
			Role:    "assistant",
			Content: reply,
		},
		DurationMs: duration,
	})
}
