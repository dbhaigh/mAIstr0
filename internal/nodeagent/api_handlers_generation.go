package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
)

func (a *Agent) handleExecute(w http.ResponseWriter, r *http.Request) {
	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
	var req generateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateResponse{Error: err.Error()})
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

	release := a.executorComponent.beginActiveTask()
	defer release()

	start := time.Now()
	options := engine.GenerationOptions{Temperature: req.Temperature, MaxTokens: req.MaxTokens}
	if strings.EqualFold(r.URL.Query().Get("stream"), "true") {
		a.streamGenerate(w, r, eng, modelName, prompt, options)
		return
	}
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

	if err != nil {
		writeJSONError(w, apperr.InternalError(err), generateResponse{Model: modelName, DurationMs: duration, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, generateResponse{Model: modelName, Output: output, DurationMs: duration})
}

func (a *Agent) handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
