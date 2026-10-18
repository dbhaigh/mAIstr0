package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func (s *Server) handleListAllModels(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	writeJSON(w, http.StatusOK, backend.ListClusterModels())
}

func (s *Server) handleGenerate(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get(inferroute.HeaderIdempotencyKey)
	if err := inferroute.ValidateIdempotencyKey(idempotencyKey); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateAPIResponse{Error: err.Error()})
		return
	}
	streaming := strings.EqualFold(r.URL.Query().Get("stream"), "true")
	if idempotencyKey != "" && streaming {
		err := apperr.Invalid(errors.New("Idempotency-Key is supported only for non-streaming generate requests"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	var req generateAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateAPIResponse{Error: err.Error()})
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		err := apperr.Invalid(errors.New("prompt is required"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}

	nodes := s.schedulerNodes()
	if len(nodes) == 0 {
		err := apperr.UnavailableError(errors.New("no healthy nodes in cluster"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}

	candidates := nodes
	if req.NodeID != "" {
		n, ok := s.registry.Get(req.NodeID)
		if !ok || !n.Healthy {
			err := apperr.NotFoundError(fmt.Errorf("node not found or offline: %s", req.NodeID))
			writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
			return
		}
		for _, candidate := range nodes {
			if candidate.ID == req.NodeID {
				candidates = []cluster.NodeStatus{candidate}
				break
			}
		}
	}
	if req.Model != "" {
		hasModel := false
		for i := range candidates {
			var compatible []engine.Model
			for _, model := range candidates[i].Models {
				if strings.EqualFold(model.Name, req.Model) {
					compatible = append(compatible, model)
					hasModel = true
				}
			}
			candidates[i].Models = compatible
		}
		if !hasModel {
			err := apperr.NotFoundError(fmt.Errorf("model not found on any cluster node: %s", req.Model))
			writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
			return
		}
	}

	taskType := strings.TrimSpace(req.TaskType)
	if taskType == "" {
		taskType = "general"
	}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{{
		ID: "generate", Description: req.Prompt, TaskType: taskType,
	}}, candidates, s.experience())
	if err != nil {
		err := apperr.UnavailableError(errors.New("could not assign request to a healthy node"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	if len(assignments) != 1 {
		err := apperr.UnavailableError(errors.New("scheduler returned no assignment"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	assignment := assignments[0]
	targetNode, ok := s.registry.Get(assignment.NodeID)
	if !ok || !targetNode.Healthy || targetNode.Address != assignment.Address {
		err := apperr.UnavailableError(errors.New("selected node is no longer healthy"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	targetModel := assignment.Model

	fullPrompt := req.Prompt
	if req.System != "" {
		fullPrompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", req.System, req.Prompt)
	}

	s.registry.IncrementLoad(targetNode.ID, 1)
	defer s.registry.IncrementLoad(targetNode.ID, -1)
	if streaming {
		s.streamNodeGenerate(w, r, targetNode.Address, targetModel, req, fullPrompt)
		return
	}

	start := time.Now()
	var output string
	if idempotencyKey != "" {
		output, err = s.callNodeGenerateWithOptionsContextAndIdempotency(
			r.Context(), targetNode.Address, targetModel, fullPrompt,
			req.Temperature, req.MaxTokens, idempotencyKey,
		)
	} else {
		output, err = s.callNodeGenerateWithOptions(targetNode.Address, targetModel, fullPrompt, req.Temperature, req.MaxTokens)
	}
	duration := time.Since(start).Milliseconds()

	if err != nil {
		var nodeErr *nodeResponseError
		if idempotencyKey != "" && errors.As(err, &nodeErr) {
			message := nodeErr.body
			var backend generateAPIResponse
			if json.Unmarshal([]byte(nodeErr.body), &backend) == nil && backend.Error != "" {
				message = backend.Error
			}
			writeJSON(w, nodeErr.statusCode, generateAPIResponse{
				NodeID: targetNode.ID, NodeAddress: targetNode.Address, Model: targetModel,
				DurationMs: duration, Error: message,
			})
			return
		}
		appErr := apperr.InternalError(err)
		writeJSONError(w, appErr, generateAPIResponse{
			NodeID:      targetNode.ID,
			NodeAddress: targetNode.Address,
			Model:       targetModel,
			DurationMs:  duration,
			Error:       appErr.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, generateAPIResponse{
		NodeID:      targetNode.ID,
		NodeAddress: targetNode.Address,
		Model:       targetModel,
		Output:      output,
		DurationMs:  duration,
	})
}

func (s *Server) handleNodeGenerate(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	var req generateAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateAPIResponse{Error: err.Error()})
		return
	}
	req.NodeID = nodeID
	body, _ := json.Marshal(req)
	r.Body = io.NopCloser(bytes.NewReader(body))
	s.handleGenerate(w, r)
}

func (s *Server) handleNodeModelGenerate(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	model := r.PathValue("model")
	var req generateAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateAPIResponse{Error: err.Error()})
		return
	}
	req.NodeID = nodeID
	req.Model = model
	body, _ := json.Marshal(req)
	r.Body = io.NopCloser(bytes.NewReader(body))
	s.handleGenerate(w, r)
}

func (s *Server) handleNodeModelTest(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	model := r.PathValue("model")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		writeJSONError(w, apperr.NotFoundError(errors.New("node offline")), map[string]any{"healthy": false, "error": "node offline"})
		return
	}

	testPrompt := "Respond with OK in 3 words."
	start := time.Now()
	s.registry.IncrementLoad(n.ID, 1)
	out, err := s.callNodeGenerate(n.Address, model, testPrompt)
	s.registry.IncrementLoad(n.ID, -1)
	elapsed := time.Since(start).Milliseconds()

	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"node_id":     n.ID,
			"model":       model,
			"healthy":     false,
			"error":       err.Error(),
			"duration_ms": elapsed,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":     n.ID,
		"model":       model,
		"healthy":     true,
		"output":      out,
		"duration_ms": elapsed,
	})
}

func (s *Server) handleOpenAIModels(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	models := backend.ListClusterModels()
	list := make([]openAIModelObj, 0, len(models))
	seen := make(map[string]bool)
	now := time.Now().Unix()

	for _, m := range models {
		if !seen[m.Name] {
			seen[m.Name] = true
			list = append(list, openAIModelObj{
				ID:      m.Name,
				Object:  "model",
				Created: now,
				OwnedBy: m.NodeID,
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   list,
	})
}

func (s *Server) handleOpenAIChatCompletions(w http.ResponseWriter, r *http.Request) {
	var req openAIChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}

	if len(req.Messages) == 0 {
		writeError(w, apperr.Invalid(errors.New("messages array is required")))
		return
	}

	// Preserve every message role supported by the engine prompt format.
	var sb strings.Builder
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "user", "assistant", "tool":
			sb.WriteString("<|im_start|>" + m.Role + "\n" + m.Content + "<|im_end|>\n")
		}
	}
	sb.WriteString("<|im_start|>assistant\n")

	genReq := generateAPIRequest{
		Model:       req.Model,
		Prompt:      sb.String(),
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}

	genBody, _ := json.Marshal(genReq)
	generateURL := "/api/generate"
	if req.Stream {
		generateURL += "?stream=true"
	}
	fakeReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, generateURL, bytes.NewReader(genBody))
	if req.Stream {
		streamWriter := newOpenAIStreamWriter(w, req.Model)
		s.handleGenerate(streamWriter, fakeReq)
		streamWriter.finish()
		return
	}
	rec := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	s.handleGenerate(rec, fakeReq)

	if rec.status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
		return
	}

	var genResp generateAPIResponse
	_ = json.Unmarshal(rec.body.Bytes(), &genResp)

	writeJSON(w, http.StatusOK, map[string]any{
		"id":                 "chatcmpl-" + genResp.NodeID,
		"object":             "chat.completion",
		"created":            time.Now().Unix(),
		"model":              genResp.Model,
		"system_fingerprint": "maistr0-" + genResp.NodeID,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]string{
					"role":    "assistant",
					"content": genResp.Output,
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]int{
			"prompt_tokens":     len(genReq.Prompt) / 4,
			"completion_tokens": len(genResp.Output) / 4,
			"total_tokens":      (len(genReq.Prompt) + len(genResp.Output)) / 4,
		},
	})
}

func (s *Server) handleOpenAICompletions(w http.ResponseWriter, r *http.Request) {
	var req openAICompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}

	genReq := generateAPIRequest{
		Model:       req.Model,
		Prompt:      req.Prompt,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}

	genBody, _ := json.Marshal(genReq)
	fakeReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "/api/generate", bytes.NewReader(genBody))
	rec := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	s.handleGenerate(rec, fakeReq)

	if rec.status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
		return
	}

	var genResp generateAPIResponse
	_ = json.Unmarshal(rec.body.Bytes(), &genResp)

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "cmpl-" + genResp.NodeID,
		"object":  "text_completion",
		"created": time.Now().Unix(),
		"model":   genResp.Model,
		"choices": []map[string]any{
			{
				"text":          genResp.Output,
				"index":         0,
				"finish_reason": "stop",
			},
		},
	})
}
