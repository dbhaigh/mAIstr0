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
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
)

type ollamaOptions struct {
	Temperature *float64 `json:"temperature"`
	NumPredict  *int     `json:"num_predict"`
}

type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Stream   *bool           `json:"stream"`
	Options  ollamaOptions   `json:"options"`
}

type ollamaGenerateRequest struct {
	Model     string          `json:"model"`
	Prompt    string          `json:"prompt"`
	System    string          `json:"system"`
	Stream    *bool           `json:"stream"`
	Options   ollamaOptions   `json:"options"`
	Format    json.RawMessage `json:"format"`
	KeepAlive json.RawMessage `json:"keep_alive"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type anthropicRequest struct {
	Model       string             `json:"model"`
	System      json.RawMessage    `json:"system"`
	Messages    []anthropicMessage `json:"messages"`
	MaxTokens   int                `json:"max_tokens"`
	Temperature float64            `json:"temperature"`
	Stream      bool               `json:"stream"`
}

func (s *Server) handleOllamaTags(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	models := backend.ListClusterModels()
	type modelInfo struct {
		Name       string         `json:"name"`
		Model      string         `json:"model"`
		ModifiedAt time.Time      `json:"modified_at"`
		Size       int64          `json:"size"`
		Digest     string         `json:"digest"`
		Details    map[string]any `json:"details"`
	}
	list := make([]modelInfo, 0, len(models))
	seen := make(map[string]bool)
	for _, model := range models {
		if seen[model.Name] {
			continue
		}
		seen[model.Name] = true
		list = append(list, modelInfo{
			Name: model.Name, Model: model.Name, ModifiedAt: time.Now().UTC(),
			Digest: model.Name, Details: map[string]any{"family": model.Engine},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"models": list})
}

func (s *Server) handleOllamaChat(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSON[ollamaChatRequest](w, r, 4<<20)
	if err != nil {
		writeCompatError(w, apperr.Invalid(err), compatOllamaChat)
		return
	}
	if len(request.Messages) == 0 {
		writeCompatError(w, apperr.Invalid(errors.New("messages is required")), compatOllamaChat)
		return
	}
	var prompt strings.Builder
	for _, message := range request.Messages {
		switch message.Role {
		case "system", "user", "assistant", "tool":
			fmt.Fprintf(&prompt, "<|im_start|>%s\n%s<|im_end|>\n", message.Role, message.Content)
		default:
			writeCompatError(w, apperr.Invalid(fmt.Errorf("unsupported message role: %s", message.Role)), compatOllamaChat)
			return
		}
	}
	prompt.WriteString("<|im_start|>assistant\n")
	temperature, maxTokens := ollamaGenerationOptions(request.Options)
	stream := request.Stream == nil || *request.Stream
	s.generateCompat(w, r, generateAPIRequest{
		Model: request.Model, Prompt: prompt.String(), Temperature: temperature, MaxTokens: maxTokens,
	}, stream, compatOllamaChat)
}

func (s *Server) handleOllamaGenerate(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
	if err != nil {
		writeCompatError(w, apperr.Invalid(err), compatOllamaGenerate)
		return
	}
	var shape map[string]json.RawMessage
	r.Body = io.NopCloser(bytes.NewReader(raw))
	if err := json.Unmarshal(raw, &shape); err != nil {
		s.handleGenerate(w, r)
		return
	}
	_, hasStream := shape["stream"]
	_, hasOptions := shape["options"]
	_, hasFormat := shape["format"]
	_, hasKeepAlive := shape["keep_alive"]
	_, hasSystem := shape["system"]
	ollamaClient := strings.Contains(strings.ToLower(r.UserAgent()), "ollama")
	if !hasStream && !hasOptions && !hasFormat && !hasKeepAlive && !hasSystem && !ollamaClient {
		s.handleGenerate(w, r)
		return
	}
	var request ollamaGenerateRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		writeCompatError(w, apperr.Invalid(err), compatOllamaGenerate)
		return
	}
	if strings.TrimSpace(request.Prompt) == "" {
		writeCompatError(w, apperr.Invalid(errors.New("prompt is required")), compatOllamaGenerate)
		return
	}
	temperature, maxTokens := ollamaGenerationOptions(request.Options)
	stream := request.Stream == nil || *request.Stream
	s.generateCompat(w, r, generateAPIRequest{
		Model: request.Model, Prompt: request.Prompt, System: request.System,
		Temperature: temperature, MaxTokens: maxTokens,
	}, stream, compatOllamaGenerate)
}

func ollamaGenerationOptions(options ollamaOptions) (float64, int) {
	temperature := 0.0
	if options.Temperature != nil {
		temperature = *options.Temperature
	}
	maxTokens := 0
	if options.NumPredict != nil {
		maxTokens = *options.NumPredict
	}
	return temperature, maxTokens
}

func (s *Server) handleAnthropicMessages(w http.ResponseWriter, r *http.Request) {
	request, err := httpx.DecodeJSON[anthropicRequest](w, r, 4<<20)
	if err != nil {
		writeCompatError(w, apperr.Invalid(err), compatAnthropic)
		return
	}
	if request.MaxTokens <= 0 {
		writeCompatError(w, apperr.Invalid(errors.New("max_tokens must be greater than zero")), compatAnthropic)
		return
	}
	system, err := anthropicText(request.System)
	if err != nil {
		writeCompatError(w, apperr.Invalid(fmt.Errorf("system: %w", err)), compatAnthropic)
		return
	}
	if len(request.Messages) == 0 {
		writeCompatError(w, apperr.Invalid(errors.New("messages is required")), compatAnthropic)
		return
	}
	var prompt strings.Builder
	if system != "" {
		fmt.Fprintf(&prompt, "<|im_start|>system\n%s<|im_end|>\n", system)
	}
	for _, message := range request.Messages {
		if message.Role != "user" && message.Role != "assistant" {
			writeCompatError(w, apperr.Invalid(errors.New("message roles must be user or assistant")), compatAnthropic)
			return
		}
		content, err := anthropicText(message.Content)
		if err != nil {
			writeCompatError(w, apperr.Invalid(err), compatAnthropic)
			return
		}
		fmt.Fprintf(&prompt, "<|im_start|>%s\n%s<|im_end|>\n", message.Role, content)
	}
	prompt.WriteString("<|im_start|>assistant\n")
	s.generateCompat(w, r, generateAPIRequest{
		Model: request.Model, Prompt: prompt.String(), MaxTokens: request.MaxTokens, Temperature: request.Temperature,
	}, request.Stream, compatAnthropic)
}

func anthropicText(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", fmt.Errorf("content must be text or an array of text blocks")
	}
	var out strings.Builder
	for _, block := range blocks {
		if block.Type != "text" {
			return "", fmt.Errorf("unsupported content block type %q; only text is supported", block.Type)
		}
		out.WriteString(block.Text)
	}
	return out.String(), nil
}

type compatMode int

const (
	compatOllamaChat compatMode = iota
	compatOllamaGenerate
	compatAnthropic
)

func (s *Server) generateCompat(w http.ResponseWriter, r *http.Request, request generateAPIRequest, stream bool, mode compatMode) {
	idempotencyKey := r.Header.Get(inferroute.HeaderIdempotencyKey)
	if err := inferroute.ValidateIdempotencyKey(idempotencyKey); err != nil {
		writeCompatError(w, apperr.Invalid(err), mode)
		return
	}
	if idempotencyKey != "" && stream {
		writeCompatError(w, apperr.Invalid(errors.New("Idempotency-Key is supported only for non-streaming generate requests")), mode)
		return
	}
	if stream {
		var target http.ResponseWriter
		switch mode {
		case compatAnthropic:
			target = newAnthropicStreamWriter(w, request.Model)
		default:
			target = newOllamaStreamWriter(w, request.Model, mode == compatOllamaChat)
		}
		if mode == compatAnthropic {
			adapter := target.(*anthropicStreamWriter)
			s.handleGenerateRequest(adapter, r, request, true, idempotencyKey)
			adapter.finish()
			return
		}
		adapter := target.(*ollamaStreamWriter)
		s.handleGenerateRequest(adapter, r, request, true, idempotencyKey)
		adapter.finish()
		return
	}

	recorder := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	s.handleGenerateRequest(recorder, r, request, false, idempotencyKey)
	var result generateAPIResponse
	if err := json.Unmarshal(recorder.body.Bytes(), &result); err != nil {
		writeCompatError(w, apperr.UpstreamError(errors.New("invalid response from generation backend")), mode)
		return
	}
	if recorder.status >= 400 || result.Error != "" {
		status := recorder.status
		if status == 0 {
			status = http.StatusInternalServerError
		}
		writeCompatError(w, apperr.FromStatusCode(status, errors.New(result.Error)), mode)
		return
	}
	if mode == compatAnthropic {
		writeJSON(w, http.StatusOK, map[string]any{
			"id": "msg-" + result.NodeID, "type": "message", "role": "assistant", "model": result.Model,
			"content":     []map[string]string{{"type": "text", "text": result.Output}},
			"stop_reason": "end_turn", "stop_sequence": nil,
			"usage": map[string]int{"input_tokens": len(request.Prompt) / 4, "output_tokens": len(result.Output) / 4},
		})
		return
	}
	if mode == compatOllamaChat {
		writeJSON(w, http.StatusOK, map[string]any{
			"model": result.Model, "created_at": time.Now().UTC(), "message": ollamaMessage{Role: "assistant", Content: result.Output},
			"done": true, "done_reason": "stop", "total_duration": result.DurationMs * int64(time.Millisecond),
			"eval_count": len(result.Output) / 4,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model": result.Model, "created_at": time.Now().UTC(), "response": result.Output, "done": true, "done_reason": "stop",
		"total_duration": result.DurationMs * int64(time.Millisecond), "eval_count": len(result.Output) / 4,
	})
}

func writeOllamaError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeAnthropicError(w http.ResponseWriter, status int, kind, message string) {
	writeJSON(w, status, map[string]any{"type": "error", "error": map[string]string{"type": kind, "message": message}})
}

func writeCompatError(w http.ResponseWriter, err error, mode compatMode) {
	status := apperr.StatusCode(err)
	message := err.Error()
	if message == "" {
		message = http.StatusText(status)
	}
	if mode == compatAnthropic {
		kind := "api_error"
		if errors.Is(err, apperr.InvalidInput) {
			kind = "invalid_request_error"
		}
		writeAnthropicError(w, status, kind, message)
		return
	}
	writeOllamaError(w, status, message)
}
