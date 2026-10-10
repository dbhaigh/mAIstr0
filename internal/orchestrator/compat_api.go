package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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
	var request ollamaChatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&request); err != nil {
		writeOllamaError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(request.Messages) == 0 {
		writeOllamaError(w, http.StatusBadRequest, "messages is required")
		return
	}
	var prompt strings.Builder
	for _, message := range request.Messages {
		switch message.Role {
		case "system", "user", "assistant", "tool":
			fmt.Fprintf(&prompt, "<|im_start|>%s\n%s<|im_end|>\n", message.Role, message.Content)
		default:
			writeOllamaError(w, http.StatusBadRequest, "unsupported message role: "+message.Role)
			return
		}
	}
	prompt.WriteString("<|im_start|>assistant\n")
	temperature, maxTokens := ollamaGenerationOptions(request.Options)
	stream := request.Stream == nil || *request.Stream
	generateCompat(w, r, generateAPIRequest{
		Model: request.Model, Prompt: prompt.String(), Temperature: temperature, MaxTokens: maxTokens,
	}, stream, compatOllamaChat, s.handleGenerate)
}

func (s *Server) handleOllamaGenerate(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
	if err != nil {
		writeOllamaError(w, http.StatusBadRequest, err.Error())
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
		writeOllamaError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(request.Prompt) == "" {
		writeOllamaError(w, http.StatusBadRequest, "prompt is required")
		return
	}
	temperature, maxTokens := ollamaGenerationOptions(request.Options)
	stream := request.Stream == nil || *request.Stream
	generateCompat(w, r, generateAPIRequest{
		Model: request.Model, Prompt: request.Prompt, System: request.System,
		Temperature: temperature, MaxTokens: maxTokens,
	}, stream, compatOllamaGenerate, s.handleGenerate)
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
	var request anthropicRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&request); err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if request.MaxTokens <= 0 {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "max_tokens must be greater than zero")
		return
	}
	system, err := anthropicText(request.System)
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "system: "+err.Error())
		return
	}
	if len(request.Messages) == 0 {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "messages is required")
		return
	}
	var prompt strings.Builder
	if system != "" {
		fmt.Fprintf(&prompt, "<|im_start|>system\n%s<|im_end|>\n", system)
	}
	for _, message := range request.Messages {
		if message.Role != "user" && message.Role != "assistant" {
			writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "message roles must be user or assistant")
			return
		}
		content, err := anthropicText(message.Content)
		if err != nil {
			writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
			return
		}
		fmt.Fprintf(&prompt, "<|im_start|>%s\n%s<|im_end|>\n", message.Role, content)
	}
	prompt.WriteString("<|im_start|>assistant\n")
	generateCompat(w, r, generateAPIRequest{
		Model: request.Model, Prompt: prompt.String(), MaxTokens: request.MaxTokens, Temperature: request.Temperature,
	}, request.Stream, compatAnthropic, s.handleGenerate)
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

func generateCompat(w http.ResponseWriter, r *http.Request, request generateAPIRequest, stream bool, mode compatMode, handler http.HandlerFunc) {
	body, err := json.Marshal(request)
	if err != nil {
		writeCompatError(w, http.StatusInternalServerError, mode, err.Error())
		return
	}
	path := "/api/generate"
	if stream {
		path += "?stream=true"
	}
	fakeRequest, err := http.NewRequestWithContext(r.Context(), http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		writeCompatError(w, http.StatusInternalServerError, mode, err.Error())
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
			handler(adapter, fakeRequest)
			adapter.finish()
			return
		}
		adapter := target.(*ollamaStreamWriter)
		handler(adapter, fakeRequest)
		adapter.finish()
		return
	}

	recorder := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	handler(recorder, fakeRequest)
	var result generateAPIResponse
	if err := json.Unmarshal(recorder.body.Bytes(), &result); err != nil {
		writeCompatError(w, http.StatusBadGateway, mode, "invalid response from generation backend")
		return
	}
	if recorder.status >= 400 || result.Error != "" {
		status := recorder.status
		if status == 0 {
			status = http.StatusInternalServerError
		}
		writeCompatError(w, status, mode, result.Error)
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

func writeCompatError(w http.ResponseWriter, status int, mode compatMode, message string) {
	if message == "" {
		message = http.StatusText(status)
	}
	if mode == compatAnthropic {
		writeAnthropicError(w, status, "api_error", message)
		return
	}
	writeOllamaError(w, status, message)
}

type ollamaStreamWriter struct {
	target  http.ResponseWriter
	model   string
	chat    bool
	status  int
	buffer  strings.Builder
	flusher http.Flusher
}

func newOllamaStreamWriter(target http.ResponseWriter, model string, chat bool) *ollamaStreamWriter {
	flusher, _ := target.(http.Flusher)
	return &ollamaStreamWriter{target: target, model: model, chat: chat, flusher: flusher}
}

func (w *ollamaStreamWriter) Header() http.Header { return w.target.Header() }

func (w *ollamaStreamWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	if status < 400 {
		w.target.Header().Set("Content-Type", "application/x-ndjson")
		w.target.Header().Set("Cache-Control", "no-cache")
	}
	w.target.WriteHeader(status)
}

func (w *ollamaStreamWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= 400 {
		return w.target.Write(data)
	}
	w.buffer.Write(data)
	for {
		chunk := w.buffer.String()
		newline := strings.IndexByte(chunk, '\n')
		if newline < 0 {
			break
		}
		line := strings.TrimSpace(chunk[:newline])
		w.buffer.Reset()
		w.buffer.WriteString(chunk[newline+1:])
		if strings.HasPrefix(line, "data:") {
			if err := w.consume(strings.TrimSpace(strings.TrimPrefix(line, "data:"))); err != nil {
				return len(data), err
			}
		}
	}
	return len(data), nil
}

func (w *ollamaStreamWriter) consume(payload string) error {
	var event struct {
		Output string `json:"output"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return err
	}
	if event.Error != "" {
		return w.write(map[string]any{"error": event.Error, "done": true})
	}
	if event.Output == "" {
		return nil
	}
	response := map[string]any{
		"model": w.model, "created_at": time.Now().UTC(), "done": false,
	}
	if w.chat {
		response["message"] = ollamaMessage{Role: "assistant", Content: event.Output}
	} else {
		response["response"] = event.Output
	}
	return w.write(response)
}

func (w *ollamaStreamWriter) write(value any) error {
	if err := json.NewEncoder(w.target).Encode(value); err != nil {
		return err
	}
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return nil
}

func (w *ollamaStreamWriter) finish() {
	if w.status >= 400 {
		return
	}
	if rest := strings.TrimSpace(w.buffer.String()); strings.HasPrefix(rest, "data:") {
		_ = w.consume(strings.TrimSpace(strings.TrimPrefix(rest, "data:")))
	}
	value := map[string]any{"model": w.model, "created_at": time.Now().UTC(), "done": true, "done_reason": "stop"}
	if w.chat {
		value["message"] = ollamaMessage{Role: "assistant", Content: ""}
	} else {
		value["response"] = ""
	}
	_ = w.write(value)
}

type anthropicStreamWriter struct {
	target  http.ResponseWriter
	model   string
	status  int
	buffer  strings.Builder
	flusher http.Flusher
	started bool
}

func newAnthropicStreamWriter(target http.ResponseWriter, model string) *anthropicStreamWriter {
	flusher, _ := target.(http.Flusher)
	return &anthropicStreamWriter{target: target, model: model, flusher: flusher}
}

func (w *anthropicStreamWriter) Header() http.Header { return w.target.Header() }

func (w *anthropicStreamWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	if status < 400 {
		w.target.Header().Set("Content-Type", "text/event-stream")
		w.target.Header().Set("Cache-Control", "no-cache")
	}
	w.target.WriteHeader(status)
}

func (w *anthropicStreamWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= 400 {
		return w.target.Write(data)
	}
	w.buffer.Write(data)
	for {
		chunk := w.buffer.String()
		newline := strings.IndexByte(chunk, '\n')
		if newline < 0 {
			break
		}
		line := strings.TrimSpace(chunk[:newline])
		w.buffer.Reset()
		w.buffer.WriteString(chunk[newline+1:])
		if strings.HasPrefix(line, "data:") {
			if err := w.consume(strings.TrimSpace(strings.TrimPrefix(line, "data:"))); err != nil {
				return len(data), err
			}
		}
	}
	return len(data), nil
}

func (w *anthropicStreamWriter) consume(payload string) error {
	var event struct {
		Output string `json:"output"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return err
	}
	if event.Error != "" {
		return w.writeEvent("error", map[string]any{
			"type": "error", "error": map[string]string{"type": "api_error", "message": event.Error},
		})
	}
	if event.Output == "" {
		return nil
	}
	if !w.started {
		w.started = true
		if err := w.writeEvent("message_start", map[string]any{
			"type": "message_start", "message": map[string]any{
				"id": "msg-maistr0", "type": "message", "role": "assistant", "model": w.model,
				"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
				"usage": map[string]int{"input_tokens": 0, "output_tokens": 0},
			},
		}); err != nil {
			return err
		}
		if err := w.writeEvent("content_block_start", map[string]any{
			"type": "content_block_start", "index": 0, "content_block": map[string]string{"type": "text", "text": ""},
		}); err != nil {
			return err
		}
	}
	return w.writeEvent("content_block_delta", map[string]any{
		"type": "content_block_delta", "index": 0,
		"delta": map[string]string{"type": "text_delta", "text": event.Output},
	})
}

func (w *anthropicStreamWriter) writeEvent(name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w.target, "event: %s\ndata: %s\n\n", name, raw); err != nil {
		return err
	}
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return nil
}

func (w *anthropicStreamWriter) finish() {
	if w.status >= 400 {
		return
	}
	if rest := strings.TrimSpace(w.buffer.String()); strings.HasPrefix(rest, "data:") {
		_ = w.consume(strings.TrimSpace(strings.TrimPrefix(rest, "data:")))
	}
	if !w.started {
		w.started = true
		_ = w.writeEvent("message_start", map[string]any{
			"type": "message_start", "message": map[string]any{
				"id": "msg-maistr0", "type": "message", "role": "assistant", "model": w.model,
				"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
				"usage": map[string]int{"input_tokens": 0, "output_tokens": 0},
			},
		})
		_ = w.writeEvent("content_block_start", map[string]any{
			"type": "content_block_start", "index": 0, "content_block": map[string]string{"type": "text", "text": ""},
		})
	}
	_ = w.writeEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	_ = w.writeEvent("message_delta", map[string]any{
		"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
		"usage": map[string]int{"output_tokens": 0},
	})
	_ = w.writeEvent("message_stop", map[string]string{"type": "message_stop"})
}

func (w *anthropicStreamWriter) Flush() {
	if w.flusher != nil {
		w.flusher.Flush()
	}
}

func (w *ollamaStreamWriter) Flush() {
	if w.flusher != nil {
		w.flusher.Flush()
	}
}
