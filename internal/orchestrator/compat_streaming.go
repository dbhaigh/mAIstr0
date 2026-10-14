package orchestrator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

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
