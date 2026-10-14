package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type openAIModelObj struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatRequest struct {
	Model       string              `json:"model"`
	Messages    []openAIChatMessage `json:"messages"`
	Stream      bool                `json:"stream"`
	Temperature float64             `json:"temperature"`
	MaxTokens   int                 `json:"max_tokens,omitempty"`
}

type openAICompletionRequest struct {
	Model       string  `json:"model"`
	Prompt      string  `json:"prompt"`
	Temperature float64 `json:"temperature"`
	MaxTokens   int     `json:"max_tokens,omitempty"`
}

type responseCapture struct {
	header http.Header
	body   *bytes.Buffer
	status int
}

func (r *responseCapture) Header() http.Header { return r.header }
func (r *responseCapture) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(b)
}
func (r *responseCapture) WriteHeader(status int) { r.status = status }

type openAIStreamWriter struct {
	target  http.ResponseWriter
	header  http.Header
	model   string
	status  int
	buffer  strings.Builder
	flusher http.Flusher
}

func newOpenAIStreamWriter(target http.ResponseWriter, model string) *openAIStreamWriter {
	flusher, _ := target.(http.Flusher)
	return &openAIStreamWriter{target: target, header: make(http.Header), model: model, flusher: flusher}
}

func (w *openAIStreamWriter) Header() http.Header { return w.header }

func (w *openAIStreamWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	for key, values := range w.header {
		for _, value := range values {
			w.target.Header().Add(key, value)
		}
	}
	if status < 400 {
		w.target.Header().Set("Content-Type", "text/event-stream")
		w.target.Header().Set("Cache-Control", "no-cache")
		w.target.Header().Set("Connection", "keep-alive")
	}
	w.target.WriteHeader(status)
}

func (w *openAIStreamWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= 400 {
		return w.target.Write(data)
	}
	w.buffer.Write(data)
	for {
		buffer := w.buffer.String()
		newline := strings.IndexByte(buffer, '\n')
		if newline < 0 {
			break
		}
		line := buffer[:newline]
		w.buffer.Reset()
		w.buffer.WriteString(buffer[newline+1:])
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var event struct {
			Output string `json:"output"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return len(data), err
		}
		if event.Error != "" {
			if err := w.writeChunk(map[string]any{"error": map[string]string{"message": event.Error, "type": "server_error"}}); err != nil {
				return len(data), err
			}
		}
		if event.Output != "" {
			chunk := map[string]any{
				"id": "chatcmpl-maistr0", "object": "chat.completion.chunk",
				"created": time.Now().Unix(), "model": w.model,
				"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": event.Output}, "finish_reason": nil}},
			}
			if err := w.writeChunk(chunk); err != nil {
				return len(data), err
			}
		}
	}
	return len(data), nil
}

func (w *openAIStreamWriter) writeChunk(chunk any) error {
	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w.target, "data: %s\n\n", data); err != nil {
		return err
	}
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return nil
}

func (w *openAIStreamWriter) finish() {
	if w.status >= 400 {
		return
	}
	_ = w.writeChunk(map[string]any{
		"id": "chatcmpl-maistr0", "object": "chat.completion.chunk",
		"created": time.Now().Unix(), "model": w.model,
		"choices": []map[string]any{{"index": 0, "delta": map[string]string{}, "finish_reason": "stop"}},
	})
	_, _ = fmt.Fprint(w.target, "data: [DONE]\n\n")
	if w.flusher != nil {
		w.flusher.Flush()
	}
}
