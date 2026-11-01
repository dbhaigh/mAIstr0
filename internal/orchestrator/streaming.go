package orchestrator

import (
	"bytes"
	"encoding/json"
	"net/http"
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

// Header returns the headers captured from a compatibility handler.
//
// Returns: the captured HTTP header map.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (r *responseCapture) Header() http.Header { return r.header }

// Write appends response bytes to the capture buffer and defaults status to
// HTTP 200 if no status has been set.
//
// Parameters: b ([]byte) contains the bytes to capture.
// Returns: the number of bytes written and the buffer's write error.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (r *responseCapture) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(b)
}
// WriteHeader records the status code produced by a compatibility handler.
//
// Parameters: status (int) is the HTTP status code to capture.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (r *responseCapture) WriteHeader(status int) { r.status = status }

type openAIStreamWriter struct {
	compatStreamResponse
	model  string
	parser compatStreamParser
}

func newOpenAIStreamWriter(target http.ResponseWriter, model string) *openAIStreamWriter {
	return &openAIStreamWriter{
		compatStreamResponse: newCompatStreamResponse(
			target, make(http.Header), "text/event-stream", "keep-alive",
		),
		model: model,
	}
}

// Write parses upstream stream bytes and emits OpenAI-compatible chat
// completion chunks.
//
// Parameters: data ([]byte) is the next upstream chunk.
// Returns: the number of input bytes consumed, or an error parsing the event
// or writing the compatibility response.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (w *openAIStreamWriter) Write(data []byte) (int, error) {
	return w.compatStreamResponse.write(data, &w.parser, w.consume)
}

func (w *openAIStreamWriter) consume(payload []byte) error {
	event, err := decodeCompatStreamEvent(payload)
	if err != nil {
		return err
	}
	if event.Error != "" {
		if err := w.writeChunk(map[string]any{"error": map[string]string{"message": event.Error, "type": "server_error"}}); err != nil {
			return err
		}
	}
	if event.Output != "" {
		chunk := map[string]any{
			"id": "chatcmpl-maistr0", "object": "chat.completion.chunk",
			"created": time.Now().Unix(), "model": w.model,
			"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": event.Output}, "finish_reason": nil}},
		}
		return w.writeChunk(chunk)
	}
	return nil
}

func (w *openAIStreamWriter) writeChunk(chunk any) error {
	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	return w.stream.WriteSSEData(data)
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
	_ = w.stream.WriteSSEData([]byte("[DONE]"))
}
