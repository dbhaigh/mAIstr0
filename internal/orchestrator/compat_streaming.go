package orchestrator

import (
	"encoding/json"
	"net/http"
	"time"
)

type ollamaStreamWriter struct {
	compatStreamResponse
	model  string
	chat   bool
	parser compatStreamParser
}

func newOllamaStreamWriter(target http.ResponseWriter, model string, chat bool) *ollamaStreamWriter {
	return &ollamaStreamWriter{
		compatStreamResponse: newCompatStreamResponse(target, nil, "application/x-ndjson", ""),
		model:                model,
		chat:                 chat,
	}
}

// Write parses upstream stream bytes and emits the corresponding Ollama
// newline-delimited JSON response.
//
// Parameters: data ([]byte) is the next upstream chunk.
// Returns: the number of input bytes consumed, or an error encoding or writing
// the compatibility response.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (w *ollamaStreamWriter) Write(data []byte) (int, error) {
	return w.compatStreamResponse.write(data, &w.parser, w.consume)
}

func (w *ollamaStreamWriter) consume(payload []byte) error {
	event, err := decodeCompatStreamEvent(payload)
	if err != nil {
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
	return w.stream.WriteJSONLine(value)
}

func (w *ollamaStreamWriter) finish() {
	if w.status >= 400 {
		return
	}
	_ = w.parser.flushPending(w.consume)
	value := map[string]any{"model": w.model, "created_at": time.Now().UTC(), "done": true, "done_reason": "stop"}
	if w.chat {
		value["message"] = ollamaMessage{Role: "assistant", Content: ""}
	} else {
		value["response"] = ""
	}
	_ = w.write(value)
}

type anthropicStreamWriter struct {
	compatStreamResponse
	model   string
	parser  compatStreamParser
	started bool
}

func newAnthropicStreamWriter(target http.ResponseWriter, model string) *anthropicStreamWriter {
	return &anthropicStreamWriter{
		compatStreamResponse: newCompatStreamResponse(target, nil, "text/event-stream", ""),
		model:                model,
	}
}

// Write parses upstream stream bytes and emits the corresponding Anthropic
// server-sent events.
//
// Parameters: data ([]byte) is the next upstream chunk.
// Returns: the number of input bytes consumed, or an error encoding or writing
// the compatibility response.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (w *anthropicStreamWriter) Write(data []byte) (int, error) {
	return w.compatStreamResponse.write(data, &w.parser, w.consume)
}

func (w *anthropicStreamWriter) consume(payload []byte) error {
	event, err := decodeCompatStreamEvent(payload)
	if err != nil {
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
	return w.stream.WriteSSEEvent(name, raw)
}

func (w *anthropicStreamWriter) finish() {
	if w.status >= 400 {
		return
	}
	_ = w.parser.flushPending(w.consume)
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

// Flush sends any buffered compatibility response data to the client.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (w *anthropicStreamWriter) Flush() {
	w.compatStreamResponse.flush()
}

// Flush sends any buffered compatibility response data to the client.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (w *ollamaStreamWriter) Flush() {
	w.compatStreamResponse.flush()
}
