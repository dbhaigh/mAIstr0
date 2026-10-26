package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type compatibilityStreamWriter interface {
	http.ResponseWriter
	finish()
}

type compatibilityStreamWriterCase struct {
	name string
	new  func(http.ResponseWriter) compatibilityStreamWriter
}

func compatibilityStreamWriterCases() []compatibilityStreamWriterCase {
	return []compatibilityStreamWriterCase{
		{
			name: "openai",
			new: func(w http.ResponseWriter) compatibilityStreamWriter {
				return newOpenAIStreamWriter(w, "test-model")
			},
		},
		{
			name: "ollama",
			new: func(w http.ResponseWriter) compatibilityStreamWriter {
				return newOllamaStreamWriter(w, "test-model", true)
			},
		},
		{
			name: "anthropic",
			new: func(w http.ResponseWriter) compatibilityStreamWriter {
				return newAnthropicStreamWriter(w, "test-model")
			},
		},
	}
}

func TestCompatStreamWritersPreserveChunkBoundariesAndTerminalOutput(t *testing.T) {
	input := []byte("data: {\"output\":\"café\"}\n")
	utf8Start := bytes.Index(input, []byte("é"))
	if utf8Start < 0 {
		t.Fatal("test payload does not contain its UTF-8 boundary marker")
	}
	chunkCases := []struct {
		name  string
		split int
	}{
		{name: "mid-line", split: len(`data: {"out`)},
		{name: "mid-UTF-8", split: utf8Start + 1},
	}

	for _, writerCase := range compatibilityStreamWriterCases() {
		for _, chunkCase := range chunkCases {
			t.Run(writerCase.name+"/"+chunkCase.name, func(t *testing.T) {
				response := httptest.NewRecorder()
				writer := writerCase.new(response)
				chunks := [][]byte{input[:chunkCase.split], {}, input[chunkCase.split:]}
				for _, chunk := range chunks {
					n, err := writer.Write(chunk)
					if err != nil {
						t.Fatalf("write chunk: %v", err)
					}
					if n != len(chunk) {
						t.Fatalf("write returned %d bytes for %d-byte chunk", n, len(chunk))
					}
				}
				writer.finish()

				body := response.Body.String()
				switch writerCase.name {
				case "openai":
					if response.Header().Get("Content-Type") != "text/event-stream" ||
						response.Header().Get("Cache-Control") != "no-cache" ||
						response.Header().Get("Connection") != "keep-alive" {
						t.Fatalf("OpenAI stream headers = %v", response.Header())
					}
					for _, expected := range []string{`"content":"café"`, "data: [DONE]\n\n"} {
						if !strings.Contains(body, expected) {
							t.Errorf("OpenAI stream omitted %q: %s", expected, body)
						}
					}
				case "ollama":
					if response.Header().Get("Content-Type") != "application/x-ndjson" ||
						response.Header().Get("Cache-Control") != "no-cache" {
						t.Fatalf("Ollama stream headers = %v", response.Header())
					}
					if !strings.Contains(body, `"content":"café"`) {
						t.Errorf("Ollama stream omitted UTF-8 output: %s", body)
					}
					lines := strings.Split(strings.TrimSpace(body), "\n")
					var terminal map[string]any
					if err := json.Unmarshal([]byte(lines[len(lines)-1]), &terminal); err != nil {
						t.Fatalf("decode Ollama terminal line: %v", err)
					}
					if terminal["done"] != true || terminal["done_reason"] != "stop" {
						t.Errorf("Ollama terminal line = %v", terminal)
					}
				case "anthropic":
					if response.Header().Get("Content-Type") != "text/event-stream" ||
						response.Header().Get("Cache-Control") != "no-cache" {
						t.Fatalf("Anthropic stream headers = %v", response.Header())
					}
					for _, expected := range []string{
						`"text":"café"`, "event: content_block_stop",
						"event: message_delta", "event: message_stop",
					} {
						if !strings.Contains(body, expected) {
							t.Errorf("Anthropic stream omitted %q: %s", expected, body)
						}
					}
				}
			})
		}
	}
}

func TestCompatStreamWritersContinueAfterUpstreamError(t *testing.T) {
	input := []byte(
		"data: {\"output\":\"before\"}\n" +
			"data: {\"error\":\"upstream failed\"}\n" +
			"data: {\"output\":\"after\"}\n",
	)
	for _, writerCase := range compatibilityStreamWriterCases() {
		t.Run(writerCase.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writer := writerCase.new(response)
			if n, err := writer.Write(input); err != nil || n != len(input) {
				t.Fatalf("write upstream events = (%d, %v), want (%d, nil)", n, err, len(input))
			}
			writer.finish()

			body := response.Body.String()
			if !strings.Contains(body, "upstream failed") || !strings.Contains(body, "after") {
				t.Fatalf("stream did not preserve the error and subsequent output: %s", body)
			}
			switch writerCase.name {
			case "openai":
				if !strings.Contains(body, `"type":"server_error"`) || !strings.Contains(body, "data: [DONE]") {
					t.Errorf("OpenAI error or terminal frame missing: %s", body)
				}
			case "ollama":
				lines := strings.Split(strings.TrimSpace(body), "\n")
				var errorLine map[string]any
				if err := json.Unmarshal([]byte(lines[1]), &errorLine); err != nil {
					t.Fatalf("decode Ollama error line: %v", err)
				}
				if errorLine["error"] != "upstream failed" || errorLine["done"] != true {
					t.Errorf("Ollama error line = %v", errorLine)
				}
			case "anthropic":
				if !strings.Contains(body, "event: error") || !strings.Contains(body, "event: message_stop") {
					t.Errorf("Anthropic error or terminal event missing: %s", body)
				}
			}
		})
	}
}

type failingCompatResponseWriter struct {
	header http.Header
	err    error
}

func (w *failingCompatResponseWriter) Header() http.Header { return w.header }

func (w *failingCompatResponseWriter) WriteHeader(int) {}

func (w *failingCompatResponseWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestCompatStreamWritersPropagateClientDisconnect(t *testing.T) {
	input := []byte("data: {\"output\":\"disconnect\"}\n")
	for _, writerCase := range compatibilityStreamWriterCases() {
		t.Run(writerCase.name, func(t *testing.T) {
			target := &failingCompatResponseWriter{
				header: make(http.Header),
				err:    io.ErrClosedPipe,
			}
			writer := writerCase.new(target)
			_, err := writer.Write(input)
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("write error = %v, want %v", err, io.ErrClosedPipe)
			}
		})
	}
}

type discardCompatResponseWriter struct {
	header http.Header
}

func (w *discardCompatResponseWriter) Header() http.Header { return w.header }

func (w *discardCompatResponseWriter) WriteHeader(int) {}

func (w *discardCompatResponseWriter) Write(data []byte) (int, error) {
	return len(data), nil
}

func BenchmarkCompatStreamWriters(b *testing.B) {
	input := []byte("data: {\"output\":\"benchmark token\"}\n")
	for _, writerCase := range compatibilityStreamWriterCases() {
		b.Run(writerCase.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				target := &discardCompatResponseWriter{header: make(http.Header)}
				writer := writerCase.new(target)
				if _, err := writer.Write(input); err != nil {
					b.Fatal(err)
				}
				writer.finish()
			}
		})
	}
}
