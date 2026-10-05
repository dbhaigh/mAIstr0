package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestOllamaGenerateStreamForwardsOptionsAndDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/generate" {
			t.Fatalf("request path = %q", r.URL.Path)
		}
		var request struct {
			Stream  bool           `json:"stream"`
			Options map[string]any `json:"options"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if !request.Stream || request.Options["temperature"] != 0.4 || request.Options["num_predict"] != float64(48) {
			t.Errorf("unexpected Ollama request: %+v", request)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(w, `{"response":"first "}`)
		_, _ = fmt.Fprintln(w, `{"response":"second","done":true}`)
	}))
	defer server.Close()

	var deltas []string
	err := NewOllama(server.URL).GenerateStream(context.Background(), "model", "prompt", GenerationOptions{
		Temperature: 0.4,
		MaxTokens:   48,
	}, func(delta string) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(deltas, []string{"first ", "second"}) {
		t.Fatalf("deltas = %q", deltas)
	}
}

func TestVLLMGenerateStreamForwardsOptionsAndDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Stream      bool    `json:"stream"`
			Temperature float64 `json:"temperature"`
			MaxTokens   int     `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if !request.Stream || request.Temperature != 0.4 || request.MaxTokens != 48 {
			t.Errorf("unexpected vLLM request: %+v", request)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"first "}}]}`)
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"second"}}]}`)
		_, _ = fmt.Fprintln(w, "data: [DONE]")
	}))
	defer server.Close()

	var deltas []string
	err := NewVLLM(server.URL).GenerateStream(context.Background(), "model", "prompt", GenerationOptions{
		Temperature: 0.4,
		MaxTokens:   48,
	}, func(delta string) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(deltas, []string{"first ", "second"}) {
		t.Fatalf("deltas = %q", deltas)
	}
}
