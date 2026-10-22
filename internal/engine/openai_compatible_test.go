package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConfiguredOpenAICompatibleEngines(t *testing.T) {
	const model = "served-model"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			if r.Method != http.MethodGet {
				t.Errorf("models method = %q, want GET", r.Method)
			}
			_, _ = fmt.Fprint(w, `{"data":[{"id":"served-model"}]}`)
		case "/v1/chat/completions":
			var request struct {
				Model    string        `json:"model"`
				Messages []ChatMessage `json:"messages"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode chat request: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if request.Model != model || len(request.Messages) != 1 ||
				request.Messages[0].Content != "say hello" {
				t.Errorf("unexpected chat request: %+v", request)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []any{map[string]any{
					"message": ChatMessage{Role: "assistant", Content: "hello"},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	for _, name := range []string{"tgi", "localai", "mlx", "sglang", "openai"} {
		t.Run(name, func(t *testing.T) {
			configured := NewConfigured(name, server.URL)
			if configured == nil || configured.Name() != name {
				t.Fatalf("configured engine = %v, want %q", configured, name)
			}
			if !SupportsProtocol(name, "openai") || SupportsProtocol(name, "ollama") {
				t.Fatalf("unexpected protocols for %s: %v", name, ProtocolsForEngine(name))
			}
			if !configured.Available() {
				t.Fatalf("%s should be available", name)
			}
			models, err := configured.ListModels()
			if err != nil {
				t.Fatal(err)
			}
			if len(models) != 1 || models[0].Name != model || models[0].Engine != name {
				t.Fatalf("unexpected models for %s: %+v", name, models)
			}
			generator, ok := configured.(EngineWithOptions)
			if !ok {
				t.Fatalf("%s does not support generation options", name)
			}
			response, err := generator.GenerateWithOptions(context.Background(), model, "say hello", GenerationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if response != "hello" {
				t.Fatalf("response = %q, want hello", response)
			}
		})
	}
}
