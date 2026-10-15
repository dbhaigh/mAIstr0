package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLlamaCPPUsesOpenAICompatibleAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			if r.Method != http.MethodGet {
				t.Errorf("models method = %q, want GET", r.Method)
			}
			_, _ = fmt.Fprint(w, `{"data":[{"id":"local-gguf"}]}`)
		case "/v1/chat/completions":
			var request struct {
				Model       string        `json:"model"`
				Messages    []ChatMessage `json:"messages"`
				Temperature float64       `json:"temperature"`
				MaxTokens   int           `json:"max_tokens"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode chat request: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if request.Model != "local-gguf" || len(request.Messages) != 1 ||
				request.Messages[0].Content != "say hello" || request.Temperature != 0.2 ||
				request.MaxTokens != 32 {
				t.Errorf("unexpected llama.cpp chat request: %+v", request)
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

	llamacpp := NewLlamaCPP(server.URL + "/")
	if llamacpp.Name() != "llamacpp" || llamacpp.URL() != server.URL {
		t.Fatalf("llama.cpp identity = %q at %q", llamacpp.Name(), llamacpp.URL())
	}
	if !llamacpp.Available() {
		t.Fatal("llama.cpp server should be available")
	}

	models, err := llamacpp.ListModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Name != "local-gguf" || models[0].Engine != "llamacpp" {
		t.Fatalf("unexpected llama.cpp models: %+v", models)
	}

	output, err := llamacpp.GenerateWithOptions(context.Background(), models[0].Name, "say hello", GenerationOptions{
		Temperature: 0.2,
		MaxTokens:   32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if output != "hello" {
		t.Fatalf("generated output = %q, want hello", output)
	}
}

func TestNewConfiguredLlamaCPP(t *testing.T) {
	configured := NewConfigured("llamacpp", "http://127.0.0.1:8080")
	llamacpp, ok := configured.(*LlamaCPP)
	if !ok {
		t.Fatalf("configured engine = %T, want *LlamaCPP", configured)
	}
	if llamacpp.Name() != "llamacpp" || llamacpp.URL() != "http://127.0.0.1:8080" {
		t.Fatalf("configured llama.cpp identity = %q at %q", llamacpp.Name(), llamacpp.URL())
	}
	if !SupportsProtocol(llamacpp.Name(), "openai") || SupportsProtocol(llamacpp.Name(), "ollama") {
		t.Fatalf("unexpected llama.cpp protocols: %v", ProtocolsForEngine(llamacpp.Name()))
	}
}

func TestDetectLlamaCPPUsesFirstAvailableServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":[{"id":"local-gguf"}]}`)
	}))
	defer server.Close()

	detected := firstAvailableLlamaCPP(
		NewLlamaCPP("http://127.0.0.1:1"),
		NewLlamaCPP(server.URL),
	)
	llamacpp, ok := detected.(*LlamaCPP)
	if !ok || llamacpp.URL() != server.URL {
		t.Fatalf("detected llama.cpp = %v, want server at %q", detected, server.URL)
	}
}

func TestDetectLlamaCPPReturnsNilWhenUnavailable(t *testing.T) {
	if detected := firstAvailableLlamaCPP(NewLlamaCPP("http://127.0.0.1:1")); detected != nil {
		t.Fatalf("detected llama.cpp = %v, want nil when unavailable", detected)
	}
}

func TestLlamaCPPGenerateStreamForwardsOptionsAndDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Model       string  `json:"model"`
			Stream      bool    `json:"stream"`
			Temperature float64 `json:"temperature"`
			MaxTokens   int     `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Model != "local-gguf" || !request.Stream ||
			request.Temperature != 0.4 || request.MaxTokens != 48 {
			t.Errorf("unexpected llama.cpp stream request: %+v", request)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"first "}}]}`)
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"second"}}]}`)
		_, _ = fmt.Fprintln(w, "data: [DONE]")
	}))
	defer server.Close()

	var deltas []string
	err := NewLlamaCPP(server.URL).GenerateStream(context.Background(), "local-gguf", "say hello", GenerationOptions{
		Temperature: 0.4,
		MaxTokens:   48,
	}, func(delta string) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 2 || deltas[0] != "first " || deltas[1] != "second" {
		t.Fatalf("deltas = %q", deltas)
	}
}
