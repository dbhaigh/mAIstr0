package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLMStudioOpenAICompatibleAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			if r.Method != http.MethodGet {
				t.Errorf("models method = %q, want GET", r.Method)
			}
			_, _ = fmt.Fprint(w, `{"data":[{"id":"qwen2.5-coder"}]}`)
		case "/v1/chat/completions":
			var request struct {
				Model       string        `json:"model"`
				Messages    []ChatMessage `json:"messages"`
				Temperature float64       `json:"temperature"`
				MaxTokens   int           `json:"max_tokens"`
				Stream      bool          `json:"stream"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode chat request: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			if request.Model != "qwen2.5-coder" || len(request.Messages) != 1 ||
				request.Messages[0].Content != "write a test" || request.Temperature != 0.2 ||
				request.MaxTokens != 32 || request.Stream {
				t.Errorf("unexpected LM Studio chat request: %+v", request)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []any{map[string]any{
					"message": ChatMessage{Role: "assistant", Content: "done"},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	lmstudio := NewLMStudio(server.URL)
	if !lmstudio.Available() {
		t.Fatal("LM Studio server should be available")
	}
	models, err := lmstudio.ListModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Name != "qwen2.5-coder" || models[0].Engine != "lmstudio" {
		t.Fatalf("unexpected LM Studio models: %+v", models)
	}

	output, err := lmstudio.GenerateWithOptions(context.Background(), models[0].Name, "write a test", GenerationOptions{
		Temperature: 0.2,
		MaxTokens:   32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if output != "done" {
		t.Fatalf("generated output = %q, want done", output)
	}
}

func TestNewConfiguredLMStudio(t *testing.T) {
	configured := NewConfigured("lmstudio", "http://127.0.0.1:1234")
	lmstudio, ok := configured.(*LMStudio)
	if !ok {
		t.Fatalf("configured engine = %T, want *LMStudio", configured)
	}
	if lmstudio.Name() != "lmstudio" || lmstudio.URL() != "http://127.0.0.1:1234" {
		t.Fatalf("configured LM Studio identity = %q at %q", lmstudio.Name(), lmstudio.URL())
	}
}

func TestDetectLMStudioFallsBackToAlternatePort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":[{"id":"local-model"}]}`)
	}))
	defer server.Close()

	detected := firstAvailableLMStudio(
		NewLMStudio("http://127.0.0.1:1"),
		NewLMStudio(server.URL),
	)
	lmstudio, ok := detected.(*LMStudio)
	if !ok || lmstudio.URL() != server.URL {
		t.Fatalf("detected LM Studio = %v, want server at %q", detected, server.URL)
	}
}

func TestDetectLMStudioReturnsNilInterfaceWhenUnavailable(t *testing.T) {
	if detected := firstAvailableLMStudio(NewLMStudio("http://127.0.0.1:1")); detected != nil {
		t.Fatalf("detected LM Studio = %v, want nil when unavailable", detected)
	}
}
