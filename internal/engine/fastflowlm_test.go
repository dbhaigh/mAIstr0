package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFastFlowLMConfiguredEngineUsesOpenAICompatibleAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
			_, _ = fmt.Fprint(w, `{"data":[{"id":"fastflow-model"}]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
			var request struct {
				Stream bool `json:"stream"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if request.Stream {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"FastFlowLM stream"}}]}`)
				_, _ = fmt.Fprintln(w, "data: [DONE]")
				return
			}
			_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"FastFlowLM response"}}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	configured := NewConfigured("fastflowlm", server.URL)
	fastFlow, ok := configured.(*FastFlowLM)
	if !ok {
		t.Fatalf("NewConfigured returned %T, want *FastFlowLM", configured)
	}
	if fastFlow.Name() != "fastflowlm" || !SupportsProtocol(fastFlow.Name(), "openai") {
		t.Fatalf("FastFlowLM engine identity or protocol is incorrect")
	}
	if !fastFlow.Available() {
		t.Fatal("FastFlowLM server was not detected")
	}

	models, err := fastFlow.ListModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Name != "fastflow-model" || models[0].Engine != "fastflowlm" {
		t.Fatalf("FastFlowLM models = %+v", models)
	}

	response, err := fastFlow.Generate("fastflow-model", "prompt")
	if err != nil || response != "FastFlowLM response" {
		t.Fatalf("FastFlowLM Generate() = (%q, %v)", response, err)
	}

	streaming, ok := configured.(StreamingEngine)
	if !ok {
		t.Fatal("FastFlowLM does not expose streaming generation")
	}
	var deltas []string
	err = streaming.GenerateStream(context.Background(), "fastflow-model", "prompt", GenerationOptions{}, func(delta string) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil || strings.Join(deltas, "") != "FastFlowLM stream" {
		t.Fatalf("FastFlowLM GenerateStream() = (%q, %v)", strings.Join(deltas, ""), err)
	}
}
