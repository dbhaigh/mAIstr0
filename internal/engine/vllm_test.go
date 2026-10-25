package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestVLLMListModelsIncludesReportedMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Method != http.MethodGet {
			t.Errorf("request = %s %s, want GET /v1/models", r.Method, r.URL.Path)
		}
		_, _ = fmt.Fprint(w, `{"data":[{"id":"served-model","max_model_len":32768,"size_gb":12.5,"device":"cuda"}]}`)
	}))
	defer server.Close()

	models, err := NewVLLM(server.URL).ListModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Name != "served-model" ||
		models[0].Engine != "vllm" || models[0].ContextLength != 32768 ||
		models[0].SizeGB != 12.5 || models[0].Device != "gpu" {
		t.Fatalf("models = %+v, want reported model metadata", models)
	}
}

func TestVLLMChatWithOptionsUsesSharedRequestAndBearerAuth(t *testing.T) {
	const apiKey = "test-vllm-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s, want POST /v1/chat/completions", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiKey {
			t.Errorf("authorization = %q, want bearer token", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content type = %q, want application/json", got)
		}
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
		if request.Model != "served-model" || len(request.Messages) != 1 ||
			request.Messages[0].Role != "user" || request.Messages[0].Content != "hello" ||
			request.Temperature != 0.25 || request.MaxTokens != 64 || request.Stream {
			t.Errorf("chat request = %+v", request)
		}
		_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"world"}}]}`)
	}))
	defer server.Close()

	configured := NewConfiguredWithAPIKey("vllm", server.URL, apiKey)
	chatEngine, ok := configured.(EngineWithChatOptions)
	if !ok {
		t.Fatalf("configured engine %T does not support chat options", configured)
	}
	output, err := chatEngine.ChatWithOptions(context.Background(), "served-model", []ChatMessage{
		{Role: "user", Content: "hello"},
	}, GenerationOptions{Temperature: 0.25, MaxTokens: 64})
	if err != nil {
		t.Fatal(err)
	}
	if output != "world" {
		t.Fatalf("chat output = %q, want world", output)
	}
}

func TestOpenAICompatibleConfiguredEnginesSendAPIKey(t *testing.T) {
	const apiKey = "shared-engine-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiKey {
			t.Errorf("%s authorization = %q, want bearer token", r.URL.Path, got)
		}
		switch r.URL.Path {
		case "/v1/models":
			_, _ = fmt.Fprint(w, `{"data":[{"id":"served-model"}]}`)
		case "/props":
			_, _ = fmt.Fprint(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	for _, name := range []string{"vllm", "llamacpp", "lmstudio", "fastflowlm"} {
		t.Run(name, func(t *testing.T) {
			models, err := NewConfiguredWithAPIKey(name, server.URL, apiKey).ListModels()
			if err != nil {
				t.Fatal(err)
			}
			if len(models) != 1 || models[0].Name != "served-model" {
				t.Fatalf("models = %+v", models)
			}
		})
	}
}

func TestVLLMRequestBuilderStreamsOptionsAndDeltas(t *testing.T) {
	const apiKey = "stream-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Errorf("authorization header missing or incorrect")
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("accept = %q, want text/event-stream", got)
		}
		var request struct {
			Model       string  `json:"model"`
			Stream      bool    `json:"stream"`
			Temperature float64 `json:"temperature"`
			MaxTokens   int     `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode stream request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if request.Model != "served-model" || !request.Stream ||
			request.Temperature != 0.5 || request.MaxTokens != 128 {
			t.Errorf("stream request = %+v", request)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"first "}}]}`)
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"second"}}]}`)
		_, _ = fmt.Fprintln(w, "data: [DONE]")
	}))
	defer server.Close()

	vllm := NewConfiguredWithAPIKey("vllm", server.URL, apiKey).(*VLLM)
	var deltas []string
	err := vllm.GenerateStream(context.Background(), "served-model", "hello", GenerationOptions{
		Temperature: 0.5,
		MaxTokens:   128,
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

func TestVLLMIncludesErrorBodiesAndRedactsAPIKey(t *testing.T) {
	const apiKey = "very-secret-key"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "request rejected for token "+apiKey, http.StatusUnauthorized)
	}))
	defer server.Close()

	vllm := NewConfiguredWithAPIKey("vllm", server.URL, apiKey).(*VLLM)
	_, err := vllm.GenerateWithOptions(context.Background(), "model", "prompt", GenerationOptions{})
	if err == nil || !strings.Contains(err.Error(), "401 Unauthorized") ||
		!strings.Contains(err.Error(), "request rejected for token [REDACTED]") ||
		strings.Contains(err.Error(), apiKey) {
		t.Fatalf("chat error = %v, want status and redacted body", err)
	}

	_, err = vllm.ListModels()
	if err == nil || !strings.Contains(err.Error(), "vllm list models failed") ||
		strings.Contains(err.Error(), apiKey) {
		t.Fatalf("list error = %v, want engine-specific redacted error", err)
	}
}

func TestVLLMEmptyChoicesUsesConfiguredEngineName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"choices":[]}`)
	}))
	defer server.Close()

	_, err := NewConfigured("fastflowlm", server.URL).(EngineWithChatContext).
		ChatWithContext(context.Background(), "model", []ChatMessage{{Role: "user", Content: "hello"}})
	if err == nil || !strings.Contains(err.Error(), "fastflowlm returned no choices") {
		t.Fatalf("empty choices error = %v, want configured engine name", err)
	}
}

func TestLlamaCPPListModelsUsesPropsMetadata(t *testing.T) {
	const apiKey = "llama-props-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Errorf("%s authorization header missing or incorrect", r.URL.Path)
		}
		switch r.URL.Path {
		case "/v1/models":
			_, _ = fmt.Fprint(w, `{"data":[{"id":"local-gguf"}]}`)
		case "/props":
			_, _ = fmt.Fprint(w, `{"default_generation_settings":{"n_ctx":8192},"model_size":4294967296,"n_gpu_layers":1,"total_slots":4,"backends":["cuda"],"quantizations":["q4_k_m"],"quantization":"q4_k_m","supports_sharding":["llamacpp-rpc"],"validated_rpc":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend := NewConfiguredWithAPIKey("llamacpp", server.URL, apiKey)
	models, err := backend.ListModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].ContextLength != 8192 || models[0].SizeGB != 4 {
		t.Fatalf("models = %+v, want llama-server metadata", models)
	}
	if models[0].Device != "gpu" {
		t.Fatalf("model device = %q, want gpu", models[0].Device)
	}
	if models[0].Quantization != "q4_k_m" {
		t.Fatalf("model quantization = %q, want q4_k_m", models[0].Quantization)
	}
	capabilities := backend.(CapabilityProvider).Capabilities()
	if capabilities.MaxParallelSeqs != 4 || capabilities.MaxContextLength != 8192 || capabilities.ValidatedRPC ||
		!containsString(capabilities.Devices, "gpu") ||
		!containsString(capabilities.SupportsSharding, "llamacpp-rpc") {
		t.Fatalf("llama.cpp capabilities = %+v, want RPC support and operator-gated validation", capabilities)
	}
	probed, err := backend.(CapabilityProber).ProbeCapabilities(context.Background())
	if err != nil || !reflect.DeepEqual(probed, capabilities) {
		t.Fatalf("llama.cpp capability probe = %+v, %v; want cached props-derived capabilities", probed, err)
	}
}

func TestBuiltinEnginesExposeConservativeCapabilities(t *testing.T) {
	for _, backend := range []Engine{
		NewOllama("http://127.0.0.1:11434"),
		NewVLLM("http://127.0.0.1:8000"),
		NewLlamaCPP("http://127.0.0.1:8080"),
		NewLMStudio("http://127.0.0.1:1234"),
		NewFastFlowLM("http://127.0.0.1:8000"),
		NewConfigured("openai", "http://127.0.0.1:9000"),
		NewSimulated(),
	} {
		if _, ok := backend.(CapabilityProvider); !ok {
			t.Errorf("%s does not implement CapabilityProvider", backend.Name())
		}
	}
	if capabilities := NewOllama("http://127.0.0.1:11434").Capabilities(); len(capabilities.Devices) != 0 {
		t.Fatalf("Ollama reported unproven devices: %+v", capabilities.Devices)
	}
	if capabilities := NewVLLM("http://127.0.0.1:8000").Capabilities(); len(capabilities.Devices) != 0 {
		t.Fatalf("vLLM reported unproven devices: %+v", capabilities.Devices)
	}
	if capabilities := NewSimulated().Capabilities(); !containsString(capabilities.Devices, "cpu") {
		t.Fatalf("simulated engine capabilities = %+v, want CPU", capabilities)
	}
}

func TestVLLMProbeCapabilitiesConfirmsVersionFeatures(t *testing.T) {
	const apiKey = "capability-probe-key"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/version" || r.Method != http.MethodGet {
			t.Errorf("request = %s %s, want GET /version", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiKey {
			t.Errorf("authorization = %q, want bearer token", got)
		}
		_, _ = fmt.Fprint(w, `{"version":"0.6.0","ray_version":"2.9.0","devices":["cuda"],"backends":["cuda"],"quantizations":["awq"],"max_parallel_seqs":16,"requires_context_cap":true}`)
	}))
	defer server.Close()

	backend := NewConfiguredWithAPIKey("vllm", server.URL, apiKey).(*VLLM)
	capabilities, err := backend.ProbeCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(capabilities.Devices, "gpu") ||
		!containsString(capabilities.SupportsSharding, "vllm-mp") ||
		!containsString(capabilities.SupportsSharding, "vllm-ray") ||
		!capabilities.RequiresContextCap || capabilities.MaxParallelSeqs != 16 {
		t.Fatalf("probed capabilities = %+v", capabilities)
	}
	if got := backend.Capabilities(); !reflect.DeepEqual(got, capabilities) {
		t.Fatalf("cached capabilities = %+v, want %+v", got, capabilities)
	}
}

func TestVLLMProbeCapabilitiesAcceptsStandardVersionString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `"0.6.0"`)
	}))
	defer server.Close()

	capabilities, err := NewVLLM(server.URL).ProbeCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(capabilities.SupportsSharding, "vllm-mp") ||
		!capabilities.HomogeneousOnly {
		t.Fatalf("version-derived capabilities = %+v", capabilities)
	}
}

func TestVLLMRequestHonorsContextCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := NewVLLM(server.URL).GenerateWithOptions(ctx, "model", "prompt", GenerationOptions{})
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not reach the test server")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("generation error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("generation did not return after context cancellation")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
