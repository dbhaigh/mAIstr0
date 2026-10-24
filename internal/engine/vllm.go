package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// VLLM contains the shared OpenAI-compatible client used by vLLM, llama.cpp,
// and other configured servers. engineName preserves each backend's inventory
// identity while sharing the same HTTP protocol.
type VLLM struct {
	baseURL           string
	engineName        string
	apiKey            string
	client            *http.Client
	probeClient       *http.Client
	capabilitiesMu    sync.RWMutex
	capabilities      Capabilities
	capabilitiesKnown bool
}

func NewVLLM(baseURL string) *VLLM {
	return newOpenAICompatibleEngine(baseURL, "vllm")
}

func newOpenAICompatibleEngine(baseURL, engineName string) *VLLM {
	return newOpenAICompatibleEngineWithAPIKey(baseURL, engineName, "")
}

func newOpenAICompatibleEngineWithAPIKey(baseURL, engineName, apiKey string) *VLLM {
	return &VLLM{
		baseURL:     strings.TrimRight(baseURL, "/"),
		engineName:  engineName,
		apiKey:      apiKey,
		client:      &http.Client{Timeout: 10 * time.Minute},
		probeClient: &http.Client{Timeout: 5 * time.Second},
	}
}

func (v *VLLM) Name() string { return v.engineName }
func (v *VLLM) URL() string  { return v.baseURL }

func (v *VLLM) Capabilities() Capabilities {
	v.capabilitiesMu.RLock()
	if v.capabilitiesKnown {
		capabilities := CloneCapabilities(v.capabilities)
		v.capabilitiesMu.RUnlock()
		return capabilities
	}
	v.capabilitiesMu.RUnlock()
	return v.defaultCapabilities()
}

func (v *VLLM) defaultCapabilities() Capabilities {
	if v.Name() == "mlx" {
		return Capabilities{OS: []string{"darwin"}}
	}
	if template, ok := ManagedEngineTemplateFor(v.Name()); ok {
		return template.Capabilities
	}
	return Capabilities{}
}

func (v *VLLM) ProbeCapabilities(ctx context.Context) (Capabilities, error) {
	capabilities := v.Capabilities()
	if v.Name() != "vllm" {
		return capabilities, nil
	}
	req, err := v.newRequest(ctx, http.MethodGet, v.baseURL+"/version", nil, "")
	if err != nil {
		return capabilities, fmt.Errorf("%s build version request: %w", v.Name(), err)
	}
	resp, err := v.probeClient.Do(req)
	if err != nil {
		return capabilities, fmt.Errorf("%s get version: %w", v.Name(), err)
	}
	if resp.StatusCode >= 300 {
		return capabilities, v.responseError("get version", resp)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return capabilities, fmt.Errorf("%s read version: %w", v.Name(), err)
	}
	var version struct {
		Version            string   `json:"version"`
		RayVersion         string   `json:"ray_version"`
		Devices            []string `json:"devices"`
		Backends           []string `json:"backends"`
		Quantizations      []string `json:"quantizations"`
		MaxParallelSeqs    int      `json:"max_parallel_seqs"`
		MaxContextLength   int      `json:"max_context_length"`
		RequiresContextCap bool     `json:"requires_context_cap"`
		SupportsSharding   []string `json:"supports_sharding"`
		HomogeneousOnly    bool     `json:"homogeneous_only"`
		OperatingSystems   []string `json:"os"`
	}
	if err := json.Unmarshal(data, &version); err != nil {
		var versionString string
		if stringErr := json.Unmarshal(data, &versionString); stringErr != nil {
			return capabilities, fmt.Errorf("%s decode version: %w", v.Name(), err)
		}
		version.Version = versionString
	}
	if strings.TrimSpace(version.Version) == "" {
		return capabilities, fmt.Errorf("%s version response did not include a version", v.Name())
	}
	capabilities = MergeCapabilities(capabilities, Capabilities{
		Devices: version.Devices, Backends: version.Backends,
		Quantizations: version.Quantizations, MaxParallelSeqs: version.MaxParallelSeqs,
		MaxContextLength:   version.MaxContextLength,
		RequiresContextCap: version.RequiresContextCap,
		SupportsSharding:   append(version.SupportsSharding, "vllm-mp"),
		HomogeneousOnly:    true,
		OS:                 version.OperatingSystems,
	})
	if version.RayVersion != "" {
		capabilities.SupportsSharding = uniqueCapabilityValues(capabilities.SupportsSharding, []string{"vllm-ray"})
	}
	v.capabilitiesMu.Lock()
	v.capabilities = CloneCapabilities(capabilities)
	v.capabilitiesKnown = true
	v.capabilitiesMu.Unlock()
	return capabilities, nil
}

func (v *VLLM) newRequest(ctx context.Context, method, url string, body io.Reader, accept string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if v.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+v.apiKey)
	}
	return req, nil
}

func (v *VLLM) doRequest(req *http.Request, operation string) (*http.Response, error) {
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, v.responseError(operation, resp)
	}
	return resp, nil
}

func (v *VLLM) responseError(operation string, resp *http.Response) error {
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	body := strings.TrimSpace(string(data))
	if v.apiKey != "" {
		body = strings.ReplaceAll(body, v.apiKey, "[REDACTED]")
	}
	return fmt.Errorf("%s %s failed: %s: %s", v.Name(), operation, resp.Status, body)
}

func (v *VLLM) ProbePipelineStageCapabilities(ctx context.Context) (*PipelineStageCapabilities, error) {
	stageEngine, err := NewHTTPPipelineStageEngine(v.engineName, v.baseURL, v.client)
	if err != nil {
		return nil, err
	}
	return stageEngine.ProbePipelineStageCapabilities(ctx)
}

func (v *VLLM) Available() bool {
	req, err := v.newRequest(context.Background(), http.MethodGet, v.baseURL+"/v1/models", nil, "")
	if err != nil {
		return false
	}
	resp, err := v.probeClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode < 300
}

func (v *VLLM) ListModels() ([]Model, error) {
	req, err := v.newRequest(context.Background(), http.MethodGet, v.baseURL+"/v1/models", nil, "")
	if err != nil {
		return nil, err
	}
	resp, err := v.doRequest(req, "list models")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result struct {
		Data []struct {
			ID             string  `json:"id"`
			MaxModelLen    int     `json:"max_model_len"`
			SizeGB         float64 `json:"size_gb"`
			SizeBytes      int64   `json:"size_bytes"`
			ModelSizeBytes int64   `json:"model_size_bytes"`
			Device         string  `json:"device"`
			Quantization   string  `json:"quantization"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	models := make([]Model, 0, len(result.Data))
	for _, item := range result.Data {
		sizeGB := item.SizeGB
		if sizeGB == 0 {
			sizeBytes := item.SizeBytes
			if sizeBytes == 0 {
				sizeBytes = item.ModelSizeBytes
			}
			sizeGB = float64(sizeBytes) / (1024 * 1024 * 1024)
		}
		models = append(models, Model{
			Name: item.ID, Engine: v.Name(), Tags: inferTags(item.ID),
			SizeGB: sizeGB, ContextLength: item.MaxModelLen, Device: NormalizeDeviceKind(item.Device),
			Quantization: strings.ToLower(strings.TrimSpace(item.Quantization)),
		})
	}
	return models, nil
}

func (v *VLLM) Generate(model, prompt string) (string, error) {
	return v.GenerateWithOptions(context.Background(), model, prompt, GenerationOptions{})
}

func (v *VLLM) GenerateWithOptions(ctx context.Context, model, prompt string, options GenerationOptions) (string, error) {
	return v.chat(ctx, model, []ChatMessage{{Role: "user", Content: prompt}}, options)
}

func (v *VLLM) Chat(model string, messages []ChatMessage) (string, error) {
	return v.ChatWithContext(context.Background(), model, messages)
}

func (v *VLLM) ChatWithContext(ctx context.Context, model string, messages []ChatMessage) (string, error) {
	return v.ChatWithOptions(ctx, model, messages, GenerationOptions{})
}

func (v *VLLM) ChatWithOptions(ctx context.Context, model string, messages []ChatMessage, options GenerationOptions) (string, error) {
	return v.chat(ctx, model, messages, options)
}

func (v *VLLM) chat(ctx context.Context, model string, messages []ChatMessage, options GenerationOptions) (string, error) {
	resp, err := v.chatRequest(ctx, model, messages, options, false)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		Choices []struct {
			Message ChatMessage `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("%s returned no choices", v.Name())
	}
	return result.Choices[0].Message.Content, nil
}

func (v *VLLM) chatRequest(ctx context.Context, model string, messages []ChatMessage, options GenerationOptions, stream bool) (*http.Response, error) {
	payload := map[string]any{
		"model":    model,
		"messages": messages,
		"stream":   stream,
	}
	if options.Temperature != 0 {
		payload["temperature"] = options.Temperature
	}
	if options.MaxTokens > 0 {
		payload["max_tokens"] = options.MaxTokens
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	accept := ""
	operation := "chat"
	if stream {
		accept = "text/event-stream"
		operation = "chat stream"
	}
	req, err := v.newRequest(ctx, http.MethodPost, v.baseURL+"/v1/chat/completions", bytes.NewReader(body), accept)
	if err != nil {
		return nil, err
	}
	return v.doRequest(req, operation)
}

func (v *VLLM) GenerateStream(ctx context.Context, model, prompt string, options GenerationOptions, onDelta func(string) error) error {
	resp, err := v.chatRequest(ctx, model, []ChatMessage{{Role: "user", Content: prompt}}, options, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			return nil
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return err
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				if err := onDelta(choice.Delta.Content); err != nil {
					return err
				}
			}
		}
	}
	return scanner.Err()
}
