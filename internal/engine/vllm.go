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
	"time"
)

// VLLM contains the shared OpenAI-compatible client used by vLLM, llama.cpp,
// and other configured servers. engineName preserves each backend's inventory
// identity while sharing the same HTTP protocol.
type VLLM struct {
	baseURL     string
	engineName  string
	client      *http.Client
	probeClient *http.Client
}

func NewVLLM(baseURL string) *VLLM {
	return newOpenAICompatibleEngine(baseURL, "vllm")
}

func newOpenAICompatibleEngine(baseURL, engineName string) *VLLM {
	return &VLLM{
		baseURL:     strings.TrimRight(baseURL, "/"),
		engineName:  engineName,
		client:      &http.Client{Timeout: 10 * time.Minute},
		probeClient: &http.Client{Timeout: 5 * time.Second},
	}
}

func (v *VLLM) Name() string { return v.engineName }
func (v *VLLM) URL() string  { return v.baseURL }

func (v *VLLM) ProbePipelineStageCapabilities(ctx context.Context) (*PipelineStageCapabilities, error) {
	stageEngine, err := NewHTTPPipelineStageEngine(v.engineName, v.baseURL, v.client)
	if err != nil {
		return nil, err
	}
	return stageEngine.ProbePipelineStageCapabilities(ctx)
}

func (v *VLLM) Available() bool {
	resp, err := v.probeClient.Get(v.baseURL + "/v1/models")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode < 300
}

func (v *VLLM) ListModels() ([]Model, error) {
	resp, err := v.client.Get(v.baseURL + "/v1/models")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("%s list models failed: %s: %s", v.Name(), resp.Status, strings.TrimSpace(string(body)))
	}
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	models := make([]Model, 0, len(result.Data))
	for _, item := range result.Data {
		models = append(models, Model{Name: item.ID, Engine: v.Name(), Tags: inferTags(item.ID)})
	}
	return models, nil
}

func (v *VLLM) Generate(model, prompt string) (string, error) {
	return v.GenerateWithOptions(context.Background(), model, prompt, GenerationOptions{})
}

func (v *VLLM) GenerateWithOptions(ctx context.Context, model, prompt string, options GenerationOptions) (string, error) {
	payload := map[string]any{
		"model":    model,
		"messages": []ChatMessage{{Role: "user", Content: prompt}},
	}
	if options.Temperature != 0 {
		payload["temperature"] = options.Temperature
	}
	if options.MaxTokens > 0 {
		payload["max_tokens"] = options.MaxTokens
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("%s chat failed: %s: %s", v.Name(), resp.Status, strings.TrimSpace(string(data)))
	}
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

func (v *VLLM) Chat(model string, messages []ChatMessage) (string, error) {
	return v.ChatWithContext(context.Background(), model, messages)
}

func (v *VLLM) ChatWithContext(ctx context.Context, model string, messages []ChatMessage) (string, error) {
	body, err := json.Marshal(map[string]any{"model": model, "messages": messages, "stream": false})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("%s chat failed: %s: %s", v.Name(), resp.Status, strings.TrimSpace(string(data)))
	}
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

func (v *VLLM) GenerateStream(ctx context.Context, model, prompt string, options GenerationOptions, onDelta func(string) error) error {
	payload := map[string]any{
		"model":    model,
		"messages": []ChatMessage{{Role: "user", Content: prompt}},
		"stream":   true,
	}
	if options.Temperature != 0 {
		payload["temperature"] = options.Temperature
	}
	if options.MaxTokens > 0 {
		payload["max_tokens"] = options.MaxTokens
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s chat stream failed: %s: %s", v.Name(), resp.Status, strings.TrimSpace(string(data)))
	}
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
