package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Ollama is an Engine implementation backed by a local Ollama server
// (https://ollama.com), the most common cross-platform local LLM runner.
type Ollama struct {
	baseURL   string
	client    *http.Client // short timeout, used for health/list checks
	genClient *http.Client // long timeout, generation can take minutes on a cold model load
}

func NewOllama(baseURL string) *Ollama {
	return &Ollama{
		baseURL:   strings.TrimRight(baseURL, "/"),
		client:    &http.Client{Timeout: 3 * time.Second},
		genClient: &http.Client{Timeout: 10 * time.Minute},
	}
}

func (o *Ollama) Name() string { return "ollama" }
func (o *Ollama) URL() string  { return o.baseURL }

func (o *Ollama) Available() bool {
	resp, err := o.client.Get(o.baseURL + "/api/tags")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Pull downloads a model from the Ollama registry and waits for completion.
func (o *Ollama) Pull(model string) error {
	body, err := json.Marshal(map[string]any{"name": model, "stream": false})
	if err != nil {
		return err
	}
	resp, err := o.genClient.Post(o.baseURL+"/api/pull", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("ollama pull %q failed: %s: %s", model, resp.Status, strings.TrimSpace(string(data)))
	}
	return nil
}

type ollamaTagsResponse struct {
	Models []struct {
		Name string `json:"name"`
		Size int64  `json:"size"`
	} `json:"models"`
}

func (o *Ollama) ListModels() ([]Model, error) {
	resp, err := o.client.Get(o.baseURL + "/api/tags")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("ollama list models failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var tags ollamaTagsResponse
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, err
	}

	models := make([]Model, 0, len(tags.Models))
	for _, m := range tags.Models {
		models = append(models, Model{
			Name:   m.Name,
			Engine: o.Name(),
			Tags:   inferTags(m.Name),
			SizeGB: float64(m.Size) / (1024 * 1024 * 1024),
		})
	}
	return models, nil
}

type ollamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
	Raw    bool   `json:"raw,omitempty"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
}

func (o *Ollama) Generate(model, prompt string) (string, error) {
	// If the prompt contains explicit ChatML / template control tokens, pass raw=true
	// so Ollama feeds the exact tokens without re-wrapping in another template.
	raw := strings.Contains(prompt, "<|im_start|>") || strings.Contains(prompt, "<|start_header_id|>") || strings.Contains(prompt, "[INST]")
	body, err := json.Marshal(ollamaGenerateRequest{Model: model, Prompt: prompt, Stream: false, Raw: raw})
	if err != nil {
		return "", err
	}
	resp, err := o.genClient.Post(o.baseURL+"/api/generate", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("ollama generate failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var out ollamaGenerateResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Response, nil
}

type ollamaChatRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type ollamaChatResponse struct {
	Message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
}

func (o *Ollama) Chat(model string, messages []ChatMessage) (string, error) {
	body, err := json.Marshal(ollamaChatRequest{Model: model, Messages: messages, Stream: false})
	if err != nil {
		return "", err
	}
	resp, err := o.genClient.Post(o.baseURL+"/api/chat", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("ollama chat failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var out ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Message.Content, nil
}

// inferTags does a best-effort classification of a model's strengths based
// on well-known naming conventions used by Ollama's model library.
func inferTags(name string) []string {
	n := strings.ToLower(name)
	tags := []string{"chat"}
	switch {
	case strings.Contains(n, "code") || strings.Contains(n, "coder"):
		tags = append(tags, "code")
	case strings.Contains(n, "vision") || strings.Contains(n, "llava"):
		tags = append(tags, "vision")
	case strings.Contains(n, "math"):
		tags = append(tags, "math")
	}
	if strings.Contains(n, "1b") || strings.Contains(n, "2b") || strings.Contains(n, "3b") || strings.Contains(n, "mini") {
		tags = append(tags, "fast", "small")
	} else if strings.Contains(n, "70b") || strings.Contains(n, "72b") || strings.Contains(n, "large") {
		tags = append(tags, "reasoning", "large")
	} else {
		tags = append(tags, "general")
	}
	return tags
}
