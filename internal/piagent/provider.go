package piagent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
)

type Provider interface {
	Name() string
	Complete(ctx context.Context, request CompletionRequest, onDelta func(string)) (*Completion, error)
}

type CompletionRequest struct {
	Model       string
	Temperature float64
	Messages    []ChatMessage
	Tools       []agent.ToolDef
}

type ChatMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	ToolCalls  []ChatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type ChatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function ChatToolFunction `json:"function"`
}

type ChatToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type Completion struct {
	Content   string
	ToolCalls []ChatToolCall
}

type OpenAICompatibleConfig struct {
	BaseURL string
	APIKey  string
	Model   string
}

type OpenAICompatibleProvider struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

func NewOpenAICompatibleProvider(cfg OpenAICompatibleConfig) *OpenAICompatibleProvider {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = "gpt-4.1-mini"
	}
	return &OpenAICompatibleProvider{
		baseURL: baseURL,
		apiKey:  cfg.APIKey,
		model:   model,
		client:  &http.Client{Timeout: 10 * time.Minute},
	}
}

func (p *OpenAICompatibleProvider) Name() string { return "openai-compatible" }

func (p *OpenAICompatibleProvider) Model() string { return p.model }

func (p *OpenAICompatibleProvider) Complete(ctx context.Context, request CompletionRequest, onDelta func(string)) (*Completion, error) {
	model := strings.TrimSpace(request.Model)
	if model == "" {
		model = p.model
	}
	payload := map[string]any{
		"model":    model,
		"messages": request.Messages,
		"stream":   true,
	}
	if request.Temperature > 0 {
		payload["temperature"] = request.Temperature
	}
	if len(request.Tools) > 0 {
		tools := make([]map[string]any, 0, len(request.Tools))
		for _, def := range request.Tools {
			tools = append(tools, map[string]any{
				"type": "function",
				"function": map[string]any{
					"name": def.Name, "description": def.Description, "parameters": def.Parameters,
				},
			})
		}
		payload["tools"] = tools
		payload["tool_choice"] = "auto"
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode model request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create model request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call %s model provider: %w", p.Name(), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return nil, fmt.Errorf("model provider returned %s (read error: %w)", resp.Status, readErr)
		}
		return nil, fmt.Errorf("model provider returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	return readCompletionStream(resp.Body, onDelta)
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
	} `json:"choices"`
}

func readCompletionStream(body io.Reader, onDelta func(string)) (*Completion, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	result := &Completion{}
	tools := make(map[int]*ChatToolCall)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			break
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return nil, fmt.Errorf("decode model stream chunk: %w", err)
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				result.Content += choice.Delta.Content
				if onDelta != nil {
					onDelta(choice.Delta.Content)
				}
			}
			for _, delta := range choice.Delta.ToolCalls {
				call := tools[delta.Index]
				if call == nil {
					call = &ChatToolCall{Type: "function"}
					tools[delta.Index] = call
				}
				if delta.ID != "" {
					call.ID = delta.ID
				}
				if delta.Type != "" {
					call.Type = delta.Type
				}
				call.Function.Name += delta.Function.Name
				call.Function.Arguments += delta.Function.Arguments
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read model stream: %w", err)
	}
	if len(tools) > 0 {
		for i := 0; i < len(tools); i++ {
			call, ok := tools[i]
			if !ok {
				return nil, errors.New("model stream contains non-contiguous tool call indexes")
			}
			result.ToolCalls = append(result.ToolCalls, *call)
		}
	}
	return result, nil
}
