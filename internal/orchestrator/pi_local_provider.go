package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/deepseek"
	"github.com/maistr0/maistr0/internal/piagent"
)

type localPiProvider struct {
	server       *Server
	defaultModel string
}

func (p *localPiProvider) Name() string { return "mAIstr0 local cluster" }

func (p *localPiProvider) Complete(ctx context.Context, request piagent.CompletionRequest, onDelta func(string)) (*piagent.Completion, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	model := strings.TrimSpace(request.Model)
	if model == "" {
		model = p.defaultModel
	}
	prompt, err := localPiPrompt(request)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(generateAPIRequest{
		Model: model, Prompt: prompt, Temperature: request.Temperature,
	})
	if err != nil {
		return nil, fmt.Errorf("encode local Pi request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/api/generate?stream=true", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create local Pi request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	writer := &localPiResponseWriter{header: make(http.Header), onDelta: onDelta}
	p.server.handleGenerate(writer, req)
	writer.finish()

	if writer.status >= http.StatusBadRequest {
		message := writer.body.String()
		var response generateAPIResponse
		if json.Unmarshal([]byte(message), &response) == nil && response.Error != "" {
			message = response.Error
		}
		if message == "" {
			message = http.StatusText(writer.status)
		}
		return nil, fmt.Errorf("local cluster model request failed (%d): %s", writer.status, message)
	}
	if writer.errMessage != "" {
		return nil, errors.New(writer.errMessage)
	}

	output := writer.output.String()
	if output == "" {
		var response generateAPIResponse
		if err := json.Unmarshal([]byte(writer.body.String()), &response); err != nil {
			return nil, fmt.Errorf("decode local cluster response: %w", err)
		}
		if response.Error != "" {
			return nil, errors.New(response.Error)
		}
		output = response.Output
		if output != "" && onDelta != nil {
			onDelta(output)
		}
	}
	if output == "" {
		return nil, errors.New("local cluster returned an empty completion")
	}

	parsed := deepseek.ParseResponse(output)
	completion := &piagent.Completion{Content: parsed.Content}
	for _, call := range parsed.ToolCalls {
		args, err := json.Marshal(call.Arguments)
		if err != nil {
			return nil, fmt.Errorf("encode local tool call %q: %w", call.Name, err)
		}
		completion.ToolCalls = append(completion.ToolCalls, piagent.ChatToolCall{
			ID: call.ID, Type: "function",
			Function: piagent.ChatToolFunction{Name: call.Name, Arguments: string(args)},
		})
	}
	return completion, nil
}

func localPiPrompt(request piagent.CompletionRequest) (string, error) {
	var prompt strings.Builder
	systemPrompt := ""
	if len(request.Messages) > 0 && request.Messages[0].Role == "system" {
		systemPrompt = request.Messages[0].Content
	}
	prompt.WriteString("<|im_start|>system\n")
	prompt.WriteString(systemPrompt)
	if len(request.Tools) > 0 {
		prompt.WriteString("\n\nYou can use the following tools:\n<tools>\n")
		tools, err := json.MarshalIndent(request.Tools, "", "  ")
		if err != nil {
			return "", fmt.Errorf("encode local Pi tools: %w", err)
		}
		prompt.Write(tools)
		prompt.WriteString("\n</tools>\n")
		prompt.WriteString(piToolGuidance(request.Tools))
		prompt.WriteString("Call only tools listed above. To call a tool, respond with exactly this format:\n")
		prompt.WriteString("<tool_call>\n{\"name\":\"tool_name\",\"arguments\":{}}\n</tool_call>\n")
		prompt.WriteString("Wait for the matching tool response before continuing. Do not invent tool results or claim an action succeeded unless its result confirms it. If no tool is needed, answer normally.\n")
	}
	prompt.WriteString("<|im_end|>\n")

	for _, message := range request.Messages {
		if message.Role == "system" {
			continue
		}
		switch message.Role {
		case "user":
			fmt.Fprintf(&prompt, "<|im_start|>user\n%s<|im_end|>\n", message.Content)
		case "assistant":
			prompt.WriteString("<|im_start|>assistant\n")
			if message.Content != "" {
				prompt.WriteString(message.Content + "\n")
			}
			for _, call := range message.ToolCalls {
				arguments := json.RawMessage(call.Function.Arguments)
				if !json.Valid(arguments) {
					arguments = json.RawMessage("{}")
				}
				callJSON, err := json.Marshal(map[string]any{
					"name": call.Function.Name, "arguments": arguments,
				})
				if err != nil {
					return "", fmt.Errorf("encode previous Pi tool call: %w", err)
				}
				fmt.Fprintf(&prompt, "<tool_call>\n%s\n</tool_call>\n", callJSON)
			}
			prompt.WriteString("<|im_end|>\n")
		case "tool":
			fmt.Fprintf(&prompt, "<tool_response>\nTool call ID: %s\n%s\n</tool_response>\n", message.ToolCallID, message.Content)
		}
	}
	prompt.WriteString("<|im_start|>assistant\n")
	return prompt.String(), nil
}

func piToolGuidance(tools []agent.ToolDef) string {
	available := make(map[string]bool, len(tools))
	for _, tool := range tools {
		available[tool.Name] = true
	}
	guidance := []struct {
		name string
		text string
	}{
		{"cluster_status", "Inspect cluster health, hardware, leadership, and active workloads."},
		{"list_node_models", "Discover which models are available on cluster nodes."},
		{"node_converse", "Continue a multi-turn conversation with a specific node model."},
		{"node_collaborate", "Iteratively collaborate with a worker model on a complex task."},
		{"cluster_converse", "Ask the least-loaded matching cluster model a question."},
		{"node_llm_query", "Send one direct query to a specific node model."},
		{"cluster_llm_query", "Send one query to a matching model anywhere in the cluster."},
		{"cluster_parallel_dispatch", "Run independent subtasks concurrently across cluster nodes."},
		{"eval_expression", "Evaluate arithmetic or a mathematical expression."},
		{"node_to_node_conversation", "Have two worker models debate or review a topic directly."},
		{"recall_memory", "Look up relevant past work before repeating a task."},
		{"memory_insights", "Review learned cluster performance and routing recommendations."},
		{"remember_fact", "Save a durable fact or preference for future sessions."},
		{"session_memory", "Read or update information in this conversation's memory."},
	}
	var prompt strings.Builder
	prompt.WriteString("Choose the appropriate tool when the request calls for cluster inspection, model work, math, web access, or memory; use multiple tools when needed and wait for each result before deciding the next step.\n")
	for _, item := range guidance {
		if available[item.name] {
			fmt.Fprintf(&prompt, "- `%s`: %s\n", item.name, item.text)
		}
	}
	return prompt.String()
}

type localPiResponseWriter struct {
	header     http.Header
	status     int
	pending    strings.Builder
	body       strings.Builder
	output     strings.Builder
	errMessage string
	onDelta    func(string)
}

func (w *localPiResponseWriter) Header() http.Header { return w.header }

func (w *localPiResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *localPiResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.pending.Write(data)
	for {
		pending := w.pending.String()
		newline := strings.IndexByte(pending, '\n')
		if newline < 0 {
			break
		}
		line := strings.TrimSuffix(pending[:newline], "\r")
		w.pending.Reset()
		w.pending.WriteString(pending[newline+1:])
		w.consumeLine(line)
	}
	return len(data), nil
}

func (w *localPiResponseWriter) Flush() {}

func (w *localPiResponseWriter) finish() {
	if w.pending.Len() > 0 {
		w.consumeLine(strings.TrimSuffix(w.pending.String(), "\r"))
		w.pending.Reset()
	}
	if w.status == 0 {
		w.status = http.StatusOK
	}
}

func (w *localPiResponseWriter) consumeLine(line string) {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "data:") {
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" || payload == "" {
			return
		}
		var event struct {
			Output string `json:"output"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			w.body.WriteString(line)
			return
		}
		if event.Error != "" {
			w.errMessage = event.Error
		}
		if event.Output != "" {
			w.output.WriteString(event.Output)
			if w.onDelta != nil {
				w.onDelta(event.Output)
			}
		}
		return
	}
	if line != "" {
		w.body.WriteString(line)
	}
}

var _ piagent.Provider = (*localPiProvider)(nil)
var _ http.ResponseWriter = (*localPiResponseWriter)(nil)
