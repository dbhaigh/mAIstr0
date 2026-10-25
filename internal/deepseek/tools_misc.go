package deepseek

import (
	"context"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/scheduler"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
)

type EvalExpressionTool struct{}

func (t *EvalExpressionTool) Def() ToolDef {
	return toolDefinition[evalExpressionArgs](
		"eval_expression",
		"Perform direct, deterministic arithmetic calculations, math functions (sqrt, pow, sin, cos, round, abs), or data transformations safely.",
	)
}

func (t *EvalExpressionTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	expr, _ := args["expression"].(string)
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, "", "", "", errors.New("expression is required")
	}

	result, err := evaluateMathExpression(expr)
	if err != nil {
		return nil, "", "", "", err
	}
	return map[string]any{
		"expression": expr,
		"result":     result,
	}, "", "", "builtin-math", nil
}

func evaluateMathExpression(expr string) (any, error) {
	clean := strings.ToLower(strings.TrimSpace(expr))

	// Handle functional helper patterns
	if strings.HasPrefix(clean, "sqrt(") && strings.HasSuffix(clean, ")") {
		inner := strings.TrimSuffix(strings.TrimPrefix(clean, "sqrt("), ")")
		val, err := strconv.ParseFloat(strings.TrimSpace(inner), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid sqrt argument: %v", err)
		}
		return math.Sqrt(val), nil
	}

	if strings.HasPrefix(clean, "sum(") && strings.HasSuffix(clean, ")") {
		inner := strings.TrimSuffix(strings.TrimPrefix(clean, "sum("), ")")
		parts := strings.Split(inner, ",")
		var total float64
		for _, p := range parts {
			val, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid sum term %q: %v", p, err)
			}
			total += val
		}
		return total, nil
	}

	if strings.HasPrefix(clean, "mean(") && strings.HasSuffix(clean, ")") || strings.HasPrefix(clean, "avg(") && strings.HasSuffix(clean, ")") {
		prefix := "mean("
		if strings.HasPrefix(clean, "avg(") {
			prefix = "avg("
		}
		inner := strings.TrimSuffix(strings.TrimPrefix(clean, prefix), ")")
		parts := strings.Split(inner, ",")
		if len(parts) == 0 {
			return 0, nil
		}
		var total float64
		for _, p := range parts {
			val, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid term %q: %v", p, err)
			}
			total += val
		}
		return total / float64(len(parts)), nil
	}

	// Simple 2-operand or single operand arithmetic parser
	// Tokens: +, -, *, /, %, ^
	operators := []string{"+", "-", "*", "/", "%", "^"}
	for _, op := range operators {
		idx := strings.Index(clean, op)
		if idx > 0 && idx < len(clean)-1 {
			leftStr := strings.TrimSpace(clean[:idx])
			rightStr := strings.TrimSpace(clean[idx+1:])
			left, err1 := strconv.ParseFloat(leftStr, 64)
			right, err2 := strconv.ParseFloat(rightStr, 64)
			if err1 == nil && err2 == nil {
				switch op {
				case "+":
					return left + right, nil
				case "-":
					return left - right, nil
				case "*":
					return left * right, nil
				case "/":
					if right == 0 {
						return nil, errors.New("division by zero")
					}
					return left / right, nil
				case "%":
					return math.Mod(left, right), nil
				case "^":
					return math.Pow(left, right), nil
				}
			}
		}
	}

	// Try direct numeric parse
	if val, err := strconv.ParseFloat(clean, 64); err == nil {
		return val, nil
	}

	return nil, fmt.Errorf("could not parse expression %q", expr)
}

// --- Tool: fetch_web ---

type FetchWebTool struct{}

func (t *FetchWebTool) Def() ToolDef {
	return toolDefinition[fetchWebArgs](
		"fetch_web",
		"Perform an HTTP GET request to fetch text, documentation, or JSON data from a URL.",
	)
}

func (t *FetchWebTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	urlStr, _ := args["url"].(string)
	if urlStr == "" {
		return nil, "", "", "", errors.New("url is required")
	}

	client := httpx.NewClient(httpx.ClientOptions{Timeout: httpx.ControlTimeout})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, "", "", "", err
	}
	req.Header.Set("User-Agent", "mAIstr0-DeepSeek/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", "", "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	if err != nil {
		return nil, "", "", "", err
	}

	return map[string]any{
		"url":         urlStr,
		"status_code": resp.StatusCode,
		"content":     string(body),
	}, "", "", "builtin-http", nil
}

// --- Tool: session_memory ---

type SessionMemoryTool struct{}

func (t *SessionMemoryTool) Def() ToolDef {
	return toolDefinition[sessionMemoryArgs](
		"session_memory",
		"Persist or recall working notes, findings, and context across turns in the interactive session scratchpad.",
	)
}

func (t *SessionMemoryTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	action, _ := args["action"].(string)
	key, _ := args["key"].(string)
	val, _ := args["value"].(string)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Memory == nil {
		s.Memory = make(map[string]string)
	}

	switch action {
	case "set":
		if key == "" {
			return nil, "", "", "", errors.New("key is required for set")
		}
		s.Memory[key] = val
		return map[string]string{"status": "saved", "key": key}, "", "", "memory", nil
	case "get":
		if key == "" {
			return nil, "", "", "", errors.New("key is required for get")
		}
		v, ok := s.Memory[key]
		if !ok {
			return map[string]any{"found": false, "key": key}, "", "", "memory", nil
		}
		return map[string]any{"found": true, "key": key, "value": v}, "", "", "memory", nil
	case "list":
		return s.Memory, "", "", "memory", nil
	case "delete":
		delete(s.Memory, key)
		return map[string]string{"status": "deleted", "key": key}, "", "", "memory", nil
	default:
		return nil, "", "", "", fmt.Errorf("unknown memory action: %s", action)
	}
}

// --- Tool: create_subagent ---

type CreateSubAgentTool struct{}

func (t *CreateSubAgentTool) Def() ToolDef {
	return toolDefinition[createSubAgentArgs](
		"create_subagent",
		"Create a specialized virtual worker (sub-agent) at runtime from a 4-tuple specification: instruction/persona, required tools, required/preferred model capabilities, and context. Dynamically selects the best compute node and model, registers the virtual worker profile for scheduling, and optionally executes an initial task.",
	)
}

func (t *CreateSubAgentTool) Execute(ctx context.Context, h *Harness, s *Session, args map[string]any) (any, string, string, string, error) {
	name, _ := args["name"].(string)
	instructions, _ := args["instructions"].(string)
	role, _ := args["role"].(string)
	contextStr, _ := args["context"].(string)
	taskPrompt, _ := args["task"].(string)

	var reqTools []string
	if raw, ok := args["required_tools"].([]any); ok {
		for _, v := range raw {
			if str, ok := v.(string); ok {
				reqTools = append(reqTools, str)
			}
		}
	}
	var reqCaps []string
	if raw, ok := args["required_capabilities"].([]any); ok {
		for _, v := range raw {
			if str, ok := v.(string); ok {
				reqCaps = append(reqCaps, str)
			}
		}
	}
	var prefCaps []string
	if raw, ok := args["preferred_capabilities"].([]any); ok {
		for _, v := range raw {
			if str, ok := v.(string); ok {
				prefCaps = append(prefCaps, str)
			}
		}
	}

	nodes := h.registry.Active()
	factory := scheduler.NewSubAgentFactory()
	worker, err := factory.Create(scheduler.SubAgentSpec{
		Name:                  name,
		Role:                  role,
		Instruction:           instructions,
		RequiredTools:         reqTools,
		RequiredCapabilities:  reqCaps,
		PreferredCapabilities: prefCaps,
		Context:               contextStr,
	}, nodes, nil)
	if err != nil {
		return nil, "", "", "", fmt.Errorf("create sub-agent: %w", err)
	}

	result := map[string]any{
		"id":             worker.ID,
		"name":           worker.Spec.Name,
		"role":           worker.Spec.Role,
		"node_id":        worker.NodeID,
		"node_address":   worker.Address,
		"model":          worker.Model,
		"score":          worker.Score,
		"required_tools": worker.Spec.RequiredTools,
		"instruction":    worker.Spec.Instruction,
	}

	if strings.TrimSpace(taskPrompt) != "" {
		formattedPrompt := worker.FormatPrompt(taskPrompt, "")
		output, err := h.dispatchToNode(ctx, worker.Address, worker.Model, formattedPrompt)
		if err != nil {
			result["execution_error"] = err.Error()
		} else {
			result["output"] = output
		}
	}

	return result, worker.NodeID, worker.Address, worker.Model, nil
}
