package deepseek

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var (
	toolCallTagRegex  = regexp.MustCompile(`(?s)<tool_call>\s*(.*?)\s*</tool_call>`)
	thinkTagRegex     = regexp.MustCompile(`(?s)<think>\s*(.*?)\s*</think>`)
	markdownCodeRegex = regexp.MustCompile("(?s)```(?:tool_call|json)?\\s*(\\{.*?\\})\\s*```")
)

// ParsedResponse holds the decomposed segments of a model turn.
type ParsedResponse struct {
	Thought   string
	Content   string
	ToolCalls []ToolCall
}

// ParseDeepSeekResponse extracts thoughts, direct content, and tool calls
// from DeepSeek responses and JSON fallbacks.
func ParseDeepSeekResponse(raw string) ParsedResponse {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ParsedResponse{}
	}

	var toolCalls []ToolCall
	thoughtParts := []string{}

	// 1. Check for standard tool-call XML tags: <tool_call>...</tool_call>
	matches := toolCallTagRegex.FindAllStringSubmatchIndex(raw, -1)
	if len(matches) > 0 {
		lastIdx := 0
		for i, loc := range matches {
			prefix := strings.TrimSpace(raw[lastIdx:loc[0]])
			if prefix != "" {
				thoughtParts = append(thoughtParts, prefix)
			}
			callJSON := raw[loc[2]:loc[3]]
			tc, err := parseSingleToolCall(callJSON, fmt.Sprintf("call_%d", i+1))
			if err == nil {
				toolCalls = append(toolCalls, tc)
			}
			lastIdx = loc[1]
		}
		postfix := strings.TrimSpace(raw[lastIdx:])
		if postfix != "" {
			thoughtParts = append(thoughtParts, postfix)
		}

		return ParsedResponse{
			Thought:   strings.Join(thoughtParts, "\n\n"),
			ToolCalls: toolCalls,
		}
	}

	// 2. Check for markdown code blocks containing a tool call JSON object
	codeMatches := markdownCodeRegex.FindAllStringSubmatch(raw, -1)
	for i, m := range codeMatches {
		if len(m) >= 2 {
			tc, err := parseSingleToolCall(m[1], fmt.Sprintf("call_%d", i+1))
			if err == nil && tc.Name != "" {
				toolCalls = append(toolCalls, tc)
			}
		}
	}
	if len(toolCalls) > 0 {
		cleaned := markdownCodeRegex.ReplaceAllString(raw, "")
		return ParsedResponse{
			Thought:   strings.TrimSpace(cleaned),
			ToolCalls: toolCalls,
		}
	}

	// 3. Check if the entire raw string is a bare JSON tool call object
	if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
		tc, err := parseSingleToolCall(raw, "call_1")
		if err == nil && tc.Name != "" {
			return ParsedResponse{
				ToolCalls: []ToolCall{tc},
			}
		}
	}

	// 4. No tool calls detected; treated as direct conversation response
	return ParsedResponse{
		Content: raw,
	}
}

// ParseResponse applies the response conventions for the selected harness.
func ParseResponse(raw string) ParsedResponse {
	parsed := ParseDeepSeekResponse(raw)
	if matches := thinkTagRegex.FindStringSubmatch(raw); len(matches) == 2 {
		parsed.Thought = strings.TrimSpace(matches[1])
		if parsed.Content == strings.TrimSpace(raw) {
			parsed.Content = strings.TrimSpace(thinkTagRegex.ReplaceAllString(raw, ""))
		}
	}
	return parsed
}

func parseSingleToolCall(jsonStr string, fallbackID string) (ToolCall, error) {
	jsonStr = strings.TrimSpace(jsonStr)
	var rawMap map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &rawMap); err != nil {
		return ToolCall{}, err
	}

	name := ""
	if n, ok := rawMap["name"].(string); ok {
		name = n
	} else if t, ok := rawMap["tool"].(string); ok {
		name = t
	} else if f, ok := rawMap["function"].(string); ok {
		name = f
	}

	if name == "" {
		return ToolCall{}, fmt.Errorf("no tool name found in JSON: %s", jsonStr)
	}

	args := make(map[string]any)
	if a, ok := rawMap["arguments"].(map[string]any); ok {
		args = a
	} else if a, ok := rawMap["args"].(map[string]any); ok {
		args = a
	} else if a, ok := rawMap["parameters"].(map[string]any); ok {
		args = a
	} else if aStr, ok := rawMap["arguments"].(string); ok {
		_ = json.Unmarshal([]byte(aStr), &args)
	}

	callID := fallbackID
	if id, ok := rawMap["id"].(string); ok && id != "" {
		callID = id
	}

	return ToolCall{
		ID:        callID,
		Name:      name,
		Arguments: args,
		RawArgs:   jsonStr,
	}, nil
}
