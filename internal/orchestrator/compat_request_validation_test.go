package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTranslatedCompatibilityEndpointsRejectUnsupportedFields(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		body  string
		field string
	}{
		{
			name:  "OpenAI tools",
			path:  "/v1/chat/completions",
			body:  `{"model":"model","messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"run"}}]}`,
			field: "tools",
		},
		{
			name:  "OpenAI tool call result shape",
			path:  "/v1/chat/completions",
			body:  `{"model":"model","messages":[{"role":"assistant","content":"","tool_calls":[{"id":"call-1","type":"function","function":{"name":"run","arguments":"{}"}}]}]}`,
			field: "tool_calls",
		},
		{
			name:  "OpenAI structured output",
			path:  "/v1/chat/completions",
			body:  `{"model":"model","messages":[{"role":"user","content":"hello"}],"response_format":{"type":"json_object"}}`,
			field: "response_format",
		},
		{
			name:  "OpenAI unsupported role",
			path:  "/v1/chat/completions",
			body:  `{"model":"model","messages":[{"role":"developer","content":"hello"}]}`,
			field: "unsupported message role",
		},
		{
			name:  "Anthropic tools",
			path:  "/v1/messages",
			body:  `{"model":"model","max_tokens":20,"messages":[{"role":"user","content":"hello"}],"tools":[{"name":"run","input_schema":{"type":"object"}}]}`,
			field: "tools",
		},
		{
			name:  "Anthropic unsupported content block fields",
			path:  "/v1/messages",
			body:  `{"model":"model","max_tokens":20,"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]}`,
			field: "cache_control",
		},
		{
			name:  "Ollama structured output",
			path:  "/api/generate",
			body:  `{"model":"model","prompt":"hello","stream":false,"format":"json"}`,
			field: "format",
		},
		{
			name:  "generic generate tools",
			path:  "/api/generate",
			body:  `{"model":"model","prompt":"hello","tools":[{"type":"function"}]}`,
			field: "tools",
		},
	}

	srv := newTestServer(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			srv.Mux().ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("request returned %d, want 400: %s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), test.field) {
				t.Fatalf("response does not identify %s as unsupported: %s", test.field, response.Body.String())
			}
		})
	}
}
