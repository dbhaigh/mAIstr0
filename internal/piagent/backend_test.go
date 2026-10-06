package piagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/hub"
)

type fakeProvider struct {
	responses []*Completion
	requests  []CompletionRequest
}

func (p *fakeProvider) Name() string { return "test-provider" }

func (p *fakeProvider) Complete(_ context.Context, request CompletionRequest, onDelta func(string)) (*Completion, error) {
	p.requests = append(p.requests, request)
	response := p.responses[0]
	p.responses = p.responses[1:]
	if onDelta != nil && response.Content != "" {
		onDelta(response.Content)
	}
	return response, nil
}

func testToolHost() *agent.DeepSeek {
	return agent.NewDeepSeek(cluster.NewRegistry(), nil, nil, hub.New())
}

func TestNativeAgentLoopExecutesToolsAndKeepsHistory(t *testing.T) {
	provider := &fakeProvider{responses: []*Completion{
		{ToolCalls: []ChatToolCall{{
			ID: "call-1", Type: "function",
			Function: ChatToolFunction{Name: "cluster_status", Arguments: `{}`},
		}}},
		{Content: "There are no active nodes."},
	}}
	host := testToolHost()
	backend := New(provider, host)
	session, err := backend.CreateSession("test", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan agent.StreamEvent, 16)
	final, err := backend.SendMessage(context.Background(), session.ID, "Check the cluster.", events)
	if err != nil {
		t.Fatal(err)
	}
	if final.Content != "There are no active nodes." {
		t.Fatalf("final response = %q", final.Content)
	}
	if len(provider.requests) != 2 {
		t.Fatalf("provider calls = %d, want 2", len(provider.requests))
	}
	if len(provider.requests[0].Tools) == 0 || provider.requests[0].Tools[0].Name == "" {
		t.Fatal("agent did not pass its tool catalog to the provider")
	}
	toolReplyFound := false
	for _, message := range provider.requests[1].Messages {
		if message.Role == "tool" && message.ToolCallID == "call-1" && strings.Contains(message.Content, "total_nodes") {
			toolReplyFound = true
		}
	}
	if !toolReplyFound {
		t.Fatalf("tool result missing from follow-up request: %#v", provider.requests[1].Messages)
	}
	stored, ok := backend.GetSession(session.ID)
	if !ok || len(stored.Messages) != 4 {
		t.Fatalf("stored messages = %#v", stored)
	}
	if stored.Messages[1].Role != agent.RoleAssistant || len(stored.Messages[1].ToolCalls) != 1 ||
		stored.Messages[2].Role != agent.RoleTool || stored.Messages[3].Content != final.Content {
		t.Fatalf("unexpected conversation history: %#v", stored.Messages)
	}
	foundToolEvent, foundFinalEvent, foundDoneEvent := false, false, false
	close(events)
	for event := range events {
		foundToolEvent = foundToolEvent || event.Type == agent.EventToolResponse
		foundFinalEvent = foundFinalEvent || event.Type == agent.EventFinalMessage
		foundDoneEvent = foundDoneEvent || event.Type == agent.EventDone
	}
	if !foundToolEvent || !foundFinalEvent || !foundDoneEvent {
		t.Fatalf("missing expected stream events: tool=%t final=%t done=%t", foundToolEvent, foundFinalEvent, foundDoneEvent)
	}
	if !backend.DeleteSession(session.ID) || len(host.ListSessions()) != 0 {
		t.Fatal("deleting the Pi session did not clean up its tool-host session")
	}
}

func TestOpenAICompatibleProviderStreamsTextAndToolCalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("authorization = %q", got)
		}
		var request struct {
			Model    string        `json:"model"`
			Stream   bool          `json:"stream"`
			Tools    []any         `json:"tools"`
			Messages []ChatMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Model != "unit-model" || !request.Stream || len(request.Tools) != 1 {
			t.Errorf("unexpected completion request: %#v", request)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"Answer "}}]}`)
		_, _ = fmt.Fprintln(w)
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-2","type":"function","function":{"name":"cluster_","arguments":"{\"scope\":"}}]}}]}`)
		_, _ = fmt.Fprintln(w)
		_, _ = fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"status","arguments":"\"all\"}"}}]}}]}`)
		_, _ = fmt.Fprintln(w)
		_, _ = fmt.Fprintln(w, "data: [DONE]")
		_, _ = fmt.Fprintln(w)
	}))
	defer server.Close()

	provider := NewOpenAICompatibleProvider(OpenAICompatibleConfig{
		BaseURL: server.URL + "/v1", APIKey: "secret", Model: "unit-model",
	})
	var deltas []string
	result, err := provider.Complete(context.Background(), CompletionRequest{
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
		Tools:    []agent.ToolDef{{Name: "cluster_status", Parameters: map[string]any{"type": "object"}}},
	}, func(delta string) { deltas = append(deltas, delta) })
	if err != nil {
		t.Fatal(err)
	}
	if result.Content != "Answer " || strings.Join(deltas, "") != result.Content {
		t.Fatalf("content = %q, deltas = %#v", result.Content, deltas)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].ID != "call-2" ||
		result.ToolCalls[0].Function.Name != "cluster_status" ||
		result.ToolCalls[0].Function.Arguments != `{"scope":"all"}` {
		t.Fatalf("unexpected tool calls: %#v", result.ToolCalls)
	}
}

func TestOpenAICompatibleProviderReportsHTTPFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "provider unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	provider := NewOpenAICompatibleProvider(OpenAICompatibleConfig{BaseURL: server.URL})
	if _, err := provider.Complete(context.Background(), CompletionRequest{}, nil); err == nil ||
		!strings.Contains(err.Error(), "provider unavailable") {
		t.Fatalf("expected provider error, got %v", err)
	}
}
