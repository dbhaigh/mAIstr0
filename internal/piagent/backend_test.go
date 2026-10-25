package piagent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/agent/deepseekadapter"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/deepseek"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hub"
)

type fakeProvider struct {
	responses []*Completion
	requests  []CompletionRequest
}

type recordingToolHost struct {
	agent.Backend
	calls []string
}

func (h *recordingToolHost) ExecuteTool(_ context.Context, _, name string, _ map[string]any) (agent.ToolResult, error) {
	h.calls = append(h.calls, name)
	return agent.ToolResult{Output: map[string]any{"ok": true}}, nil
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

func testToolHost() *deepseekadapter.DeepSeek {
	return deepseekadapter.NewDeepSeek(cluster.NewRegistry(), nil, nil, hub.New())
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

func TestScriptedConversationRunsThroughBothHarnesses(t *testing.T) {
	var deepSeekCalls atomic.Int32
	coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deepSeekCalls.Add(1)
		var request map[string]string
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode DeepSeek coordinator request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if strings.Contains(request["prompt"], "<tool_response>") {
			_ = json.NewEncoder(w).Encode(map[string]string{"output": "The calculation result is 144."})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"output": "I will calculate 12 * 12.\n<tool_call>\n{\"name\":\"eval_expression\",\"arguments\":{\"expression\":\"12 * 12\"}}\n</tool_call>",
		})
	}))
	defer coordinator.Close()

	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{
		ID: "coordinator", Address: coordinator.URL,
		Hardware:     hardware.Info{CPUCores: 8, TotalRAMMB: 16384, Score: 100},
		Models:       []engine.Model{{Name: "scripted-local", Tags: []string{"chat", "reasoning"}}},
		DefaultModel: "scripted-local", Healthy: true, Leader: true,
	})

	deepSeek := deepseek.New(registry, coordinator.Client())
	deepSeekSession := deepSeek.CreateSession("scripted", deepseek.SessionConfig{CoordinatorModel: "scripted-local"})
	deepSeekFinal, err := deepSeek.SendMessage(context.Background(), deepSeekSession.ID, "What is 12 * 12?", nil)
	if err != nil {
		t.Fatalf("DeepSeek SendMessage: %v", err)
	}

	piProvider := &fakeProvider{responses: []*Completion{
		{ToolCalls: []ChatToolCall{{
			ID: "call-1", Type: "function",
			Function: ChatToolFunction{Name: "eval_expression", Arguments: `{"expression":"12 * 12"}`},
		}}},
		{Content: "The calculation result is 144."},
	}}
	toolHost := deepseekadapter.NewDeepSeek(registry, coordinator.Client(), nil, hub.New())
	piBackend := New(piProvider, toolHost)
	piSession, err := piBackend.CreateSession("scripted", agent.SessionConfig{CoordinatorModel: "scripted-local"})
	if err != nil {
		t.Fatal(err)
	}
	piFinal, err := piBackend.SendMessage(context.Background(), piSession.ID, "What is 12 * 12?", nil)
	if err != nil {
		t.Fatalf("Pi SendMessage: %v", err)
	}

	if deepSeekFinal.Content != piFinal.Content || deepSeekFinal.Content != "The calculation result is 144." {
		t.Fatalf("final answers differ: DeepSeek=%q Pi=%q", deepSeekFinal.Content, piFinal.Content)
	}
	if deepSeekCalls.Load() != 2 || len(piProvider.requests) != 2 {
		t.Fatalf("model calls: DeepSeek=%d Pi=%d, want two each", deepSeekCalls.Load(), len(piProvider.requests))
	}

	deepSeekStored, ok := deepSeek.GetSession(deepSeekSession.ID)
	if !ok {
		t.Fatal("DeepSeek session disappeared")
	}
	piStored, ok := piBackend.GetSession(piSession.ID)
	if !ok {
		t.Fatal("Pi session disappeared")
	}
	assertScriptedHistory := func(name string, messages []agent.Message) {
		t.Helper()
		if len(messages) != 4 || messages[0].Role != agent.RoleUser ||
			messages[1].Role != agent.RoleAssistant || len(messages[1].ToolCalls) != 1 ||
			messages[1].ToolCalls[0].Name != "eval_expression" ||
			messages[2].Role != agent.RoleTool || messages[3].Role != agent.RoleAssistant {
			t.Fatalf("%s conversation history = %#v", name, messages)
		}
	}
	deepSeekHistory := make([]agent.Message, 0, len(deepSeekStored.Messages))
	for _, message := range deepSeekStored.Messages {
		converted := agent.Message{Role: agent.MessageRole(message.Role)}
		for _, call := range message.ToolCalls {
			converted.ToolCalls = append(converted.ToolCalls, agent.ToolCall(call))
		}
		deepSeekHistory = append(deepSeekHistory, converted)
	}
	assertScriptedHistory("DeepSeek", deepSeekHistory)
	assertScriptedHistory("Pi", piStored.Messages)
}

func TestNativeAgentExecutesEveryToolRequestedInOneTurn(t *testing.T) {
	host := &recordingToolHost{Backend: testToolHost()}
	tools := host.ListTools()
	calls := make([]ChatToolCall, 0, len(tools))
	expected := make(map[string]bool, len(tools))
	for i, tool := range tools {
		calls = append(calls, ChatToolCall{
			ID: fmt.Sprintf("call-%d", i), Type: "function",
			Function: ChatToolFunction{Name: tool.Name, Arguments: `{}`},
		})
		expected[tool.Name] = true
	}
	provider := &fakeProvider{responses: []*Completion{
		{ToolCalls: calls},
		{Content: "All requested tools have run."},
	}}
	backend := New(provider, host)
	session, err := backend.CreateSession("all tools", agent.SessionConfig{MaxSteps: 2})
	if err != nil {
		t.Fatal(err)
	}
	final, err := backend.SendMessage(context.Background(), session.ID, "Run all available tools.", nil)
	if err != nil {
		t.Fatal(err)
	}
	if final.Content != "All requested tools have run." {
		t.Fatalf("final response = %q", final.Content)
	}
	if len(host.calls) != len(expected) {
		t.Fatalf("executed %d tools, want %d", len(host.calls), len(expected))
	}
	for _, name := range host.calls {
		if !expected[name] {
			t.Errorf("executed unexpected tool %q", name)
		}
		delete(expected, name)
	}
	if len(expected) != 0 {
		t.Errorf("tools not executed: %#v", expected)
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

func TestOpenAICompatibleProviderDoesNotDefaultToCloud(t *testing.T) {
	provider := NewOpenAICompatibleProvider(OpenAICompatibleConfig{})
	if _, err := provider.Complete(context.Background(), CompletionRequest{}, nil); err == nil ||
		!strings.Contains(err.Error(), "local cluster") {
		t.Fatalf("expected missing local provider configuration error, got %v", err)
	}
}

func TestOpenAICompatibleProviderReportsStreamErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintln(w, `data: {"error":{"message":"model is unavailable"}}`)
		_, _ = fmt.Fprintln(w)
	}))
	defer server.Close()

	provider := NewOpenAICompatibleProvider(OpenAICompatibleConfig{BaseURL: server.URL})
	_, err := provider.Complete(context.Background(), CompletionRequest{}, nil)
	if err == nil || !strings.Contains(err.Error(), "model is unavailable") {
		t.Fatalf("expected stream provider error, got %v", err)
	}
}

func TestOpenAICompatibleProviderRejectsEmptyStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintln(w, "data: [DONE]")
		_, _ = fmt.Fprintln(w)
	}))
	defer server.Close()

	provider := NewOpenAICompatibleProvider(OpenAICompatibleConfig{BaseURL: server.URL})
	_, err := provider.Complete(context.Background(), CompletionRequest{}, nil)
	if err == nil || !strings.Contains(err.Error(), "empty completion") {
		t.Fatalf("expected empty completion error, got %v", err)
	}
}

type blockingProvider struct {
	started chan struct{}
}

func (p *blockingProvider) Name() string { return "blocking-provider" }
func (p *blockingProvider) Complete(ctx context.Context, _ CompletionRequest, _ func(string)) (*Completion, error) {
	close(p.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestInterruptSession(t *testing.T) {
	provider := &blockingProvider{started: make(chan struct{})}
	host := testToolHost()
	backend := New(provider, host)
	session, err := backend.CreateSession("test-interrupt", agent.SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}

	events := make(chan agent.StreamEvent, 16)
	errCh := make(chan error, 1)
	go func() {
		_, sendErr := backend.SendMessage(context.Background(), session.ID, "Hello", events)
		errCh <- sendErr
	}()

	<-provider.started

	ok := backend.InterruptSession(session.ID)
	if !ok {
		t.Fatal("expected InterruptSession to return true")
	}

	err = <-errCh
	if err == nil {
		t.Fatal("expected SendMessage to return error on interrupt")
	}

	stored, ok := backend.GetSession(session.ID)
	if !ok {
		t.Fatal("session not found")
	}
	if stored.Active {
		t.Fatal("expected session to not be active after interrupt")
	}
	if len(stored.Messages) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(stored.Messages))
	}
	lastMsg := stored.Messages[len(stored.Messages)-1]
	if lastMsg.Role != agent.RoleAssistant || !strings.Contains(lastMsg.Content, "interrupted") {
		t.Fatalf("expected last message to indicate interruption, got: %#v", lastMsg)
	}

	foundInterruptedEvent := false
	close(events)
	for ev := range events {
		if ev.Type == agent.EventInterrupted {
			foundInterruptedEvent = true
			break
		}
	}
	if !foundInterruptedEvent {
		t.Fatal("expected EventInterrupted stream event")
	}
}
