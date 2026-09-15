package deepseek

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestParseDeepSeekThinkingResponse(t *testing.T) {
	t.Run("Standard DeepSeek Tool Call", func(t *testing.T) {
		raw := `I will check the cluster status.
<tool_call>
{"name": "cluster_status", "arguments": {}}
</tool_call>`

		parsed := ParseDeepSeekResponse(raw)
		if parsed.Thought != "I will check the cluster status." {
			t.Errorf("unexpected thought: %q", parsed.Thought)
		}
		if len(parsed.ToolCalls) != 1 {
			t.Fatalf("expected 1 tool call, got %d", len(parsed.ToolCalls))
		}
		if parsed.ToolCalls[0].Name != "cluster_status" {
			t.Errorf("expected tool cluster_status, got %q", parsed.ToolCalls[0].Name)
		}
	})

	t.Run("Multiple Tool Calls in DeepSeek format", func(t *testing.T) {
		raw := `Let's query two worker nodes.
<tool_call>
{"name": "cluster_llm_query", "arguments": {"prompt": "Write hello world in Go", "task_type": "code"}}
</tool_call>
<tool_call>
{"name": "eval_expression", "arguments": {"expression": "25 * 4"}}
</tool_call>`

		parsed := ParseDeepSeekResponse(raw)
		if len(parsed.ToolCalls) != 2 {
			t.Fatalf("expected 2 tool calls, got %d", len(parsed.ToolCalls))
		}
		if parsed.ToolCalls[0].Name != "cluster_llm_query" {
			t.Errorf("expected first tool cluster_llm_query, got %s", parsed.ToolCalls[0].Name)
		}
		if parsed.ToolCalls[1].Name != "eval_expression" {
			t.Errorf("expected second tool eval_expression, got %s", parsed.ToolCalls[1].Name)
		}
	})

	t.Run("Markdown Fenced Code Block Fallback", func(t *testing.T) {
		raw := "```tool_call\n{\"name\": \"eval_expression\", \"arguments\": {\"expression\": \"sqrt(144)\"}}\n```"
		parsed := ParseDeepSeekResponse(raw)
		if len(parsed.ToolCalls) != 1 {
			t.Fatalf("expected 1 tool call, got %d", len(parsed.ToolCalls))
		}
		if parsed.ToolCalls[0].Name != "eval_expression" {
			t.Errorf("unexpected tool name %s", parsed.ToolCalls[0].Name)
		}
	})

	t.Run("Direct Conversational Response", func(t *testing.T) {
		raw := "Hello! I am ready to coordinate the cluster."
		parsed := ParseDeepSeekResponse(raw)
		if len(parsed.ToolCalls) != 0 {
			t.Errorf("expected 0 tool calls, got %d", len(parsed.ToolCalls))
		}
		if parsed.Content != raw {
			t.Errorf("unexpected content: %q", parsed.Content)
		}
	})
}

func TestParseDeepSeekResponse(t *testing.T) {
	parsed := ParseResponse(`<think>Plan the cluster query.</think>\nThe answer is ready.`)
	if parsed.Thought != "Plan the cluster query." {
		t.Fatalf("unexpected DeepSeek thought: %q", parsed.Thought)
	}
	if parsed.Content != `\nThe answer is ready.` {
		t.Fatalf("unexpected DeepSeek content: %q", parsed.Content)
	}
}

func TestEvalExpressionTool(t *testing.T) {
	tool := &EvalExpressionTool{}
	h := New(cluster.NewRegistry(), nil)
	s := h.CreateSession("test", SessionConfig{})

	tests := []struct {
		expr     string
		expected float64
	}{
		{"10 + 20", 30},
		{"50 - 18", 32},
		{"12 * 12", 144},
		{"100 / 4", 25},
		{"sqrt(64)", 8},
		{"sum(10, 20, 30, 40)", 100},
		{"avg(10, 20, 30)", 20},
	}

	for _, tt := range tests {
		out, _, _, _, err := tool.Execute(context.Background(), h, s, map[string]any{"expression": tt.expr})
		if err != nil {
			t.Fatalf("eval error for %q: %v", tt.expr, err)
		}
		m, ok := out.(map[string]any)
		if !ok {
			t.Fatalf("expected map result for %q", tt.expr)
		}
		val, ok := m["result"].(float64)
		if !ok || val != tt.expected {
			t.Errorf("for expr %q expected %v, got %v", tt.expr, tt.expected, m["result"])
		}
	}
}

func TestSessionMemoryTool(t *testing.T) {
	tool := &SessionMemoryTool{}
	h := New(cluster.NewRegistry(), nil)
	s := h.CreateSession("test", SessionConfig{})

	// Set
	_, _, _, _, err := tool.Execute(context.Background(), h, s, map[string]any{
		"action": "set",
		"key":    "goal",
		"value":  "distribute cluster load",
	})
	if err != nil {
		t.Fatalf("set error: %v", err)
	}

	// Get
	out, _, _, _, err := tool.Execute(context.Background(), h, s, map[string]any{
		"action": "get",
		"key":    "goal",
	})
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	m := out.(map[string]any)
	if m["value"] != "distribute cluster load" {
		t.Errorf("unexpected memory value: %v", m["value"])
	}
}

func TestClusterWorkloadDistribution(t *testing.T) {
	// Create two mock node servers
	node1Calls := 0
	node2Calls := 0

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node1Calls++
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"output": "node1 result for " + req["prompt"],
		})
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node2Calls++
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"output": "node2 result for " + req["prompt"],
		})
	}))
	defer srv2.Close()

	reg := cluster.NewRegistry()
	reg.Upsert(cluster.NodeStatus{
		ID:      "node-1",
		Address: srv1.URL,
		Hardware: hardware.Info{
			CPUCores:   8,
			TotalRAMMB: 32768,
			Score:      50,
		},
		Models: []engine.Model{
			{Name: "qwen-code", Tags: []string{"code"}},
		},
		Healthy: true,
	})
	reg.Upsert(cluster.NodeStatus{
		ID:      "node-2",
		Address: srv2.URL,
		Hardware: hardware.Info{
			CPUCores:   8,
			TotalRAMMB: 32768,
			Score:      50,
		},
		Models: []engine.Model{
			{Name: "qwen-code", Tags: []string{"code"}},
		},
		Healthy: true,
	})

	h := New(reg, http.DefaultClient)
	s := h.CreateSession("test", SessionConfig{})

	// Parallel dispatch tool should distribute subtasks across BOTH nodes
	parallelTool := &ClusterParallelDispatchTool{}
	out, _, _, _, err := parallelTool.Execute(context.Background(), h, s, map[string]any{
		"subtasks": []any{
			map[string]any{"description": "task 1", "task_type": "code"},
			map[string]any{"description": "task 2", "task_type": "code"},
		},
	})
	if err != nil {
		t.Fatalf("parallel dispatch error: %v", err)
	}

	resMap := out.(map[string]any)
	if resMap["total_subtasks"] != 2 {
		t.Errorf("expected 2 subtasks, got %v", resMap["total_subtasks"])
	}

	// Verify both nodes were called to spread the workload
	if node1Calls != 1 || node2Calls != 1 {
		t.Errorf("expected workload split 1 & 1, got node1=%d, node2=%d", node1Calls, node2Calls)
	}
}

func TestInteractiveAgentHarnessLoop(t *testing.T) {
	// Coordinator server that simulates a DeepSeek model:
	// Turn 1: Outputs a tool call to eval_expression
	// Turn 2: Outputs the final answer
	turn := 0
	mockCoordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		turn++
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)

		if strings.Contains(req["prompt"], "<tool_response>") {
			// Second turn after tool response
			_ = json.NewEncoder(w).Encode(map[string]string{
				"output": "The calculation result is 144.",
			})
		} else {
			// First turn: generate DeepSeek tool call
			_ = json.NewEncoder(w).Encode(map[string]string{
				"output": "I will calculate 12 * 12.\n<tool_call>\n{\"name\": \"eval_expression\", \"arguments\": {\"expression\": \"12 * 12\"}}\n</tool_call>",
			})
		}
	}))
	defer mockCoordinator.Close()

	reg := cluster.NewRegistry()
	reg.Upsert(cluster.NodeStatus{
		ID:      "coord-node",
		Address: mockCoordinator.URL,
		Hardware: hardware.Info{
			CPUCores:   16,
			TotalRAMMB: 65536,
			Score:      100,
		},
		Models: []engine.Model{
			{Name: "deepseek-3-8b", Tags: []string{"chat", "reasoning"}},
		},
		DefaultModel: "deepseek-3-8b",
		Healthy:      true,
		Leader:       true,
	})

	h := New(reg, http.DefaultClient)
	s := h.CreateSession("test-calc", SessionConfig{CoordinatorModel: "deepseek-3-8b"})

	events := make([]StreamEvent, 0)
	streamChan := make(chan StreamEvent, 20)

	msg, err := h.SendMessage(context.Background(), s.ID, "What is 12 * 12?", streamChan)
	close(streamChan)
	for ev := range streamChan {
		events = append(events, ev)
	}

	if err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}

	if !strings.Contains(msg.Content, "144") {
		t.Errorf("expected final answer to contain 144, got %q", msg.Content)
	}
	if msg.Coordinator != "orchestrator" || msg.RawOutput == "" {
		t.Errorf("expected orchestrator coordinator and raw output, got coordinator=%q raw=%q", msg.Coordinator, msg.RawOutput)
	}
	foundNodeOutput := false
	for _, event := range events {
		if event.Type == EventNodeOutput && event.NodeID == "coord-node" && event.Output != "" && event.Coordinator == "orchestrator" {
			foundNodeOutput = true
			break
		}
	}
	if !foundNodeOutput {
		t.Error("expected attributed node_output event from orchestrator coordinator")
	}

	// Verify session history has user, assistant with tool call, tool response, and final assistant message
	if len(s.Messages) < 4 {
		t.Fatalf("expected at least 4 messages in session history, got %d", len(s.Messages))
	}

	if s.Messages[0].Role != RoleUser {
		t.Errorf("expected msg[0] user, got %s", s.Messages[0].Role)
	}
	if s.Messages[1].Role != RoleAssistant || len(s.Messages[1].ToolCalls) == 0 {
		t.Errorf("expected msg[1] assistant with tool calls")
	}
	if s.Messages[2].Role != RoleTool {
		t.Errorf("expected msg[2] tool response")
	}
	if s.Messages[3].Role != RoleAssistant {
		t.Errorf("expected msg[3] final assistant")
	}
}

func TestCoordinatorRoleOwnsPromptRouting(t *testing.T) {
	reg := cluster.NewRegistry()
	reg.Upsert(cluster.NodeStatus{
		ID:      "worker-node",
		Address: "http://worker",
		Models:  []engine.Model{{Name: "worker-model"}},
		Healthy: true,
	})
	reg.Upsert(cluster.NodeStatus{
		ID:      "coordinator-node",
		Address: "http://coordinator",
		Models:  []engine.Model{{Name: "coordinator-model"}},
		Healthy: true,
		Leader:  true,
	})

	h := New(reg, http.DefaultClient)
	s := h.CreateSession("routing", SessionConfig{CoordinatorModel: "worker-model"})
	node, model, err := h.selectCoordinator(s)
	if err != nil {
		t.Fatal(err)
	}
	if node.ID != "coordinator-node" || model != "coordinator-model" {
		t.Fatalf("prompt routed to %s/%s, want coordinator-node/coordinator-model", node.ID, model)
	}
}

func TestDirectNodeTools(t *testing.T) {
	mockNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"output": "Result from node for model " + req["model"] + ": " + req["prompt"],
		})
	}))
	defer mockNode.Close()

	reg := cluster.NewRegistry()
	reg.Upsert(cluster.NodeStatus{
		ID:      "worker-target",
		Address: mockNode.URL,
		Hardware: hardware.Info{
			CPUCores:   8,
			TotalRAMMB: 16384,
			Score:      50,
		},
		Models: []engine.Model{
			{Name: "mistral-7b", Tags: []string{"chat"}},
			{Name: "codellama-7b", Tags: []string{"code"}},
		},
		DefaultModel: "mistral-7b",
		Healthy:      true,
	})

	h := New(reg, http.DefaultClient)
	s := h.CreateSession("test-direct", SessionConfig{})

	// 1. Test ListNodeModelsTool
	listTool := &ListNodeModelsTool{}
	out, _, _, _, err := listTool.Execute(context.Background(), h, s, map[string]any{})
	if err != nil {
		t.Fatalf("list models failed: %v", err)
	}
	m := out.(map[string]any)
	if m["total_models"].(int) != 2 {
		t.Errorf("expected 2 models, got %v", m["total_models"])
	}

	// 2. Test NodeLLMQueryTool
	queryTool := &NodeLLMQueryTool{}
	out, nodeID, _, model, err := queryTool.Execute(context.Background(), h, s, map[string]any{
		"node_id": "worker-target",
		"model":   "codellama-7b",
		"prompt":  "generate code",
	})
	if err != nil {
		t.Fatalf("node_llm_query failed: %v", err)
	}
	if nodeID != "worker-target" || model != "codellama-7b" {
		t.Errorf("unexpected query result: nodeID=%s, model=%s", nodeID, model)
	}

	// 3. Test TestNodeModelTool
	testTool := &TestNodeModelTool{}
	out, _, _, _, err = testTool.Execute(context.Background(), h, s, map[string]any{
		"node_id": "worker-target",
		"model":   "mistral-7b",
	})
	if err != nil {
		t.Fatalf("test_node_model failed: %v", err)
	}
	tm := out.(map[string]any)
	if tm["healthy"] != true {
		t.Errorf("expected test_node_model healthy true, got %v", tm)
	}

	// 4. Test NodeConverseTool (multi-turn conversation with node)
	converseTool := &NodeConverseTool{}
	out, nodeID, _, model, err = converseTool.Execute(context.Background(), h, s, map[string]any{
		"node_id":     "worker-target",
		"message":     "Design a cache algorithm",
		"dialogue_id": "dlg-test-1",
	})
	if err != nil {
		t.Fatalf("node_converse failed: %v", err)
	}
	cm := out.(map[string]any)
	if cm["dialogue_id"] != "dlg-test-1" || cm["node_id"] != "worker-target" {
		t.Errorf("unexpected converse result: %+v", cm)
	}

	// 5. Test NodeCollaborateTool (multi-round collaboration)
	collabTool := &NodeCollaborateTool{}
	out, nodeID, _, model, err = collabTool.Execute(context.Background(), h, s, map[string]any{
		"node_id": "worker-target",
		"task":    "Write Go worker pool",
		"rounds":  2.0,
	})
	if err != nil {
		t.Fatalf("node_collaborate failed: %v", err)
	}
	clm := out.(map[string]any)
	if clm["total_rounds"] != 2 {
		t.Errorf("expected 2 rounds, got %v", clm["total_rounds"])
	}
}
