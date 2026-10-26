package scheduler

import (
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

func TestSubAgentFactory_CreationAndDispatch(t *testing.T) {
	factory := NewSubAgentFactory()

	nodes := []cluster.NodeStatus{
		{
			ID:           "fast-coder-node",
			Address:      "http://coder:8080",
			Healthy:      true,
			Capabilities: []string{"coder", "fast"},
			Hardware:     hardware.Info{Score: 90.0},
			Models: []engine.Model{
				{Name: "qwen2.5-coder:7b", Tags: []string{"code", "general"}, SizeGB: 4.5},
			},
		},
		{
			ID:           "research-node",
			Address:      "http://research:8080",
			Healthy:      true,
			Capabilities: []string{"researcher"},
			Hardware:     hardware.Info{Score: 70.0},
			Models: []engine.Model{
				{Name: "deepseek-r1:7b", Tags: []string{"reasoning", "general"}, SizeGB: 7.0},
			},
		},
	}

	// 4-tuple style request
	spec := SubAgentSpec{
		Name:                 "security-auditor",
		Role:                 "auditor",
		Instruction:          "You are a specialized security auditor examining code for memory leaks and vulnerabilities.",
		RequiredTools:        []string{"file_search", "eval_expression"},
		RequiredCapabilities: []string{"coder"},
		Context:              "The codebase is written in Go and communicates over mTLS.",
		TaskType:             "code",
	}

	handle, err := factory.Create(spec, nodes, nil)
	if err != nil {
		t.Fatalf("factory.Create failed: %v", err)
	}

	if handle.ID != "security-auditor" {
		t.Errorf("Expected handle ID 'security-auditor', got %s", handle.ID)
	}
	if handle.NodeID != "fast-coder-node" {
		t.Errorf("Expected handle NodeID 'fast-coder-node', got %s", handle.NodeID)
	}
	if handle.Model != "qwen2.5-coder:7b" {
		t.Errorf("Expected model 'qwen2.5-coder:7b', got %s", handle.Model)
	}

	// Verify custom role registration
	profile, ok := AgentProfileFor("auditor")
	if !ok {
		t.Fatal("Expected custom profile 'auditor' to be registered")
	}
	if profile.Instructions != spec.Instruction {
		t.Errorf("Profile instructions mismatch")
	}

	// Convert to Subtask and Assignment
	st := handle.ToSubtask("sub-sec-1", "Audit the mTLS handshake")
	if st.Role != "auditor" || st.TaskType != "code" {
		t.Errorf("Subtask role/taskType mismatch: %+v", st)
	}

	assign := handle.ToAssignment("sub-sec-1", "Audit the mTLS handshake")
	if assign.NodeID != "fast-coder-node" || assign.Model != "qwen2.5-coder:7b" {
		t.Errorf("Assignment mismatch: %+v", assign)
	}

	// Verify the formatted prompt contains every 4-tuple part.
	prompt := handle.FormatPrompt("Find race conditions", "Prereq output here")
	for _, want := range []string{
		"You are a specialized security auditor",
		"Available tools: file_search, eval_expression",
		"Context:\nThe codebase is written in Go",
		"Prereq output here",
		"Task: Find race conditions",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("formatted prompt missing %q:\n%s", want, prompt)
		}
	}
}
