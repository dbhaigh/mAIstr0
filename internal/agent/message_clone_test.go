package agent

import "testing"

func TestCloneMessageCopiesToolState(t *testing.T) {
	original := Message{
		ToolCalls:    []ToolCall{{Name: "cluster_status"}},
		ToolResponse: &ToolResponse{Name: "cluster_status"},
	}
	clone := CloneMessage(original)

	if &clone.ToolCalls[0] == &original.ToolCalls[0] || clone.ToolResponse == original.ToolResponse {
		t.Fatal("clone retained mutable tool state references")
	}
	original.ToolCalls[0].Name = "mutated"
	original.ToolResponse.Name = "mutated"
	if clone.ToolCalls[0].Name != "cluster_status" || clone.ToolResponse.Name != "cluster_status" {
		t.Fatalf("clone changed with source: %+v", clone)
	}
}

func TestCloneStreamEventCopiesNestedMessagesAndTools(t *testing.T) {
	original := StreamEvent{
		Message:  &Message{Content: "before", ToolCalls: []ToolCall{{Name: "before"}}},
		ToolCall: &ToolCall{Name: "before"},
		ToolResp: &ToolResponse{Name: "before"},
	}
	clone := CloneStreamEvent(original)

	original.Message.Content = "after"
	original.Message.ToolCalls[0].Name = "after"
	original.ToolCall.Name = "after"
	original.ToolResp.Name = "after"
	if clone.Message.Content != "before" || clone.Message.ToolCalls[0].Name != "before" ||
		clone.ToolCall.Name != "before" || clone.ToolResp.Name != "before" {
		t.Fatalf("clone changed with source: %+v", clone)
	}
}
