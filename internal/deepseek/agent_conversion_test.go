package deepseek

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSharedAgentSessionConversionPreservesSnapshotJSON(t *testing.T) {
	at := time.Date(2026, time.October, 31, 12, 0, 0, 0, time.UTC)
	session := &Session{
		ID: "session-1", Title: "shared contract", CreatedAt: at, UpdatedAt: at,
		Config: SessionConfig{CoordinatorModel: "local", MaxSteps: 3, Temperature: 0.2, SystemPrompt: "system"},
		Memory: map[string]string{"fact": "known"},
		Messages: []Message{
			{
				ID: "assistant-1", Role: RoleAssistant, Content: "calling tool",
				ToolCalls: []ToolCall{{ID: "call-1", Name: "cluster_status", Arguments: map[string]any{"scope": "all"}}},
				Timestamp: at,
			},
			{
				ID: "tool-1", Role: RoleTool,
				ToolResponse: &ToolResponse{ToolCallID: "call-1", Name: "cluster_status", Output: map[string]any{"nodes": 2}},
				Timestamp:    at,
			},
		},
	}
	before, err := json.Marshal(session.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	shared := ToAgentSession(session)
	after, err := json.Marshal(ToSessionSnapshot(shared))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("shared framework conversion changed snapshot JSON:\nbefore %s\nafter  %s", before, after)
	}

	session.Messages[0].ToolCalls[0].Name = "mutated"
	session.Messages[1].ToolResponse.Name = "mutated"
	if shared.Messages[0].ToolCalls[0].Name != "cluster_status" ||
		shared.Messages[1].ToolResponse.Name != "cluster_status" {
		t.Fatalf("framework snapshot retained source tool references: %+v", shared.Messages)
	}
}
