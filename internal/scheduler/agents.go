package scheduler

import (
	"fmt"
	"strings"
	"sync"
)

// AgentProfile describes the bounded role a worker performs for a subtask.
type AgentProfile struct {
	Role         string
	TaskType     string
	Instructions string
	Tools        []string
}

var (
	// customProfilesMu protects the custom profile map only; profile lookup
	// releases it before any agent work, with no nested lock order.
	customProfilesMu sync.RWMutex
	customProfiles   = make(map[string]AgentProfile)
)

// RegisterCustomAgentProfile dynamically registers a role profile for specialized workers.
func RegisterCustomAgentProfile(role string, profile AgentProfile) {
	customProfilesMu.Lock()
	defer customProfilesMu.Unlock()
	customProfiles[strings.ToLower(strings.TrimSpace(role))] = profile
}

// UnregisterCustomAgentProfile removes a registered custom profile.
func UnregisterCustomAgentProfile(role string) {
	customProfilesMu.Lock()
	defer customProfilesMu.Unlock()
	delete(customProfiles, strings.ToLower(strings.TrimSpace(role)))
}

var agentProfiles = map[string]AgentProfile{
	"analyst": {
		Role: "analyst", TaskType: "math",
		Instructions: "Analyze the problem carefully, show the relevant reasoning, and distinguish facts from assumptions.",
	},
	"coder": {
		Role: "coder", TaskType: "code",
		Instructions: "Produce correct, maintainable code or a concrete implementation plan. State important assumptions and edge cases.",
	},
	"general": {
		Role: "general", TaskType: "general",
		Instructions: "Complete the assigned task accurately and keep the response focused on its expected output.",
	},
	"researcher": {
		Role: "researcher", TaskType: "general",
		Instructions: "Gather and organize relevant evidence. Clearly mark uncertainty and do not invent citations or facts.",
	},
	"reviewer": {
		Role: "reviewer", TaskType: "general",
		Instructions: "Review the supplied work against the requested criteria. Return a JSON object with approved (boolean) and issues (array of concrete errors, omissions, or unsupported claims). Approve only when the work satisfies the stated criteria.",
	},
	"summarizer": {
		Role: "summarizer", TaskType: "summarize",
		Instructions: "Preserve the key facts and qualifications while removing repetition and irrelevant detail.",
	},
	"translator": {
		Role: "translator", TaskType: "translate",
		Instructions: "Translate faithfully, preserving meaning, tone, names, and formatting.",
	},
	"writer": {
		Role: "writer", TaskType: "creative",
		Instructions: "Write a clear, coherent response that follows the requested audience, format, and style.",
	},
}

// AgentProfileFor returns the registered role profile.
func AgentProfileFor(role string) (AgentProfile, bool) {
	key := strings.ToLower(strings.TrimSpace(role))
	customProfilesMu.RLock()
	custom, ok := customProfiles[key]
	customProfilesMu.RUnlock()
	if ok {
		return custom, true
	}
	profile, ok := agentProfiles[key]
	return profile, ok
}

func roleForTaskType(taskType string) string {
	switch taskType {
	case "code":
		return "coder"
	case "math":
		return "analyst"
	case "summarize":
		return "summarizer"
	case "translate":
		return "translator"
	case "creative":
		return "writer"
	case "vision":
		return "analyst"
	default:
		return "general"
	}
}

func validateSubtask(subtask Subtask) (Subtask, error) {
	subtask.Role = strings.ToLower(strings.TrimSpace(subtask.Role))
	if subtask.Role == "" {
		subtask.Role = roleForTaskType(subtask.TaskType)
	}
	profile, ok := AgentProfileFor(subtask.Role)
	if !ok {
		return Subtask{}, fmt.Errorf("scheduler: subtask %s has unknown agent role %q", subtask.ID, subtask.Role)
	}
	if subtask.TaskType == "" {
		subtask.TaskType = profile.TaskType
	}
	subtask.ExpectedOutput = strings.TrimSpace(subtask.ExpectedOutput)
	subtask.OutputFormat = strings.ToLower(strings.TrimSpace(subtask.OutputFormat))
	if subtask.OutputFormat == "" {
		subtask.OutputFormat = "text"
	}
	if subtask.OutputFormat != "text" && subtask.OutputFormat != "json" {
		return Subtask{}, fmt.Errorf("scheduler: subtask %s has unsupported output_format %q", subtask.ID, subtask.OutputFormat)
	}
	if subtask.Role == "reviewer" {
		subtask.OutputFormat = "json"
		if len(subtask.RequiredFields) == 0 {
			subtask.RequiredFields = []string{"approved", "issues"}
		}
	}
	if len(subtask.RequiredFields) > 0 && subtask.OutputFormat != "json" {
		return Subtask{}, fmt.Errorf("scheduler: subtask %s required_fields need output_format json", subtask.ID)
	}
	if subtask.MaxRetries < 0 || subtask.MaxRetries > 2 {
		return Subtask{}, fmt.Errorf("scheduler: subtask %s max_retries must be between 0 and 2", subtask.ID)
	}
	if subtask.OutputFormat == "json" && subtask.MaxRetries == 0 {
		subtask.MaxRetries = 1
	}
	for i, field := range subtask.RequiredFields {
		subtask.RequiredFields[i] = strings.TrimSpace(field)
		if subtask.RequiredFields[i] == "" {
			return Subtask{}, fmt.Errorf("scheduler: subtask %s has an empty required field", subtask.ID)
		}
	}
	subtask.RequiredFields = uniqueStrings(subtask.RequiredFields)
	return subtask, nil
}
