package taskmgr

import (
	"context"
	"fmt"
	"github.com/maistr0/maistr0/internal/scheduler"
	"strings"
)

// WaitForDependencies waits until every prerequisite has completed. It
// returns false if the task is cancelled or any prerequisite fails.
func (m *Manager) WaitForDependencies(ctx context.Context, taskID, subtaskID string) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		m.mu.RLock()
		t, ok := m.tasks[taskID]
		if !ok || t.Status == StatusCancelled {
			m.mu.RUnlock()
			return false
		}
		var dependencies []string
		found := false
		for i := range t.Subtasks {
			result := &t.Subtasks[i]
			if result.Subtask.ID == subtaskID {
				dependencies = append([]string(nil), result.Subtask.DependsOn...)
				found = true
				break
			}
		}
		if !found {
			m.mu.RUnlock()
			return false
		}
		required := make(map[string]Status, len(dependencies))
		for _, dependency := range dependencies {
			required[dependency] = StatusPending
		}
		if len(required) > 0 {
			for i := range t.Subtasks {
				result := &t.Subtasks[i]
				if _, needed := required[result.Subtask.ID]; needed {
					required[result.Subtask.ID] = result.Status
				}
			}
		}
		ready := true
		for _, status := range required {
			switch status {
			case StatusCompleted:
			case StatusFailed, StatusCancelled:
				m.mu.RUnlock()
				return false
			default:
				ready = false
			}
		}
		if ready {
			m.mu.RUnlock()
			return ctx.Err() == nil
		}
		changed := t.changed
		m.mu.RUnlock()

		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

// InputForSubtask includes completed prerequisite outputs alongside the
// assigned subtask description.
func (m *Manager) InputForSubtask(taskID, subtaskID string) (string, bool) {
	m.mu.RLock()
	t, ok := m.tasks[taskID]
	if !ok {
		m.mu.RUnlock()
		return "", false
	}
	var current *SubtaskResult
	description := t.Description
	for i := range t.Subtasks {
		if t.Subtasks[i].Subtask.ID == subtaskID {
			current = &t.Subtasks[i]
			break
		}
	}
	if current == nil {
		m.mu.RUnlock()
		return "", false
	}
	subtask := cloneSubtask(current.Subtask)
	outputs := make(map[string]string, len(subtask.DependsOn))
	dependencies := make(map[string]bool, len(subtask.DependsOn))
	for _, dependency := range subtask.DependsOn {
		dependencies[dependency] = true
	}
	for i := range t.Subtasks {
		result := &t.Subtasks[i]
		if _, needed := dependencies[result.Subtask.ID]; needed {
			if result.Status != StatusCompleted {
				m.mu.RUnlock()
				return "", false
			}
			outputs[result.Subtask.ID] = result.Output
		}
	}
	m.mu.RUnlock()

	var input strings.Builder
	for _, dependency := range subtask.DependsOn {
		output, exists := outputs[dependency]
		if !exists {
			return "", false
		}
		fmt.Fprintf(&input, "Prerequisite %s:\n%s\n\n", dependency, strings.TrimSpace(output))
	}
	profile, ok := scheduler.AgentProfileFor(subtask.Role)
	if !ok {
		profile, _ = scheduler.AgentProfileFor("general")
	}
	fmt.Fprintf(&input, "You are the %s agent.\n%s\n\n", profile.Role, profile.Instructions)
	fmt.Fprintf(&input, "Overall task: %s\n\nSubtask: %s", description, subtask.Description)
	if subtask.ExpectedOutput != "" {
		fmt.Fprintf(&input, "\n\nExpected output: %s", subtask.ExpectedOutput)
	}
	if subtask.OutputFormat == "json" {
		input.WriteString("\n\nReturn only a valid JSON object.")
		if len(subtask.RequiredFields) > 0 {
			fmt.Fprintf(&input, " Include these top-level fields: %s.", strings.Join(subtask.RequiredFields, ", "))
		}
	}
	return input.String(), true
}
