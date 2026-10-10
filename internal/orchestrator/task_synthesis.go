package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

const maxSynthesisInputBytes = 128 * 1024

func (s *Server) synthesizeTask(ctx context.Context, task *taskmgr.Task) (string, error) {
	if len(task.Subtasks) == 1 {
		return task.Subtasks[0].Output, nil
	}

	var prompt strings.Builder
	fmt.Fprintf(&prompt, "Produce one clear, cohesive answer to the user's task using the completed subtask outputs below. Preserve important details, resolve overlap, and do not invent facts that are not supported by the outputs.\n\nUser task:\n%s\n\n", task.Description)
	for _, subtask := range task.Subtasks {
		if subtask.Status != taskmgr.StatusCompleted {
			continue
		}
		fmt.Fprintf(&prompt, "Subtask: %s\nOutput:\n%s\n\n", subtask.Subtask.Description, strings.TrimSpace(subtask.Output))
		if prompt.Len() > maxSynthesisInputBytes {
			return "", fmt.Errorf("completed subtask outputs exceed the %d-byte synthesis limit", maxSynthesisInputBytes)
		}
	}

	synthesis := scheduler.Subtask{
		ID:          "synthesis",
		Description: "Synthesize completed task results",
		TaskType:    "general",
		Tags:        []string{"general"},
	}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{synthesis}, s.registry.Active(), s.experience())
	if err != nil {
		return "", fmt.Errorf("select synthesis model: %w", err)
	}
	assignment := assignments[0]
	result, err := s.callNodeGenerateWithOptionsContext(ctx, assignment.Address, assignment.Model, prompt.String(), 0.2, 2048)
	if err != nil {
		return "", fmt.Errorf("synthesize task on node %s with model %s: %w", assignment.NodeID, assignment.Model, err)
	}
	if strings.TrimSpace(result) == "" {
		return "", errors.New("synthesis model returned an empty response")
	}
	return result, nil
}
