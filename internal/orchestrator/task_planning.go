package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type createTaskRequest struct {
	Description     string               `json:"description"`
	Subtasks        []scheduler.Subtask  `json:"subtasks,omitempty"`
	Graph           *scheduler.TaskGraph `json:"graph,omitempty"`
	PlanMode        string               `json:"plan_mode,omitempty"`
	RequireReview   bool                 `json:"require_review,omitempty"`
	RequireApproval bool                 `json:"require_approval,omitempty"`
}

type taskCreateError struct {
	status int
	err    error
}

func (e *taskCreateError) Error() string { return e.err.Error() }
func (e *taskCreateError) Unwrap() error { return e.err }

func (s *Server) createTask(ctx context.Context, req createTaskRequest) (*taskmgr.Task, error) {
	if strings.TrimSpace(req.Description) == "" {
		return nil, &taskCreateError{status: 400, err: errors.New("description is required")}
	}
	subtasks := req.Subtasks
	graph := req.Graph
	planMode := strings.ToLower(strings.TrimSpace(req.PlanMode))
	modelPlanned := planMode == "auto"

	switch planMode {
	case "", "heuristic":
		if graph != nil {
			if err := graph.Validate(); err != nil {
				return nil, &taskCreateError{status: 400, err: fmt.Errorf("invalid task graph: %w", err)}
			}
			sorted, err := graph.TopologicalSort()
			if err != nil {
				return nil, &taskCreateError{status: 400, err: fmt.Errorf("invalid task graph: %w", err)}
			}
			subtasks = sorted
		} else if len(subtasks) == 0 {
			subtasks = scheduler.Decompose(req.Description)
			var err error
			graph, err = scheduler.TaskGraphFromSubtasks(subtasks)
			if err != nil {
				return nil, &taskCreateError{status: 400, err: fmt.Errorf("build task graph: %w", err)}
			}
		} else {
			var err error
			graph, err = scheduler.TaskGraphFromSubtasks(subtasks)
			if err != nil {
				return nil, &taskCreateError{status: 400, err: fmt.Errorf("build task graph: %w", err)}
			}
		}
	case "auto":
		if len(subtasks) != 0 || graph != nil {
			return nil, &taskCreateError{status: 400, err: errors.New("subtasks or graph cannot be supplied with plan_mode auto")}
		}
		plannedGraph, plannedSubtasks, err := s.planSubtasks(ctx, req.Description)
		if err != nil {
			status := 502
			if errors.Is(err, scheduler.ErrNoHealthyNodes) {
				status = 503
			}
			var unassignable *scheduler.UnassignableSubtasksError
			if errors.As(err, &unassignable) {
				status = 503
			}
			return nil, &taskCreateError{status: status, err: fmt.Errorf("create structured task plan: %w", err)}
		}
		graph = plannedGraph
		subtasks = plannedSubtasks
	default:
		return nil, &taskCreateError{status: 400, err: errors.New("plan_mode must be heuristic or auto")}
	}

	subtasks, err := scheduler.ValidatePlan(subtasks)
	if err != nil {
		status := 400
		if modelPlanned {
			status = 502
		}
		return nil, &taskCreateError{status: status, err: err}
	}
	if req.RequireReview {
		subtasks = addReviewerSubtask(subtasks)
		subtasks, err = scheduler.ValidatePlan(subtasks)
		if err != nil {
			return nil, &taskCreateError{status: 400, err: err}
		}
		if graph != nil {
			reviewerNode := subtasks[len(subtasks)-1]
			_ = graph.AddNode(reviewerNode)
			for _, dep := range reviewerNode.DependsOn {
				_ = graph.AddEdge(dep, reviewerNode.ID)
			}
		}
	}
	assignments, err := scheduler.AssignWithExperience(subtasks, s.schedulerNodes(), s.experience())
	if err != nil {
		return nil, &taskCreateError{status: 503, err: err}
	}
	if len(assignments) != len(subtasks) {
		return nil, &taskCreateError{status: 503, err: errors.New("scheduler returned an incomplete assignment")}
	}
	task, err := s.tasks.CreateWithGraphAndOptionsChecked(req.Description, graph, assignments, req.RequireApproval)
	if err != nil {
		return nil, &taskCreateError{status: 500, err: fmt.Errorf("could not persist task: %w", err)}
	}
	for _, assignment := range assignments {
		go s.dispatch(task.ID, assignment)
	}
	return task, nil
}

func (s *Server) executeWorkflow(ctx context.Context, description string, subtasks []scheduler.Subtask, requireReview bool) (string, error) {
	planMode := ""
	if len(subtasks) == 0 {
		planMode = "auto"
	}
	task, err := s.createTask(ctx, createTaskRequest{
		Description:   description,
		Subtasks:      subtasks,
		PlanMode:      planMode,
		RequireReview: requireReview,
	})
	if err != nil {
		return "", err
	}
	result, err := s.tasks.Wait(ctx, task.ID)
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			_ = s.tasks.Cancel(task.ID)
		}
		return "", fmt.Errorf("workflow %s: %w", task.ID, err)
	}
	if result.Status != taskmgr.StatusCompleted {
		if result.Error != "" {
			return "", fmt.Errorf("workflow %s %s: %s", task.ID, result.Status, result.Error)
		}
		return "", fmt.Errorf("workflow %s ended with status %s", task.ID, result.Status)
	}
	return result.Result, nil
}

func addReviewerSubtask(subtasks []scheduler.Subtask) []scheduler.Subtask {
	id := "review"
	existing := make(map[string]bool, len(subtasks))
	dependencies := make([]string, 0, len(subtasks))
	for _, subtask := range subtasks {
		existing[subtask.ID] = true
		dependencies = append(dependencies, subtask.ID)
	}
	for suffix := 2; existing[id]; suffix++ {
		id = fmt.Sprintf("review-%d", suffix)
	}
	return append(subtasks, scheduler.Subtask{
		ID: id, Description: "Review the completed work and approve it only if it meets the expected outputs and contains no unsupported claims.",
		Role: "reviewer", TaskType: "general", DependsOn: dependencies,
		ExpectedOutput: "A JSON object with approved=true or false and an issues array.",
		OutputFormat:   "json", RequiredFields: []string{"approved", "issues"},
	})
}

func (s *Server) planSubtasks(ctx context.Context, description string) (*scheduler.TaskGraph, []scheduler.Subtask, error) {
	planning := scheduler.Subtask{ID: "plan", Description: "Create a workflow plan", TaskType: "general"}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{planning}, s.schedulerNodes(), s.experience())
	if err != nil {
		return nil, nil, fmt.Errorf("select planning model: %w", err)
	}
	if len(assignments) != 1 {
		return nil, nil, errors.New("scheduler returned no planning model")
	}
	prompt := fmt.Sprintf(`You are the mAIstr0 workflow planner. Convert the user's request into a small, executable dependency-aware task plan. Return only one JSON object with either this shape:
{"graph":{"nodes":{"research":{"id":"research","description":"...","role":"researcher","task_type":"general","depends_on":[],"expected_output":"...","output_format":"text","required_capabilities":[]}},"edges":[]}}
or this shape:
{"subtasks":[{"id":"research","description":"...","role":"researcher","task_type":"general","depends_on":[],"expected_output":"...","output_format":"text","required_capabilities":[],"max_retries":0}]}

Use only these roles: researcher, analyst, coder, writer, reviewer, summarizer, translator, general.
Use task_type values supported by a local model registry when relevant: code, summarize, translate, math, vision, creative, general.
Supported capability tags: researcher, coder, long-context, vision, math, fast, high-quality.
Make independent tasks parallel; add depends_on only when a task genuinely needs another task's result. Keep the plan focused, use 1-8 tasks, and set expected_output for each task. Use output_format json and required_fields only when the result must be machine-readable. JSON output gets one corrective retry by default; use max_retries 0 for plain-text output and at most 2 for any task.
User request:
%s`, strings.TrimSpace(description))
	output, err := s.callNodeGenerateWithOptionsContext(ctx, assignments[0].Address, assignments[0].Model, prompt, 0.1, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("planning model %s on node %s: %w", assignments[0].Model, assignments[0].NodeID, err)
	}
	payload, err := extractJSONObject(output)
	if err != nil {
		return nil, nil, fmt.Errorf("planning model returned invalid JSON: %w", err)
	}
	var raw struct {
		Graph    *scheduler.TaskGraph         `json:"graph,omitempty"`
		Nodes    map[string]scheduler.Subtask `json:"nodes,omitempty"`
		Edges    []scheduler.Edge             `json:"edges,omitempty"`
		Subtasks []scheduler.Subtask          `json:"subtasks,omitempty"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, nil, fmt.Errorf("decode planner response: %w", err)
	}

	var graph *scheduler.TaskGraph
	if raw.Graph != nil && len(raw.Graph.Nodes) > 0 {
		graph = raw.Graph
	} else if len(raw.Nodes) > 0 {
		graph = &scheduler.TaskGraph{
			Nodes: raw.Nodes,
			Edges: raw.Edges,
		}
	} else if len(raw.Subtasks) > 0 {
		var err error
		graph, err = scheduler.TaskGraphFromSubtasks(raw.Subtasks)
		if err != nil {
			return nil, nil, fmt.Errorf("build graph from planner subtasks: %w", err)
		}
	} else {
		return nil, nil, errors.New("planner response contains neither graph nor subtasks")
	}

	if len(graph.Nodes) > 8 {
		return nil, nil, errors.New("planning model returned more than 8 subtasks")
	}
	if err := graph.Validate(); err != nil {
		return nil, nil, fmt.Errorf("planner graph validation: %w", err)
	}
	sorted, err := graph.TopologicalSort()
	if err != nil {
		return nil, nil, fmt.Errorf("planner graph sort: %w", err)
	}
	return graph, sorted, nil
}

func extractJSONObject(output string) ([]byte, error) {
	start := strings.IndexByte(output, '{')
	if start < 0 {
		return nil, errors.New("no JSON object found")
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(output); i++ {
		switch char := output[i]; {
		case inString && escaped:
			escaped = false
		case inString && char == '\\':
			escaped = true
		case char == '"':
			inString = !inString
		case !inString && char == '{':
			depth++
		case !inString && char == '}':
			depth--
			if depth == 0 {
				return []byte(output[start : i+1]), nil
			}
		}
	}
	return nil, errors.New("unterminated JSON object")
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func normalizeTaskOutput(subtask scheduler.Subtask, output string) string {
	if subtask.OutputFormat != "json" {
		return output
	}
	trimmed := strings.TrimSpace(output)
	lines := strings.Split(trimmed, "\n")
	if len(lines) >= 3 {
		opening := strings.TrimSpace(strings.TrimSuffix(lines[0], "\r"))
		closing := strings.TrimSpace(strings.TrimSuffix(lines[len(lines)-1], "\r"))
		if (opening == "```" || strings.EqualFold(opening, "```json")) && closing == "```" {
			trimmed = strings.TrimSpace(strings.Join(lines[1:len(lines)-1], "\n"))
		}
	}
	if len(trimmed) >= 4 && strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		if strings.HasPrefix(inner, "{") && json.Valid([]byte(inner)) {
			return inner
		}
	}
	return trimmed
}

func validateTaskOutput(subtask scheduler.Subtask, output string) error {
	if strings.TrimSpace(output) == "" {
		return errors.New("worker returned an empty result")
	}
	if subtask.OutputFormat != "json" {
		return nil
	}
	output = normalizeTaskOutput(subtask, output)
	var object map[string]json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(output))
	if err := decoder.Decode(&object); err != nil {
		return fmt.Errorf("result is not valid JSON: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return fmt.Errorf("result is not one JSON value: %w", err)
	}
	if object == nil {
		return errors.New("result must be a JSON object")
	}
	for _, field := range subtask.RequiredFields {
		if value, ok := object[field]; !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("result is missing required field %q", field)
		}
	}
	if subtask.Role == "reviewer" {
		var approved bool
		if err := json.Unmarshal(object["approved"], &approved); err != nil {
			return errors.New("review result field approved must be a boolean")
		}
		if !approved {
			return fmt.Errorf("reviewer rejected the workflow output: %s", strings.TrimSpace(string(object["issues"])))
		}
	}
	return nil
}
