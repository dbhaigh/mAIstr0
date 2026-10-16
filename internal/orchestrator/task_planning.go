package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
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

type taskScheduler interface {
	AssignWithExperience([]scheduler.Subtask, []cluster.NodeStatus, scheduler.Experience) ([]scheduler.Assignment, error)
}

type taskStore interface {
	CreateWithGraphAndOptionsChecked(string, *scheduler.TaskGraph, []scheduler.Assignment, bool) (*taskmgr.Task, error)
	CompleteSubtask(string, string, string, error)
	Wait(context.Context, string) (*taskmgr.Task, error)
	Cancel(string) bool
}

type taskDispatcher interface {
	submitDispatch(string, scheduler.Assignment) error
}

type plannerModel interface {
	callNodeGenerateWithOptionsContext(context.Context, string, string, string, float64, int) (string, error)
}

type taskPlanner struct {
	scheduler  taskScheduler
	tasks      taskStore
	dispatch   taskDispatcher
	model      plannerModel
	nodes      func() []cluster.NodeStatus
	experience scheduler.Experience
}

type taskPlannerAPI interface {
	createTask(context.Context, createTaskRequest) (*taskmgr.Task, error)
	planSubtasks(context.Context, string) (*scheduler.TaskGraph, []scheduler.Subtask, error)
	executeWorkflow(context.Context, string, []scheduler.Subtask, bool) (string, error)
}

type schedulerAdapter struct{}

func (schedulerAdapter) AssignWithExperience(subtasks []scheduler.Subtask, nodes []cluster.NodeStatus, experience scheduler.Experience) ([]scheduler.Assignment, error) {
	return scheduler.AssignWithExperience(subtasks, nodes, experience)
}

func newTaskPlanner(
	scheduler taskScheduler,
	tasks taskStore,
	dispatch taskDispatcher,
	model plannerModel,
	nodes func() []cluster.NodeStatus,
	experience scheduler.Experience,
) *taskPlanner {
	return &taskPlanner{
		scheduler: scheduler, tasks: tasks, dispatch: dispatch,
		model: model, nodes: nodes, experience: experience,
	}
}

func (s *Server) createTask(ctx context.Context, req createTaskRequest) (*taskmgr.Task, error) {
	return s.plannerPort().createTask(ctx, req)
}

func (s *Server) planSubtasks(ctx context.Context, description string) (*scheduler.TaskGraph, []scheduler.Subtask, error) {
	return s.plannerPort().planSubtasks(ctx, description)
}

func (p *taskPlanner) createTask(ctx context.Context, req createTaskRequest) (*taskmgr.Task, error) {
	if strings.TrimSpace(req.Description) == "" {
		return nil, apperr.Invalid(errors.New("description is required"))
	}
	subtasks := req.Subtasks
	graph := req.Graph
	planMode := strings.ToLower(strings.TrimSpace(req.PlanMode))
	modelPlanned := planMode == "auto"

	switch planMode {
	case "", "heuristic":
		if graph != nil {
			if err := graph.Validate(); err != nil {
				return nil, apperr.Invalid(fmt.Errorf("invalid task graph: %w", err))
			}
			sorted, err := graph.TopologicalSort()
			if err != nil {
				return nil, apperr.Invalid(fmt.Errorf("invalid task graph: %w", err))
			}
			subtasks = sorted
		} else if len(subtasks) == 0 {
			subtasks = scheduler.Decompose(req.Description)
			var err error
			graph, err = scheduler.TaskGraphFromSubtasks(subtasks)
			if err != nil {
				return nil, apperr.Invalid(fmt.Errorf("build task graph: %w", err))
			}
		} else {
			var err error
			graph, err = scheduler.TaskGraphFromSubtasks(subtasks)
			if err != nil {
				return nil, apperr.Invalid(fmt.Errorf("build task graph: %w", err))
			}
		}
	case "auto":
		if len(subtasks) != 0 || graph != nil {
			return nil, apperr.Invalid(errors.New("subtasks or graph cannot be supplied with plan_mode auto"))
		}
		plannedGraph, plannedSubtasks, err := p.planSubtasks(ctx, req.Description)
		if err != nil {
			return nil, fmt.Errorf("create structured task plan: %w", err)
		}
		graph = plannedGraph
		subtasks = plannedSubtasks
	default:
		return nil, apperr.Invalid(errors.New("plan_mode must be heuristic or auto"))
	}

	subtasks, err := scheduler.ValidatePlan(subtasks)
	if err != nil {
		if modelPlanned {
			return nil, apperr.UpstreamError(err)
		}
		return nil, apperr.Invalid(err)
	}
	if req.RequireReview {
		subtasks = addReviewerSubtask(subtasks)
		subtasks, err = scheduler.ValidatePlan(subtasks)
		if err != nil {
			return nil, apperr.Invalid(err)
		}
		if graph != nil {
			reviewerNode := subtasks[len(subtasks)-1]
			_ = graph.AddNode(reviewerNode)
			for _, dep := range reviewerNode.DependsOn {
				_ = graph.AddEdge(dep, reviewerNode.ID)
			}
		}
	}
	assignments, err := p.scheduler.AssignWithExperience(subtasks, p.nodes(), p.experience)
	if err != nil {
		return nil, apperr.UnavailableError(err)
	}
	if len(assignments) != len(subtasks) {
		return nil, apperr.UnavailableError(errors.New("scheduler returned an incomplete assignment"))
	}
	task, err := p.tasks.CreateWithGraphAndOptionsChecked(req.Description, graph, assignments, req.RequireApproval)
	if err != nil {
		return nil, apperr.InternalError(fmt.Errorf("could not persist task: %w", err))
	}
	for _, assignment := range assignments {
		if err := p.dispatch.submitDispatch(task.ID, assignment); err != nil {
			log.Printf("orchestrator: could not queue subtask %s: %v", assignment.Subtask.ID, err)
			p.tasks.CompleteSubtask(task.ID, assignment.Subtask.ID, "", err)
		}
	}
	return task, nil
}

func (s *Server) executeWorkflow(ctx context.Context, description string, subtasks []scheduler.Subtask, requireReview bool) (string, error) {
	return s.plannerPort().executeWorkflow(ctx, description, subtasks, requireReview)
}

func (p *taskPlanner) executeWorkflow(ctx context.Context, description string, subtasks []scheduler.Subtask, requireReview bool) (string, error) {
	planMode := ""
	if len(subtasks) == 0 {
		planMode = "auto"
	}
	task, err := p.createTask(ctx, createTaskRequest{
		Description:   description,
		Subtasks:      subtasks,
		PlanMode:      planMode,
		RequireReview: requireReview,
	})
	if err != nil {
		return "", err
	}
	result, err := p.tasks.Wait(ctx, task.ID)
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			_ = p.tasks.Cancel(task.ID)
			return "", apperr.ConflictError(fmt.Errorf("workflow %s: %w", task.ID, err))
		}
		return "", apperr.InternalError(fmt.Errorf("workflow %s: %w", task.ID, err))
	}
	if result.Status != taskmgr.StatusCompleted {
		if result.Error != "" {
			return "", apperr.UpstreamError(fmt.Errorf("workflow %s %s: %s", task.ID, result.Status, result.Error))
		}
		return "", apperr.UpstreamError(fmt.Errorf("workflow %s ended with status %s", task.ID, result.Status))
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

func (p *taskPlanner) planSubtasks(ctx context.Context, description string) (*scheduler.TaskGraph, []scheduler.Subtask, error) {
	planning := scheduler.Subtask{ID: "plan", Description: "Create a workflow plan", TaskType: "general"}
	nodes := p.nodes()
	failedModels := make(map[string]map[string]struct{})
	var lastModelErr error
	var output string
	for {
		candidates := make([]cluster.NodeStatus, len(nodes))
		for i, node := range nodes {
			candidates[i] = node
			if len(failedModels[node.ID]) == 0 {
				continue
			}
			candidates[i].Models = make([]engine.Model, 0, len(node.Models))
			for _, model := range node.Models {
				if _, failed := failedModels[node.ID][model.Name]; !failed {
					candidates[i].Models = append(candidates[i].Models, model)
				}
			}
		}
		assignments, err := p.scheduler.AssignWithExperience([]scheduler.Subtask{planning}, candidates, p.experience)
		if err != nil {
			if lastModelErr != nil {
				return nil, nil, apperr.UpstreamError(fmt.Errorf("all planning model attempts failed: %w", lastModelErr))
			}
			wrapped := fmt.Errorf("select planning model: %w", err)
			var unassignable *scheduler.UnassignableSubtasksError
			if errors.Is(err, scheduler.ErrNoHealthyNodes) || errors.As(err, &unassignable) {
				return nil, nil, apperr.UnavailableError(wrapped)
			}
			return nil, nil, apperr.UpstreamError(wrapped)
		}
		if len(assignments) != 1 {
			return nil, nil, apperr.UpstreamError(errors.New("scheduler returned no planning model"))
		}

		assignment := assignments[0]
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
		output, err = p.model.callNodeGenerateWithOptionsContext(ctx, assignment.Address, assignment.Model, prompt, 0.1, 2048)
		if err == nil {
			break
		}
		lastModelErr = fmt.Errorf("planning model %s on node %s: %w", assignment.Model, assignment.NodeID, err)
		if ctx.Err() != nil {
			return nil, nil, apperr.UpstreamError(lastModelErr)
		}
		if failedModels[assignment.NodeID] == nil {
			failedModels[assignment.NodeID] = make(map[string]struct{})
		}
		failedModels[assignment.NodeID][assignment.Model] = struct{}{}
	}

	var err error
	payload, err := extractJSONObject(output)
	if err != nil {
		return nil, nil, apperr.UpstreamError(fmt.Errorf("planning model returned invalid JSON: %w", err))
	}
	var raw struct {
		Graph    *scheduler.TaskGraph         `json:"graph,omitempty"`
		Nodes    map[string]scheduler.Subtask `json:"nodes,omitempty"`
		Edges    []scheduler.Edge             `json:"edges,omitempty"`
		Subtasks []scheduler.Subtask          `json:"subtasks,omitempty"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, nil, apperr.UpstreamError(fmt.Errorf("decode planner response: %w", err))
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
			return nil, nil, apperr.UpstreamError(fmt.Errorf("build graph from planner subtasks: %w", err))
		}
	} else {
		return nil, nil, apperr.UpstreamError(errors.New("planner response contains neither graph nor subtasks"))
	}

	if len(graph.Nodes) > 8 {
		return nil, nil, apperr.UpstreamError(errors.New("planning model returned more than 8 subtasks"))
	}
	if err := graph.Validate(); err != nil {
		return nil, nil, apperr.UpstreamError(fmt.Errorf("planner graph validation: %w", err))
	}
	sorted, err := graph.TopologicalSort()
	if err != nil {
		return nil, nil, apperr.UpstreamError(fmt.Errorf("planner graph sort: %w", err))
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
