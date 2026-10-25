package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/scheduler"
)

func decodeTaskPlan(output string) (*scheduler.TaskGraph, []scheduler.Subtask, error) {
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
		graph = &scheduler.TaskGraph{Nodes: raw.Nodes, Edges: raw.Edges}
	} else if len(raw.Subtasks) > 0 {
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
	sorted, err = scheduler.ValidatePlan(sorted)
	if err != nil {
		return nil, nil, apperr.UpstreamError(fmt.Errorf("planner task validation: %w", err))
	}
	return graph, sorted, nil
}
