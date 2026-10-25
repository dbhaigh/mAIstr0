package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

type planResponseModel struct {
	outputs []string
	models  []string
	prompts []string
	cancel  context.CancelFunc
}

func (m *planResponseModel) callNodeGenerateWithOptionsContext(_ context.Context, _, model, prompt string, _ float64, _ int) (string, error) {
	m.models = append(m.models, model)
	m.prompts = append(m.prompts, prompt)
	if m.cancel != nil {
		m.cancel()
	}
	index := len(m.models) - 1
	if index >= len(m.outputs) {
		return "", errors.New("unexpected model call")
	}
	return m.outputs[index], nil
}

func TestAutoPlanCorrectsInvalidResponses(t *testing.T) {
	const valid = `{"subtasks":[{"id":"work","description":"Do the work","expected_output":"A result"}]}`
	tests := []struct {
		name   string
		output string
	}{
		{"plain text", "You have several common models on your cluster."},
		{"empty", ""},
		{"malformed JSON", `{"subtasks": invalid}`},
		{"missing tasks", `{"answer":"not a plan"}`},
		{"invalid task", `{"subtasks":[{"id":"work","description":""}]}`},
		{"duplicate IDs", `{"subtasks":[{"id":"work","description":"A"},{"id":"work","description":"B"}]}`},
		{"cycle", `{"subtasks":[{"id":"a","description":"A","depends_on":["b"]},{"id":"b","description":"B","depends_on":["a"]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &planResponseModel{outputs: []string{tt.output, valid}}
			nodes := []cluster.NodeStatus{{
				ID: "planner", Address: "http://planner", Healthy: true,
				Models: []engine.Model{{Name: "general", Tags: []string{"general"}}},
			}}
			planner := newTaskPlanner(schedulerAdapter{}, nil, nil, model, func() []cluster.NodeStatus { return nodes }, nil)
			graph, tasks, err := planner.planSubtasks(context.Background(), "Do the work")
			if err != nil || graph == nil || len(tasks) != 1 {
				t.Fatalf("corrected plan = %+v, %+v, %v", graph, tasks, err)
			}
			if len(model.models) != 2 || !strings.Contains(model.prompts[1], "previous response was not a valid executable plan") {
				t.Fatalf("missing corrective attempt: %+v", model)
			}
			if tt.output != "" && strings.Contains(model.prompts[1], tt.output) {
				t.Fatal("corrective prompt echoed model output")
			}
			if _, _, err := planner.planSubtasks(context.Background(), "Do the work"); err != nil || len(model.models) != 2 {
				t.Fatalf("corrected plan was not cached: calls=%d err=%v", len(model.models), err)
			}
		})
	}
}

func TestAutoPlanInvalidResponseFailoverIsBounded(t *testing.T) {
	model := &planResponseModel{outputs: []string{"prose", "prose", "prose", "prose"}}
	nodes := []cluster.NodeStatus{{
		ID: "planner", Address: "http://planner", Healthy: true, DefaultModel: "first",
		Models: []engine.Model{{Name: "first"}, {Name: "second"}},
	}}
	planner := newTaskPlanner(schedulerAdapter{}, nil, nil, model, func() []cluster.NodeStatus { return nodes }, nil)
	_, _, err := planner.planSubtasks(context.Background(), "Do the work")
	if err == nil || !errors.Is(err, apperr.Upstream) {
		t.Fatalf("exhausted plans error = %v, want upstream error", err)
	}
	if got := strings.Join(model.models, ","); got != "first,first,second,second" {
		t.Fatalf("attempts = %s", got)
	}
}

func TestAutoPlanInvalidResponseUsesAlternateModel(t *testing.T) {
	model := &planResponseModel{outputs: []string{
		"prose", "still prose", `{"subtasks":[{"id":"work","description":"Do the work"}]}`,
	}}
	nodes := []cluster.NodeStatus{{
		ID: "planner", Address: "http://planner", Healthy: true, DefaultModel: "first",
		Models: []engine.Model{{Name: "first"}, {Name: "second"}},
	}}
	planner := newTaskPlanner(schedulerAdapter{}, nil, nil, model, func() []cluster.NodeStatus { return nodes }, nil)
	if _, _, err := planner.planSubtasks(context.Background(), "Do the work"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(model.models, ","); got != "first,first,second" {
		t.Fatalf("attempts = %s", got)
	}
}

func TestAutoPlanCancellationStopsCorrectiveRetry(t *testing.T) {
	for _, output := range []string{"prose", `{"subtasks":[{"id":"work","description":"Do the work"}]}`} {
		t.Run(output, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			model := &planResponseModel{outputs: []string{output}, cancel: cancel}
			nodes := []cluster.NodeStatus{{
				ID: "planner", Address: "http://planner", Healthy: true,
				Models: []engine.Model{{Name: "general"}},
			}}
			planner := newTaskPlanner(schedulerAdapter{}, nil, nil, model, func() []cluster.NodeStatus { return nodes }, nil)
			_, _, err := planner.planSubtasks(ctx, "Do the work")
			if !errors.Is(err, context.Canceled) || len(model.models) != 1 {
				t.Fatalf("cancellation: calls=%d error=%v", len(model.models), err)
			}
		})
	}
}
