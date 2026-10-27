package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

type qualityExpectation struct {
	taskType  string
	role      string
	dependsOn []int
	synthesis bool
	nodeID    string
	model     string
}

type qualityFixture struct {
	group       string
	description string
	expected    []qualityExpectation
}

type qualityMetrics struct {
	Fixtures             int            `json:"fixtures"`
	DecompositionMatches int            `json:"decomposition_matches"`
	AssignmentMatches    int            `json:"assignment_matches"`
	ExpectedAssignments  int            `json:"expected_assignments"`
	CountMatches         int            `json:"count_matches"`
	TaskTypeMatches      int            `json:"task_type_matches"`
	RoleMatches          int            `json:"role_matches"`
	DependencyMatches    int            `json:"dependency_matches"`
	SynthesisMatches     int            `json:"synthesis_matches"`
	FailureClasses       map[string]int `json:"failure_classes"`
	AssignmentMeanUS     float64        `json:"assignment_mean_us"`
	AssignmentP50US      float64        `json:"assignment_p50_us"`
	AssignmentP95US      float64        `json:"assignment_p95_us"`
}

type qualityRoute struct {
	nodeID string
	model  string
}

var qualityRoutes = map[string]qualityRoute{
	"code":      {nodeID: "sim-code", model: "simulated-code"},
	"creative":  {nodeID: "sim-creative", model: "simulated-creative"},
	"general":   {nodeID: "sim-general", model: "simulated-general"},
	"math":      {nodeID: "sim-math", model: "simulated-math"},
	"summarize": {nodeID: "sim-summarize", model: "simulated-summarize"},
	"translate": {nodeID: "sim-translate", model: "simulated-translate"},
	"vision":    {nodeID: "sim-vision", model: "simulated-vision"},
}

var qualityFixtures = []qualityFixture{
	{
		group: "sequential", description: "Research the topic. Then summarize the findings.",
		expected: qualitySequence(
			qualityTask("general", "researcher"),
			qualityTask("summarize", "summarizer"),
		),
	},
	{
		group: "sequential", description: "Inspect the input; after that, calculate the total.",
		expected: qualitySequence(qualityTask("general", "general"), qualityTask("math", "analyst")),
	},
	{
		group: "sequential", description: "Compare the reports. Afterwards, write a summary.",
		expected: qualitySequence(qualityTask("general", "general"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "sequential", description: "First analyze the logs. Next write a summary.",
		expected: qualitySequence(qualityTask("general", "general"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "sequential", description: "Start by collecting sources. Finally, summarize the evidence.",
		expected: qualitySequence(qualityTask("general", "general"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "sequential", description: "Read the article. Subsequently, translate the quote.",
		expected: qualitySequence(qualityTask("general", "general"), qualityTask("translate", "translator")),
	},
	{
		group: "sequential", description: "Research the topic; then write a report; finally review it.",
		expected: qualitySequence(
			qualityTask("general", "researcher"),
			qualityTask("creative", "writer"),
			qualityTask("general", "reviewer"),
		),
	},
	{
		group: "sequential", description: "Check the totals. Once complete, write the summary.",
		expected: qualitySequence(qualityTask("general", "general"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "sequential", description: "Translate the brief. Based on the result, summarize it.",
		expected: qualitySequence(qualityTask("translate", "translator"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "sequential", description: "Gather evidence. Using the prior results, calculate the trend.",
		expected: qualitySequence(qualityTask("general", "researcher"), qualityTask("math", "analyst")),
	},
	{
		group: "list_items", description: "Assess these options: code, math, and creative.",
		expected: qualityFanOut(
			qualityTask("code", "coder"), qualityTask("math", "analyst"), qualityTask("creative", "writer"),
		),
	},
	{
		group: "list_items", description: "- Implement the parser\n- Test the output\n- Summarize the result",
		expected: qualityFanOut(
			qualityTask("code", "coder"), qualityTask("general", "general"), qualityTask("summarize", "summarizer"),
		),
	},
	{
		group: "list_items", description: "Summarize these topics: climate, health, and education.",
		expected: qualityFanOut(
			qualityTask("summarize", "summarizer"), qualityTask("summarize", "summarizer"),
			qualityTask("summarize", "summarizer"),
		),
	},
	{
		group: "list_items", description: "Inspect the following files: alpha.go, beta.go.",
		expected: qualityFanOut(qualityTask("general", "general"), qualityTask("general", "general")),
	},
	{
		group: "list_items", description: "Calculate these cases: 4+2, 9-1.",
		expected: qualityFanOut(qualityTask("math", "analyst"), qualityTask("math", "analyst")),
	},
	{
		group: "list_items", description: "Translate these items: headline, caption.",
		expected: qualityFanOut(qualityTask("translate", "translator"), qualityTask("translate", "translator")),
	},
	{
		group: "list_items", description: "Inspect these steps:\n1. inspect files\n2. calculate totals.",
		expected: qualityFanOut(qualityTask("general", "general"), qualityTask("math", "analyst")),
	},
	{
		group: "list_items", description: "Summarize these issues: facts, risks, actions.",
		expected: qualityFanOut(
			qualityTask("summarize", "summarizer"), qualityTask("summarize", "summarizer"),
			qualityTask("summarize", "summarizer"),
		),
	},
	{
		group: "list_items", description: "Inspect these files: alpha.go; beta.go.",
		expected: qualityFanOut(qualityTask("general", "general"), qualityTask("general", "general")),
	},
	{
		group: "list_items", description: "Inspect these steps: 1. inspect files; 2. calculate totals.",
		expected: qualityFanOut(qualityTask("general", "general"), qualityTask("math", "analyst")),
	},
	{
		group: "conjunction", description: "Summarize the memo and translate the title.",
		expected: qualityFanOut(qualityTask("summarize", "summarizer"), qualityTask("translate", "translator")),
	},
	{
		group: "conjunction", description: "Calculate the mean and write a brief explanation.",
		expected: qualityFanOut(qualityTask("math", "analyst"), qualityTask("creative", "writer")),
	},
	{
		group: "conjunction", description: "Implement the parser and test the edge cases.",
		expected: qualityFanOut(qualityTask("code", "coder"), qualityTask("general", "general")),
	},
	{
		group: "conjunction", description: "Inspect the logs and summarize the failures.",
		expected: qualityFanOut(qualityTask("general", "general"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "conjunction", description: "Translate the title and calculate the word count.",
		expected: qualityFanOut(qualityTask("translate", "translator"), qualityTask("math", "analyst")),
	},
	{
		group: "conjunction", description: "Review the patch and summarize the changes.",
		expected: qualityFanOut(qualityTask("general", "reviewer"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "conjunction", description: "Write a poem and translate it into Spanish.",
		expected: qualityFanOut(qualityTask("creative", "writer"), qualityTask("translate", "translator")),
	},
	{
		group: "conjunction", description: "Solve the equation and explain the result.",
		expected: qualityFanOut(qualityTask("math", "analyst"), qualityTask("general", "general")),
	},
	{
		group: "conjunction", description: "Analyze the image and summarize the scene.",
		expected: qualityFanOut(qualityTask("vision", "analyst"), qualityTask("summarize", "summarizer")),
	},
	{
		group: "conjunction", description: "Search for references and write a report.",
		expected: qualityFanOut(qualityTask("general", "general"), qualityTask("creative", "writer")),
	},
	{
		group: "role_inference", description: "Research recent sources about the release.",
		expected: qualitySingle(qualityTask("general", "researcher")),
	},
	{
		group: "role_inference", description: "Investigate the cause of the test failure.",
		expected: qualitySingle(qualityTask("general", "researcher")),
	},
	{
		group: "role_inference", description: "Gather evidence about compatibility.",
		expected: qualitySingle(qualityTask("general", "researcher")),
	},
	{
		group: "role_inference", description: "Review the API design.",
		expected: qualitySingle(qualityTask("code", "reviewer")),
	},
	{
		group: "role_inference", description: "Audit the implementation plan.",
		expected: qualitySingle(qualityTask("code", "reviewer")),
	},
	{
		group: "role_inference", description: "Critique the proposed approach.",
		expected: qualitySingle(qualityTask("general", "reviewer")),
	},
	{
		group: "role_inference", description: "Summarize the report.",
		expected: qualitySingle(qualityTask("summarize", "summarizer")),
	},
	{
		group: "role_inference", description: "Translate the note.",
		expected: qualitySingle(qualityTask("translate", "translator")),
	},
	{
		group: "role_inference", description: "Calculate the total.",
		expected: qualitySingle(qualityTask("math", "analyst")),
	},
	{
		group: "role_inference", description: "Write a story.",
		expected: qualitySingle(qualityTask("creative", "writer")),
	},
}

func qualityTask(taskType, role string, dependsOn ...int) qualityExpectation {
	route, ok := qualityRoutes[taskType]
	if !ok {
		panic("missing expected route for task type " + taskType)
	}
	return qualityExpectation{
		taskType: taskType, role: role, dependsOn: append([]int(nil), dependsOn...),
		nodeID: route.nodeID, model: route.model,
	}
}

func qualitySingle(task qualityExpectation) []qualityExpectation {
	return []qualityExpectation{task}
}

func qualitySequence(tasks ...qualityExpectation) []qualityExpectation {
	for i := 1; i < len(tasks); i++ {
		tasks[i].dependsOn = []int{i - 1}
	}
	if len(tasks) == 0 {
		return nil
	}
	return append(tasks, qualitySynthesis(len(tasks)-1))
}

func qualityFanOut(tasks ...qualityExpectation) []qualityExpectation {
	if len(tasks) == 0 {
		return nil
	}
	dependencies := make([]int, len(tasks))
	for i := range tasks {
		dependencies[i] = i
	}
	return append(tasks, qualitySynthesis(dependencies...))
}

func qualitySynthesis(dependencies ...int) qualityExpectation {
	task := qualityTask("summarize", "summarizer", dependencies...)
	task.synthesis = true
	return task
}

func TestRoutingQualityFixtures(t *testing.T) {
	if len(qualityFixtures) != 40 {
		t.Fatalf("quality fixture count = %d, want 40", len(qualityFixtures))
	}
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	nodes, err := qualityCluster()
	if err != nil {
		t.Fatal(err)
	}

	const assignmentRepetitions = 100
	metrics := qualityMetrics{
		Fixtures:            len(qualityFixtures),
		ExpectedAssignments: 0,
		FailureClasses:      make(map[string]int),
	}
	latencySamples := make([]float64, 0, len(qualityFixtures))
	groupMatches := make(map[string]int)
	groupCounts := make(map[string]int)

	for _, fixture := range qualityFixtures {
		groupCounts[fixture.group]++
		actual := Decompose(fixture.description)
		metrics.ExpectedAssignments += len(fixture.expected)
		planMatches, componentMatches := compareQualityPlan(actual, fixture.expected, metrics.FailureClasses)
		if planMatches {
			metrics.DecompositionMatches++
			groupMatches[fixture.group]++
		}
		metrics.CountMatches += componentMatches.count
		metrics.TaskTypeMatches += componentMatches.taskType
		metrics.RoleMatches += componentMatches.role
		metrics.DependencyMatches += componentMatches.dependencies
		metrics.SynthesisMatches += componentMatches.synthesis

		graph, graphErr := TaskGraphFromSubtasks(actual)
		var assignments []Assignment
		var assignmentErr error
		if graphErr == nil {
			started := time.Now()
			repetitions := 0
			for range assignmentRepetitions {
				assignments, assignmentErr = AssignGraph(graph, nodes, nil, nil)
				repetitions++
				if assignmentErr != nil {
					break
				}
			}
			if repetitions > 0 {
				latencySamples = append(latencySamples,
					float64(time.Since(started).Nanoseconds())/float64(repetitions)/float64(time.Microsecond))
			}
		} else {
			assignmentErr = graphErr
		}
		if assignmentErr != nil {
			metrics.FailureClasses["assignment.error"]++
		}
		compareQualityAssignments(
			t, fixture.group, actual, assignments, fixture.expected, assignmentErr, metrics.FailureClasses, &metrics,
		)
	}

	metrics.AssignmentMeanUS, metrics.AssignmentP50US, metrics.AssignmentP95US = summarizeQualityLatency(latencySamples)
	encoded, err := json.Marshal(metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("routing_quality=%s", encoded)
	for _, group := range []string{"sequential", "list_items", "conjunction", "role_inference"} {
		t.Logf("routing_quality_group=%s matched=%d/%d", group, groupMatches[group], groupCounts[group])
	}
}

func qualityCluster() ([]cluster.NodeStatus, error) {
	simulated := engine.NewSimulated()
	models, err := simulated.ListModels()
	if err != nil {
		return nil, fmt.Errorf("list simulated models: %w", err)
	}
	if len(models) < 2 {
		return nil, fmt.Errorf("simulated model inventory has %d models, want at least 2", len(models))
	}
	byType := map[string]engine.Model{
		"general": models[0],
		"code":    models[1],
	}
	for _, taskType := range []string{"creative", "math", "summarize", "translate", "vision"} {
		byType[taskType] = engine.Model{
			Name: "simulated-" + taskType, Engine: simulated.Name(), Tags: []string{taskType},
		}
	}

	nodes := make([]cluster.NodeStatus, 0, len(qualityRoutes))
	for _, taskType := range []string{"code", "creative", "general", "math", "summarize", "translate", "vision"} {
		route := qualityRoutes[taskType]
		model := byType[taskType]
		nodes = append(nodes, cluster.NodeStatus{
			ID: route.nodeID, Address: "http://" + route.nodeID + ".invalid", Healthy: true,
			Engines: []engine.Status{{Name: simulated.Name(), Ready: true, ModelCount: 1}},
			Models:  []engine.Model{model},
		})
	}
	return nodes, nil
}

type qualityComponentMatches struct {
	count        int
	taskType     int
	role         int
	dependencies int
	synthesis    int
}

func compareQualityPlan(
	actual []Subtask,
	expected []qualityExpectation,
	failures map[string]int,
) (bool, qualityComponentMatches) {
	matches := qualityComponentMatches{}
	exact := len(actual) == len(expected)
	if exact {
		matches.count++
	} else {
		failures["decomposition.count"]++
	}

	positions := make(map[string]int, len(actual))
	for i, subtask := range actual {
		positions[subtask.ID] = i
	}
	for i, want := range expected {
		if i >= len(actual) {
			failures["decomposition.task_type"]++
			failures["decomposition.role"]++
			failures["decomposition.dependencies"]++
			failures["decomposition.synthesis"]++
			exact = false
			continue
		}
		got := actual[i]
		if got.TaskType == want.taskType {
			matches.taskType++
		} else {
			failures["decomposition.task_type"]++
			exact = false
		}
		if got.Role == want.role {
			matches.role++
		} else {
			failures["decomposition.role"]++
			exact = false
		}
		dependencies := make([]int, 0, len(got.DependsOn))
		for _, dependency := range got.DependsOn {
			if position, ok := positions[dependency]; ok {
				dependencies = append(dependencies, position)
			} else {
				dependencies = append(dependencies, -1)
			}
		}
		if equalInts(dependencies, want.dependsOn) {
			matches.dependencies++
		} else {
			failures["decomposition.dependencies"]++
			exact = false
		}
		if got.Synthesis == want.synthesis {
			matches.synthesis++
		} else {
			failures["decomposition.synthesis"]++
			exact = false
		}
	}
	return exact, matches
}

func compareQualityAssignments(
	t *testing.T,
	group string,
	actual []Subtask,
	assignments []Assignment,
	expected []qualityExpectation,
	assignmentErr error,
	failures map[string]int,
	metrics *qualityMetrics,
) {
	assignmentByID := make(map[string]Assignment, len(assignments))
	for _, assignment := range assignments {
		assignmentByID[assignment.Subtask.ID] = assignment
	}
	for i, want := range expected {
		if assignmentErr != nil || i >= len(actual) {
			failures["assignment.missing"]++
			continue
		}
		got, ok := assignmentByID[actual[i].ID]
		if !ok {
			failures["assignment.missing"]++
			continue
		}
		matched := true
		if got.NodeID != want.nodeID {
			failures["assignment.node"]++
			matched = false
		}
		if got.Model != want.model {
			failures["assignment.model"]++
			matched = false
		}
		if matched {
			metrics.AssignmentMatches++
		} else {
			t.Logf("routing_quality_assignment_mismatch group=%s task_type=%s expected=%s/%s actual=%s/%s",
				group, actual[i].TaskType, want.nodeID, want.model, got.NodeID, got.Model)
		}
	}
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func summarizeQualityLatency(samples []float64) (mean, p50, p95 float64) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	sort.Float64s(samples)
	for _, sample := range samples {
		mean += sample
	}
	mean /= float64(len(samples))
	p50 = samples[int(math.Ceil(float64(len(samples))*0.50))-1]
	p95 = samples[int(math.Ceil(float64(len(samples))*0.95))-1]
	return mean, p50, p95
}

func TestRoutingQualityFixtureDescriptionsAreNonEmpty(t *testing.T) {
	seen := make(map[string]bool, len(qualityFixtures))
	groupCounts := make(map[string]int)
	for _, fixture := range qualityFixtures {
		if strings.TrimSpace(fixture.description) == "" || len(fixture.expected) == 0 {
			t.Fatalf("invalid fixture in group %q", fixture.group)
		}
		if seen[fixture.description] {
			t.Fatalf("duplicate task description %q", fixture.description)
		}
		seen[fixture.description] = true
		groupCounts[fixture.group]++
	}
	for _, group := range []string{"sequential", "list_items", "conjunction", "role_inference"} {
		if groupCounts[group] != 10 {
			t.Errorf("fixture group %q contains %d cases, want 10", group, groupCounts[group])
		}
	}
}
