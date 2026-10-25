package scheduler

import (
	"fmt"
	"strings"
	"testing"
)

func TestDecomposeSplitsCommonSentenceEndingsAndKeepsUniqueIDs(t *testing.T) {
	description := "Summarize the report. Translate the result? Calculate the total!"
	subtasks := DecomposeV1(description)
	if len(subtasks) != 3 {
		t.Fatalf("Decompose returned %d subtasks, want 3: %+v", len(subtasks), subtasks)
	}

	wantTypes := []string{"summarize", "translate", "math"}
	for i, st := range subtasks {
		if st.TaskType != wantTypes[i] {
			t.Errorf("subtask %d type = %q, want %q", i, st.TaskType, wantTypes[i])
		}
	}

	if got := idFor(36); got != "sub-10" {
		t.Fatalf("idFor(36) = %q, want sub-10", got)
	}
}

func TestIDForIsUniqueAcrossExtendedRange(t *testing.T) {
	seen := make(map[string]bool, 64)
	for i := 0; i < 64; i++ {
		id := idFor(i)
		if seen[id] {
			t.Fatalf("idFor(%d) returned duplicate ID %q", i, id)
		}
		seen[id] = true
	}
}

func TestDecomposeRecognizesExplicitDependencies(t *testing.T) {
	subtasks := DecomposeV1("Research the topic. Then write a summary. Calculate a separate total.")
	if len(subtasks) != 3 {
		t.Fatalf("Decompose returned %d subtasks, want 3", len(subtasks))
	}
	if len(subtasks[0].DependsOn) != 0 || len(subtasks[1].DependsOn) != 1 || subtasks[1].DependsOn[0] != subtasks[0].ID {
		t.Fatalf("unexpected explicit dependency: %+v", subtasks)
	}
	if len(subtasks[2].DependsOn) != 0 {
		t.Fatalf("independent subtask should remain parallelizable: %+v", subtasks[2])
	}
}

func TestDecomposeLargeEnumerationKeepsSynthesisWithinPlanLimit(t *testing.T) {
	options := make([]string, 70)
	for i := range options {
		options[i] = fmt.Sprintf("Option-%02d", i)
	}

	subtasks := Decompose("Review these options: " + strings.Join(options, ", "))
	if len(subtasks) != maxHeuristicPlanSubtasks {
		t.Fatalf("Decompose returned %d subtasks, want %d", len(subtasks), maxHeuristicPlanSubtasks)
	}
	if !subtasks[len(subtasks)-1].Synthesis {
		t.Fatal("large fan-out plan is missing its final synthesis subtask")
	}
	if !strings.Contains(subtasks[len(subtasks)-2].Description, "Option-62") ||
		!strings.Contains(subtasks[len(subtasks)-2].Description, "Option-69") {
		t.Fatalf("compacted subtask did not preserve the remaining options: %q", subtasks[len(subtasks)-2].Description)
	}
	if _, err := ValidatePlan(subtasks); err != nil {
		t.Fatalf("large fan-out plan is invalid: %v", err)
	}
}

func TestValidatePlanRejectsInvalidDependencies(t *testing.T) {
	tests := []struct {
		name     string
		subtasks []Subtask
	}{
		{
			name: "unknown dependency",
			subtasks: []Subtask{
				{ID: "a", Description: "First", DependsOn: []string{"missing"}},
			},
		},
		{
			name: "cycle",
			subtasks: []Subtask{
				{ID: "a", Description: "First", DependsOn: []string{"b"}},
				{ID: "b", Description: "Second", DependsOn: []string{"a"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidatePlan(test.subtasks); err == nil {
				t.Fatal("ValidatePlan accepted invalid dependencies")
			}
		})
	}
}

func TestValidatePlanAddsAgentAndOutputDefaults(t *testing.T) {
	plan, err := ValidatePlan([]Subtask{{
		ID: "code", Description: "Implement a parser", TaskType: "code",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if plan[0].Role != "coder" || plan[0].OutputFormat != "text" {
		t.Fatalf("plan defaults = role %q, output format %q", plan[0].Role, plan[0].OutputFormat)
	}
}

func TestValidatePlanDefaultsJSONRepairRetry(t *testing.T) {
	plan, err := ValidatePlan([]Subtask{{
		ID: "coordinate", Description: "Return machine-readable coordination data",
		OutputFormat: "json", RequiredFields: []string{"assignments"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if plan[0].MaxRetries != 1 {
		t.Fatalf("JSON max retries = %d, want one correction retry", plan[0].MaxRetries)
	}
}

func TestValidatePlanRejectsInvalidAgentAndOutputContracts(t *testing.T) {
	tests := []struct {
		name    string
		subtask Subtask
	}{
		{name: "unknown role", subtask: Subtask{Description: "Work", Role: "made-up"}},
		{name: "required fields need JSON", subtask: Subtask{Description: "Work", RequiredFields: []string{"answer"}}},
		{name: "retry bound", subtask: Subtask{Description: "Work", MaxRetries: 3}},
		{name: "reviewer rejects unsupported format", subtask: Subtask{Description: "Review", Role: "reviewer", OutputFormat: "xml"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidatePlan([]Subtask{test.subtask}); err == nil {
				t.Fatal("ValidatePlan accepted invalid agent/output configuration")
			}
		})
	}
}
