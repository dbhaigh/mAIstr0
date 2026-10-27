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

func TestDecomposeRecognizesAdditionalSequentialCues(t *testing.T) {
	tests := []struct {
		name        string
		description string
	}{
		{name: "next", description: "First analyze the logs. Next write a summary."},
		{name: "finally", description: "Start by collecting sources. Finally, summarize the evidence."},
		{name: "subsequently", description: "Read the article. Subsequently, translate the quote."},
		{name: "finally after then", description: "Research the topic; then write a report; finally review it."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subtasks := Decompose(test.description)
			if len(subtasks) < 3 {
				t.Fatalf("Decompose returned %d subtasks, want work plus synthesis: %+v", len(subtasks), subtasks)
			}
			for i := 1; i < len(subtasks)-1; i++ {
				if len(subtasks[i].DependsOn) != 1 || subtasks[i].DependsOn[0] != subtasks[i-1].ID {
					t.Errorf("subtask %d dependencies = %v, want previous subtask %q",
						i, subtasks[i].DependsOn, subtasks[i-1].ID)
				}
			}
			last := subtasks[len(subtasks)-1]
			if len(last.DependsOn) != 1 || last.DependsOn[0] != subtasks[len(subtasks)-2].ID {
				t.Errorf("synthesis dependencies = %v, want final work subtask %q",
					last.DependsOn, subtasks[len(subtasks)-2].ID)
			}
		})
	}
}

func TestDecomposeV1KeepsLegacySequentialCues(t *testing.T) {
	subtasks := DecomposeV1("Research the topic. Finally, summarize the findings.")
	if len(subtasks) != 2 {
		t.Fatalf("DecomposeV1 returned %d subtasks, want 2: %+v", len(subtasks), subtasks)
	}
	if len(subtasks[1].DependsOn) != 0 {
		t.Fatalf("legacy subtask dependencies = %v, want no new sequential edge", subtasks[1].DependsOn)
	}
	if subtasks[0].Role != "general" {
		t.Fatalf("legacy subtask role = %q, want general", subtasks[0].Role)
	}
}

func TestDecomposeRecognizesSemicolonSeparatedListItems(t *testing.T) {
	tests := []struct {
		name        string
		description string
		taskTypes   []string
	}{
		{
			name:        "plain items",
			description: "Review these files: alpha.go; beta.go.",
			taskTypes:   []string{"general", "general"},
		},
		{
			name:        "numbered items",
			description: "Review these steps: 1. inspect files; 2. calculate totals.",
			taskTypes:   []string{"general", "math"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subtasks := Decompose(test.description)
			if len(subtasks) != len(test.taskTypes)+1 {
				t.Fatalf("Decompose returned %d subtasks, want %d work items plus synthesis: %+v",
					len(subtasks), len(test.taskTypes)+1, subtasks)
			}
			for i, taskType := range test.taskTypes {
				if subtasks[i].TaskType != taskType {
					t.Errorf("subtask %d type = %q, want %q", i, subtasks[i].TaskType, taskType)
				}
			}
			synthesis := subtasks[len(subtasks)-1]
			if !synthesis.Synthesis || len(synthesis.DependsOn) != len(test.taskTypes) {
				t.Errorf("synthesis subtask = %+v, want dependencies on all %d items", synthesis, len(test.taskTypes))
			}
		})
	}
}

func TestDecomposeRecognizesTopicEnumerations(t *testing.T) {
	subtasks := Decompose("Summarize these topics: climate, health, and education.")
	if len(subtasks) != 4 {
		t.Fatalf("Decompose returned %d subtasks, want 3 topics plus synthesis: %+v", len(subtasks), subtasks)
	}
	for i := 0; i < 3; i++ {
		if subtasks[i].TaskType != "summarize" {
			t.Errorf("topic subtask %d type = %q, want summarize", i, subtasks[i].TaskType)
		}
	}
	if !subtasks[3].Synthesis || len(subtasks[3].DependsOn) != 3 {
		t.Errorf("synthesis subtask = %+v, want dependencies on all topics", subtasks[3])
	}
}

func TestDecomposeSplitsConjoinedActions(t *testing.T) {
	tests := []struct {
		name        string
		description string
		taskTypes   []string
	}{
		{
			name:        "different task types",
			description: "Summarize the memo and translate the title.",
			taskTypes:   []string{"summarize", "translate"},
		},
		{
			name:        "same task type",
			description: "Implement the parser and test the edge cases.",
			taskTypes:   []string{"code", "general"},
		},
		{
			name:        "non-decomposable and",
			description: "Summarize the memo and its appendix.",
			taskTypes:   []string{"summarize"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subtasks := Decompose(test.description)
			wantCount := len(test.taskTypes)
			if wantCount > 1 {
				wantCount++
			}
			if len(subtasks) != wantCount {
				t.Fatalf("Decompose returned %d subtasks, want %d: %+v", len(subtasks), wantCount, subtasks)
			}
			for i, taskType := range test.taskTypes {
				if subtasks[i].TaskType != taskType {
					t.Errorf("subtask %d type = %q, want %q", i, subtasks[i].TaskType, taskType)
				}
				if len(subtasks[i].DependsOn) != 0 {
					t.Errorf("conjoined work subtask %d has dependencies %v, want independent work", i, subtasks[i].DependsOn)
				}
			}
			if wantCount > len(test.taskTypes) {
				synthesis := subtasks[len(subtasks)-1]
				if !synthesis.Synthesis || len(synthesis.DependsOn) != len(test.taskTypes) {
					t.Errorf("synthesis subtask = %+v, want dependencies on all work items", synthesis)
				}
			}
		})
	}
}

func TestDecomposeInfersResearcherAndReviewerRoles(t *testing.T) {
	tests := []struct {
		description string
		taskType    string
		role        string
	}{
		{description: "Research recent sources about the release.", taskType: "general", role: "researcher"},
		{description: "Investigate the cause of the test failure.", taskType: "general", role: "researcher"},
		{description: "Gather evidence about compatibility.", taskType: "general", role: "researcher"},
		{description: "Review the API design.", taskType: "code", role: "reviewer"},
		{description: "Audit the implementation plan.", taskType: "code", role: "reviewer"},
		{description: "Critique the proposed approach.", taskType: "general", role: "reviewer"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			subtasks := Decompose(test.description)
			if len(subtasks) != 1 {
				t.Fatalf("Decompose returned %d subtasks, want 1: %+v", len(subtasks), subtasks)
			}
			if subtasks[0].TaskType != test.taskType || subtasks[0].Role != test.role {
				t.Fatalf("decomposed subtask = (%q, %q), want (%q, %q)",
					subtasks[0].TaskType, subtasks[0].Role, test.taskType, test.role)
			}
		})
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
