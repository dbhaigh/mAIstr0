package scheduler

import "testing"

func TestDecomposeSplitsCommonSentenceEndingsAndKeepsUniqueIDs(t *testing.T) {
	description := "Summarize the report. Translate the result? Calculate the total!"
	subtasks := Decompose(description)
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

func TestDecomposeRecognizesExplicitDependencies(t *testing.T) {
	subtasks := Decompose("Research the topic. Then write a summary. Calculate a separate total.")
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
