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
