package taskmgr

import "testing"

func TestActiveCountIncludesNonterminalStates(t *testing.T) {
	manager := &Manager{
		tasks: map[string]*Task{
			"pending":   {Status: StatusPending},
			"running":   {Status: StatusRunning},
			"synthesis": {Status: StatusSynthesis},
			"approval":  {Status: StatusAwaitingApproval},
			"completed": {Status: StatusCompleted},
			"failed":    {Status: StatusFailed},
			"cancelled": {Status: StatusCancelled},
		},
	}
	if got := manager.ActiveCount(); got != 4 {
		t.Fatalf("active task count = %d, want 4", got)
	}
}
