package orchestrator

import "github.com/maistr0/maistr0/internal/scheduler"

type dispatchResult struct {
	output       string
	err          error
	failureClass string
}

type dispatchResultAssembler struct{}

func (dispatchResultAssembler) assemble(subtask scheduler.Subtask, output string) dispatchResult {
	output = normalizeTaskOutput(subtask, output)
	if err := validateTaskOutput(subtask, output); err != nil {
		return dispatchResult{output: output, err: err, failureClass: "validation"}
	}
	return dispatchResult{output: output}
}
