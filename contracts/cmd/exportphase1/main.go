package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type request struct {
	Method   string           `json:"method"`
	Path     string           `json:"path"`
	Body     json.RawMessage  `json:"body,omitempty"`
	Setup    string           `json:"setup,omitempty"`
	Variants []requestVariant `json:"variants,omitempty"`
}

type requestVariant struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type fixture struct {
	Name               string          `json:"name"`
	Request            request         `json:"request"`
	ExpectedResponse   json.RawMessage `json:"expected_response"`
	NormalizationRules []string        `json:"normalization_rules"`
}

func main() {
	sourceDir := filepath.Join("..", "internal", "orchestrator", "testdata", "contracts")
	outputDir := filepath.Join("fixtures", "phase1")
	requests := phase1Requests()
	sourceFiles, err := filepath.Glob(filepath.Join(sourceDir, "*.json"))
	if err != nil {
		fail(fmt.Errorf("list source fixtures: %w", err))
	}
	if len(sourceFiles) != len(requests) {
		fail(fmt.Errorf("source fixture count is %d but request metadata covers %d cases", len(sourceFiles), len(requests)))
	}
	for _, sourcePath := range sourceFiles {
		name := strings.TrimSuffix(filepath.Base(sourcePath), ".json")
		if _, ok := requests[name]; !ok {
			fail(fmt.Errorf("source fixture %s has no request metadata", filepath.Base(sourcePath)))
		}
	}
	for name, request := range requests {
		filename := name + ".json"
		expected, err := os.ReadFile(filepath.Join(sourceDir, filename))
		if err != nil {
			fail(fmt.Errorf("read source fixture %s: %w", filename, err))
		}
		if !json.Valid(expected) {
			fail(fmt.Errorf("source fixture %s is not valid JSON", filename))
		}
		rules := jsonNormalizationRules
		if strings.HasPrefix(name, "compat-") && strings.Contains(name, "-stream") {
			rules = streamNormalizationRules
		}
		encoded, err := json.MarshalIndent(fixture{
			Name: name, Request: request, ExpectedResponse: expected, NormalizationRules: rules,
		}, "", "  ")
		if err != nil {
			fail(fmt.Errorf("marshal exported fixture %s: %w", filename, err))
		}
		encoded = append(encoded, '\n')
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			fail(err)
		}
		if err := os.WriteFile(filepath.Join(outputDir, filename), encoded, 0o644); err != nil {
			fail(fmt.Errorf("write exported fixture %s: %w", filename, err))
		}
	}
}

var jsonNormalizationRules = []string{
	"Replace timestamps with <TIME>.",
	"Replace generated task and checkpoint IDs with <TASK_ID> and <CHECKPOINT_ID>.",
	"Replace local worker URLs with <WORKER_URL>.",
	"Normalize volatile scores, task origins, and sequences as documented by the Go contract tests.",
}

var streamNormalizationRules = []string{
	"Replace local worker URLs with <WORKER_URL>.",
	"Normalize created timestamps and duration values to zero.",
	"Keep event ordering, event names, payload shapes, and terminal events unchanged.",
}

func phase1Requests() map[string]request {
	jsonBody := func(value string) json.RawMessage { return json.RawMessage(value) }
	return map[string]request{
		"compat-anthropic-messages": {
			Method: "POST", Path: "/v1/messages",
			Body: jsonBody(`{"model":"simulated-general","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`),
		},
		"compat-anthropic-messages-stream": {
			Method: "POST", Path: "/v1/messages",
			Body: jsonBody(`{"model":"simulated-general","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hello"}]}`),
		},
		"compat-ollama-chat": {
			Method: "POST", Path: "/api/chat",
			Body: jsonBody(`{"model":"simulated-general","messages":[{"role":"user","content":"hello"}],"stream":false}`),
		},
		"compat-ollama-chat-stream": {
			Method: "POST", Path: "/api/chat",
			Body: jsonBody(`{"model":"simulated-general","stream":true,"messages":[{"role":"user","content":"hello"}]}`),
		},
		"compat-ollama-generate": {
			Method: "POST", Path: "/api/generate",
			Body: jsonBody(`{"model":"simulated-general","prompt":"hello","stream":false}`),
		},
		"compat-ollama-generate-stream": {
			Method: "POST", Path: "/api/generate",
			Body: jsonBody(`{"model":"simulated-general","stream":true,"prompt":"hello"}`),
		},
		"compat-ollama-tags": {Method: "GET", Path: "/api/tags"},
		"compat-openai-chat": {
			Method: "POST", Path: "/v1/chat/completions",
			Body: jsonBody(`{"model":"simulated-general","messages":[{"role":"user","content":"hello"}]}`),
		},
		"compat-openai-chat-stream": {
			Method: "POST", Path: "/v1/chat/completions",
			Body: jsonBody(`{"model":"simulated-general","stream":true,"messages":[{"role":"user","content":"hello"}]}`),
		},
		"system-status": {Method: "GET", Path: "/api/status"},
		"task-approved": {
			Method: "POST", Path: "/api/tasks/{id}/approve",
			Setup: "Create a task with require_approval=true, wait for awaiting_approval, then submit this request.",
		},
		"task-awaiting-approval": {
			Method: "GET", Path: "/api/tasks/{id}",
			Setup: "Create a task with require_approval=true and wait for awaiting_approval.",
		},
		"task-cancelled": {
			Method: "POST", Path: "/api/tasks/{id}/cancel",
			Setup: "Create a task with a worker blocked in execution, then cancel it.",
		},
		"task-checkpoints-completed": {
			Method: "GET", Path: "/api/tasks/{id}/checkpoints",
			Setup: "Create and complete a task, then read its checkpoints.",
		},
		"task-checkpoints-initial": {
			Method: "GET", Path: "/api/tasks/{id}/checkpoints",
			Setup: "Create a task and read its initial checkpoint history.",
		},
		"task-create-explicit-graph": {
			Method: "POST", Path: "/api/tasks",
			Body: jsonBody(`{"description":"Graph workflow","graph":{"nodes":{"prepare":{"id":"prepare","description":"Prepare the source","role":"general","task_type":"general","tags":null},"summarize":{"id":"summarize","description":"Summarize the source","role":"general","task_type":"general","tags":null,"depends_on":["prepare"]}},"edges":[{"from":"prepare","to":"summarize"}]}}`),
		},
		"task-create-explicit-subtasks": {
			Method: "POST", Path: "/api/tasks",
			Body: jsonBody(`{"description":"Two-step workflow","subtasks":[{"id":"prepare","description":"Prepare the source","task_type":"general"},{"id":"summarize","description":"Summarize the source","task_type":"general","depends_on":["prepare"]}]}`),
		},
		"task-create-heuristic": {
			Method: "POST", Path: "/api/tasks",
			Body: jsonBody(`{"description":"Summarize this report"}`),
		},
		"task-dependencies": {
			Method: "GET", Path: "/api/tasks/{id}",
			Setup: "Create a task with prepare followed by follow-up and wait for completion.",
		},
		"task-failed": {
			Method: "GET", Path: "/api/tasks/{id}",
			Setup: "Create a task with a worker returning an execution error, then wait for the task to fail.",
		},
		"task-get-explicit-graph": {
			Method: "GET", Path: "/api/tasks/{id}",
			Setup: "POST the explicit graph workflow and wait for completion.",
		},
		"task-get-explicit-subtasks": {
			Method: "GET", Path: "/api/tasks/{id}",
			Setup: "POST the explicit subtasks workflow and wait for completion.",
		},
		"task-get-heuristic": {
			Method: "GET", Path: "/api/tasks/{id}",
			Setup: "POST a heuristic task and wait for completion.",
		},
		"task-invalid-plan": {
			Method: "POST", Path: "/api/tasks",
			Body: jsonBody(`{"description":"Missing dependency","subtasks":[{"id":"work","description":"Work","depends_on":["missing"]}]}`),
		},
		"task-list": {Method: "GET", Path: "/api/tasks", Setup: "Create and complete a heuristic task first."},
		"task-no-healthy-nodes": {
			Method: "POST", Path: "/api/tasks",
			Body: jsonBody(`{"description":"No worker","subtasks":[{"id":"work","description":"Work","task_type":"general"}]}`),
		},
		"task-not-found": {
			Method: "GET", Path: "/api/tasks/unknown",
			Variants: []requestVariant{
				{Method: "POST", Path: "/api/tasks/unknown/cancel"},
				{Method: "POST", Path: "/api/tasks/unknown/approve"},
				{Method: "POST", Path: "/api/tasks/unknown/resume"},
				{Method: "GET", Path: "/api/tasks/unknown/checkpoints"},
			},
		},
		"task-resumable": {Method: "GET", Path: "/api/tasks/resumable"},
		"task-resume": {
			Method: "POST", Path: "/api/tasks/{id}/resume",
			Setup: "Create a dependent task, complete its first subtask, cancel it, then resume it.",
		},
		"task-review": {
			Method: "GET", Path: "/api/tasks/{id}",
			Setup: "Create a task with require_review=true and wait for completion.",
		},
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
