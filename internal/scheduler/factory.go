package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
)

// SubAgentSpec is a 4-tuple style request to create a specialized worker.
type SubAgentSpec struct {
	Name                  string   `json:"name,omitempty"`                   // Optional worker name
	Role                  string   `json:"role,omitempty"`                   // Virtual role name (e.g. "code-auditor")
	Instruction           string   `json:"instruction"`                      // 1. Instruction / system prompt
	RequiredTools         []string `json:"required_tools,omitempty"`         // 2. Required tools
	RequiredCapabilities  []string `json:"required_capabilities,omitempty"`  // 3. Required model capabilities
	PreferredCapabilities []string `json:"preferred_capabilities,omitempty"` // 3b. Preferred model capabilities
	Context               string   `json:"context,omitempty"`                // 4. Relevant context
	TaskType              string   `json:"task_type,omitempty"`
	OutputFormat          string   `json:"output_format,omitempty"`
	ExpectedOutput        string   `json:"expected_output,omitempty"`
	MaxRetries            int      `json:"max_retries,omitempty"`
}

// SubAgentHandle is a handle to a specialized worker configuration that can be dispatched by the scheduler.
type SubAgentHandle struct {
	ID        string       `json:"id"`
	Spec      SubAgentSpec `json:"spec"`
	NodeID    string       `json:"node_id"`
	Address   string       `json:"address"`
	Model     string       `json:"model"`
	Score     float64      `json:"score"`
	Factors   ScoreFactors `json:"score_factors"`
	CreatedAt time.Time    `json:"created_at"`
}

// ToSubtask generates a concrete Subtask struct targeting this specialized worker.
func (h *SubAgentHandle) ToSubtask(subtaskID, description string) Subtask {
	role := h.Spec.Role
	if role == "" {
		role = h.Spec.Name
	}
	if role == "" {
		role = "general"
	}
	taskType := h.Spec.TaskType
	if taskType == "" {
		taskType = "general"
	}
	return Subtask{
		ID:                    subtaskID,
		Description:           description,
		Role:                  role,
		TaskType:              taskType,
		Tags:                  tagsFor(taskType),
		RequiredCapabilities:  append([]string(nil), h.Spec.RequiredCapabilities...),
		PreferredCapabilities: append([]string(nil), h.Spec.PreferredCapabilities...),
		ExpectedOutput:        h.Spec.ExpectedOutput,
		OutputFormat:          h.Spec.OutputFormat,
		MaxRetries:            h.Spec.MaxRetries,
	}
}

// ToAssignment constructs an Assignment pairing the subtask with the chosen node and model.
func (h *SubAgentHandle) ToAssignment(subtaskID, description string) Assignment {
	return Assignment{
		Subtask: h.ToSubtask(subtaskID, description),
		NodeID:  h.NodeID,
		Address: h.Address,
		Model:   h.Model,
		Score:   h.Score,
		Factors: h.Factors,
	}
}

// FormatPrompt constructs the execution prompt including instruction, tools, context, and prerequisite outputs.
func (h *SubAgentHandle) FormatPrompt(subtaskDescription, prereqOutputs string) string {
	var b strings.Builder
	if h.Spec.Instruction != "" {
		b.WriteString(strings.TrimSpace(h.Spec.Instruction))
		b.WriteString("\n\n")
	}
	if len(h.Spec.RequiredTools) > 0 {
		fmt.Fprintf(&b, "Available tools: %s\n\n", strings.Join(h.Spec.RequiredTools, ", "))
	}
	if h.Spec.Context != "" {
		fmt.Fprintf(&b, "Context:\n%s\n\n", strings.TrimSpace(h.Spec.Context))
	}
	if prereqOutputs != "" {
		fmt.Fprintf(&b, "%s\n\n", strings.TrimSpace(prereqOutputs))
	}
	fmt.Fprintf(&b, "Task: %s", strings.TrimSpace(subtaskDescription))
	if h.Spec.ExpectedOutput != "" {
		fmt.Fprintf(&b, "\n\nExpected output: %s", h.Spec.ExpectedOutput)
	}
	if h.Spec.OutputFormat == "json" {
		b.WriteString("\n\nReturn only valid JSON.")
	}
	return b.String()
}

// SubAgentFactory creates specialized virtual workers for the scheduler.
type SubAgentFactory struct {
	// mu protects the sequence; it is released before invoking scheduler behavior.
	mu      sync.Mutex
	seq     int
	scoring ScoringConfig
}

// NewSubAgentFactory creates a new SubAgentFactory.
func NewSubAgentFactory() *SubAgentFactory {
	return &SubAgentFactory{
		scoring: DefaultScoringConfig(),
	}
}

// Create selects the best node+model, configures the virtual worker, and registers its role.
func (f *SubAgentFactory) Create(spec SubAgentSpec, nodes []cluster.NodeStatus, exp Experience) (*SubAgentHandle, error) {
	if strings.TrimSpace(spec.Instruction) == "" {
		return nil, errors.New("subagent: instruction is required")
	}

	f.mu.Lock()
	f.seq++
	seqID := f.seq
	f.mu.Unlock()

	role := strings.ToLower(strings.TrimSpace(spec.Role))
	if role == "" && spec.Name != "" {
		role = strings.ToLower(strings.TrimSpace(spec.Name))
	}
	if role == "" {
		role = fmt.Sprintf("worker-%d", seqID)
	}
	spec.Role = role

	taskType := spec.TaskType
	if taskType == "" {
		taskType = classify(spec.Instruction)
		spec.TaskType = taskType
	}

	// Dynamically register the virtual agent profile for this specialized worker
	RegisterCustomAgentProfile(role, AgentProfile{
		Role:         role,
		TaskType:     taskType,
		Instructions: spec.Instruction,
		Tools:        spec.RequiredTools,
	})

	// Synthesize a subtask to evaluate node assignments
	probe := Subtask{
		ID:                    fmt.Sprintf("subagent-probe-%d", seqID),
		Description:           spec.Instruction,
		Role:                  role,
		TaskType:              taskType,
		RequiredCapabilities:  append([]string(nil), spec.RequiredCapabilities...),
		PreferredCapabilities: append([]string(nil), spec.PreferredCapabilities...),
	}

	assignments, err := AssignWithConfig([]Subtask{probe}, nodes, &f.scoring, exp)
	if err != nil {
		return nil, fmt.Errorf("subagent: failed to select worker node: %w", err)
	}
	if len(assignments) == 0 {
		return nil, errors.New("subagent: no candidate node matched required capabilities")
	}

	chosen := assignments[0]
	handleID := spec.Name
	if handleID == "" {
		handleID = fmt.Sprintf("subagent-%d", seqID)
	}

	handle := &SubAgentHandle{
		ID:        handleID,
		Spec:      spec,
		NodeID:    chosen.NodeID,
		Address:   chosen.Address,
		Model:     chosen.Model,
		Score:     chosen.Score,
		Factors:   chosen.Factors,
		CreatedAt: time.Now(),
	}

	return handle, nil
}
