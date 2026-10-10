// Package scheduler breaks a submitted task down into subtasks and assigns
// each subtask to the cluster node best suited to run it.
package scheduler

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

// Subtask is one unit of work produced by decomposing a submitted task.
type Subtask struct {
	ID          string   `json:"id"`
	Description string   `json:"description"`
	TaskType    string   `json:"task_type"` // e.g. code, summarize, translate, math, creative, general
	Tags        []string `json:"tags"`
	DependsOn   []string `json:"depends_on,omitempty"`
}

var listItemPattern = regexp.MustCompile(`(?m)^\s*(?:[-*]|\d+[.)])\s+`)
var dependencyPrefix = regexp.MustCompile(`(?i)^(?:then\b|after(?:wards| that)?\b|once\b|using (?:the )?(?:previous|prior|above|result)\b|based on (?:the )?(?:previous|prior|above|result)\b)`)

// Decompose splits a free-form task description into an ordered list of
// subtasks. It recognises explicit numbered/bulleted lists first; failing
// that, it falls back to splitting on sentence boundaries when the task
// reads as multiple distinct instructions, otherwise it returns the whole
// description as a single subtask.
func Decompose(description string) []Subtask {
	description = strings.TrimSpace(description)
	if description == "" {
		return nil
	}

	var parts []string
	if listItemPattern.MatchString(description) {
		items := listItemPattern.Split(description, -1)
		for _, it := range items {
			it = strings.TrimSpace(it)
			if it != "" {
				parts = append(parts, it)
			}
		}
	}

	if len(parts) == 0 {
		sentences := splitSentences(description)
		if len(sentences) > 1 {
			parts = sentences
		} else {
			parts = []string{description}
		}
	}

	subtasks := make([]Subtask, 0, len(parts))
	for i, p := range parts {
		taskType := classify(p)
		subtask := Subtask{
			ID:          idFor(i),
			Description: p,
			TaskType:    taskType,
			Tags:        tagsFor(taskType),
		}
		if i > 0 && dependencyPrefix.MatchString(p) {
			subtask.DependsOn = []string{idFor(i - 1)}
		}
		subtasks = append(subtasks, subtask)
	}
	return subtasks
}

// ValidatePlan normalizes caller-provided subtasks and verifies that their
// dependency graph refers only to known subtasks and contains no cycles.
func ValidatePlan(subtasks []Subtask) ([]Subtask, error) {
	if len(subtasks) == 0 {
		return nil, errors.New("scheduler: plan must contain at least one subtask")
	}
	if len(subtasks) > 64 {
		return nil, errors.New("scheduler: plan cannot contain more than 64 subtasks")
	}

	out := make([]Subtask, len(subtasks))
	positions := make(map[string]int, len(subtasks))
	for i, subtask := range subtasks {
		subtask.ID = strings.TrimSpace(subtask.ID)
		if subtask.ID == "" {
			subtask.ID = idFor(i)
		}
		if _, exists := positions[subtask.ID]; exists {
			return nil, errors.New("scheduler: duplicate subtask id " + subtask.ID)
		}
		subtask.Description = strings.TrimSpace(subtask.Description)
		if subtask.Description == "" {
			return nil, errors.New("scheduler: subtask " + subtask.ID + " has no description")
		}
		subtask.TaskType = strings.ToLower(strings.TrimSpace(subtask.TaskType))
		if subtask.TaskType == "" {
			subtask.TaskType = classify(subtask.Description)
		}
		if len(subtask.Tags) == 0 {
			subtask.Tags = tagsFor(subtask.TaskType)
		} else {
			subtask.Tags = append([]string(nil), subtask.Tags...)
		}
		subtask.DependsOn = uniqueStrings(subtask.DependsOn)
		positions[subtask.ID] = i
		out[i] = subtask
	}

	for _, subtask := range out {
		for _, dependency := range subtask.DependsOn {
			if dependency == subtask.ID {
				return nil, errors.New("scheduler: subtask " + subtask.ID + " cannot depend on itself")
			}
			if _, exists := positions[dependency]; !exists {
				return nil, errors.New("scheduler: subtask " + subtask.ID + " depends on unknown subtask " + dependency)
			}
		}
	}

	state := make(map[string]uint8, len(out))
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return errors.New("scheduler: plan contains a dependency cycle at " + id)
		case 2:
			return nil
		}
		state[id] = 1
		for _, dependency := range out[positions[id]].DependsOn {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for _, subtask := range out {
		if err := visit(subtask.ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func idFor(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	if i < len(letters) {
		return "sub-" + string(letters[i])
	}
	return "sub-" + strconv.FormatInt(int64(i), 36)
}

var sentenceSplit = regexp.MustCompile(`[.!?;]\s+`)

func splitSentences(s string) []string {
	raw := sentenceSplit.Split(s, -1)
	var out []string
	for _, r := range raw {
		r = strings.TrimSpace(r)
		if r != "" {
			out = append(out, r)
		}
	}
	return out
}

// classify makes a keyword-based guess at what kind of work a subtask
// represents, used to pick a suitably-tagged model on a node.
func classify(desc string) string {
	d := strings.ToLower(desc)
	switch {
	case containsAny(d, "code", "function", "bug", "implement", "refactor", "compile", "script", "api"):
		return "code"
	case containsAny(d, "summarize", "summary", "tl;dr", "condense"):
		return "summarize"
	case containsAny(d, "translate", "translation"):
		return "translate"
	case containsAny(d, "solve", "calculate", "equation", "math", "compute"):
		return "math"
	case containsAny(d, "image", "photo", "picture", "diagram", "screenshot"):
		return "vision"
	case containsAny(d, "story", "poem", "write a", "creative", "imagine"):
		return "creative"
	default:
		return "general"
	}
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}

func tagsFor(taskType string) []string {
	if taskType == "general" {
		return []string{"general"}
	}
	return []string{taskType, "general"}
}
