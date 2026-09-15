// Package scheduler breaks a submitted task down into subtasks and assigns
// each subtask to the cluster node best suited to run it.
package scheduler

import (
	"regexp"
	"strings"
)

// Subtask is one unit of work produced by decomposing a submitted task.
type Subtask struct {
	ID          string   `json:"id"`
	Description string   `json:"description"`
	TaskType    string   `json:"task_type"` // e.g. code, summarize, translate, math, creative, general
	Tags        []string `json:"tags"`
}

var listItemPattern = regexp.MustCompile(`(?m)^\s*(?:[-*]|\d+[.)])\s+`)

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
		subtasks = append(subtasks, Subtask{
			ID:          idFor(i),
			Description: p,
			TaskType:    taskType,
			Tags:        tagsFor(taskType),
		})
	}
	return subtasks
}

func idFor(i int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	if i < len(letters) {
		return "sub-" + string(letters[i])
	}
	return "sub-" + string(rune('0'+i))
}

var sentenceSplit = regexp.MustCompile(`(?:\.|;)\s+`)

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
