// Package scheduler breaks a submitted task down into subtasks and assigns
// each subtask to the cluster node best suited to run it.
package scheduler

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	RoutingPreferenceQuality = "quality"
	RoutingPreferenceLatency = "latency"
	maxHeuristicPlanSubtasks = 64
)

// Subtask is one unit of work produced by decomposing a submitted task.
type Subtask struct {
	ID                    string   `json:"id"`
	Description           string   `json:"description"`
	Role                  string   `json:"role,omitempty"`
	TaskType              string   `json:"task_type"` // e.g. code, summarize, translate, math, creative, general
	Tags                  []string `json:"tags"`
	DependsOn             []string `json:"depends_on,omitempty"`
	ExpectedOutput        string   `json:"expected_output,omitempty"`
	OutputFormat          string   `json:"output_format,omitempty"`
	RequiredFields        []string `json:"required_fields,omitempty"`
	MaxRetries            int      `json:"max_retries,omitempty"` // Output-validation retries; JSON defaults to one.
	RequiredCapabilities  []string `json:"required_capabilities,omitempty"`
	PreferredCapabilities []string `json:"preferred_capabilities,omitempty"`
	MinModelSizeGB        float64  `json:"min_model_size_gb,omitempty"`
	PreferredModel        string   `json:"preferred_model,omitempty"`
	RoutingPreference     string   `json:"routing_preference,omitempty"`
	Synthesis             bool     `json:"synthesis,omitempty"`
}

// Clone creates a deep copy of the Subtask.
func (s Subtask) Clone() Subtask {
	cloned := s
	cloned.Tags = append([]string(nil), s.Tags...)
	cloned.DependsOn = append([]string(nil), s.DependsOn...)
	cloned.RequiredFields = append([]string(nil), s.RequiredFields...)
	cloned.RequiredCapabilities = append([]string(nil), s.RequiredCapabilities...)
	cloned.PreferredCapabilities = append([]string(nil), s.PreferredCapabilities...)
	return cloned
}

var listItemPattern = regexp.MustCompile(`(?m)^\s*(?:[-*]|\d+[.)])\s+`)
var dependencyPrefix = regexp.MustCompile(`(?i)^(?:then\b|after(?:wards| that)?\b|once\b|using (?:the )?(?:previous|prior|above|result)\b|based on (?:the )?(?:previous|prior|above|result)\b)`)
var sequentialDependencyPrefix = regexp.MustCompile(`(?i)^(?:then\b|next\b|after(?:wards| that)?\b|once\b|subsequently\b|thereafter\b|finally\b|lastly\b|using (?:the )?(?:previous|prior|above|result)\b|based on (?:the )?(?:previous|prior|above|result)\b)`)
var comparisonPattern = regexp.MustCompile(`(?i)^\s*compare\s+(.+?)\s+and\s+(.+?)[.!?;]*\s*$`)
var forEachPattern = regexp.MustCompile(`(?is)\bfor each(?: of)?\s+(.+?)\s*[,;:]\s*((?:analy[sz]e|assess|check|compare|contrast|evaluate|explain|inspect|measure|review|run|summari[sz]e|test|translate|verify|write|do)\b.*)$`)
var conjunctionSeparatorPattern = regexp.MustCompile(`(?i)\s+and\s+`)
var imperativeActionPattern = regexp.MustCompile(`(?i)^(?:analy[sz]e|audit|calculate|compare|compute|create|explain|implement|inspect|investigate|review|search|solve|summari[sz]e|test|translate|verify|write)\b`)
var enumerationIntroPattern = regexp.MustCompile(`(?i)\b(?:these|following|items|options|models|nodes|tasks|steps|cases|files|engines|approaches|alternatives|points|issues|questions|topics)\s*:\s*`)
var listConjunctionPattern = regexp.MustCompile(`(?i)\s*,?\s+(?:and|or)\s+`)
var finalPunctuationPattern = regexp.MustCompile(`[.!?;:]+\s*$`)
var roleCuePrefix = regexp.MustCompile(`(?i)^(?:then|next|after(?:wards| that)?|once|subsequently|thereafter|finally|lastly)\b[,:\s]+`)
var researcherActionPattern = regexp.MustCompile(`(?i)^(?:research|investigate|gather evidence|find evidence|collect sources)\b`)
var reviewerActionPattern = regexp.MustCompile(`(?i)^(?:audit|critique)\b|^review (?:it\b|(?:the|this) (?:api )?(?:design|patch|code|implementation|proposal|work|output|results?|document|report|changes)\b)`)

// Decompose creates an improved heuristic plan. Fan-out work receives a final
// synthesis subtask that depends on every leaf in the generated graph.
func Decompose(description string) []Subtask {
	description = strings.TrimSpace(description)
	if description == "" {
		return nil
	}

	if subtasks := decomposeForEach(description); len(subtasks) > 0 {
		return subtasks
	}
	if subtasks := decomposeComparison(description); len(subtasks) > 0 {
		return subtasks
	}
	if subtasks := decomposeEnumeration(description); len(subtasks) > 0 {
		return subtasks
	}
	if subtasks := decomposeConjunction(description); len(subtasks) > 0 {
		return subtasks
	}

	subtasks := DecomposeV1(description)
	applySequentialDependencies(subtasks)
	applyDescriptionRoles(subtasks)
	if len(subtasks) > 1 {
		subtasks = compactPlanForSynthesis(subtasks)
		return addSynthesisSubtask(subtasks, description)
	}
	return subtasks
}

// DecomposeV1 preserves the original sentence/list heuristic for callers that
// need the legacy plan shape.
func DecomposeV1(description string) []Subtask {
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
			Role:        roleForTaskType(taskType),
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

func decomposeForEach(description string) []Subtask {
	match := forEachPattern.FindStringSubmatch(description)
	if len(match) != 3 {
		return nil
	}
	subjects := strings.TrimSpace(match[1])
	subjects = regexp.MustCompile(`(?i)^(?:of|the following)\s+`).ReplaceAllString(subjects, "")
	items := splitEnumerationItems(subjects)
	if len(items) < 2 {
		return nil
	}
	action := strings.TrimSpace(finalPunctuationPattern.ReplaceAllString(match[2], ""))
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s for %s", action, item))
	}
	return fanOutPlan(parts, description)
}

func decomposeComparison(description string) []Subtask {
	match := comparisonPattern.FindStringSubmatch(description)
	if len(match) != 3 {
		return nil
	}
	first := strings.TrimSpace(match[1])
	second := strings.TrimSpace(match[2])
	if first == "" || second == "" {
		return nil
	}
	return fanOutPlan([]string{
		fmt.Sprintf("Analyze %s to compare it with %s for the original request: %s", first, second, description),
		fmt.Sprintf("Analyze %s to compare it with %s for the original request: %s", second, first, description),
	}, description)
}

func decomposeConjunction(description string) []Subtask {
	for _, separator := range conjunctionSeparatorPattern.FindAllStringIndex(description, -1) {
		first := strings.TrimSpace(description[:separator[0]])
		second := strings.TrimSpace(description[separator[1]:])
		if !imperativeActionPattern.MatchString(first) || !imperativeActionPattern.MatchString(second) {
			continue
		}
		first = strings.TrimSpace(finalPunctuationPattern.ReplaceAllString(first, ""))
		second = strings.TrimSpace(finalPunctuationPattern.ReplaceAllString(second, ""))
		return fanOutPlan([]string{first, second}, description)
	}
	return nil
}

func decomposeEnumeration(description string) []Subtask {
	if match := listItemPattern.FindStringIndex(description); match != nil {
		matches := listItemPattern.FindAllStringIndex(description, -1)
		if len(matches) >= 2 {
			prefix := strings.TrimSpace(description[:matches[0][0]])
			items := make([]string, 0, len(matches))
			for i, marker := range matches {
				end := len(description)
				if i+1 < len(matches) {
					end = matches[i+1][0]
				}
				item := strings.TrimSpace(description[marker[1]:end])
				if item != "" {
					items = append(items, item)
				}
			}
			return enumerationPlan(prefix, items, description)
		}
	}

	match := enumerationIntroPattern.FindStringIndex(description)
	if match == nil {
		return nil
	}
	prefix := strings.TrimSpace(description[:match[1]])
	items := splitEnumerationItems(description[match[1]:])
	return enumerationPlan(prefix, items, description)
}

func enumerationPlan(prefix string, items []string, original string) []Subtask {
	if len(items) < 2 {
		return nil
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if prefix != "" {
			parts = append(parts, prefix+" "+item)
		} else {
			parts = append(parts, item)
		}
	}
	return fanOutPlan(parts, original)
}

func splitEnumerationItems(value string) []string {
	value = strings.TrimSpace(finalPunctuationPattern.ReplaceAllString(value, ""))
	value = strings.ReplaceAll(value, ";", ",")
	value = listConjunctionPattern.ReplaceAllString(value, ",")
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.Trim(part, " \t\r\n,"))
		if part != "" {
			items = append(items, part)
		}
	}
	return items
}

func fanOutPlan(parts []string, description string) []Subtask {
	subtasks := make([]Subtask, 0, len(parts)+1)
	for i, part := range parts {
		taskType := classify(part)
		subtasks = append(subtasks, Subtask{
			ID: idFor(i), Description: part, Role: roleForDescription(part, taskType),
			TaskType: taskType, Tags: tagsFor(taskType),
		})
	}
	subtasks = compactPlanForSynthesis(subtasks)
	return addSynthesisSubtask(subtasks, description)
}

func compactPlanForSynthesis(subtasks []Subtask) []Subtask {
	if len(subtasks) < maxHeuristicPlanSubtasks {
		return subtasks
	}
	keepCount := maxHeuristicPlanSubtasks - 2
	keptIDs := make(map[string]bool, keepCount)
	for _, subtask := range subtasks[:keepCount] {
		keptIDs[subtask.ID] = true
	}
	descriptions := make([]string, 0, len(subtasks)-keepCount)
	dependencies := make([]string, 0, keepCount)
	dependencySet := make(map[string]bool, keepCount)
	for _, subtask := range subtasks[keepCount:] {
		descriptions = append(descriptions, subtask.Description)
		for _, dependency := range subtask.DependsOn {
			if keptIDs[dependency] && !dependencySet[dependency] {
				dependencySet[dependency] = true
				dependencies = append(dependencies, dependency)
			}
		}
	}
	description := "Complete these remaining steps in order:\n- " + strings.Join(descriptions, "\n- ")
	taskType := classify(description)
	merged := Subtask{
		ID: subtasks[keepCount].ID, Description: description, Role: roleForDescription(description, taskType),
		TaskType: taskType, Tags: tagsFor(taskType), DependsOn: dependencies,
	}
	compacted := make([]Subtask, 0, maxHeuristicPlanSubtasks-1)
	compacted = append(compacted, subtasks[:keepCount]...)
	return append(compacted, merged)
}

func addSynthesisSubtask(subtasks []Subtask, description string) []Subtask {
	if len(subtasks) == 0 || len(subtasks) >= maxHeuristicPlanSubtasks {
		return subtasks
	}
	dependedOn := make(map[string]bool, len(subtasks))
	for _, subtask := range subtasks {
		for _, dependency := range subtask.DependsOn {
			dependedOn[dependency] = true
		}
	}
	dependencies := make([]string, 0, len(subtasks))
	for _, subtask := range subtasks {
		if !dependedOn[subtask.ID] {
			dependencies = append(dependencies, subtask.ID)
		}
	}
	id := "synthesis"
	usedIDs := make(map[string]bool, len(subtasks))
	for _, subtask := range subtasks {
		usedIDs[subtask.ID] = true
	}
	for suffix := 2; usedIDs[id]; suffix++ {
		id = fmt.Sprintf("synthesis-%d", suffix)
	}
	return append(subtasks, Subtask{
		ID: id, Description: "Synthesize the completed results into a coherent answer to the original task: " + strings.TrimSpace(description),
		Role: "summarizer", TaskType: "summarize", Tags: tagsFor("summarize"),
		DependsOn: dependencies, ExpectedOutput: "A concise integrated response addressing the original task.",
		Synthesis: true,
	})
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
		subtask.RoutingPreference = strings.ToLower(strings.TrimSpace(subtask.RoutingPreference))
		if subtask.RoutingPreference != "" && subtask.RoutingPreference != RoutingPreferenceQuality &&
			subtask.RoutingPreference != RoutingPreferenceLatency {
			return nil, errors.New("scheduler: routing_preference must be quality or latency")
		}
		if subtask.TaskType == "" {
			subtask.TaskType = classify(subtask.Description)
		}
		normalized, err := validateSubtask(subtask)
		if err != nil {
			return nil, err
		}
		subtask = normalized
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

func applySequentialDependencies(subtasks []Subtask) {
	for i := 1; i < len(subtasks); i++ {
		if sequentialDependencyPrefix.MatchString(subtasks[i].Description) {
			subtasks[i].DependsOn = []string{subtasks[i-1].ID}
		}
	}
}

func applyDescriptionRoles(subtasks []Subtask) {
	for i := range subtasks {
		subtasks[i].Role = roleForDescription(subtasks[i].Description, subtasks[i].TaskType)
	}
}

func roleForDescription(description, taskType string) string {
	description = roleCuePrefix.ReplaceAllString(strings.TrimSpace(description), "")
	switch {
	case researcherActionPattern.MatchString(description):
		return "researcher"
	case reviewerActionPattern.MatchString(description):
		return "reviewer"
	default:
		return roleForTaskType(taskType)
	}
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
	if i < 36 {
		return "sub-0" + strconv.FormatInt(int64(i-len(letters)), 36)
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
