package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

// ScoreFactors aliases score.ScoreFactors and exposes the score components
// explaining an Assignment.
type ScoreFactors = score.ScoreFactors

// ScoringConfig aliases score.ScoringConfig and configures the additive
// weights used by AssignWithConfig. A nil config selects the defaults.
type ScoringConfig = score.ScoringConfig

// DefaultScoringConfig returns a value populated with the scheduler's default
// scoring weights. Callers may customize the value before passing its address
// to AssignWithConfig.
//
// Input contract: this function takes no input.
// Output contract: a caller-owned ScoringConfig value; no shared config is
// mutated.
// Failure modes: none.
//
// Example:
//
//	cfg := scheduler.DefaultScoringConfig()
//	cfg.LatencyPreferenceWeight = 20
//	assignments, err := scheduler.AssignWithConfig(subtasks, nodes, &cfg, nil)
//
// See [assign_test.go](assign_test.go) and [contracts_test.go](contracts_test.go).
func DefaultScoringConfig() ScoringConfig {
	return score.DefaultScoringConfig()
}

// Assignment is the scheduler's result for one Subtask. It identifies the
// selected node, address, model, and engine and includes the score and its
// explanatory ScoreFactors.
type Assignment struct {
	Subtask Subtask      `json:"subtask"`
	NodeID  string       `json:"node_id"`
	Address string       `json:"address"`
	Model   string       `json:"model"`
	Engine  string       `json:"engine,omitempty"`
	Score   float64      `json:"score"`
	Factors ScoreFactors `json:"score_factors"`
}

// ErrNoHealthyNodes is returned when no supplied node is both healthy and
// non-draining. Callers can test it with errors.Is.
var ErrNoHealthyNodes = errors.New("scheduler: no healthy nodes available")

// UnassignableSubtasksError reports subtasks for which no eligible node has a
// ready model satisfying the task's model and capability constraints.
// Assignment is all-or-nothing: assignment functions return no partial results
// when this error occurs. Use errors.As to inspect SubtaskIDs and any
// ModelRejections.
type UnassignableSubtasksError struct {
	SubtaskIDs      []string
	ModelRejections []score.ModelEligibilityRejection
}

// Error formats the unassignable subtask IDs and available model-rejection
// details.
//
// Output contract: a human-readable, non-empty error message for a non-nil
// receiver. NPU rejection details are included when present.
// Failure modes: none.
//
// Example:
//
//	var unassignable *scheduler.UnassignableSubtasksError
//	if errors.As(err, &unassignable) {
//	    message := unassignable.Error()
//	    _ = message
//	}
func (e *UnassignableSubtasksError) Error() string {
	message := "scheduler: no compatible model for subtasks: " + strings.Join(e.SubtaskIDs, ", ")
	if len(e.ModelRejections) == 0 {
		return message
	}
	details := make([]string, 0, len(e.ModelRejections))
	for _, rejection := range e.ModelRejections {
		details = append(details, rejection.Model+" on node "+rejection.NodeID+": "+strings.Join(rejection.Reasons, ", "))
	}
	return message + "; NPU model rejections: " + strings.Join(details, "; ")
}

// Experience supplies learned routing adjustments for a node, model, and task
// pairing. It is optional; pass nil when no history is available. An
// implementation may also satisfy CostExperience to provide throughput data.
type Experience interface {
	// LearnedBias returns the learned additive adjustment for the specified
	// node, model, and task type.
	//
	// Caller contract: use identifiers consistent with the supplied node and
	// subtask snapshots; return zero when no history is available.
	// Output contract: the value is weighted by ScoringConfig.LearnedBiasWeight.
	// Failure modes: the method has no error return.
	//
	// Example:
	//
	//     bias := history.LearnedBias("worker-a", "general-model", "code")
	//     _ = bias
	LearnedBias(nodeID, model, taskType string) float64
}

// CostExperience is an optional Experience extension that supplies historical
// generation throughput for scoring latency preferences.
type CostExperience interface {
	// TokensPerSecond returns the historical generation rate for a node, model,
	// and task type.
	//
	// Caller contract: use identifiers consistent with the supplied node and
	// subtask snapshots; return a nonpositive value when no usable history
	// exists.
	// Output contract: nonpositive values use the scheduler's model-size
	// fallback estimate.
	// Failure modes: the method has no error return.
	//
	// Example:
	//
	//     rate := history.TokensPerSecond("worker-a", "general-model", "code")
	//     _ = rate
	TokensPerSecond(nodeID, model, taskType string) float64
}

// Assign chooses the best node and model for each subtask using default scoring
// and no learned experience.
//
// Caller contract: subtasks describe the intended work, and nodes is a
// consistent point-in-time NodeStatus snapshot with current health, endpoint,
// engine/model inventory, load, and telemetry. The scheduler filters unhealthy
// and draining nodes and models that are not ready or do not satisfy the
// subtask's constraints; it does not refresh node state. Provide a valid
// dependency plan when dependency order matters. If flat-plan normalization
// fails, Assign falls back to the original subtask order without graph-aware
// scheduling.
// Output contract: one Assignment per subtask, in dependency order when the
// plan is valid. On any error, no partial assignments are returned.
// Failure modes: ErrNoHealthyNodes when every supplied node is unhealthy or
// draining; *UnassignableSubtasksError when any subtask has no eligible
// node/model.
//
// Example:
//
//	assignments, err := scheduler.Assign(subtasks, nodeSnapshot)
//	if errors.Is(err, scheduler.ErrNoHealthyNodes) {
//	    return err
//	}
//	var unassignable *scheduler.UnassignableSubtasksError
//	if errors.As(err, &unassignable) {
//	    return fmt.Errorf("cannot schedule %v: %w", unassignable.SubtaskIDs, err)
//	}
//	if err != nil {
//	    return err
//	}
//	_ = assignments
//
// Performance note: scoring examines each subtask against healthy nodes and
// their advertised models, approximately O(S×(N+M)) for S subtasks, N nodes,
// and M total models.
//
// See [assign_test.go](assign_test.go) and [contracts_test.go](contracts_test.go).
func Assign(subtasks []Subtask, nodes []cluster.NodeStatus) ([]Assignment, error) {
	return AssignWithExperience(subtasks, nodes, nil)
}

// AssignWithExperience chooses assignments using default scoring adjusted by
// optional learned routing history.
//
// Caller contract: subtasks and nodes follow Assign's contract; exp may be nil,
// otherwise it must return useful learned bias for the supplied identifiers.
// If exp also implements CostExperience, its throughput values inform latency
// scoring.
// Output contract: the same all-or-nothing assignments and ordering as Assign.
// Failure modes: ErrNoHealthyNodes or *UnassignableSubtasksError, with no
// partial assignments.
//
// Example:
//
//	assignments, err := scheduler.AssignWithExperience(subtasks, nodeSnapshot, history)
//	if err != nil {
//	    return err
//	}
//	_ = assignments
//
// Performance note: the candidate scan has the same O(S×(N+M)) cost as Assign;
// experience lookups are performed for eligible node/model/subtask candidates.
//
// See [assign_test.go](assign_test.go) and [contracts_test.go](contracts_test.go).
func AssignWithExperience(subtasks []Subtask, nodes []cluster.NodeStatus, exp Experience) ([]Assignment, error) {
	cfg := DefaultScoringConfig()
	return AssignWithConfig(subtasks, nodes, &cfg, exp)
}

// AssignWithConfig chooses assignments using cfg and optional learned
// experience. A nil cfg selects DefaultScoringConfig; non-nil values are used
// as supplied and are not range-validated.
//
// Caller contract: subtasks and nodes follow Assign's contract; cfg must remain
// stable during the call; exp may be nil. Supply a valid acyclic dependency
// plan for graph-aware ordering. If flat-plan normalization fails, the
// scheduler falls back to the original subtask order rather than returning
// that normalization error.
// Output contract: one Assignment per subtask in dependency order when
// normalization succeeds; errors never include partial assignments.
// Failure modes: ErrNoHealthyNodes or *UnassignableSubtasksError. Invalid
// scoring weights are accepted and can produce undesirable ranking rather
// than an error.
//
// Example:
//
//	cfg := scheduler.DefaultScoringConfig()
//	cfg.LearnedBiasWeight = 0.5
//	assignments, err := scheduler.AssignWithConfig(subtasks, nodeSnapshot, &cfg, history)
//	if err != nil {
//	    return err
//	}
//	_ = assignments
//
// Performance note: candidate scoring is approximately O(S×(N+M)) for S
// subtasks, N nodes, and M total advertised models.
//
// See [assign_test.go](assign_test.go) and [contracts_test.go](contracts_test.go).
func AssignWithConfig(subtasks []Subtask, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	g, err := TaskGraphFromSubtasks(subtasks)
	if err != nil {
		// Fall back to direct subtask list if normalization fails
		return assignSubtaskList(subtasks, nil, nodes, cfg, exp)
	}
	return AssignGraph(g, nodes, cfg, exp)
}

// AssignGraph assigns subtasks from a non-nil TaskGraph using its full
// dependency topology. A nil cfg selects default scoring; exp is optional.
//
// Caller contract: g must be a valid graph containing 1–64 nodes, with
// dependencies referring to existing nodes and no self-dependencies or cycles.
// nodes must be a consistent point-in-time NodeStatus snapshot; the scheduler
// filters unhealthy, draining, and model-ineligible nodes. Topological ordering
// synchronizes node DependsOn values into graph edges, so pass a graph that may
// be normalized in place.
// Output contract: one Assignment per graph node in topological order; no
// partial assignments are returned on error.
// Failure modes: graph-validation errors (plain error), ErrNoHealthyNodes, or
// *UnassignableSubtasksError.
//
// Example:
//
//	graph := scheduler.NewTaskGraph()
//	if err := graph.AddNode(scheduler.Subtask{
//	    ID: "summarize", Description: "Summarize the report", TaskType: "general",
//	}); err != nil {
//	    return err
//	}
//	assignments, err := scheduler.AssignGraph(graph, nodeSnapshot, nil, nil)
//	if err != nil {
//	    return err
//	}
//	_ = assignments
//
// Performance note: candidate scoring is approximately O(S×(N+M)) for S
// graph nodes, N nodes, and M total advertised models, plus graph validation
// and topological ordering.
//
// See [graph_test.go](graph_test.go), [assign_test.go](assign_test.go), and
// [contracts_test.go](contracts_test.go).
func AssignGraph(g *TaskGraph, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	if cfg == nil {
		c := DefaultScoringConfig()
		cfg = &c
	}
	sortedSubtasks, err := g.ToSubtasks()
	if err != nil {
		return nil, err
	}
	return assignSubtaskList(sortedSubtasks, g, nodes, cfg, exp)
}

type simulatedNodeLoad struct {
	active            int
	pendingDependents int
}

func assignSubtaskList(subtasks []Subtask, graph *TaskGraph, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	if cfg == nil {
		c := DefaultScoringConfig()
		cfg = &c
	}
	healthy := make([]*cluster.NodeStatus, 0, len(nodes))
	for i := range nodes {
		n := &nodes[i]
		if n.Healthy && !n.Draining {
			healthy = append(healthy, n)
		} else if n.Draining {
			LogDecision("planning", "draining", *n, "", 0, ScoreFactors{})
		} else {
			LogDecision("planning", "unhealthy", *n, "", 0, ScoreFactors{})
		}
	}
	if len(healthy) == 0 {
		return nil, ErrNoHealthyNodes
	}

	simulatedLoad := make(map[string]simulatedNodeLoad, len(healthy))
	capabilityRanges := make([][2]int, len(healthy))
	capabilities := make([]string, 0, len(healthy))
	for i, n := range healthy {
		start := len(capabilities)
		capabilities = n.AppendEffectiveCapabilities(capabilities)
		capabilityRanges[i] = [2]int{start, len(capabilities)}
	}
	nodeCapabilities := make([][]string, len(healthy))
	for i, n := range healthy {
		simulatedLoad[n.ID] = simulatedNodeLoad{active: n.ActiveTasks + n.QueuedTasks}
		nodeCapabilities[i] = capabilities[capabilityRanges[i][0]:capabilityRanges[i][1]]
	}

	assignments := make([]Assignment, 0, len(subtasks))
	var unassignable []string
	var modelRejections []score.ModelEligibilityRejection
	var healthyValues []cluster.NodeStatus
	now := time.Now()

	for _, st := range subtasks {
		outDegree := 0
		if graph != nil {
			outDegree = graph.OutDegree(st.ID)
		}

		logger := slog.Default()
		if logger.Enabled(context.Background(), slog.LevelDebug) {
			logger = logger.With("decision_seq", decisionSequence.Add(1))
		}
		bestNode, bestModel, bestEngine, bestScore, factors := pickBest(st, outDegree, healthy, nodeCapabilities, simulatedLoad, exp, cfg, now, logger)
		if bestNode == nil {
			unassignable = append(unassignable, st.ID)
			if healthyValues == nil {
				healthyValues = make([]cluster.NodeStatus, len(healthy))
				for i, node := range healthy {
					healthyValues[i] = *node
				}
			}
			modelRejections = append(
				modelRejections,
				score.ExplainNPUModelRejections(healthyValues, toScoringSubtask(st, outDegree))...,
			)
			continue
		}

		load := simulatedLoad[bestNode.ID]
		load.active++
		if graph != nil {
			load.pendingDependents += outDegree
		}
		simulatedLoad[bestNode.ID] = load

		logDecision(logger, slog.LevelInfo, "planning", "selected", *bestNode,
			bestModel, bestScore, factors, score.Eligibility{})

		assignments = append(assignments, Assignment{
			Subtask: st,
			NodeID:  bestNode.ID,
			Address: bestNode.Address,
			Model:   bestModel,
			Engine:  bestEngine,
			Score:   bestScore,
			Factors: factors,
		})
	}

	if len(unassignable) > 0 {
		return nil, &UnassignableSubtasksError{
			SubtaskIDs: unassignable, ModelRejections: modelRejections,
		}
	}
	return assignments, nil
}

func pickBest(st Subtask, outDegree int, nodes []*cluster.NodeStatus, nodeCapabilities [][]string, load map[string]simulatedNodeLoad, exp Experience, cfg *ScoringConfig, now time.Time, logger *slog.Logger) (*cluster.NodeStatus, string, string, float64, ScoreFactors) {
	var best *cluster.NodeStatus
	bestScore := math.Inf(-1)
	bestBias := 0.0
	bestThroughput := 0.0
	bestLoad := simulatedNodeLoad{}
	var bestCapabilities []string
	scoringSubtask := toScoringSubtask(st, outDegree)
	debug := logger.Enabled(context.Background(), slog.LevelDebug)

	for i, node := range nodes {
		candidate, ok := prepareScoringCandidateForScore(node, scoringSubtask, nodeCapabilities[i])
		if !ok {
			if debug {
				eligibility := score.ExplainEligibility(*node, scoringSubtask)
				logDecision(logger, slog.LevelDebug, "planning", eligibility.Reason,
					*node, "", 0, ScoreFactors{}, eligibility)
			}
			continue
		}
		node := candidate.node
		learnedBias := 0.0
		if exp != nil {
			learnedBias = exp.LearnedBias(node.ID, candidate.prepared.Model(), st.TaskType)
		}
		tokensPerSecond := 0.0
		if history, ok := exp.(CostExperience); ok {
			tokensPerSecond = history.TokensPerSecond(node.ID, candidate.prepared.Model(), st.TaskType)
		}
		nodeLoad := load[node.ID]
		scoreValue := score.ScorePreparedValue(
			*node, scoringSubtask, candidate.prepared, nodeLoad.active, nodeLoad.pendingDependents,
			score.Experience{LearnedBias: learnedBias, TokensPerSecond: tokensPerSecond}, *cfg, now,
		)
		if debug {
			_, details := score.ScorePrepared(*node, scoringSubtask, candidate.prepared,
				nodeLoad.active, nodeLoad.pendingDependents,
				score.Experience{LearnedBias: learnedBias, TokensPerSecond: tokensPerSecond}, *cfg, now)
			logDecision(logger, slog.LevelDebug, "planning", "eligible", *node,
				candidate.prepared.Model(), scoreValue, details, score.ExplainEligibility(*node, scoringSubtask))
		}
		if scoreValue > bestScore {
			bestScore = scoreValue
			best = node
			bestBias = learnedBias
			bestThroughput = tokensPerSecond
			bestLoad = nodeLoad
			bestCapabilities = nodeCapabilities[i]
		}
	}
	if best == nil {
		return nil, "", "", bestScore, ScoreFactors{}
	}
	prepared, ok := score.PrepareWithCapabilities(*best, scoringSubtask, bestCapabilities)
	if !ok {
		return nil, "", "", math.Inf(-1), ScoreFactors{}
	}
	bestScore, bestFactors := score.ScorePrepared(
		*best, scoringSubtask, prepared, bestLoad.active, bestLoad.pendingDependents,
		score.Experience{LearnedBias: bestBias, TokensPerSecond: bestThroughput}, *cfg, now,
	)
	return best, prepared.Model(), prepared.Engine(), bestScore, bestFactors
}

type scoringCandidate struct {
	node     *cluster.NodeStatus
	prepared score.Candidate
}

// filterSubtaskCandidates excludes unhealthy nodes and nodes without required
// capabilities or a ready model matching the subtask tags and minimum size.
// Capacity-full nodes remain candidates because the established score penalizes
// them rather than excluding them, preserving assignments and score factors.
func filterSubtaskCandidates(st Subtask, outDegree int, nodes []cluster.NodeStatus) []scoringCandidate {
	nodeCapabilities := make([][]string, len(nodes))
	for i, node := range nodes {
		nodeCapabilities[i] = node.EffectiveCapabilities()
	}
	return filterSubtaskCandidatesWithCapabilities(st, outDegree, nodes, nodeCapabilities)
}

func filterSubtaskCandidatesWithCapabilities(st Subtask, outDegree int, nodes []cluster.NodeStatus, nodeCapabilities [][]string) []scoringCandidate {
	subtask := toScoringSubtask(st, outDegree)
	candidates := make([]scoringCandidate, 0, len(nodes))
	for i := range nodes {
		candidate, ok := prepareScoringCandidate(&nodes[i], subtask, nodeCapabilities[i])
		if ok {
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

func prepareScoringCandidate(node *cluster.NodeStatus, subtask score.Subtask, nodeCapabilities []string) (scoringCandidate, bool) {
	if !node.Healthy || node.Draining {
		return scoringCandidate{}, false
	}
	prepared, ok := score.PrepareWithCapabilities(*node, subtask, nodeCapabilities)
	if !ok {
		return scoringCandidate{}, false
	}
	return scoringCandidate{node: node, prepared: prepared}, true
}

func prepareScoringCandidateForScore(node *cluster.NodeStatus, subtask score.Subtask, nodeCapabilities []string) (scoringCandidate, bool) {
	if !node.Healthy || node.Draining {
		return scoringCandidate{}, false
	}
	prepared, ok := score.PrepareWithCapabilitiesForScoring(*node, subtask, nodeCapabilities)
	if !ok {
		return scoringCandidate{}, false
	}
	return scoringCandidate{node: node, prepared: prepared}, true
}

func toScoringSubtask(st Subtask, outDegree int) score.Subtask {
	return score.Subtask{
		Description:           st.Description,
		ExpectedOutput:        st.ExpectedOutput,
		RoutingPreference:     st.RoutingPreference,
		TaskType:              st.TaskType,
		PreferredModel:        st.PreferredModel,
		RequiredCapabilities:  st.RequiredCapabilities,
		PreferredCapabilities: st.PreferredCapabilities,
		MinModelSizeGB:        st.MinModelSizeGB,
		OutDegree:             outDegree,
	}
}
