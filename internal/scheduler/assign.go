package scheduler

import (
	"errors"
	"log"
	"math"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

// ScoreFactors and ScoringConfig remain aliases so callers of the scheduler
// package keep their existing public type names.
type ScoreFactors = score.ScoreFactors
type ScoringConfig = score.ScoringConfig

// DefaultScoringConfig returns the established scheduler defaults.
func DefaultScoringConfig() ScoringConfig {
	return score.DefaultScoringConfig()
}

// Assignment pairs a subtask with the node and model chosen to run it.
type Assignment struct {
	Subtask Subtask      `json:"subtask"`
	NodeID  string       `json:"node_id"`
	Address string       `json:"address"`
	Model   string       `json:"model"`
	Engine  string       `json:"engine,omitempty"`
	Score   float64      `json:"score"`
	Factors ScoreFactors `json:"score_factors"`
}

// ErrNoHealthyNodes is returned when the cluster has no node available to
// take on any work.
var ErrNoHealthyNodes = errors.New("scheduler: no healthy nodes available")

// UnassignableSubtasksError reports work for which no healthy node advertises
// a compatible model. Assign is all-or-nothing so callers cannot silently
// report a partially executed task as complete.
type UnassignableSubtasksError struct {
	SubtaskIDs []string
}

func (e *UnassignableSubtasksError) Error() string {
	return "scheduler: no compatible model for subtasks: " + strings.Join(e.SubtaskIDs, ", ")
}

// Experience supplies a learned routing adjustment for a node+model+task
// pairing, derived from the cluster's recorded history.
type Experience interface {
	LearnedBias(nodeID, model, taskType string) float64
}

// Assign picks the best node+model for each subtask.
func Assign(subtasks []Subtask, nodes []cluster.NodeStatus) ([]Assignment, error) {
	return AssignWithExperience(subtasks, nodes, nil)
}

// AssignWithExperience is Assign biased by what the cluster has learned.
func AssignWithExperience(subtasks []Subtask, nodes []cluster.NodeStatus, exp Experience) ([]Assignment, error) {
	cfg := DefaultScoringConfig()
	return AssignWithConfig(subtasks, nodes, &cfg, exp)
}

// AssignWithConfig runs assignment using a custom scoring configuration.
func AssignWithConfig(subtasks []Subtask, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	g, err := TaskGraphFromSubtasks(subtasks)
	if err != nil {
		// Fall back to direct subtask list if normalization fails
		return assignSubtaskList(subtasks, nil, nodes, cfg, exp)
	}
	return AssignGraph(g, nodes, cfg, exp)
}

// AssignGraph assigns subtasks using full graph topology to inform scheduling decisions.
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

func assignSubtaskList(subtasks []Subtask, graph *TaskGraph, nodes []cluster.NodeStatus, cfg *ScoringConfig, exp Experience) ([]Assignment, error) {
	if cfg == nil {
		c := DefaultScoringConfig()
		cfg = &c
	}
	healthy := make([]cluster.NodeStatus, 0, len(nodes))
	for _, n := range nodes {
		if n.Healthy {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) == 0 {
		return nil, ErrNoHealthyNodes
	}

	simulatedLoad := make(map[string]int, len(healthy))
	pendingDependents := make(map[string]int, len(healthy))
	nodeCapabilities := make([][]string, len(healthy))
	for i, n := range healthy {
		simulatedLoad[n.ID] = n.ActiveTasks + n.QueuedTasks
		nodeCapabilities[i] = n.EffectiveCapabilities()
	}

	assignments := make([]Assignment, 0, len(subtasks))
	var unassignable []string
	now := time.Now()

	for _, st := range subtasks {
		outDegree := 0
		if graph != nil {
			outDegree = graph.OutDegree(st.ID)
		}

		bestNode, bestModel, bestEngine, bestScore, factors := pickBest(st, outDegree, healthy, nodeCapabilities, simulatedLoad, pendingDependents, exp, cfg, now)
		if bestNode == nil {
			unassignable = append(unassignable, st.ID)
			continue
		}

		simulatedLoad[bestNode.ID]++
		if graph != nil {
			pendingDependents[bestNode.ID] += outDegree
		}

		log.Printf("scheduler: subtask %s (%s) assigned to node %s (model %s, engine %s, score %.2f) [match=%.1f cap=%.1f qual=%.1f hw=%.1f load=-%.1f tele_fresh=%v]",
			st.ID, st.Role, bestNode.ID, bestModel, bestEngine, bestScore,
			factors.ModelMatch, factors.CapabilityScore, factors.ModelQualityScore,
			factors.Hardware, factors.LoadPenalty, factors.TelemetryFresh)

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
		return nil, &UnassignableSubtasksError{SubtaskIDs: unassignable}
	}
	return assignments, nil
}

func pickBest(st Subtask, outDegree int, nodes []cluster.NodeStatus, nodeCapabilities [][]string, load map[string]int, pendingDependents map[string]int, exp Experience, cfg *ScoringConfig, now time.Time) (*cluster.NodeStatus, string, string, float64, ScoreFactors) {
	var best *cluster.NodeStatus
	bestModel := ""
	bestEngine := ""
	bestScore := math.Inf(-1)
	var bestFactors ScoreFactors
	scoringSubtask := toScoringSubtask(st, outDegree)

	for i := range nodes {
		candidate, ok := prepareScoringCandidate(&nodes[i], scoringSubtask, nodeCapabilities[i])
		if !ok {
			continue
		}
		node := candidate.node
		learnedBias := 0.0
		if exp != nil {
			learnedBias = exp.LearnedBias(node.ID, candidate.prepared.Model(), st.TaskType)
		}
		scoreValue, factors := score.ScorePrepared(
			*node, scoringSubtask, candidate.prepared, load[node.ID], pendingDependents[node.ID],
			score.Experience{LearnedBias: learnedBias}, *cfg, now,
		)
		if scoreValue > bestScore {
			bestScore = scoreValue
			best = node
			bestModel = candidate.prepared.Model()
			bestEngine = candidate.prepared.Engine()
			bestFactors = factors
		}
	}
	return best, bestModel, bestEngine, bestScore, bestFactors
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
	if !node.Healthy {
		return scoringCandidate{}, false
	}
	prepared, ok := score.PrepareWithCapabilities(*node, subtask, nodeCapabilities)
	if !ok {
		return scoringCandidate{}, false
	}
	return scoringCandidate{node: node, prepared: prepared}, true
}

func toScoringSubtask(st Subtask, outDegree int) score.Subtask {
	return score.Subtask{
		TaskType:              st.TaskType,
		PreferredModel:        st.PreferredModel,
		RequiredCapabilities:  st.RequiredCapabilities,
		PreferredCapabilities: st.PreferredCapabilities,
		MinModelSizeGB:        st.MinModelSizeGB,
		OutDegree:             outDegree,
	}
}
