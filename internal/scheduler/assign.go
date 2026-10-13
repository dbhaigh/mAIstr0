package scheduler

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
)

// Assignment pairs a subtask with the node and model chosen to run it.
type Assignment struct {
	Subtask Subtask      `json:"subtask"`
	NodeID  string       `json:"node_id"`
	Address string       `json:"address"`
	Model   string       `json:"model"`
	Score   float64      `json:"score"`
	Factors ScoreFactors `json:"score_factors"`
}

// ScoreFactors explains the additive scheduler score for an assignment.
type ScoreFactors struct {
	ModelMatch            float64 `json:"model_match"`
	Hardware              float64 `json:"hardware"`
	ActiveTasks           int     `json:"active_tasks"`
	QueuedTasks           int     `json:"queued_tasks"`
	LoadPenalty           float64 `json:"load_penalty"`
	GPUUtilizationPenalty float64 `json:"gpu_utilization_penalty"`
	NPUUtilizationPenalty float64 `json:"npu_utilization_penalty"`
	LearnedBias           float64 `json:"learned_bias"`
	TelemetryFresh        bool    `json:"telemetry_fresh"`
	TelemetryAgeMs        int64   `json:"telemetry_age_ms,omitempty"`
}

// ErrNoHealthyNodes is returned when the cluster has no node available to
// take on any work.
var ErrNoHealthyNodes = errors.New("scheduler: no healthy nodes available")

const telemetryFreshnessWindow = 30 * time.Second

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

// Assign picks the best node+model for each subtask. It accounts for
// capability match, node hardware score, and current load, and it updates a
// local load counter as it goes so a single submission's subtasks spread
// across the cluster instead of piling onto one fast node.
func Assign(subtasks []Subtask, nodes []cluster.NodeStatus) ([]Assignment, error) {
	return AssignWithExperience(subtasks, nodes, nil)
}

// AssignWithExperience is Assign biased by what the cluster has learned:
// node+model pairings with a strong track record on this task type are
// preferred, repeatedly-failing ones are avoided.
func AssignWithExperience(subtasks []Subtask, nodes []cluster.NodeStatus, exp Experience) ([]Assignment, error) {
	healthy := make([]cluster.NodeStatus, 0, len(nodes))
	for _, n := range nodes {
		if n.Healthy {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) == 0 {
		return nil, ErrNoHealthyNodes
	}

	// Track simulated load locally so assignments within this batch spread out.
	simulatedLoad := make(map[string]int, len(healthy))
	for _, n := range healthy {
		simulatedLoad[n.ID] = n.ActiveTasks + n.QueuedTasks
	}

	assignments := make([]Assignment, 0, len(subtasks))
	var unassignable []string
	now := time.Now()
	for _, st := range subtasks {
		bestNode, bestModel, bestScore, factors := pickBest(st, healthy, simulatedLoad, exp, now)
		if bestNode == nil {
			unassignable = append(unassignable, st.ID)
			continue
		}
		simulatedLoad[bestNode.ID]++
		assignments = append(assignments, Assignment{
			Subtask: st,
			NodeID:  bestNode.ID,
			Address: bestNode.Address,
			Model:   bestModel,
			Score:   bestScore,
			Factors: factors,
		})
	}
	if len(unassignable) > 0 {
		return nil, &UnassignableSubtasksError{SubtaskIDs: unassignable}
	}
	return assignments, nil
}

func pickBest(st Subtask, nodes []cluster.NodeStatus, load map[string]int, exp Experience, now time.Time) (*cluster.NodeStatus, string, float64, ScoreFactors) {
	var best *cluster.NodeStatus
	bestModel := ""
	bestScore := math.Inf(-1)
	var bestFactors ScoreFactors

	for i := range nodes {
		n := &nodes[i]
		model, matchScore := bestModelFor(st, n.Models, n.DefaultModel)
		if model == "" {
			continue
		}
		hw := n.Hardware
		telemetryAge := time.Duration(0)
		if !n.TelemetryAt.IsZero() && !n.TelemetryAt.After(now) {
			telemetryAge = now.Sub(n.TelemetryAt)
		}
		telemetryFresh := !n.TelemetryAt.IsZero() && !n.TelemetryAt.After(now) && telemetryAge <= telemetryFreshnessWindow
		if !telemetryFresh {
			hw.GPUStatsAvailable = false
			hw.NPUStatsAvailable = false
		}
		hardwareScore := hw.Score
		loadPenalty := float64(load[n.ID]) * 8.0
		gpuPenalty := cluster.GPUUtilizationPenalty(hw)
		npuPenalty := cluster.NPUUtilizationPenalty(hw)

		learned := 0.0
		if exp != nil {
			learned = exp.LearnedBias(n.ID, model, st.TaskType)
		}

		factors := ScoreFactors{
			ModelMatch:            matchScore * 10.0,
			Hardware:              hardwareScore * 0.3,
			ActiveTasks:           n.ActiveTasks,
			QueuedTasks:           load[n.ID] - n.ActiveTasks,
			LoadPenalty:           loadPenalty,
			GPUUtilizationPenalty: gpuPenalty,
			NPUUtilizationPenalty: npuPenalty,
			LearnedBias:           learned,
			TelemetryFresh:        telemetryFresh,
			TelemetryAgeMs:        telemetryAge.Milliseconds(),
		}
		score := factors.ModelMatch + factors.Hardware - factors.LoadPenalty -
			factors.GPUUtilizationPenalty - factors.NPUUtilizationPenalty + factors.LearnedBias
		if score > bestScore {
			bestScore = score
			best = n
			bestModel = model
			bestFactors = factors
		}
	}
	return best, bestModel, bestScore, bestFactors
}

// bestModelFor returns the model on a node that best matches the subtask's
// task type, along with a 0-2 match score (2 = exact task-type tag match).
func bestModelFor(st Subtask, models []engine.Model, defaultModel string) (string, float64) {
	bestName := ""
	bestScore := -1.0
	for _, m := range models {
		if st.TaskType != "general" && hasSpecialtyTag(m.Tags) &&
			!containsTag(m.Tags, st.TaskType) && !containsTag(m.Tags, "general") {
			continue
		}
		score := 0.0
		if m.Name == defaultModel {
			score += 1.0
		}
		for _, tag := range m.Tags {
			if tag == st.TaskType {
				score += 2
			} else if tag == "general" {
				score += 0.5
			} else if tag == "fast" {
				score += 0.25
			}
		}
		if score > bestScore {
			bestScore = score
			bestName = m.Name
		}
	}
	if bestName == "" {
		return "", 0
	}
	return bestName, bestScore
}

func hasSpecialtyTag(tags []string) bool {
	for _, tag := range tags {
		switch tag {
		case "code", "summarize", "translate", "math", "vision", "creative":
			return true
		}
	}
	return false
}

func containsTag(tags []string, target string) bool {
	for _, tag := range tags {
		if tag == target {
			return true
		}
	}
	return false
}
