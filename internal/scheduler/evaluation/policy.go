// Package evaluation compares ranking policies using synthetic data only.
// It is not used by production routing.
package evaluation

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/scheduler/score"
)

type Policy string

const (
	RoundRobin Policy = "round_robin"
	LeastLoad  Policy = "least_loaded"
	Capability Policy = "capability_aware"
	Existing   Policy = "existing_scorer"
)

func Policies() []Policy { return []Policy{RoundRobin, LeastLoad, Capability, Existing} }

type Selection struct {
	Node     int
	Examined int
	Eligible int
}

type selector struct {
	policy       Policy
	cursor       int
	capabilities [][]string
	config       score.ScoringConfig
}

func newSelector(policy Policy, nodes []cluster.NodeStatus) (*selector, error) {
	switch policy {
	case RoundRobin, LeastLoad, Capability, Existing:
	default:
		return nil, fmt.Errorf("unknown evaluation policy %q", policy)
	}
	s := &selector{policy: policy, config: score.DefaultScoringConfig()}
	for _, node := range nodes {
		s.capabilities = append(s.capabilities, node.EffectiveCapabilities())
	}
	return s, nil
}

// All policies use production model/capability/resource eligibility and model
// preparation. Full nodes may be planned, but the simulator must queue them.
func (s *selector) choose(nodes []cluster.NodeStatus, task score.Subtask, excluded []bool, now time.Time) Selection {
	result := Selection{Node: -1}
	if s.policy == RoundRobin {
		for offset := range len(nodes) {
			i := (s.cursor + offset) % len(nodes)
			result.Examined++
			if len(excluded) > 0 && excluded[i] {
				continue
			}
			if _, ok := score.PrepareWithCapabilitiesForScoring(nodes[i], task, s.capabilities[i]); ok {
				result.Node, result.Eligible = i, 1
				s.cursor = (i + 1) % len(nodes)
				return result
			}
		}
		return result
	}
	best := math.Inf(-1)
	for i := range nodes {
		result.Examined++
		if len(excluded) > 0 && excluded[i] {
			continue
		}
		node := nodes[i]
		candidate, ok := score.PrepareWithCapabilitiesForScoring(node, task, s.capabilities[i])
		if !ok {
			continue
		}
		result.Eligible++
		load := node.ActiveTasks + node.QueuedTasks
		var value float64
		switch s.policy {
		case LeastLoad:
			value = -float64(load) / float64(max(1, node.MaxConcurrentTasks))
		case Capability:
			value = node.Hardware.Score*s.config.HardwareWeight +
				float64(len(task.RequiredCapabilities))*s.config.RequiredCapMatchBonus
			if node.Hardware.HasGPU {
				value += s.config.GPUCompatibilityBonus
			}
			for _, preferred := range task.PreferredCapabilities {
				for _, capability := range s.capabilities[i] {
					if strings.EqualFold(strings.TrimSpace(preferred), capability) {
						value += s.config.PreferredCapMatchBonus
						break
					}
				}
			}
		case Existing:
			value = score.ScorePreparedValue(node, task, candidate, load, 0, score.Experience{}, s.config, now)
		}
		// Strict improvement preserves the production input-order tie break.
		if value > best {
			best, result.Node = value, i
		}
	}
	return result
}
