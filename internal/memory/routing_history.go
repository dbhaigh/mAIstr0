package memory

import (
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	routingHistoryLimit  = 256
	minimumP95Samples    = 5
	estimatedRunesPerTok = 4
)

func (s *boltStore) TokensPerSecond(nodeID, model, taskType string) float64 {
	return historicalTokensPerSecond(s.Experiences(routingHistoryLimit), nodeID, model, taskType)
}

func (s *memoryStore) TokensPerSecond(nodeID, model, taskType string) float64 {
	return historicalTokensPerSecond(s.Experiences(routingHistoryLimit), nodeID, model, taskType)
}

func (s *boltStore) P95Duration(model, taskType string) time.Duration {
	return historicalP95Duration(s.Experiences(routingHistoryLimit), model, taskType)
}

func (s *memoryStore) P95Duration(model, taskType string) time.Duration {
	return historicalP95Duration(s.Experiences(routingHistoryLimit), model, taskType)
}

func historicalTokensPerSecond(experiences []Experience, nodeID, model, taskType string) float64 {
	samples := make([]float64, 0, len(experiences))
	for _, experience := range experiences {
		if !experience.Success || experience.NodeID != nodeID ||
			!strings.EqualFold(experience.Model, model) ||
			!strings.EqualFold(experience.TaskType, taskType) ||
			experience.DurationMs <= 0 || experience.Output == "" {
			continue
		}
		tokens := float64(utf8.RuneCountInString(experience.Output)) / estimatedRunesPerTok
		if tokens < 1 {
			tokens = 1
		}
		samples = append(samples, tokens/(float64(experience.DurationMs)/1000))
	}
	if len(samples) == 0 {
		return 0
	}
	sort.Float64s(samples)
	middle := len(samples) / 2
	if len(samples)%2 == 0 {
		return (samples[middle-1] + samples[middle]) / 2
	}
	return samples[middle]
}

func historicalP95Duration(experiences []Experience, model, taskType string) time.Duration {
	durations := make([]int64, 0, len(experiences))
	for _, experience := range experiences {
		if experience.Success && strings.EqualFold(experience.Model, model) &&
			strings.EqualFold(experience.TaskType, taskType) && experience.DurationMs > 0 {
			durations = append(durations, experience.DurationMs)
		}
	}
	if len(durations) < minimumP95Samples {
		return 0
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	index := int(math.Ceil(0.95*float64(len(durations)))) - 1
	return time.Duration(durations[index]) * time.Millisecond
}
