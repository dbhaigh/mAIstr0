package memory

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func (s *memoryStore) updatePerformanceLocked(experience Experience) {
	taskType := experience.TaskType
	if taskType == "" {
		taskType = "general"
	}
	key := perfKey(experience.NodeID, experience.Model, taskType)
	performance, ok := s.performances[key]
	if !ok {
		performance = Performance{
			NodeID: experience.NodeID, Model: experience.Model, TaskType: strings.ToLower(taskType),
		}
	}
	performance.Attempts++
	if experience.Success {
		performance.Successes++
	} else {
		performance.Failures++
	}
	performance.TotalDuration += experience.DurationMs
	performance.AvgDurationMs = performance.TotalDuration / int64(max(performance.Attempts, 1))
	performance.SuccessRate = float64(performance.Successes) / float64(max(performance.Attempts, 1))
	performance.RatingSum += experience.Rating
	performance.LastUsed = experience.CreatedAt
	performance.Origin = s.origin
	performance.Sequence = s.nextSequenceLocked(s.origin)
	s.performances[key] = performance
	s.publishPerformanceSnapshotLocked()
}

func (s *memoryStore) publishPerformanceSnapshotLocked() {
	records := make(map[string]Performance, len(s.performances))
	for key, performance := range s.performances {
		records[key] = performance
	}
	s.learnedBiases.Store(&performanceSnapshot{records: records})
}

func (s *memoryStore) Performances() []Performance {
	s.mu.RLock()
	performances := make([]Performance, 0, len(s.performances))
	for _, performance := range s.performances {
		performances = append(performances, performance)
	}
	s.mu.RUnlock()
	sort.Slice(performances, func(i, j int) bool {
		if performances[i].SuccessRate != performances[j].SuccessRate {
			return performances[i].SuccessRate > performances[j].SuccessRate
		}
		return performances[i].AvgDurationMs < performances[j].AvgDurationMs
	})
	return performances
}

func (s *memoryStore) MergePerformances(incoming []Performance) {
	s.mergePerformancesFrom(incoming, legacyOrigin)
}

func (s *memoryStore) mergePerformancesFrom(incoming []Performance, sourceOrigin string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for _, performance := range incoming {
		if performance.NodeID == "" || performance.Model == "" {
			continue
		}
		if performance.TaskType == "" {
			performance.TaskType = "general"
		}
		key := perfKey(performance.NodeID, performance.Model, performance.TaskType)
		if current, ok := s.performances[key]; ok && current.Attempts >= performance.Attempts {
			s.observeRecordLocked(performance.Origin, performance.Sequence)
			continue
		}
		if performance.Origin == "" || performance.Sequence == 0 {
			performance.Origin = sourceOrigin
			performance.Sequence = s.nextSequenceLocked(sourceOrigin)
		} else {
			s.observeRecordLocked(performance.Origin, performance.Sequence)
		}
		s.performances[key] = performance
		changed = true
	}
	if changed {
		s.publishPerformanceSnapshotLocked()
	}
}

func (s *memoryStore) LearnedBias(nodeID, model, taskType string) float64 {
	return s.learnedBiases.Load().learnedBias(nodeID, model, taskType)
}

func (s *memoryStore) Recommend(taskType string, limit int) []Recommendation {
	if limit <= 0 {
		limit = 5
	}
	want := strings.ToLower(taskType)
	s.mu.RLock()
	performances := make([]Performance, 0, len(s.performances))
	for _, performance := range s.performances {
		performances = append(performances, performance)
	}
	s.mu.RUnlock()
	recommendations := make([]Recommendation, 0, len(performances))
	for _, performance := range performances {
		if want != "" && performance.TaskType != want || performance.Attempts < 1 {
			continue
		}
		confidence := float64(performance.Attempts) / (float64(performance.Attempts) + 5.0)
		recommendations = append(recommendations, Recommendation{
			TaskType: performance.TaskType, NodeID: performance.NodeID, Model: performance.Model,
			SuccessRate: performance.SuccessRate, AvgDurationMs: performance.AvgDurationMs,
			Attempts: performance.Attempts, Confidence: confidence,
			Reason: fmt.Sprintf("%d prior runs, %.0f%% success, avg %dms",
				performance.Attempts, performance.SuccessRate*100, performance.AvgDurationMs),
		})
	}
	sort.Slice(recommendations, func(i, j int) bool {
		left := recommendations[i].SuccessRate*recommendations[i].Confidence - float64(recommendations[i].AvgDurationMs)/1e6
		right := recommendations[j].SuccessRate*recommendations[j].Confidence - float64(recommendations[j].AvgDurationMs)/1e6
		return left > right
	})
	if len(recommendations) > limit {
		recommendations = recommendations[:limit]
	}
	return recommendations
}

func (s *memoryStore) Insights() Insights {
	experiences := s.Experiences(int(^uint(0) >> 1))
	insights := Insights{
		TaskTypes: make(map[string]int), DatabasePath: s.Path(),
		Facts: len(s.Facts("")), Dialogues: len(s.Dialogues(int(^uint(0) >> 1))),
		TrackedPairings: len(s.Performances()),
	}
	var totalDuration int64
	for _, experience := range experiences {
		insights.TotalExperiences++
		if experience.Success {
			insights.Successes++
		} else {
			insights.Failures++
		}
		totalDuration += experience.DurationMs
		taskType := experience.TaskType
		if taskType == "" {
			taskType = "general"
		}
		insights.TaskTypes[taskType]++
	}
	if insights.TotalExperiences > 0 {
		insights.SuccessRate = float64(insights.Successes) / float64(insights.TotalExperiences)
		insights.AvgDurationMs = totalDuration / int64(insights.TotalExperiences)
	}
	all := s.Recommend("", 100)
	if len(all) > 3 {
		insights.TopPairings = all[:3]
		insights.WeakPairings = all[len(all)-3:]
	} else {
		insights.TopPairings = all
	}
	insights.Lessons = lessons(all)
	return insights
}

func (s *memoryStore) Prune(olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.experiences[:0]
	removed := 0
	for _, experience := range s.experiences {
		if experience.CreatedAt.Before(cutoff) {
			removed++
			continue
		}
		kept = append(kept, experience)
	}
	s.experiences = kept
	if removed > 0 {
		s.changes.Notify()
	}
	return removed, nil
}

func (s *memoryStore) ContextBrief(query, taskType string, limit int) string {
	return contextBrief(s, query, taskType, limit)
}

func cloneExperience(experience Experience) Experience {
	experience.Tags = append([]string(nil), experience.Tags...)
	return experience
}

func cloneDialogue(dialogue DialogueRecord) DialogueRecord {
	dialogue.Participants = append([]string(nil), dialogue.Participants...)
	dialogue.Turns = append([]Turn(nil), dialogue.Turns...)
	return dialogue
}

func contextBrief(store Store, query, taskType string, limit int) string {
	if store == nil {
		return ""
	}
	recalled := store.Recall(SearchFilter{Query: query, TaskType: taskType, Limit: limit})
	facts := store.Facts("cluster")
	recommendations := store.Recommend(taskType, 3)
	if len(recalled) == 0 && len(facts) == 0 && len(recommendations) == 0 {
		return ""
	}

	var builder strings.Builder
	builder.WriteString("<cluster_memory>\n")
	if len(facts) > 0 {
		builder.WriteString("Known facts:\n")
		for i, fact := range facts {
			if i >= 8 {
				break
			}
			fmt.Fprintf(&builder, "- %s: %s\n", fact.Key, fact.Value)
		}
	}
	if len(recommendations) > 0 {
		builder.WriteString("Learned routing preferences:\n")
		for _, recommendation := range recommendations {
			fmt.Fprintf(&builder, "- %s on %s handles %s work well (%s)\n",
				recommendation.Model, recommendation.NodeID, recommendation.TaskType, recommendation.Reason)
		}
	}
	if len(recalled) > 0 {
		builder.WriteString("Relevant past experience:\n")
		for _, experience := range recalled {
			status := "succeeded"
			if !experience.Success {
				status = "failed: " + experience.Error
			}
			fmt.Fprintf(&builder, "- [%s] %s on %s/%s %s (%dms)\n",
				experience.CreatedAt.Format("2006-01-02 15:04"), truncate(experience.Description, 160),
				experience.NodeID, experience.Model, status, experience.DurationMs)
			if experience.Success && experience.Output != "" {
				fmt.Fprintf(&builder, "  prior result: %s\n", truncate(oneLine(experience.Output), 300))
			}
		}
	}
	builder.WriteString("</cluster_memory>\n")
	return builder.String()
}

func (s *memoryStore) mergeExperienceFrom(e Experience, sourceOrigin string) {
	key := string(experienceFingerprint(e))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeRecordLocked(e.Origin, e.Sequence)
	if s.fingerprints[key] {
		return
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	if e.Origin == "" || e.Sequence == 0 {
		e.Origin = sourceOrigin
		e.Sequence = s.nextSequenceLocked(sourceOrigin)
	}
	s.nextID++
	e.ID = s.nextID
	s.experiences = append(s.experiences, cloneExperience(e))
	s.fingerprints[key] = true
}
