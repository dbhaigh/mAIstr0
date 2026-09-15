package memory

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func perfKey(nodeID, model, taskType string) string {
	if taskType == "" {
		taskType = "general"
	}
	return nodeID + "|" + model + "|" + strings.ToLower(taskType)
}

func (s *Store) loadPerfCache() {
	_ = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPerf).ForEach(func(k, v []byte) error {
			var p Performance
			if json.Unmarshal(v, &p) == nil {
				s.perfCache[string(k)] = &p
			}
			return nil
		})
	})
}

// updatePerformance folds one experience into the rolling per node+model
// aggregate. This is the cluster's learning signal: repeated successes make
// a pairing more attractive, failures and slow runs make it less so.
func (s *Store) updatePerformance(e Experience) {
	if e.NodeID == "" || e.Model == "" {
		return
	}
	key := perfKey(e.NodeID, e.Model, e.TaskType)

	s.mu.Lock()
	p, ok := s.perfCache[key]
	if !ok {
		taskType := e.TaskType
		if taskType == "" {
			taskType = "general"
		}
		p = &Performance{NodeID: e.NodeID, Model: e.Model, TaskType: strings.ToLower(taskType)}
		s.perfCache[key] = p
	}
	p.Attempts++
	if e.Success {
		p.Successes++
	} else {
		p.Failures++
	}
	p.TotalDuration += e.DurationMs
	p.AvgDurationMs = p.TotalDuration / int64(max(p.Attempts, 1))
	p.SuccessRate = float64(p.Successes) / float64(max(p.Attempts, 1))
	p.RatingSum += e.Rating

	p.LastUsed = e.CreatedAt
	snapshot := *p
	s.mu.Unlock()

	_ = s.db.Update(func(tx *bolt.Tx) error {
		buf, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketPerf).Put([]byte(key), buf)
	})
}

// Performances returns every learned node+model+taskType aggregate,
// strongest first.
func (s *Store) Performances() []Performance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Performance, 0, len(s.perfCache))
	for _, p := range s.perfCache {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SuccessRate != out[j].SuccessRate {
			return out[i].SuccessRate > out[j].SuccessRate
		}
		return out[i].AvgDurationMs < out[j].AvgDurationMs
	})
	return out
}

// MergePerformances folds aggregates reported by another store (typically a
// node pushing its local learning up to the orchestrator) into this one,
// keeping whichever record has seen more evidence.
func (s *Store) MergePerformances(incoming []Performance) {
	if len(incoming) == 0 {
		return
	}
	s.mu.Lock()
	merged := make([]Performance, 0, len(incoming))
	for _, in := range incoming {
		if in.NodeID == "" || in.Model == "" {
			continue
		}
		if in.TaskType == "" {
			in.TaskType = "general"
		}
		key := perfKey(in.NodeID, in.Model, in.TaskType)
		existing, ok := s.perfCache[key]
		if ok && existing.Attempts >= in.Attempts {
			continue
		}
		copyOf := in
		s.perfCache[key] = &copyOf
		merged = append(merged, copyOf)
	}
	s.mu.Unlock()

	if len(merged) == 0 {
		return
	}
	_ = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketPerf)
		for _, p := range merged {
			buf, err := json.Marshal(p)
			if err != nil {
				continue
			}
			if err := b.Put([]byte(perfKey(p.NodeID, p.Model, p.TaskType)), buf); err != nil {
				return err
			}
		}
		return nil
	})
}

// LearnedBias returns a routing adjustment in roughly -10..+10 for a
// node+model+taskType pairing, derived purely from recorded history. The
// scheduler adds this to its static capability score so the cluster gets
// better at placing work the longer it runs.
func (s *Store) LearnedBias(nodeID, model, taskType string) float64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	p, ok := s.perfCache[perfKey(nodeID, model, taskType)]
	if !ok {
		// Fall back to the pairing's general-purpose record.
		p, ok = s.perfCache[perfKey(nodeID, model, "general")]
	}
	if !ok {
		s.mu.RUnlock()
		return 0
	}
	snapshot := *p
	s.mu.RUnlock()

	if snapshot.Attempts < 2 {
		return 0
	}

	// Confidence ramps in with evidence so two lucky runs don't dominate.
	confidence := float64(snapshot.Attempts) / (float64(snapshot.Attempts) + 5.0)
	bias := (snapshot.SuccessRate - 0.5) * 12.0 * confidence

	// Reward pairings that are consistently fast relative to a 30s baseline.
	if snapshot.AvgDurationMs > 0 {
		speed := (30000.0 - float64(snapshot.AvgDurationMs)) / 30000.0
		if speed > 1 {
			speed = 1
		}
		if speed < -1 {
			speed = -1
		}
		bias += speed * 2.0 * confidence
	}

	if snapshot.Attempts > 0 {
		bias += (snapshot.RatingSum / float64(snapshot.Attempts)) * 2.0 * confidence
	}

	if bias > 10 {
		bias = 10
	}
	if bias < -10 {
		bias = -10
	}
	return bias
}

// Recommendation is advice derived from history for handling a task type.
type Recommendation struct {
	TaskType      string  `json:"task_type"`
	NodeID        string  `json:"node_id"`
	Model         string  `json:"model"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs int64   `json:"avg_duration_ms"`
	Attempts      int     `json:"attempts"`
	Confidence    float64 `json:"confidence"`
	Reason        string  `json:"reason"`
}

// Recommend returns the historically best node+model pairings for a task
// type (or across all task types when taskType is empty).
func (s *Store) Recommend(taskType string, limit int) []Recommendation {
	if limit <= 0 {
		limit = 5
	}
	want := strings.ToLower(taskType)

	s.mu.RLock()
	var recs []Recommendation
	for _, p := range s.perfCache {
		if want != "" && p.TaskType != want {
			continue
		}
		if p.Attempts < 1 {
			continue
		}
		confidence := float64(p.Attempts) / (float64(p.Attempts) + 5.0)
		recs = append(recs, Recommendation{
			TaskType:      p.TaskType,
			NodeID:        p.NodeID,
			Model:         p.Model,
			SuccessRate:   p.SuccessRate,
			AvgDurationMs: p.AvgDurationMs,
			Attempts:      p.Attempts,
			Confidence:    confidence,
			Reason: fmt.Sprintf("%d prior runs, %.0f%% success, avg %dms",
				p.Attempts, p.SuccessRate*100, p.AvgDurationMs),
		})
	}
	s.mu.RUnlock()

	sort.Slice(recs, func(i, j int) bool {
		si := recs[i].SuccessRate*recs[i].Confidence - float64(recs[i].AvgDurationMs)/1e6
		sj := recs[j].SuccessRate*recs[j].Confidence - float64(recs[j].AvgDurationMs)/1e6
		return si > sj
	})
	if len(recs) > limit {
		recs = recs[:limit]
	}
	return recs
}

// Insights is the learning summary surfaced to the agent and the dashboard.
type Insights struct {
	TotalExperiences int              `json:"total_experiences"`
	Successes        int              `json:"successes"`
	Failures         int              `json:"failures"`
	SuccessRate      float64          `json:"success_rate"`
	AvgDurationMs    int64            `json:"avg_duration_ms"`
	TrackedPairings  int              `json:"tracked_pairings"`
	Facts            int              `json:"facts"`
	Dialogues        int              `json:"dialogues"`
	TopPairings      []Recommendation `json:"top_pairings"`
	WeakPairings     []Recommendation `json:"weak_pairings"`
	TaskTypes        map[string]int   `json:"task_types"`
	Lessons          []string         `json:"lessons"`
	DatabasePath     string           `json:"database_path"`
}

// Insights computes what the cluster has learned so far.
func (s *Store) Insights() Insights {
	ins := Insights{TaskTypes: map[string]int{}, DatabasePath: s.path}

	var totalDuration int64
	_ = s.db.View(func(tx *bolt.Tx) error {
		_ = tx.Bucket(bucketExperiences).ForEach(func(k, v []byte) error {
			var e Experience
			if json.Unmarshal(v, &e) != nil {
				return nil
			}
			ins.TotalExperiences++
			if e.Success {
				ins.Successes++
			} else {
				ins.Failures++
			}
			totalDuration += e.DurationMs
			tt := e.TaskType
			if tt == "" {
				tt = "general"
			}
			ins.TaskTypes[tt]++
			return nil
		})
		_ = tx.Bucket(bucketFacts).ForEach(func(k, v []byte) error {
			ins.Facts++
			return nil
		})
		_ = tx.Bucket(bucketDialogues).ForEach(func(k, v []byte) error {
			ins.Dialogues++
			return nil
		})
		return nil
	})

	if ins.TotalExperiences > 0 {
		ins.SuccessRate = float64(ins.Successes) / float64(ins.TotalExperiences)
		ins.AvgDurationMs = totalDuration / int64(ins.TotalExperiences)
	}

	s.mu.RLock()
	ins.TrackedPairings = len(s.perfCache)
	s.mu.RUnlock()

	all := s.Recommend("", 100)
	if len(all) > 3 {
		ins.TopPairings = all[:3]
		ins.WeakPairings = all[len(all)-3:]
	} else {
		ins.TopPairings = all
	}
	ins.Lessons = s.lessons(all)
	return ins
}

func (s *Store) lessons(recs []Recommendation) []string {
	var out []string
	for _, r := range recs {
		if r.Attempts < 3 {
			continue
		}
		switch {
		case r.SuccessRate >= 0.9:
			out = append(out, fmt.Sprintf("Prefer %s on %s for %s work (%.0f%% success over %d runs).",
				r.Model, r.NodeID, r.TaskType, r.SuccessRate*100, r.Attempts))
		case r.SuccessRate <= 0.4:
			out = append(out, fmt.Sprintf("Avoid %s on %s for %s work (%.0f%% success over %d runs).",
				r.Model, r.NodeID, r.TaskType, r.SuccessRate*100, r.Attempts))
		}
		if len(out) >= 8 {
			break
		}
	}
	return out
}

// Prune deletes experiences older than the retention window, keeping the
// learned performance aggregates intact.
func (s *Store) Prune(olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)
	removed := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketExperiences)
		c := b.Cursor()
		var doomed [][]byte
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var e Experience
			if json.Unmarshal(v, &e) != nil {
				continue
			}
			if e.CreatedAt.Before(cutoff) {
				key := make([]byte, len(k))
				copy(key, k)
				doomed = append(doomed, key)
			}
		}
		for _, k := range doomed {
			if err := b.Delete(k); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	return removed, err
}

// ContextBrief renders recalled experience and facts as a compact block the
// agent can paste straight into an LLM prompt.
func (s *Store) ContextBrief(query, taskType string, limit int) string {
	if s == nil {
		return ""
	}
	recalled := s.Recall(SearchFilter{Query: query, TaskType: taskType, Limit: limit})
	facts := s.Facts("cluster")
	recs := s.Recommend(taskType, 3)

	if len(recalled) == 0 && len(facts) == 0 && len(recs) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("<cluster_memory>\n")

	if len(facts) > 0 {
		sb.WriteString("Known facts:\n")
		for i, f := range facts {
			if i >= 8 {
				break
			}
			sb.WriteString(fmt.Sprintf("- %s: %s\n", f.Key, f.Value))
		}
	}

	if len(recs) > 0 {
		sb.WriteString("Learned routing preferences:\n")
		for _, r := range recs {
			sb.WriteString(fmt.Sprintf("- %s on %s handles %s work well (%s)\n", r.Model, r.NodeID, r.TaskType, r.Reason))
		}
	}

	if len(recalled) > 0 {
		sb.WriteString("Relevant past experience:\n")
		for _, e := range recalled {
			status := "succeeded"
			if !e.Success {
				status = "failed: " + e.Error
			}
			sb.WriteString(fmt.Sprintf("- [%s] %s on %s/%s %s (%dms)\n",
				e.CreatedAt.Format("2006-01-02 15:04"), truncate(e.Description, 160), e.NodeID, e.Model, status, e.DurationMs))
			if e.Success && e.Output != "" {
				sb.WriteString(fmt.Sprintf("  prior result: %s\n", truncate(oneLine(e.Output), 300)))
			}
		}
	}

	sb.WriteString("</cluster_memory>\n")
	return sb.String()
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
