package memory

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/syncinterval"
	bolt "go.etcd.io/bbolt"
)

func perfKey(nodeID, model, taskType string) string {
	if taskType == "" {
		taskType = "general"
	}
	return nodeID + "|" + model + "|" + strings.ToLower(taskType)
}

// performanceSnapshot is immutable after publication so learned-bias reads
// do not contend with memory-store updates.
type performanceSnapshot struct {
	records map[string]Performance
}

func (snapshot *performanceSnapshot) learnedBias(nodeID, model, taskType string) float64 {
	if snapshot == nil {
		return 0
	}
	performance, ok := snapshot.records[perfKey(nodeID, model, taskType)]
	if !ok {
		performance, ok = snapshot.records[perfKey(nodeID, model, "general")]
	}
	if !ok {
		return 0
	}
	return learnedBias(performance)
}

func (s *boltStore) publishPerformanceSnapshot() {
	records := make(map[string]Performance, len(s.perfCache))
	for key, performance := range s.perfCache {
		if performance != nil {
			records[key] = *performance
		}
	}
	s.learnedBiases.Store(&performanceSnapshot{records: records})
}

func (s *boltStore) loadPerfCache() {
	_ = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPerf).ForEach(func(k, v []byte) error {
			var p Performance
			if json.Unmarshal(v, &p) == nil {
				s.perfCache[string(k)] = &p
			}
			return nil
		})
	})
	s.publishPerformanceSnapshot()
}

// updatePerformance folds one experience into the rolling per node+model
// aggregate. This is the cluster's learning signal: repeated successes make
// a pairing more attractive, failures and slow runs make it less so.
func (s *boltStore) updatePerformance(e Experience) {
	if e.NodeID == "" || e.Model == "" {
		return
	}
	key := perfKey(e.NodeID, e.Model, e.TaskType)

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.perfCache[key]
	var snapshot Performance
	if !ok {
		taskType := e.TaskType
		if taskType == "" {
			taskType = "general"
		}
		snapshot = Performance{NodeID: e.NodeID, Model: e.Model, TaskType: strings.ToLower(taskType)}
	} else {
		snapshot = *current
	}
	snapshot.Attempts++
	if e.Success {
		snapshot.Successes++
	} else {
		snapshot.Failures++
	}
	snapshot.TotalDuration += e.DurationMs
	snapshot.AvgDurationMs = snapshot.TotalDuration / int64(max(snapshot.Attempts, 1))
	snapshot.SuccessRate = float64(snapshot.Successes) / float64(max(snapshot.Attempts, 1))
	snapshot.RatingSum += e.Rating

	snapshot.LastUsed = e.CreatedAt

	if err := s.db.Update(func(tx *bolt.Tx) error {
		sequence, err := nextOriginSequence(tx, s.owner)
		if err != nil {
			return err
		}
		snapshot.Origin, snapshot.Sequence = s.owner, sequence
		buf, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketPerf).Put([]byte(key), buf)
	}); err != nil {
		log.Printf("memory: persist performance for %s: %v", key, err)
		return
	}
	s.perfCache[key] = &snapshot
	s.publishPerformanceSnapshot()
	s.invalidateWatermarksCacheLocked()
}

// Performances returns every learned node+model+taskType aggregate,
// strongest first.
func (s *boltStore) Performances() []Performance {
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
func (s *boltStore) MergePerformances(incoming []Performance) {
	previousWatermarks := s.Watermarks()
	if err := s.mergePerformancesFrom(incoming, legacyOrigin); err != nil {
		log.Printf("memory: merge performances: %v", err)
		return
	}
	if syncinterval.WatermarksAdvanced(previousWatermarks, s.Watermarks()) {
		s.changes.Notify()
	}
}

func (s *boltStore) mergePerformancesFrom(incoming []Performance, sourceOrigin string) error {
	if len(incoming) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	merged := make(map[string]Performance, len(incoming))
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketPerf)
		for _, in := range incoming {
			if in.NodeID == "" || in.Model == "" {
				continue
			}
			if in.TaskType == "" {
				in.TaskType = "general"
			}
			key := perfKey(in.NodeID, in.Model, in.TaskType)
			existing, ok := merged[key]
			if !ok {
				if cached := s.perfCache[key]; cached != nil {
					existing, ok = *cached, true
				}
			}
			if ok && existing.Attempts >= in.Attempts {
				if in.Origin != "" && in.Sequence > 0 {
					if err := updateRecordWatermark(tx, in.Origin, in.Sequence); err != nil {
						return err
					}
				}
				continue
			}
			if in.Origin == "" || in.Sequence == 0 {
				sequence, err := nextOriginSequence(tx, sourceOrigin)
				if err != nil {
					return err
				}
				in.Origin, in.Sequence = sourceOrigin, sequence
			} else if err := updateRecordWatermark(tx, in.Origin, in.Sequence); err != nil {
				return err
			}
			buf, err := json.Marshal(in)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(key), buf); err != nil {
				return err
			}
			merged[key] = in
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("memory: persist merged performances: %w", err)
	}
	for key, performance := range merged {
		copyOf := performance
		s.perfCache[key] = &copyOf
	}
	if len(merged) > 0 {
		s.publishPerformanceSnapshot()
	}
	s.invalidateWatermarksCacheLocked()
	return nil
}

// LearnedBias returns a routing adjustment in roughly -10..+10 for a
// node+model+taskType pairing, derived purely from recorded history. The
// scheduler adds this to its static capability score so the cluster gets
// better at placing work the longer it runs.
func (s *boltStore) LearnedBias(nodeID, model, taskType string) float64 {
	if s == nil {
		return 0
	}
	return s.learnedBiases.Load().learnedBias(nodeID, model, taskType)
}

func learnedBias(snapshot Performance) float64 {
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

// Recommend returns the historically best node+model pairings for a task
// type (or across all task types when taskType is empty).
func (s *boltStore) Recommend(taskType string, limit int) []Recommendation {
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

// Insights computes what the cluster has learned so far.
func (s *boltStore) Insights() Insights {
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
	ins.Lessons = lessons(all)
	return ins
}

func lessons(recs []Recommendation) []string {
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
func (s *boltStore) Prune(olderThan time.Duration) (int, error) {
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
	if err == nil {
		s.pruneRecentExperienceCache(cutoff)
		s.invalidateSnapshotCache()
		if removed > 0 {
			s.changes.Notify()
		}
	}
	return removed, err
}

// ContextBrief renders recalled experience and facts as a compact block the
// agent can paste straight into an LLM prompt.
func (s *boltStore) ContextBrief(query, taskType string, limit int) string {
	if s == nil {
		return ""
	}
	return contextBrief(s, query, taskType, limit)
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
