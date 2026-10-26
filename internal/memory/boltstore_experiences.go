package memory

import (
	"encoding/json"
	bolt "go.etcd.io/bbolt"
	"sort"
	"strings"
	"time"
)

func (s *boltStore) Record(e Experience) (Experience, error) {
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	if e.Kind == "" {
		e.Kind = "task"
	}
	e.Prompt = truncate(e.Prompt, 8*1024)
	e.Output = truncate(e.Output, 32*1024)

	performanceKey := ""
	trackPerformance := e.NodeID != "" && e.Model != ""
	if trackPerformance {
		performanceKey = perfKey(e.NodeID, e.Model, e.TaskType)
		s.mu.Lock()
	}
	var performance Performance
	err := s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketExperiences)
		id, err := b.NextSequence()
		if err != nil {
			return err
		}
		sequence, err := nextOriginSequence(tx, s.owner)
		if err != nil {
			return err
		}
		e.ID = id
		e.Origin, e.Sequence = s.owner, sequence
		buf, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := b.Put(itob(id), buf); err != nil {
			return err
		}
		if err := tx.Bucket(bucketMeta).Put(experienceFingerprintKey(e), []byte{1}); err != nil {
			return err
		}
		if !trackPerformance {
			return nil
		}
		performance = performanceAfterExperience(s.perfCache[performanceKey], e)
		sequence, err = nextOriginSequence(tx, s.owner)
		if err != nil {
			return err
		}
		performance.Origin, performance.Sequence = s.owner, sequence
		encodedPerformance, err := json.Marshal(performance)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketPerf).Put([]byte(performanceKey), encodedPerformance)
	})
	if trackPerformance {
		if err == nil {
			s.perfCache[performanceKey] = &performance
			s.publishPerformanceSnapshot()
			s.invalidateWatermarksCacheLocked()
		}
		s.mu.Unlock()
	} else if err == nil {
		s.invalidateWatermarksCache()
	}
	if err != nil {
		return Experience{}, err
	}

	s.cacheRecentExperience(e)
	s.changes.Notify()
	return e, nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…[truncated]"
}

// Experiences returns the most recent experiences, newest first.
func (s *boltStore) Experiences(limit int) []Experience {
	if limit <= 0 {
		limit = 50
	}
	if cached, ok := s.cachedExperiences(limit); ok {
		return cached
	}
	var out []Experience
	_ = s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketExperiences).Cursor()
		for k, v := c.Last(); k != nil && len(out) < limit; k, v = c.Prev() {
			var e Experience
			if json.Unmarshal(v, &e) == nil {
				out = append(out, e)
			}
		}
		return nil
	})
	return out
}

// Recall returns past experiences relevant to a filter, scored by keyword
// overlap and recency so the agent can be shown what worked before.
func (s *boltStore) Recall(f SearchFilter) []Experience {
	if f.Limit <= 0 {
		f.Limit = 10
	}
	terms := tokenize(f.Query)

	type scored struct {
		exp   Experience
		score float64
	}
	var candidates []scored

	_ = s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketExperiences).Cursor()
		scanned := 0
		for k, v := c.Last(); k != nil && scanned < 5000; k, v = c.Prev() {
			scanned++
			var e Experience
			if json.Unmarshal(v, &e) != nil {
				continue
			}
			if f.TaskType != "" && !strings.EqualFold(e.TaskType, f.TaskType) {
				continue
			}
			if f.NodeID != "" && e.NodeID != f.NodeID {
				continue
			}
			if f.Model != "" && !strings.EqualFold(e.Model, f.Model) {
				continue
			}
			if f.Tool != "" && !strings.EqualFold(e.Tool, f.Tool) {
				continue
			}
			if f.SuccessOnly && !e.Success {
				continue
			}

			score := relevance(terms, e)
			if len(terms) > 0 && score <= 0 {
				continue
			}
			// Recency bonus: newer memories surface first when equally relevant.
			age := time.Since(e.CreatedAt).Hours()
			score += 1.0 / (1.0 + age/24.0)
			if e.Success {
				score += 0.5
			}
			candidates = append(candidates, scored{exp: e, score: score})
		}
		return nil
	})

	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })
	out := make([]Experience, 0, f.Limit)
	for i := 0; i < len(candidates) && i < f.Limit; i++ {
		out = append(out, candidates[i].exp)
	}
	return out
}

func tokenize(s string) []string {
	s = strings.ToLower(s)
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	var out []string
	for _, f := range fields {
		if len(f) > 2 && !stopWords[f] {
			out = append(out, f)
		}
	}
	return out
}

var stopWords = map[string]bool{
	"the": true, "and": true, "for": true, "with": true, "that": true,
	"this": true, "from": true, "into": true, "you": true, "are": true,
	"was": true, "has": true, "have": true, "can": true, "will": true,
}

func relevance(terms []string, e Experience) float64 {
	if len(terms) == 0 {
		return 0
	}
	haystack := strings.ToLower(e.Description + " " + e.Prompt + " " + e.Output + " " + e.TaskType + " " + strings.Join(e.Tags, " "))
	score := 0.0
	for _, t := range terms {
		if strings.Contains(haystack, t) {
			score += 2.0
		}
	}
	return score
}

// --- Facts ---

// PutFact stores or updates a durable piece of knowledge.
