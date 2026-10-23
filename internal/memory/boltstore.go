// Package memory provides the cluster's persistent knowledge store: a
// embedded bbolt database shared by the orchestrator and every node agent.
// It records what work was done, which node/model did it, whether it
// succeeded and how long it took, plus durable facts and conversation
// transcripts, so the cluster can learn from past experience instead of
// re-deriving the same routing decisions every run.
package memory

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/syncinterval"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketExperiences   = []byte("experiences")
	bucketFacts         = []byte("facts")
	bucketPerf          = []byte("performance")
	bucketDialogues     = []byte("dialogues")
	bucketMeta          = []byte("meta")
	bucketAgentSessions = []byte("agent_sessions")
)

var experienceFingerprintIndexKey = []byte("experience-fingerprint-index-v1")

// boltStore is the bbolt-backed implementation of Store.
type boltStore struct {
	db    *bolt.DB
	path  string
	owner string

	// mu protects persisted-record caches: Record/Merge/Prune update recent
	// entries, sequence writes invalidate snapshot/watermark caches, and
	// performance writes publish an immutable learned-bias snapshot. Some merge
	// paths hold it across bbolt updates (mu -> transaction); transaction
	// callbacks must not acquire another store mutex.
	mu                        sync.RWMutex
	perfCache                 map[string]*Performance
	learnedBiases             atomic.Pointer[performanceSnapshot]
	recentExperiences         []Experience
	recentExperiencesLoaded   bool
	watermarksCache           map[string]uint64
	watermarksCacheValid      bool
	watermarksCacheGeneration uint64
	peerWatermarksCache       map[string]map[string]uint64
	peerWatermarksLoaded      map[string]bool
	peerWatermarksGeneration  map[string]uint64
	snapshotCache             Snapshot
	snapshotCacheGeneration   uint64
	snapshotCacheValid        bool
	changes                   syncinterval.Signal
}

func (s *boltStore) Changes() <-chan struct{} { return s.changes.Changes() }

// Snapshot returns all durable knowledge in a transport-friendly form.
func (s *boltStore) Snapshot() Snapshot {
	s.mu.RLock()
	generation := s.snapshotCacheGeneration
	if s.snapshotCacheValid {
		cached := cloneSnapshot(s.snapshotCache)
		s.mu.RUnlock()
		return cached
	}
	s.mu.RUnlock()

	snapshot := Snapshot{SyncVersion: SyncProtocolVersion}
	err := s.db.View(func(tx *bolt.Tx) error {
		experiences := tx.Bucket(bucketExperiences)
		cursor := experiences.Cursor()
		for key, value := cursor.Last(); key != nil; key, value = cursor.Prev() {
			var experience Experience
			if err := json.Unmarshal(value, &experience); err != nil {
				return fmt.Errorf("memory: decode experience %s: %w", key, err)
			}
			snapshot.Experiences = append(snapshot.Experiences, experience)
		}
		if err := tx.Bucket(bucketFacts).ForEach(func(_, value []byte) error {
			var fact Fact
			if err := json.Unmarshal(value, &fact); err != nil {
				return fmt.Errorf("memory: decode fact: %w", err)
			}
			snapshot.Facts = append(snapshot.Facts, fact)
			return nil
		}); err != nil {
			return err
		}
		if err := tx.Bucket(bucketPerf).ForEach(func(_, value []byte) error {
			var performance Performance
			if err := json.Unmarshal(value, &performance); err != nil {
				return fmt.Errorf("memory: decode performance: %w", err)
			}
			snapshot.Performances = append(snapshot.Performances, performance)
			return nil
		}); err != nil {
			return err
		}
		if err := tx.Bucket(bucketDialogues).ForEach(func(_, value []byte) error {
			var dialogue DialogueRecord
			if err := json.Unmarshal(value, &dialogue); err != nil {
				return fmt.Errorf("memory: decode dialogue: %w", err)
			}
			snapshot.Dialogues = append(snapshot.Dialogues, dialogue)
			return nil
		}); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).ForEach(func(key, value []byte) error {
			if !bytes.HasPrefix(key, []byte("origin-watermark:")) {
				return nil
			}
			if len(value) != 8 {
				return fmt.Errorf("memory: invalid watermark value for %s", key)
			}
			origin := strings.TrimPrefix(string(key), "origin-watermark:")
			if snapshot.Watermarks == nil {
				snapshot.Watermarks = make(map[string]uint64)
			}
			snapshot.Watermarks[origin] = binary.BigEndian.Uint64(value)
			return nil
		})
	})
	if err != nil {
		logging.Errorf("memory", "memory: create snapshot: %v", err)
		return Snapshot{SyncVersion: SyncProtocolVersion}
	}
	sort.Slice(snapshot.Facts, func(i, j int) bool {
		return snapshot.Facts[i].UpdatedAt.After(snapshot.Facts[j].UpdatedAt)
	})
	sort.Slice(snapshot.Performances, func(i, j int) bool {
		if snapshot.Performances[i].SuccessRate != snapshot.Performances[j].SuccessRate {
			return snapshot.Performances[i].SuccessRate > snapshot.Performances[j].SuccessRate
		}
		return snapshot.Performances[i].AvgDurationMs < snapshot.Performances[j].AvgDurationMs
	})
	sort.Slice(snapshot.Dialogues, func(i, j int) bool {
		return snapshot.Dialogues[i].UpdatedAt.After(snapshot.Dialogues[j].UpdatedAt)
	})
	if snapshotCacheable(snapshot) {
		s.mu.Lock()
		if generation == s.snapshotCacheGeneration {
			s.snapshotCache = cloneSnapshot(snapshot)
			s.snapshotCacheValid = true
		}
		s.mu.Unlock()
	}
	return snapshot
}

func (s *boltStore) SnapshotSince(watermarks map[string]uint64) Snapshot {
	return snapshotSince(s.Snapshot(), watermarks)
}

// MergeSnapshot imports knowledge from another cluster member without
// duplicating records already received from that member or a peer.
func (s *boltStore) MergeSnapshot(snapshot Snapshot) error {
	return s.MergeSnapshotFrom(snapshot, legacyOrigin)
}

func (s *boltStore) MergeSnapshotFrom(snapshot Snapshot, sourceOrigin string) error {
	if sourceOrigin == "" {
		sourceOrigin = legacyOrigin
	}
	previousWatermarks := s.Watermarks()
	if len(snapshot.Experiences) > 0 || len(snapshot.Facts) > 0 {
		insertedExperiences := make([]Experience, 0, len(snapshot.Experiences))
		err := s.db.Update(func(tx *bolt.Tx) error {
			for _, experience := range snapshot.Experiences {
				inserted, added, err := mergeExperienceTx(tx, experience, sourceOrigin)
				if err != nil {
					return err
				}
				if added {
					insertedExperiences = append(insertedExperiences, inserted)
				}
			}
			for _, fact := range snapshot.Facts {
				if err := mergeFactTx(tx, fact, sourceOrigin); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		s.invalidateWatermarksCache()
		for _, experience := range insertedExperiences {
			s.cacheRecentExperience(experience)
		}
	}
	if err := s.mergePerformancesFrom(snapshot.Performances, sourceOrigin); err != nil {
		return err
	}
	var peerWatermarks map[string]uint64
	if err := s.db.Update(func(tx *bolt.Tx) error {
		for _, dialogue := range snapshot.Dialogues {
			if err := mergeDialogueTx(tx, dialogue, sourceOrigin, s.owner); err != nil {
				return err
			}
		}
		if err := updateSnapshotWatermarks(tx, snapshot.Watermarks); err != nil {
			return err
		}
		var err error
		peerWatermarks, err = mergePeerWatermarksTx(tx, sourceOrigin, snapshot.Watermarks)
		return err
	}); err != nil {
		return err
	}
	s.invalidateWatermarksCache()
	s.cachePeerWatermarks(sourceOrigin, peerWatermarks)
	if syncinterval.WatermarksAdvanced(previousWatermarks, s.Watermarks()) {
		s.changes.Notify()
	}
	return nil
}

func mergeFactTx(tx *bolt.Tx, incoming Fact, sourceOrigin string) error {
	if incoming.Key == "" {
		return errors.New("memory: fact key is required")
	}
	if incoming.Scope == "" {
		incoming.Scope = "cluster"
	}
	if incoming.UpdatedAt.IsZero() {
		incoming.UpdatedAt = incoming.CreatedAt
	}
	if incoming.UpdatedAt.IsZero() {
		incoming.UpdatedAt = time.Now()
	}
	if incoming.CreatedAt.IsZero() || incoming.UpdatedAt.Before(incoming.CreatedAt) {
		incoming.CreatedAt = incoming.UpdatedAt
	}

	b := tx.Bucket(bucketFacts)
	key := []byte(incoming.Scope + "|" + incoming.Key)
	if incoming.Origin != "" && incoming.Sequence > 0 {
		if err := updateRecordWatermark(tx, incoming.Origin, incoming.Sequence); err != nil {
			return err
		}
	}
	if raw := b.Get(key); raw != nil {
		var current Fact
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		if incoming.UpdatedAt.Before(current.UpdatedAt) ||
			(incoming.UpdatedAt.Equal(current.UpdatedAt) && !factWinsTie(incoming, current)) {
			return nil
		}
		if current.CreatedAt.Before(incoming.CreatedAt) {
			incoming.CreatedAt = current.CreatedAt
		}
		if current.Hits > incoming.Hits {
			incoming.Hits = current.Hits
		}
	}
	if incoming.Origin == "" || incoming.Sequence == 0 {
		sequence, err := nextOriginSequence(tx, sourceOrigin)
		if err != nil {
			return err
		}
		incoming.Origin, incoming.Sequence = sourceOrigin, sequence
	} else if err := updateRecordWatermark(tx, incoming.Origin, incoming.Sequence); err != nil {
		return err
	}
	buf, err := json.Marshal(incoming)
	if err != nil {
		return err
	}
	return b.Put(key, buf)
}

func factWinsTie(incoming, current Fact) bool {
	if incoming.Source != current.Source {
		return incoming.Source < current.Source
	}
	return incoming.Value > current.Value
}

func experienceFingerprint(e Experience) []byte {
	e.ID = 0
	e.Origin = ""
	e.Sequence = 0
	fingerprint, err := json.Marshal(e)
	if err != nil {
		return []byte(fmt.Sprintf("experience|%s|%s|%s", e.CreatedAt.UTC().Format(time.RFC3339Nano), e.NodeID, e.Model))
	}
	return fingerprint
}

func experienceFingerprintKey(e Experience) []byte {
	return []byte("imported:" + fmt.Sprintf("%x", sha256.Sum256(experienceFingerprint(e))))
}

func (s *boltStore) ensureExperienceFingerprintIndex() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if meta.Get(experienceFingerprintIndexKey) != nil {
			return nil
		}
		experiences := tx.Bucket(bucketExperiences)
		if err := experiences.ForEach(func(_, value []byte) error {
			var experience Experience
			if err := json.Unmarshal(value, &experience); err != nil {
				return err
			}
			return meta.Put(experienceFingerprintKey(experience), []byte{1})
		}); err != nil {
			return err
		}
		return meta.Put(experienceFingerprintIndexKey, []byte{1})
	})
}

func mergeExperienceTx(tx *bolt.Tx, e Experience, sourceOrigin string) (Experience, bool, error) {
	key := experienceFingerprintKey(e)
	meta := tx.Bucket(bucketMeta)
	if e.Origin != "" && e.Sequence > 0 {
		if err := updateRecordWatermark(tx, e.Origin, e.Sequence); err != nil {
			return Experience{}, false, err
		}
	}
	if meta.Get(key) != nil {
		return Experience{}, false, nil
	}
	if e.Origin == "" || e.Sequence == 0 {
		sequence, err := nextOriginSequence(tx, sourceOrigin)
		if err != nil {
			return Experience{}, false, err
		}
		e.Origin, e.Sequence = sourceOrigin, sequence
	}
	b := tx.Bucket(bucketExperiences)
	id, err := b.NextSequence()
	if err != nil {
		return Experience{}, false, err
	}
	e.ID = id
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	buf, err := json.Marshal(e)
	if err != nil {
		return Experience{}, false, err
	}
	if err := b.Put(itob(id), buf); err != nil {
		return Experience{}, false, err
	}
	if err := meta.Put(key, []byte{1}); err != nil {
		return Experience{}, false, err
	}
	return e, true, nil
}

func mergeDialogueTx(tx *bolt.Tx, incoming DialogueRecord, sourceOrigin, owner string) error {
	if incoming.ID == "" {
		return nil
	}
	b := tx.Bucket(bucketDialogues)
	if incoming.Origin != "" && incoming.Sequence > 0 {
		if err := updateRecordWatermark(tx, incoming.Origin, incoming.Sequence); err != nil {
			return err
		}
	}
	var current DialogueRecord
	raw := b.Get([]byte(incoming.ID))
	changed := raw == nil
	if raw != nil {
		_ = json.Unmarshal(raw, &current)
	}
	if current.ID == "" {
		current = incoming
		changed = true
	} else {
		for _, turn := range incoming.Turns {
			found := false
			for _, existing := range current.Turns {
				if existing.At.Equal(turn.At) && existing.Speaker == turn.Speaker && existing.Content == turn.Content {
					found = true
					break
				}
			}
			if !found {
				current.Turns = append(current.Turns, turn)
				changed = true
			}
		}
		for _, p := range incoming.Participants {
			if !contains(current.Participants, p) {
				current.Participants = append(current.Participants, p)
				changed = true
			}
		}
		if current.Topic == "" {
			current.Topic = incoming.Topic
			changed = changed || current.Topic != ""
		}
		if incoming.UpdatedAt.After(current.UpdatedAt) {
			current.UpdatedAt = incoming.UpdatedAt
			changed = true
		}
		if changed {
			sequence, err := nextOriginSequence(tx, owner)
			if err != nil {
				return err
			}
			current.Origin, current.Sequence = owner, sequence
		}
	}
	if current.Origin == "" || current.Sequence == 0 {
		sequence, err := nextOriginSequence(tx, sourceOrigin)
		if err != nil {
			return err
		}
		current.Origin, current.Sequence = sourceOrigin, sequence
		changed = true
	}
	if !changed {
		return nil
	}
	buf, err := json.Marshal(current)
	if err != nil {
		return err
	}
	return b.Put([]byte(current.ID), buf)
}

// Open creates or opens the memory database at path. The parent directory
// is created if needed. owner identifies the writing process (node ID or
// orchestrator ID) so records can be attributed when stores are merged.
func Open(path, owner string) (Store, error) {
	if path == "" {
		path = DefaultPath(owner)
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("memory: create data dir: %w", err)
		}
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("memory: open %s: %w", path, err)
	}
	db.MaxBatchDelay = 250 * time.Microsecond // Bound coalescing delay for synchronous memory writes.

	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketExperiences, bucketFacts, bucketPerf, bucketDialogues, bucketMeta, bucketAgentSessions} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		owner, err = resolveLocalOrigin(tx, owner, path)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("memory: init buckets: %w", err)
	}

	s := &boltStore{
		db: db, path: path, owner: owner, perfCache: make(map[string]*Performance),
		peerWatermarksCache:      make(map[string]map[string]uint64),
		peerWatermarksLoaded:     make(map[string]bool),
		peerWatermarksGeneration: make(map[string]uint64),
	}
	if err := s.backfillRecordMetadata(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("memory: backfill record metadata: %w", err)
	}
	if err := s.ensureExperienceFingerprintIndex(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("memory: index experiences: %w", err)
	}
	s.loadPerfCache()
	if err := s.loadRecentExperiences(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("memory: cache recent experiences: %w", err)
	}
	return s, nil
}

// DefaultPath returns the conventional on-disk location for an owner's store.
func DefaultPath(owner string) string {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		base = "."
	}
	safe := sanitize(owner)
	if safe == "" {
		safe = "maistr0"
	}
	return filepath.Join(base, "maistr0", "memory-"+safe+".db")
}

func sanitize(s string) string {
	var sb strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			sb.WriteRune(r)
		default:
			sb.WriteRune('-')
		}
	}
	return sb.String()
}

// Path returns the database file location.
func (s *boltStore) Path() string { return s.path }

// Close flushes and releases the database file.
func (s *boltStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// --- Experiences ---

// Record persists one experience and folds it into the performance
// aggregates the scheduler learns from.
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
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
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
func (s *boltStore) PutFact(f Fact) (Fact, error) {
	if f.Key == "" {
		return Fact{}, errors.New("memory: fact key is required")
	}
	if f.Scope == "" {
		f.Scope = "cluster"
	}
	now := time.Now()
	err := s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFacts)
		key := []byte(f.Scope + "|" + f.Key)
		if existing := b.Get(key); existing != nil {
			var prev Fact
			if json.Unmarshal(existing, &prev) == nil {
				f.CreatedAt = prev.CreatedAt
				f.Hits = prev.Hits
			}
		}
		if f.CreatedAt.IsZero() {
			f.CreatedAt = now
		}
		f.UpdatedAt = now
		sequence, err := nextOriginSequence(tx, s.owner)
		if err != nil {
			return err
		}
		f.Origin, f.Sequence = s.owner, sequence
		buf, err := json.Marshal(f)
		if err != nil {
			return err
		}
		return b.Put(key, buf)
	})
	if err == nil {
		s.invalidateWatermarksCache()
		s.changes.Notify()
	}
	return f, err
}

// GetFact recalls a stored fact and counts the hit.
func (s *boltStore) GetFact(scope, key string) (Fact, error) {
	if scope == "" {
		scope = "cluster"
	}
	var f Fact
	err := s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFacts)
		k := []byte(scope + "|" + key)
		raw := b.Get(k)
		if raw == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(raw, &f); err != nil {
			return err
		}
		f.Hits++
		sequence, err := nextOriginSequence(tx, s.owner)
		if err != nil {
			return err
		}
		f.Origin, f.Sequence = s.owner, sequence
		buf, err := json.Marshal(f)
		if err != nil {
			return err
		}
		return b.Put(k, buf)
	})
	if err == nil {
		s.invalidateWatermarksCache()
		s.changes.Notify()
	}
	return f, err
}

// Facts lists stored knowledge, optionally filtered to one scope.
func (s *boltStore) Facts(scope string) []Fact {
	var out []Fact
	_ = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketFacts)
		appendFact := func(v []byte) error {
			var f Fact
			if json.Unmarshal(v, &f) != nil {
				return nil
			}
			if scope != "" && f.Scope != scope {
				return nil
			}
			out = append(out, f)
			return nil
		}
		if scope == "" {
			return bucket.ForEach(func(_, value []byte) error {
				return appendFact(value)
			})
		}
		prefix := []byte(scope + "|")
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			if err := appendFact(value); err != nil {
				return err
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out
}

// DeleteFact forgets a stored fact.
func (s *boltStore) DeleteFact(scope, key string) error {
	if scope == "" {
		scope = "cluster"
	}
	err := s.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFacts).Delete([]byte(scope + "|" + key))
	})
	if err == nil {
		s.invalidateSnapshotCache()
		s.changes.Notify()
	}
	return err
}

// SaveAgentSession stores an opaque serialized agent session by backend and ID.
func (s *boltStore) SaveAgentSession(harness, id string, session []byte) error {
	if harness == "" || id == "" {
		return errors.New("memory: agent session harness and id are required")
	}
	if len(session) == 0 {
		return errors.New("memory: agent session payload is required")
	}
	key := []byte(harness + "\x00" + id)
	value := append([]byte(nil), session...)
	return s.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAgentSessions).Put(key, value)
	})
}

// AgentSessions returns all persisted agent session payloads.
func (s *boltStore) AgentSessions() ([][]byte, error) {
	var sessions [][]byte
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAgentSessions).ForEach(func(_, value []byte) error {
			sessions = append(sessions, append([]byte(nil), value...))
			return nil
		})
	})
	return sessions, err
}

// DeleteAgentSession removes one persisted agent session.
func (s *boltStore) DeleteAgentSession(harness, id string) error {
	if harness == "" || id == "" {
		return errors.New("memory: agent session harness and id are required")
	}
	key := []byte(harness + "\x00" + id)
	return s.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAgentSessions).Delete(key)
	})
}

// --- Dialogues ---

// SaveDialogue persists (or replaces) a conversation transcript.
func (s *boltStore) SaveDialogue(d DialogueRecord) error {
	if d.ID == "" {
		return errors.New("memory: dialogue id is required")
	}
	now := time.Now()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now
	}
	d.UpdatedAt = now
	err := s.db.Batch(func(tx *bolt.Tx) error {
		sequence, err := nextOriginSequence(tx, s.owner)
		if err != nil {
			return err
		}
		d.Origin, d.Sequence = s.owner, sequence
		buf, err := json.Marshal(d)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketDialogues).Put([]byte(d.ID), buf)
	})
	if err == nil {
		s.invalidateWatermarksCache()
		s.changes.Notify()
	}
	return err
}

// AppendDialogueTurn adds one utterance to a dialogue, creating it if new.
func (s *boltStore) AppendDialogueTurn(id string, participants []string, topic string, t Turn) error {
	return s.AppendDialogueTurns(id, participants, topic, []Turn{t})
}

// AppendDialogueTurns persists one or more utterances in a single transaction.
func (s *boltStore) AppendDialogueTurns(id string, participants []string, topic string, turns []Turn) error {
	if id == "" {
		return errors.New("memory: dialogue id is required")
	}
	if len(turns) == 0 {
		return nil
	}
	turns = append([]Turn(nil), turns...)
	for i := range turns {
		if turns[i].At.IsZero() {
			turns[i].At = time.Now()
		}
	}
	err := s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDialogues)
		var d DialogueRecord
		if raw := b.Get([]byte(id)); raw != nil {
			_ = json.Unmarshal(raw, &d)
		}
		if d.ID == "" {
			d.ID = id
			d.CreatedAt = time.Now()
			d.Participants = append([]string(nil), participants...)
			d.Topic = topic
		}
		for _, p := range participants {
			if !contains(d.Participants, p) {
				d.Participants = append(d.Participants, p)
			}
		}
		d.Turns = append(d.Turns, turns...)
		d.UpdatedAt = time.Now()
		for range turns {
			sequence, err := nextOriginSequence(tx, s.owner)
			if err != nil {
				return err
			}
			d.Origin, d.Sequence = s.owner, sequence
		}
		buf, err := json.Marshal(d)
		if err != nil {
			return err
		}
		return b.Put([]byte(id), buf)
	})
	if err == nil {
		s.invalidateWatermarksCache()
		s.changes.Notify()
	}
	return err
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// Dialogue returns a stored conversation transcript.
func (s *boltStore) Dialogue(id string) (DialogueRecord, error) {
	var d DialogueRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketDialogues).Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &d)
	})
	return d, err
}

// Dialogues lists stored conversations, most recently updated first.
func (s *boltStore) Dialogues(limit int) []DialogueRecord {
	if limit <= 0 {
		limit = 50
	}
	var out []DialogueRecord
	_ = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketDialogues)
		index := make([]dialoguePageIndex, 0)
		if err := bucket.ForEach(func(key, value []byte) error {
			var metadata struct {
				UpdatedAt time.Time `json:"updated_at"`
			}
			if json.Unmarshal(value, &metadata) == nil {
				index = append(index, dialoguePageIndex{ID: string(key), UpdatedAt: metadata.UpdatedAt})
			}
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(index, func(i, j int) bool {
			return index[i].UpdatedAt.After(index[j].UpdatedAt)
		})
		for _, item := range index {
			if len(out) == limit {
				break
			}
			raw := bucket.Get([]byte(item.ID))
			var dialogue DialogueRecord
			if raw == nil || json.Unmarshal(raw, &dialogue) != nil {
				continue
			}
			out = append(out, dialogue)
		}
		return nil
	})
	return out
}
