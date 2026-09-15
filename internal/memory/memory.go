// Package memory provides the cluster's persistent knowledge store: an
// embedded bbolt database shared by the orchestrator and every node agent.
// It records what work was done, which node/model did it, whether it
// succeeded and how long it took, plus durable facts and conversation
// transcripts, so the cluster can learn from past experience instead of
// re-deriving the same routing decisions every run.
package memory

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketExperiences = []byte("experiences")
	bucketFacts       = []byte("facts")
	bucketPerf        = []byte("performance")
	bucketDialogues   = []byte("dialogues")
	bucketMeta        = []byte("meta")
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("memory: record not found")

// Experience is one recorded unit of work the cluster performed.
type Experience struct {
	ID          uint64    `json:"id"`
	Kind        string    `json:"kind"` // task, tool_call, dialogue_turn, agent_step
	TaskType    string    `json:"task_type,omitempty"`
	Description string    `json:"description"`
	Prompt      string    `json:"prompt,omitempty"`
	Output      string    `json:"output,omitempty"`
	NodeID      string    `json:"node_id,omitempty"`
	Model       string    `json:"model,omitempty"`
	Tool        string    `json:"tool,omitempty"`
	SessionID   string    `json:"session_id,omitempty"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
	DurationMs  int64     `json:"duration_ms"`
	Rating      float64   `json:"rating,omitempty"` // optional quality feedback, -1..1
	Tags        []string  `json:"tags,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// Fact is a durable piece of knowledge the cluster has learned and can
// recall later, e.g. "node-2 has the only GPU" or a user preference.
type Fact struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Scope     string    `json:"scope,omitempty"` // cluster, node:<id>, session:<id>
	Source    string    `json:"source,omitempty"`
	Hits      int       `json:"hits"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Performance is the rolling aggregate the scheduler learns from: how a
// given node+model combination historically performs on a task type.
type Performance struct {
	NodeID        string    `json:"node_id"`
	Model         string    `json:"model"`
	TaskType      string    `json:"task_type"`
	Attempts      int       `json:"attempts"`
	Successes     int       `json:"successes"`
	Failures      int       `json:"failures"`
	TotalDuration int64     `json:"total_duration_ms"`
	AvgDurationMs int64     `json:"avg_duration_ms"`
	SuccessRate   float64   `json:"success_rate"`
	RatingSum     float64   `json:"rating_sum"`
	LastUsed      time.Time `json:"last_used"`
}

// DialogueRecord is a persisted conversation transcript between the
// orchestrator agent, a node LLM, or two node LLMs talking to each other.
type DialogueRecord struct {
	ID           string    `json:"id"`
	Participants []string  `json:"participants"`
	Topic        string    `json:"topic,omitempty"`
	Turns        []Turn    `json:"turns"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Turn is a single utterance inside a persisted dialogue.
type Turn struct {
	Speaker    string    `json:"speaker"` // orchestrator, node:<id>, user
	NodeID     string    `json:"node_id,omitempty"`
	Model      string    `json:"model,omitempty"`
	Content    string    `json:"content"`
	DurationMs int64     `json:"duration_ms,omitempty"`
	At         time.Time `json:"at"`
}

// Store is the thread-safe persistent memory backend.
type Store struct {
	db    *bolt.DB
	path  string
	owner string

	mu        sync.RWMutex
	perfCache map[string]*Performance
}

// Snapshot is the portable cluster knowledge set exchanged between nodes and
// the orchestrator. Records are merged idempotently by MergeSnapshot.
type Snapshot struct {
	Experiences  []Experience     `json:"experiences"`
	Facts        []Fact           `json:"facts"`
	Performances []Performance    `json:"performances"`
	Dialogues    []DialogueRecord `json:"dialogues"`
}

// Snapshot returns all durable knowledge in a transport-friendly form.
func (s *Store) Snapshot() Snapshot {
	return Snapshot{
		Experiences:  s.Experiences(int(^uint(0) >> 1)),
		Facts:        s.Facts(""),
		Performances: s.Performances(),
		Dialogues:    s.Dialogues(int(^uint(0) >> 1)),
	}
}

// MergeSnapshot imports knowledge from another cluster member without
// duplicating records already received from that member or a peer.
func (s *Store) MergeSnapshot(snapshot Snapshot) error {
	for _, e := range snapshot.Experiences {
		if err := s.mergeExperience(e); err != nil {
			return err
		}
	}
	for _, f := range snapshot.Facts {
		if _, err := s.PutFact(f); err != nil {
			return err
		}
	}
	s.MergePerformances(snapshot.Performances)
	for _, d := range snapshot.Dialogues {
		if err := s.mergeDialogue(d); err != nil {
			return err
		}
	}
	return nil
}

func experienceFingerprint(e Experience) []byte {
	return []byte(fmt.Sprintf("experience|%s|%s|%s|%s|%s|%s|%d|%s", e.CreatedAt.UTC().Format(time.RFC3339Nano), e.NodeID, e.Model, e.Kind, e.Description, e.Output, e.DurationMs, e.Error))
}

func (s *Store) mergeExperience(e Experience) error {
	key := "imported:" + fmt.Sprintf("%x", sha256.Sum256(experienceFingerprint(e)))
	return s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if meta.Get([]byte(key)) != nil {
			return nil
		}
		b := tx.Bucket(bucketExperiences)
		id, err := b.NextSequence()
		if err != nil {
			return err
		}
		e.ID = id
		if e.CreatedAt.IsZero() {
			e.CreatedAt = time.Now()
		}
		buf, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := b.Put(itob(id), buf); err != nil {
			return err
		}
		return meta.Put([]byte(key), []byte{1})
	})
}

func (s *Store) mergeDialogue(incoming DialogueRecord) error {
	if incoming.ID == "" {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDialogues)
		var current DialogueRecord
		if raw := b.Get([]byte(incoming.ID)); raw != nil {
			_ = json.Unmarshal(raw, &current)
		}
		if current.ID == "" {
			current = incoming
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
				}
			}
			for _, p := range incoming.Participants {
				if !contains(current.Participants, p) {
					current.Participants = append(current.Participants, p)
				}
			}
			if current.Topic == "" {
				current.Topic = incoming.Topic
			}
			if incoming.UpdatedAt.After(current.UpdatedAt) {
				current.UpdatedAt = incoming.UpdatedAt
			}
		}
		buf, err := json.Marshal(current)
		if err != nil {
			return err
		}
		return b.Put([]byte(current.ID), buf)
	})
}

// Open creates or opens the memory database at path. The parent directory
// is created if needed. owner identifies the writing process (node ID or
// orchestrator ID) so records can be attributed when stores are merged.
func Open(path, owner string) (*Store, error) {
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

	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketExperiences, bucketFacts, bucketPerf, bucketDialogues, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("memory: init buckets: %w", err)
	}

	s := &Store{db: db, path: path, owner: owner, perfCache: make(map[string]*Performance)}
	s.loadPerfCache()
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
func (s *Store) Path() string { return s.path }

// Close flushes and releases the database file.
func (s *Store) Close() error {
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
func (s *Store) Record(e Experience) (Experience, error) {
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	if e.Kind == "" {
		e.Kind = "task"
	}
	e.Prompt = truncate(e.Prompt, 8*1024)
	e.Output = truncate(e.Output, 32*1024)

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketExperiences)
		id, err := b.NextSequence()
		if err != nil {
			return err
		}
		e.ID = id
		buf, err := json.Marshal(e)
		if err != nil {
			return err
		}
		return b.Put(itob(id), buf)
	})
	if err != nil {
		return Experience{}, err
	}

	s.updatePerformance(e)
	return e, nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…[truncated]"
}

// Experiences returns the most recent experiences, newest first.
func (s *Store) Experiences(limit int) []Experience {
	if limit <= 0 {
		limit = 50
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

// SearchFilter narrows a recall query.
type SearchFilter struct {
	Query       string
	TaskType    string
	NodeID      string
	Model       string
	Tool        string
	SuccessOnly bool
	Limit       int
}

// Recall returns past experiences relevant to a filter, scored by keyword
// overlap and recency so the agent can be shown what worked before.
func (s *Store) Recall(f SearchFilter) []Experience {
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
func (s *Store) PutFact(f Fact) (Fact, error) {
	if f.Key == "" {
		return Fact{}, errors.New("memory: fact key is required")
	}
	if f.Scope == "" {
		f.Scope = "cluster"
	}
	now := time.Now()
	err := s.db.Update(func(tx *bolt.Tx) error {
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
		buf, err := json.Marshal(f)
		if err != nil {
			return err
		}
		return b.Put(key, buf)
	})
	return f, err
}

// GetFact recalls a stored fact and counts the hit.
func (s *Store) GetFact(scope, key string) (Fact, error) {
	if scope == "" {
		scope = "cluster"
	}
	var f Fact
	err := s.db.Update(func(tx *bolt.Tx) error {
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
		buf, err := json.Marshal(f)
		if err != nil {
			return err
		}
		return b.Put(k, buf)
	})
	return f, err
}

// Facts lists stored knowledge, optionally filtered to one scope.
func (s *Store) Facts(scope string) []Fact {
	var out []Fact
	_ = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFacts).ForEach(func(k, v []byte) error {
			var f Fact
			if json.Unmarshal(v, &f) != nil {
				return nil
			}
			if scope != "" && f.Scope != scope {
				return nil
			}
			out = append(out, f)
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out
}

// DeleteFact forgets a stored fact.
func (s *Store) DeleteFact(scope, key string) error {
	if scope == "" {
		scope = "cluster"
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFacts).Delete([]byte(scope + "|" + key))
	})
}

// --- Dialogues ---

// SaveDialogue persists (or replaces) a conversation transcript.
func (s *Store) SaveDialogue(d DialogueRecord) error {
	if d.ID == "" {
		return errors.New("memory: dialogue id is required")
	}
	now := time.Now()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now
	}
	d.UpdatedAt = now
	return s.db.Update(func(tx *bolt.Tx) error {
		buf, err := json.Marshal(d)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketDialogues).Put([]byte(d.ID), buf)
	})
}

// AppendDialogueTurn adds one utterance to a dialogue, creating it if new.
func (s *Store) AppendDialogueTurn(id string, participants []string, topic string, t Turn) error {
	if id == "" {
		return errors.New("memory: dialogue id is required")
	}
	if t.At.IsZero() {
		t.At = time.Now()
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDialogues)
		var d DialogueRecord
		if raw := b.Get([]byte(id)); raw != nil {
			_ = json.Unmarshal(raw, &d)
		}
		if d.ID == "" {
			d.ID = id
			d.CreatedAt = time.Now()
			d.Participants = participants
			d.Topic = topic
		}
		for _, p := range participants {
			if !contains(d.Participants, p) {
				d.Participants = append(d.Participants, p)
			}
		}
		d.Turns = append(d.Turns, t)
		d.UpdatedAt = time.Now()
		buf, err := json.Marshal(d)
		if err != nil {
			return err
		}
		return b.Put([]byte(id), buf)
	})
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
func (s *Store) Dialogue(id string) (DialogueRecord, error) {
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
func (s *Store) Dialogues(limit int) []DialogueRecord {
	if limit <= 0 {
		limit = 50
	}
	var out []DialogueRecord
	_ = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDialogues).ForEach(func(k, v []byte) error {
			var d DialogueRecord
			if json.Unmarshal(v, &d) == nil {
				out = append(out, d)
			}
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}
