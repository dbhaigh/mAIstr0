package memory

import (
	"bytes"
	"encoding/json"
	"errors"
	bolt "go.etcd.io/bbolt"
	"sort"
	"time"
)

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

func (s *boltStore) SaveShardGroup(id string, group []byte) error {
	if id == "" || len(group) == 0 {
		return errors.New("memory: shard group id and payload are required")
	}
	payload := append([]byte(nil), group...)
	return s.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketShardGroups).Put([]byte(id), payload)
	})
}

func (s *boltStore) ShardGroups() ([][]byte, error) {
	var groups [][]byte
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketShardGroups).ForEach(func(_, value []byte) error {
			groups = append(groups, append([]byte(nil), value...))
			return nil
		})
	})
	return groups, err
}

func (s *boltStore) DeleteShardGroup(id string) error {
	if id == "" {
		return errors.New("memory: shard group id is required")
	}
	return s.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketShardGroups).Delete([]byte(id))
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
