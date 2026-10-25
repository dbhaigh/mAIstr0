package memory

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/syncinterval"
	bolt "go.etcd.io/bbolt"
	"sort"
	"strings"
	"time"
)

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
