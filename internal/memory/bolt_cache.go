package memory

import (
	"encoding/json"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

const recentExperienceCacheLimit = 128
const snapshotCacheSizeLimit = 2 << 20

func (s *boltStore) loadRecentExperiences() error {
	recent := make([]Experience, 0, recentExperienceCacheLimit)
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketExperiences).Cursor()
		for key, value := cursor.Last(); key != nil && len(recent) < recentExperienceCacheLimit; key, value = cursor.Prev() {
			var experience Experience
			if json.Unmarshal(value, &experience) == nil {
				recent = append(recent, experience)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.recentExperiences = recent
	s.recentExperiencesLoaded = true
	s.mu.Unlock()
	return nil
}

func (s *boltStore) cachedExperiences(limit int) ([]Experience, bool) {
	if limit > recentExperienceCacheLimit {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.recentExperiencesLoaded {
		return nil, false
	}
	count := min(limit, len(s.recentExperiences))
	out := make([]Experience, count)
	for i := range out {
		out[i] = cloneExperience(s.recentExperiences[i])
	}
	return out, true
}

func (s *boltStore) cacheRecentExperience(experience Experience) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.recentExperiencesLoaded || experience.ID == 0 {
		return
	}
	index := sort.Search(len(s.recentExperiences), func(i int) bool {
		return s.recentExperiences[i].ID <= experience.ID
	})
	if index < len(s.recentExperiences) && s.recentExperiences[index].ID == experience.ID {
		s.recentExperiences[index] = cloneExperience(experience)
		return
	}
	s.recentExperiences = append(s.recentExperiences, Experience{})
	copy(s.recentExperiences[index+1:], s.recentExperiences[index:])
	s.recentExperiences[index] = cloneExperience(experience)
	if len(s.recentExperiences) > recentExperienceCacheLimit {
		s.recentExperiences = s.recentExperiences[:recentExperienceCacheLimit]
	}
}

func (s *boltStore) pruneRecentExperienceCache(cutoff time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.recentExperiencesLoaded {
		return
	}
	kept := s.recentExperiences[:0]
	for _, experience := range s.recentExperiences {
		if !experience.CreatedAt.Before(cutoff) {
			kept = append(kept, experience)
		}
	}
	s.recentExperiences = kept
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	cloned := snapshot
	cloned.Experiences = cloneSnapshotSlice(snapshot.Experiences)
	for i := range snapshot.Experiences {
		cloned.Experiences[i].Tags = cloneSnapshotSlice(snapshot.Experiences[i].Tags)
	}
	cloned.Facts = cloneSnapshotSlice(snapshot.Facts)
	cloned.Performances = cloneSnapshotSlice(snapshot.Performances)
	cloned.Dialogues = cloneSnapshotSlice(snapshot.Dialogues)
	for i := range snapshot.Dialogues {
		cloned.Dialogues[i].Participants = cloneSnapshotSlice(snapshot.Dialogues[i].Participants)
		cloned.Dialogues[i].Turns = cloneSnapshotSlice(snapshot.Dialogues[i].Turns)
	}
	cloned.Watermarks = cloneWatermarks(snapshot.Watermarks)
	return cloned
}

func cloneSnapshotSlice[T any](values []T) []T {
	if values == nil {
		return nil
	}
	cloned := make([]T, len(values))
	copy(cloned, values)
	return cloned
}

func snapshotCacheable(snapshot Snapshot) bool {
	size := 0
	for _, experience := range snapshot.Experiences {
		size += len(experience.Origin) + len(experience.Kind) + len(experience.TaskType) +
			len(experience.Description) + len(experience.Prompt) + len(experience.Output) +
			len(experience.NodeID) + len(experience.Model) + len(experience.Tool) +
			len(experience.SessionID) + len(experience.Error) + 128
		for _, tag := range experience.Tags {
			size += len(tag)
		}
	}
	for _, fact := range snapshot.Facts {
		size += len(fact.Key) + len(fact.Origin) + len(fact.Value) + len(fact.Scope) + len(fact.Source) + 96
	}
	for _, performance := range snapshot.Performances {
		size += len(performance.NodeID) + len(performance.Model) + len(performance.TaskType) + 64
	}
	for _, dialogue := range snapshot.Dialogues {
		size += len(dialogue.ID) + len(dialogue.Origin) + len(dialogue.Topic) + 96
		for _, participant := range dialogue.Participants {
			size += len(participant)
		}
		for _, turn := range dialogue.Turns {
			size += len(turn.Speaker) + len(turn.NodeID) + len(turn.Model) + len(turn.Content) + 64
		}
	}
	for origin := range snapshot.Watermarks {
		size += len(origin) + 16
	}
	return len(snapshot.Experiences) <= recentExperienceCacheLimit &&
		len(snapshot.Facts) <= 512 && len(snapshot.Performances) <= 512 &&
		len(snapshot.Dialogues) <= 128 && size <= snapshotCacheSizeLimit
}

func (s *boltStore) invalidateWatermarksCache() {
	s.mu.Lock()
	s.invalidateWatermarksCacheLocked()
	s.mu.Unlock()
}

func (s *boltStore) invalidateWatermarksCacheLocked() {
	s.watermarksCacheGeneration++
	s.watermarksCacheValid = false
	s.invalidateSnapshotCacheLocked()
}

func (s *boltStore) invalidateSnapshotCache() {
	s.mu.Lock()
	s.invalidateSnapshotCacheLocked()
	s.mu.Unlock()
}

func (s *boltStore) invalidateSnapshotCacheLocked() {
	s.snapshotCacheGeneration++
	s.snapshotCacheValid = false
	s.snapshotCache = Snapshot{}
}
