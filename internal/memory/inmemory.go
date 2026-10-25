package memory

import (
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/syncinterval"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var inMemoryStoreID atomic.Uint64

type memoryStore struct {
	// mu protects all in-memory records, indexes, and watermarks. Methods release
	// it before notifying the change signal; no store lock is nested beneath it.
	mu             sync.RWMutex
	origin         string
	nextID         uint64
	sequences      map[string]uint64
	watermarks     map[string]uint64
	peerWatermarks map[string]map[string]uint64
	experiences    []Experience
	fingerprints   map[string]bool
	facts          map[string]Fact
	performances   map[string]Performance
	learnedBiases  atomic.Pointer[performanceSnapshot]
	dialogues      map[string]DialogueRecord
	agentSessions  map[string][]byte
	shardGroups    map[string][]byte
	changes        syncinterval.Signal
}

func NewInMemoryStore() Store {
	origin := fmt.Sprintf("memory-store-%d", inMemoryStoreID.Add(1))
	return &memoryStore{
		origin:         origin,
		sequences:      map[string]uint64{origin: 0},
		watermarks:     make(map[string]uint64),
		peerWatermarks: make(map[string]map[string]uint64),
		fingerprints:   make(map[string]bool),
		facts:          make(map[string]Fact),
		performances:   make(map[string]Performance),
		dialogues:      make(map[string]DialogueRecord),
		agentSessions:  make(map[string][]byte),
		shardGroups:    make(map[string][]byte),
	}
}

func (s *memoryStore) Path() string { return "" }

func (s *memoryStore) Close() error { return nil }

func (s *memoryStore) Changes() <-chan struct{} { return s.changes.Changes() }

func (s *memoryStore) Snapshot() Snapshot {
	s.mu.RLock()
	snapshot := Snapshot{
		Experiences:  make([]Experience, 0, len(s.experiences)),
		Facts:        make([]Fact, 0, len(s.facts)),
		Performances: make([]Performance, 0, len(s.performances)),
		Dialogues:    make([]DialogueRecord, 0, len(s.dialogues)),
		SyncVersion:  SyncProtocolVersion,
		Watermarks:   cloneWatermarks(s.watermarks),
	}
	for index := len(s.experiences) - 1; index >= 0; index-- {
		snapshot.Experiences = append(snapshot.Experiences, cloneExperience(s.experiences[index]))
	}
	for _, fact := range s.facts {
		snapshot.Facts = append(snapshot.Facts, fact)
	}
	for _, performance := range s.performances {
		snapshot.Performances = append(snapshot.Performances, performance)
	}
	for _, dialogue := range s.dialogues {
		snapshot.Dialogues = append(snapshot.Dialogues, cloneDialogue(dialogue))
	}
	s.mu.RUnlock()
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
	return snapshot
}

func (s *memoryStore) SnapshotSince(watermarks map[string]uint64) Snapshot {
	return snapshotSince(s.Snapshot(), watermarks)
}

func (s *memoryStore) Watermarks() map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneWatermarks(s.watermarks)
}

func (s *memoryStore) PeerWatermarks(peer string) map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneWatermarks(s.peerWatermarks[peer])
}

func (s *memoryStore) SetPeerWatermarks(peer string, watermarks map[string]uint64) error {
	if strings.TrimSpace(peer) == "" {
		return errors.New("memory: peer id is required for watermarks")
	}
	s.mu.Lock()
	s.peerWatermarks[peer] = mergeWatermarks(s.peerWatermarks[peer], watermarks)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) nextSequenceLocked(origin string) uint64 {
	sequence := s.sequences[origin]
	if s.watermarks[origin] > sequence {
		sequence = s.watermarks[origin]
	}
	sequence++
	s.sequences[origin] = sequence
	s.watermarks[origin] = sequence
	s.changes.Notify()
	return sequence
}

func (s *memoryStore) observeRecordLocked(origin string, sequence uint64) {
	if origin == "" || sequence == 0 {
		return
	}
	if sequence > s.watermarks[origin] {
		s.watermarks[origin] = sequence
		s.changes.Notify()
	}
	if sequence > s.sequences[origin] {
		s.sequences[origin] = sequence
	}
}

func (s *memoryStore) MergeSnapshot(snapshot Snapshot) error {
	return s.MergeSnapshotFrom(snapshot, legacyOrigin)
}

func (s *memoryStore) MergeSnapshotFrom(snapshot Snapshot, sourceOrigin string) error {
	if sourceOrigin == "" {
		sourceOrigin = legacyOrigin
	}
	for _, experience := range snapshot.Experiences {
		s.mergeExperienceFrom(experience, sourceOrigin)
	}
	for _, fact := range snapshot.Facts {
		if err := s.mergeFactFrom(fact, sourceOrigin); err != nil {
			return err
		}
	}
	s.mergePerformancesFrom(snapshot.Performances, sourceOrigin)
	for _, dialogue := range snapshot.Dialogues {
		s.mergeDialogueFrom(dialogue, sourceOrigin)
	}
	s.mu.Lock()
	for origin, sequence := range snapshot.Watermarks {
		s.observeRecordLocked(origin, sequence)
	}
	s.peerWatermarks[sourceOrigin] = mergeWatermarks(s.peerWatermarks[sourceOrigin], snapshot.Watermarks)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) mergeFactFrom(incoming Fact, sourceOrigin string) error {
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

	key := incoming.Scope + "|" + incoming.Key
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeRecordLocked(incoming.Origin, incoming.Sequence)
	if current, ok := s.facts[key]; ok {
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
		incoming.Origin = sourceOrigin
		incoming.Sequence = s.nextSequenceLocked(sourceOrigin)
	} else {
		s.observeRecordLocked(incoming.Origin, incoming.Sequence)
	}
	s.facts[key] = incoming
	return nil
}

func (s *memoryStore) mergeDialogueFrom(incoming DialogueRecord, sourceOrigin string) {
	if incoming.ID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeRecordLocked(incoming.Origin, incoming.Sequence)
	current, ok := s.dialogues[incoming.ID]
	if !ok {
		if incoming.Origin == "" || incoming.Sequence == 0 {
			incoming.Origin = sourceOrigin
			incoming.Sequence = s.nextSequenceLocked(sourceOrigin)
		}
		s.dialogues[incoming.ID] = cloneDialogue(incoming)
		return
	}
	changed := false
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
	for _, participant := range incoming.Participants {
		if !contains(current.Participants, participant) {
			current.Participants = append(current.Participants, participant)
			changed = true
		}
	}
	if current.Topic == "" {
		current.Topic = incoming.Topic
		changed = current.Topic != "" || changed
	}
	if incoming.UpdatedAt.After(current.UpdatedAt) {
		current.UpdatedAt = incoming.UpdatedAt
		changed = true
	}
	if changed {
		origin := s.origin
		if incoming.Origin == "" || incoming.Sequence == 0 {
			origin = sourceOrigin
		}
		current.Origin = origin
		current.Sequence = s.nextSequenceLocked(origin)
		s.dialogues[incoming.ID] = current
	}
}
