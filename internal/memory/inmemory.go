package memory

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/syncinterval"
)

// inMemoryStoreID allocates unique process-local origins without store locking.
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
	dialogues      map[string]DialogueRecord
	agentSessions  map[string][]byte
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

func (s *memoryStore) Record(experience Experience) (Experience, error) {
	if experience.CreatedAt.IsZero() {
		experience.CreatedAt = time.Now()
	}
	if experience.Kind == "" {
		experience.Kind = "task"
	}
	experience.Prompt = truncate(experience.Prompt, 8*1024)
	experience.Output = truncate(experience.Output, 32*1024)

	s.mu.Lock()
	s.nextID++
	experience.ID = s.nextID
	experience.Origin = s.origin
	experience.Sequence = s.nextSequenceLocked(s.origin)
	s.experiences = append(s.experiences, cloneExperience(experience))
	s.fingerprints[string(experienceFingerprint(experience))] = true
	if experience.NodeID != "" && experience.Model != "" {
		s.updatePerformanceLocked(experience)
	}
	s.mu.Unlock()
	return experience, nil
}

func (s *memoryStore) Experiences(limit int) []Experience {
	if limit <= 0 {
		limit = 50
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Experience, 0, min(limit, len(s.experiences)))
	for i := len(s.experiences) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, cloneExperience(s.experiences[i]))
	}
	return out
}

func (s *memoryStore) Recall(filter SearchFilter) []Experience {
	if filter.Limit <= 0 {
		filter.Limit = 10
	}
	terms := tokenize(filter.Query)
	type scored struct {
		experience Experience
		score      float64
	}
	s.mu.RLock()
	candidates := make([]scored, 0)
	start := max(0, len(s.experiences)-5000)
	for i := len(s.experiences) - 1; i >= start; i-- {
		experience := s.experiences[i]
		if filter.TaskType != "" && !strings.EqualFold(experience.TaskType, filter.TaskType) ||
			filter.NodeID != "" && experience.NodeID != filter.NodeID ||
			filter.Model != "" && !strings.EqualFold(experience.Model, filter.Model) ||
			filter.Tool != "" && !strings.EqualFold(experience.Tool, filter.Tool) ||
			filter.SuccessOnly && !experience.Success {
			continue
		}
		score := relevance(terms, experience)
		if len(terms) > 0 && score <= 0 {
			continue
		}
		age := time.Since(experience.CreatedAt).Hours()
		score += 1.0 / (1.0 + age/24.0)
		if experience.Success {
			score += 0.5
		}
		candidates = append(candidates, scored{experience: cloneExperience(experience), score: score})
	}
	s.mu.RUnlock()

	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })
	out := make([]Experience, 0, min(filter.Limit, len(candidates)))
	for i := 0; i < len(candidates) && i < filter.Limit; i++ {
		out = append(out, candidates[i].experience)
	}
	return out
}

func (s *memoryStore) PutFact(fact Fact) (Fact, error) {
	if fact.Key == "" {
		return Fact{}, errors.New("memory: fact key is required")
	}
	if fact.Scope == "" {
		fact.Scope = "cluster"
	}
	now := time.Now()
	key := fact.Scope + "|" + fact.Key
	s.mu.Lock()
	if previous, ok := s.facts[key]; ok {
		fact.CreatedAt = previous.CreatedAt
		fact.Hits = previous.Hits
	}
	if fact.CreatedAt.IsZero() {
		fact.CreatedAt = now
	}
	fact.UpdatedAt = now
	fact.Origin = s.origin
	fact.Sequence = s.nextSequenceLocked(s.origin)
	s.facts[key] = fact
	s.mu.Unlock()
	return fact, nil
}

func (s *memoryStore) GetFact(scope, key string) (Fact, error) {
	if scope == "" {
		scope = "cluster"
	}
	mapKey := scope + "|" + key
	s.mu.Lock()
	defer s.mu.Unlock()
	fact, ok := s.facts[mapKey]
	if !ok {
		return Fact{}, ErrNotFound
	}
	fact.Hits++
	fact.Origin = s.origin
	fact.Sequence = s.nextSequenceLocked(s.origin)
	s.facts[mapKey] = fact
	return fact, nil
}

func (s *memoryStore) Facts(scope string) []Fact {
	s.mu.RLock()
	defer s.mu.RUnlock()
	facts := make([]Fact, 0)
	for _, fact := range s.facts {
		if scope == "" || fact.Scope == scope {
			facts = append(facts, fact)
		}
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].UpdatedAt.After(facts[j].UpdatedAt) })
	return facts
}

func (s *memoryStore) DeleteFact(scope, key string) error {
	if scope == "" {
		scope = "cluster"
	}
	s.mu.Lock()
	_, existed := s.facts[scope+"|"+key]
	delete(s.facts, scope+"|"+key)
	s.mu.Unlock()
	if existed {
		s.changes.Notify()
	}
	return nil
}

func (s *memoryStore) SaveAgentSession(harness, id string, payload []byte) error {
	if harness == "" || id == "" {
		return errors.New("memory: agent session harness and id are required")
	}
	if len(payload) == 0 {
		return errors.New("memory: agent session payload is required")
	}
	key := harness + "\x00" + id
	s.mu.Lock()
	s.agentSessions[key] = append([]byte(nil), payload...)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) AgentSessions() ([][]byte, error) {
	s.mu.RLock()
	keys := make([]string, 0, len(s.agentSessions))
	for key := range s.agentSessions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sessions := make([][]byte, 0, len(keys))
	for _, key := range keys {
		sessions = append(sessions, append([]byte(nil), s.agentSessions[key]...))
	}
	s.mu.RUnlock()
	return sessions, nil
}

func (s *memoryStore) DeleteAgentSession(harness, id string) error {
	if harness == "" || id == "" {
		return errors.New("memory: agent session harness and id are required")
	}
	s.mu.Lock()
	delete(s.agentSessions, harness+"\x00"+id)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) SaveDialogue(dialogue DialogueRecord) error {
	if dialogue.ID == "" {
		return errors.New("memory: dialogue id is required")
	}
	now := time.Now()
	if dialogue.CreatedAt.IsZero() {
		dialogue.CreatedAt = now
	}
	dialogue.UpdatedAt = now
	s.mu.Lock()
	dialogue.Origin = s.origin
	dialogue.Sequence = s.nextSequenceLocked(s.origin)
	s.dialogues[dialogue.ID] = cloneDialogue(dialogue)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) AppendDialogueTurn(id string, participants []string, topic string, turn Turn) error {
	if id == "" {
		return errors.New("memory: dialogue id is required")
	}
	if turn.At.IsZero() {
		turn.At = time.Now()
	}
	s.mu.Lock()
	dialogue := s.dialogues[id]
	if dialogue.ID == "" {
		dialogue.ID = id
		dialogue.CreatedAt = time.Now()
		dialogue.Participants = append([]string(nil), participants...)
		dialogue.Topic = topic
	}
	for _, participant := range participants {
		if !contains(dialogue.Participants, participant) {
			dialogue.Participants = append(dialogue.Participants, participant)
		}
	}
	dialogue.Turns = append(dialogue.Turns, turn)
	dialogue.UpdatedAt = time.Now()
	dialogue.Origin = s.origin
	dialogue.Sequence = s.nextSequenceLocked(s.origin)
	s.dialogues[id] = dialogue
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) Dialogue(id string) (DialogueRecord, error) {
	s.mu.RLock()
	dialogue, ok := s.dialogues[id]
	s.mu.RUnlock()
	if !ok {
		return DialogueRecord{}, ErrNotFound
	}
	return cloneDialogue(dialogue), nil
}

func (s *memoryStore) Dialogues(limit int) []DialogueRecord {
	if limit <= 0 {
		limit = 50
	}
	s.mu.RLock()
	dialogues := make([]DialogueRecord, 0, len(s.dialogues))
	for _, dialogue := range s.dialogues {
		dialogues = append(dialogues, cloneDialogue(dialogue))
	}
	s.mu.RUnlock()
	sort.Slice(dialogues, func(i, j int) bool { return dialogues[i].UpdatedAt.After(dialogues[j].UpdatedAt) })
	if len(dialogues) > limit {
		dialogues = dialogues[:limit]
	}
	return dialogues
}

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
	}
}

func (s *memoryStore) LearnedBias(nodeID, model, taskType string) float64 {
	s.mu.RLock()
	performance, ok := s.performances[perfKey(nodeID, model, taskType)]
	if !ok {
		performance, ok = s.performances[perfKey(nodeID, model, "general")]
	}
	s.mu.RUnlock()
	if !ok {
		return 0
	}
	return learnedBias(performance)
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
