package memory

import (
	"errors"
	"sort"
	"strings"
	"time"
)

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

func (s *memoryStore) SaveShardGroup(id string, group []byte) error {
	if id == "" || len(group) == 0 {
		return errors.New("memory: shard group id and payload are required")
	}
	s.mu.Lock()
	s.shardGroups[id] = append([]byte(nil), group...)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) ShardGroups() ([][]byte, error) {
	s.mu.RLock()
	ids := make([]string, 0, len(s.shardGroups))
	for id := range s.shardGroups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	groups := make([][]byte, 0, len(ids))
	for _, id := range ids {
		groups = append(groups, append([]byte(nil), s.shardGroups[id]...))
	}
	s.mu.RUnlock()
	return groups, nil
}

func (s *memoryStore) DeleteShardGroup(id string) error {
	if id == "" {
		return errors.New("memory: shard group id is required")
	}
	s.mu.Lock()
	delete(s.shardGroups, id)
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
	return s.AppendDialogueTurns(id, participants, topic, []Turn{turn})
}

func (s *memoryStore) AppendDialogueTurns(id string, participants []string, topic string, turns []Turn) error {
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
	dialogue.Turns = append(dialogue.Turns, turns...)
	dialogue.UpdatedAt = time.Now()
	for range turns {
		dialogue.Origin = s.origin
		dialogue.Sequence = s.nextSequenceLocked(s.origin)
	}
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
