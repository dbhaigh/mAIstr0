package memory

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var ErrPageRecordTooLarge = errors.New("memory: one record exceeds the page response limit")

type HistoryCursor struct {
	ID        uint64
	Key       string
	UpdatedAt time.Time
	Score     float64
	AsOf      time.Time
}

type PageOptions struct {
	Cursor   HistoryCursor
	Limit    int
	MaxBytes int64
}

type HistoryPage[T any] struct {
	Items      []T
	NextCursor HistoryCursor
	HasMore    bool
}

// HistoryPager is optional so existing Store implementations remain source
// compatible while built-in stores provide cursor-based history reads.
type HistoryPager interface {
	ExperiencesPage(SearchFilter, PageOptions) (HistoryPage[Experience], error)
	DialoguesPage(PageOptions) (HistoryPage[DialogueRecord], error)
	FactsPage(string, PageOptions) (HistoryPage[Fact], error)
}

func normalizePageOptions(options PageOptions) PageOptions {
	if options.Limit <= 0 {
		options.Limit = 50
	}
	if options.MaxBytes <= 0 {
		options.MaxBytes = 64 << 20
	}
	return options
}

func boundedPageItemSize(item any, used int64, count int, maxBytes int64) (int64, bool, error) {
	encoded, err := json.Marshal(item)
	if err != nil {
		return 0, false, err
	}
	size := int64(len(encoded))
	if size > maxBytes {
		if count > 0 {
			return 0, false, nil
		}
		return 0, false, ErrPageRecordTooLarge
	}
	required := size
	if count > 0 {
		required++
	}
	return required, used+required <= maxBytes, nil
}

func experienceMatchesFilter(experience Experience, filter SearchFilter) bool {
	return (filter.TaskType == "" || strings.EqualFold(experience.TaskType, filter.TaskType)) &&
		(filter.NodeID == "" || experience.NodeID == filter.NodeID) &&
		(filter.Model == "" || strings.EqualFold(experience.Model, filter.Model)) &&
		(filter.Tool == "" || strings.EqualFold(experience.Tool, filter.Tool)) &&
		(!filter.SuccessOnly || experience.Success)
}

func experiencePageScore(terms []string, experience Experience, asOf time.Time) float64 {
	score := relevance(terms, experience)
	age := asOf.Sub(experience.CreatedAt).Hours()
	score += 1.0 / (1.0 + age/24.0)
	if experience.Success {
		score += 0.5
	}
	return score
}

func experienceFollowsCursor(id uint64, score float64, cursor HistoryCursor, ranked bool) bool {
	if !ranked {
		return id < cursor.ID
	}
	return score < cursor.Score || (score == cursor.Score && id < cursor.ID)
}

func (s *memoryStore) ExperiencesPage(filter SearchFilter, options PageOptions) (HistoryPage[Experience], error) {
	options = normalizePageOptions(options)
	terms := tokenize(filter.Query)
	ranked := strings.TrimSpace(filter.Query) != ""
	asOf := options.Cursor.AsOf
	if asOf.IsZero() {
		asOf = time.Now()
	}

	if !ranked {
		s.mu.RLock()
		page := HistoryPage[Experience]{Items: make([]Experience, 0, options.Limit)}
		var used int64
		for index := len(s.experiences) - 1; index >= 0; index-- {
			experience := s.experiences[index]
			if !experienceMatchesFilter(experience, filter) ||
				(options.Cursor.ID > 0 && experience.ID >= options.Cursor.ID) {
				continue
			}
			if len(page.Items) == options.Limit {
				page.HasMore = true
				break
			}
			size, fits, err := boundedPageItemSize(experience, used, len(page.Items), options.MaxBytes)
			if err != nil {
				s.mu.RUnlock()
				return HistoryPage[Experience]{}, err
			}
			if !fits {
				page.HasMore = true
				break
			}
			page.Items = append(page.Items, cloneExperience(experience))
			used += size
			page.NextCursor = HistoryCursor{ID: experience.ID}
		}
		s.mu.RUnlock()
		return page, nil
	}

	type candidate struct {
		index int
		id    uint64
		score float64
	}
	s.mu.RLock()
	candidates := make([]candidate, 0, len(s.experiences))
	start := 0
	if ranked && len(s.experiences) > 5000 {
		start = len(s.experiences) - 5000
	}
	for i := len(s.experiences) - 1; i >= start; i-- {
		experience := s.experiences[i]
		if !experienceMatchesFilter(experience, filter) {
			continue
		}
		score := float64(0)
		if ranked {
			base := relevance(terms, experience)
			if len(terms) > 0 && base <= 0 {
				continue
			}
			score = experiencePageScore(terms, experience, asOf)
		}
		if options.Cursor.ID > 0 && !experienceFollowsCursor(experience.ID, score, options.Cursor, ranked) {
			continue
		}
		candidates = append(candidates, candidate{index: i, id: experience.ID, score: score})
	}
	if ranked {
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].score == candidates[j].score {
				return candidates[i].id > candidates[j].id
			}
			return candidates[i].score > candidates[j].score
		})
	}

	page := HistoryPage[Experience]{Items: make([]Experience, 0, options.Limit)}
	var used int64
	for _, item := range candidates {
		if len(page.Items) == options.Limit {
			page.HasMore = true
			break
		}
		experience := s.experiences[item.index]
		size, fits, err := boundedPageItemSize(experience, used, len(page.Items), options.MaxBytes)
		if err != nil {
			s.mu.RUnlock()
			return HistoryPage[Experience]{}, err
		}
		if !fits {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, cloneExperience(experience))
		used += size
		page.NextCursor = HistoryCursor{ID: item.id, Score: item.score, AsOf: asOf}
	}
	s.mu.RUnlock()
	return page, nil
}

func (s *boltStore) ExperiencesPage(filter SearchFilter, options PageOptions) (HistoryPage[Experience], error) {
	options = normalizePageOptions(options)
	terms := tokenize(filter.Query)
	ranked := strings.TrimSpace(filter.Query) != ""
	if !ranked {
		return s.experiencesByIDPage(filter, options)
	}
	asOf := options.Cursor.AsOf
	if asOf.IsZero() {
		asOf = time.Now()
	}

	type candidate struct {
		id    uint64
		score float64
	}
	var page HistoryPage[Experience]
	err := s.db.View(func(tx *bolt.Tx) error {
		candidates := make([]candidate, 0)
		c := tx.Bucket(bucketExperiences).Cursor()
		scanned := 0
		for key, value := c.Last(); key != nil && scanned < 5000; key, value = c.Prev() {
			scanned++
			if len(key) != 8 {
				continue
			}
			var experience Experience
			if json.Unmarshal(value, &experience) != nil || !experienceMatchesFilter(experience, filter) {
				continue
			}
			base := relevance(terms, experience)
			if len(terms) > 0 && base <= 0 {
				continue
			}
			id := binary.BigEndian.Uint64(key)
			score := experiencePageScore(terms, experience, asOf)
			if options.Cursor.ID > 0 && !experienceFollowsCursor(id, score, options.Cursor, true) {
				continue
			}
			candidates = append(candidates, candidate{id: id, score: score})
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].score == candidates[j].score {
				return candidates[i].id > candidates[j].id
			}
			return candidates[i].score > candidates[j].score
		})

		page = HistoryPage[Experience]{Items: make([]Experience, 0, options.Limit)}
		var used int64
		bucket := tx.Bucket(bucketExperiences)
		for _, candidate := range candidates {
			if len(page.Items) == options.Limit {
				page.HasMore = true
				break
			}
			raw := bucket.Get(itob(candidate.id))
			if raw == nil {
				continue
			}
			var experience Experience
			if json.Unmarshal(raw, &experience) != nil {
				continue
			}
			size, fits, err := boundedPageItemSize(experience, used, len(page.Items), options.MaxBytes)
			if err != nil {
				return err
			}
			if !fits {
				page.HasMore = true
				break
			}
			page.Items = append(page.Items, experience)
			used += size
			page.NextCursor = HistoryCursor{ID: candidate.id, Score: candidate.score, AsOf: asOf}
		}
		return nil
	})
	return page, err
}

func (s *boltStore) experiencesByIDPage(filter SearchFilter, options PageOptions) (HistoryPage[Experience], error) {
	var page HistoryPage[Experience]
	err := s.db.View(func(tx *bolt.Tx) error {
		page = HistoryPage[Experience]{Items: make([]Experience, 0, options.Limit)}
		var used int64
		cursor := tx.Bucket(bucketExperiences).Cursor()
		var key, value []byte
		if options.Cursor.ID == 0 {
			key, value = cursor.Last()
		} else {
			key, _ = cursor.Seek(itob(options.Cursor.ID))
			if key == nil {
				key, value = cursor.Last()
			} else {
				key, value = cursor.Prev()
			}
		}
		for ; key != nil; key, value = cursor.Prev() {
			if len(key) != 8 {
				continue
			}
			var experience Experience
			if json.Unmarshal(value, &experience) != nil || !experienceMatchesFilter(experience, filter) {
				continue
			}
			if len(page.Items) == options.Limit {
				page.HasMore = true
				break
			}
			size, fits, err := boundedPageItemSize(experience, used, len(page.Items), options.MaxBytes)
			if err != nil {
				return err
			}
			if !fits {
				page.HasMore = true
				break
			}
			page.Items = append(page.Items, experience)
			used += size
			page.NextCursor = HistoryCursor{ID: binary.BigEndian.Uint64(key)}
		}
		return nil
	})
	return page, err
}

type dialoguePageIndex struct {
	ID        string
	UpdatedAt time.Time
}

func dialogueFollowsCursor(dialogue dialoguePageIndex, cursor HistoryCursor) bool {
	if dialogue.UpdatedAt.Equal(cursor.UpdatedAt) {
		return dialogue.ID < cursor.Key
	}
	return dialogue.UpdatedAt.Before(cursor.UpdatedAt)
}

func sortDialoguePageIndex(items []dialoguePageIndex) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].ID > items[j].ID
		}
		return items[i].UpdatedAt.After(items[j].UpdatedAt)
	})
}

func (s *memoryStore) DialoguesPage(options PageOptions) (HistoryPage[DialogueRecord], error) {
	options = normalizePageOptions(options)
	s.mu.RLock()
	index := make([]dialoguePageIndex, 0, len(s.dialogues))
	for id, dialogue := range s.dialogues {
		index = append(index, dialoguePageIndex{ID: id, UpdatedAt: dialogue.UpdatedAt})
	}
	sortDialoguePageIndex(index)
	page := HistoryPage[DialogueRecord]{Items: make([]DialogueRecord, 0, options.Limit)}
	var used int64
	for _, item := range index {
		if options.Cursor.Key != "" && !dialogueFollowsCursor(item, options.Cursor) {
			continue
		}
		if len(page.Items) == options.Limit {
			page.HasMore = true
			break
		}
		dialogue := s.dialogues[item.ID]
		size, fits, err := boundedPageItemSize(dialogue, used, len(page.Items), options.MaxBytes)
		if err != nil {
			s.mu.RUnlock()
			return HistoryPage[DialogueRecord]{}, err
		}
		if !fits {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, cloneDialogue(dialogue))
		used += size
		page.NextCursor = HistoryCursor{Key: item.ID, UpdatedAt: item.UpdatedAt}
	}
	s.mu.RUnlock()
	return page, nil
}

func (s *boltStore) DialoguesPage(options PageOptions) (HistoryPage[DialogueRecord], error) {
	options = normalizePageOptions(options)
	var page HistoryPage[DialogueRecord]
	err := s.db.View(func(tx *bolt.Tx) error {
		index := make([]dialoguePageIndex, 0)
		if err := tx.Bucket(bucketDialogues).ForEach(func(key, value []byte) error {
			var metadata struct {
				UpdatedAt time.Time `json:"updated_at"`
			}
			if err := json.Unmarshal(value, &metadata); err != nil {
				return nil
			}
			index = append(index, dialoguePageIndex{ID: string(key), UpdatedAt: metadata.UpdatedAt})
			return nil
		}); err != nil {
			return err
		}
		sortDialoguePageIndex(index)
		page = HistoryPage[DialogueRecord]{Items: make([]DialogueRecord, 0, options.Limit)}
		var used int64
		bucket := tx.Bucket(bucketDialogues)
		for _, item := range index {
			if options.Cursor.Key != "" && !dialogueFollowsCursor(item, options.Cursor) {
				continue
			}
			if len(page.Items) == options.Limit {
				page.HasMore = true
				break
			}
			raw := bucket.Get([]byte(item.ID))
			var dialogue DialogueRecord
			if raw == nil || json.Unmarshal(raw, &dialogue) != nil {
				continue
			}
			size, fits, err := boundedPageItemSize(dialogue, used, len(page.Items), options.MaxBytes)
			if err != nil {
				return err
			}
			if !fits {
				page.HasMore = true
				break
			}
			page.Items = append(page.Items, dialogue)
			used += size
			page.NextCursor = HistoryCursor{Key: item.ID, UpdatedAt: item.UpdatedAt}
		}
		return nil
	})
	return page, err
}

type factPageIndex struct {
	Key       string
	UpdatedAt time.Time
}

func sortFactPageIndex(items []factPageIndex) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].Key > items[j].Key
		}
		return items[i].UpdatedAt.After(items[j].UpdatedAt)
	})
}

func factFollowsCursor(item factPageIndex, cursor HistoryCursor) bool {
	if item.UpdatedAt.Equal(cursor.UpdatedAt) {
		return item.Key < cursor.Key
	}
	return item.UpdatedAt.Before(cursor.UpdatedAt)
}

func (s *memoryStore) FactsPage(scope string, options PageOptions) (HistoryPage[Fact], error) {
	options = normalizePageOptions(options)
	s.mu.RLock()
	index := make([]factPageIndex, 0, len(s.facts))
	for key, fact := range s.facts {
		if scope == "" || fact.Scope == scope {
			index = append(index, factPageIndex{Key: key, UpdatedAt: fact.UpdatedAt})
		}
	}
	sortFactPageIndex(index)
	page := HistoryPage[Fact]{Items: make([]Fact, 0, options.Limit)}
	var used int64
	for _, item := range index {
		if options.Cursor.Key != "" && !factFollowsCursor(item, options.Cursor) {
			continue
		}
		if len(page.Items) == options.Limit {
			page.HasMore = true
			break
		}
		fact := s.facts[item.Key]
		size, fits, err := boundedPageItemSize(fact, used, len(page.Items), options.MaxBytes)
		if err != nil {
			s.mu.RUnlock()
			return HistoryPage[Fact]{}, err
		}
		if !fits {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, fact)
		used += size
		page.NextCursor = HistoryCursor{Key: item.Key, UpdatedAt: item.UpdatedAt}
	}
	s.mu.RUnlock()
	return page, nil
}

func (s *boltStore) FactsPage(scope string, options PageOptions) (HistoryPage[Fact], error) {
	options = normalizePageOptions(options)
	var page HistoryPage[Fact]
	err := s.db.View(func(tx *bolt.Tx) error {
		index := make([]factPageIndex, 0)
		bucket := tx.Bucket(bucketFacts)
		appendMetadata := func(key, value []byte) error {
			var metadata struct {
				Scope     string    `json:"scope"`
				UpdatedAt time.Time `json:"updated_at"`
			}
			if err := json.Unmarshal(value, &metadata); err != nil {
				return nil
			}
			if scope == "" || metadata.Scope == scope {
				index = append(index, factPageIndex{Key: string(key), UpdatedAt: metadata.UpdatedAt})
			}
			return nil
		}
		if scope == "" {
			if err := bucket.ForEach(appendMetadata); err != nil {
				return err
			}
		} else {
			prefix := []byte(scope + "|")
			cursor := bucket.Cursor()
			for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
				if err := appendMetadata(key, value); err != nil {
					return err
				}
			}
		}
		sortFactPageIndex(index)
		page = HistoryPage[Fact]{Items: make([]Fact, 0, options.Limit)}
		var used int64
		for _, item := range index {
			if options.Cursor.Key != "" && !factFollowsCursor(item, options.Cursor) {
				continue
			}
			if len(page.Items) == options.Limit {
				page.HasMore = true
				break
			}
			var fact Fact
			if raw := bucket.Get([]byte(item.Key)); raw == nil || json.Unmarshal(raw, &fact) != nil {
				continue
			}
			size, fits, err := boundedPageItemSize(fact, used, len(page.Items), options.MaxBytes)
			if err != nil {
				return err
			}
			if !fits {
				page.HasMore = true
				break
			}
			page.Items = append(page.Items, fact)
			used += size
			page.NextCursor = HistoryCursor{Key: item.Key, UpdatedAt: item.UpdatedAt}
		}
		return nil
	})
	return page, err
}

var _ HistoryPager = (*boltStore)(nil)
var _ HistoryPager = (*memoryStore)(nil)
