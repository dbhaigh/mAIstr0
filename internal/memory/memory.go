// Package memory defines the knowledge-store contract and its shared record types.
package memory

import (
	"errors"
	"time"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("memory: record not found")

type Experience struct {
	ID          uint64    `json:"id"`
	Origin      string    `json:"origin,omitempty"`
	Sequence    uint64    `json:"sequence,omitempty"`
	Kind        string    `json:"kind"`
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
	Rating      float64   `json:"rating,omitempty"`
	Tags        []string  `json:"tags,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type Fact struct {
	Key       string    `json:"key"`
	Origin    string    `json:"origin,omitempty"`
	Sequence  uint64    `json:"sequence,omitempty"`
	Value     string    `json:"value"`
	Scope     string    `json:"scope,omitempty"`
	Source    string    `json:"source,omitempty"`
	Hits      int       `json:"hits"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Performance struct {
	NodeID        string    `json:"node_id"`
	Origin        string    `json:"origin,omitempty"`
	Sequence      uint64    `json:"sequence,omitempty"`
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

type DialogueRecord struct {
	ID           string    `json:"id"`
	Origin       string    `json:"origin,omitempty"`
	Sequence     uint64    `json:"sequence,omitempty"`
	Participants []string  `json:"participants"`
	Topic        string    `json:"topic,omitempty"`
	Turns        []Turn    `json:"turns"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type Turn struct {
	Speaker    string    `json:"speaker"`
	NodeID     string    `json:"node_id,omitempty"`
	Model      string    `json:"model,omitempty"`
	Content    string    `json:"content"`
	DurationMs int64     `json:"duration_ms,omitempty"`
	At         time.Time `json:"at"`
}

type Store interface {
	Path() string
	Close() error
	Snapshot() Snapshot
	SnapshotSince(map[string]uint64) Snapshot
	Watermarks() map[string]uint64
	MergeSnapshot(Snapshot) error
	MergeSnapshotFrom(Snapshot, string) error
	PeerWatermarks(string) map[string]uint64
	SetPeerWatermarks(string, map[string]uint64) error
	Record(Experience) (Experience, error)
	Experiences(limit int) []Experience
	Recall(SearchFilter) []Experience
	PutFact(Fact) (Fact, error)
	GetFact(scope, key string) (Fact, error)
	Facts(scope string) []Fact
	DeleteFact(scope, key string) error
	SaveAgentSession(harness, id string, session []byte) error
	AgentSessions() ([][]byte, error)
	DeleteAgentSession(harness, id string) error
	SaveDialogue(DialogueRecord) error
	AppendDialogueTurn(id string, participants []string, topic string, turn Turn) error
	Dialogue(id string) (DialogueRecord, error)
	Dialogues(limit int) []DialogueRecord
	Performances() []Performance
	MergePerformances([]Performance)
	LearnedBias(nodeID, model, taskType string) float64
	Recommend(taskType string, limit int) []Recommendation
	Insights() Insights
	Prune(olderThan time.Duration) (int, error)
	ContextBrief(query, taskType string, limit int) string
}

// DialogueBatchAppender atomically appends multiple turns when supported.
type DialogueBatchAppender interface {
	AppendDialogueTurns(id string, participants []string, topic string, turns []Turn) error
}

// ChangeSource is optionally implemented by stores that can wake sync loops
// when locally visible memory changes warrant an immediate sync pass.
type ChangeSource interface {
	Changes() <-chan struct{}
}

type Snapshot struct {
	Experiences  []Experience      `json:"experiences"`
	Facts        []Fact            `json:"facts"`
	Performances []Performance     `json:"performances"`
	Dialogues    []DialogueRecord  `json:"dialogues"`
	SyncVersion  int               `json:"sync_version,omitempty"`
	Watermarks   map[string]uint64 `json:"watermarks,omitempty"`
}

type Payload struct {
	RecordIDs []string
	Snapshot  Snapshot
}

type SearchFilter struct {
	Query       string
	TaskType    string
	NodeID      string
	Model       string
	Tool        string
	SuccessOnly bool
	Limit       int
}

type Recommendation struct {
	TaskType      string  `json:"task_type"`
	NodeID        string  `json:"node_id"`
	Model         string  `json:"model"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs int64   `json:"avg_duration_ms"`
	Attempts      int     `json:"attempts"`
	Confidence    float64 `json:"confidence"`
	Reason        string  `json:"reason"`
}

type Insights struct {
	TotalExperiences int              `json:"total_experiences"`
	Successes        int              `json:"successes"`
	Failures         int              `json:"failures"`
	SuccessRate      float64          `json:"success_rate"`
	AvgDurationMs    int64            `json:"avg_duration_ms"`
	TrackedPairings  int              `json:"tracked_pairings"`
	Facts            int              `json:"facts"`
	Dialogues        int              `json:"dialogues"`
	TopPairings      []Recommendation `json:"top_pairings"`
	WeakPairings     []Recommendation `json:"weak_pairings"`
	TaskTypes        map[string]int   `json:"task_types"`
	Lessons          []string         `json:"lessons"`
	DatabasePath     string           `json:"database_path"`
}
