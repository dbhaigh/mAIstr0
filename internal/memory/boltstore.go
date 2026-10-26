package memory

import (
	"encoding/binary"
	"fmt"
	"github.com/maistr0/maistr0/internal/syncinterval"
	bolt "go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	bucketExperiences   = []byte("experiences")
	bucketFacts         = []byte("facts")
	bucketPerf          = []byte("performance")
	bucketDialogues     = []byte("dialogues")
	bucketMeta          = []byte("meta")
	bucketAgentSessions = []byte("agent_sessions")
	bucketShardGroups   = []byte("shard_groups")
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
		for _, b := range [][]byte{bucketExperiences, bucketFacts, bucketPerf, bucketDialogues, bucketMeta, bucketAgentSessions, bucketShardGroups} {
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
		_ = db.Close()
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
