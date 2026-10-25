package taskmgr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

type boltStore struct {
	db *bolt.DB
}

func openBoltStore(path string) (*boltStore, error) {
	if path == "" {
		return nil, fmt.Errorf("taskmgr: persistence path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("taskmgr: create data directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("taskmgr: open %s: %w", path, err)
	}
	db.MaxBatchDelay = 250 * time.Microsecond // Bound coalescing delay for synchronous state writes.
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("tasks")); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists([]byte("checkpoints"))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("taskmgr: initialize database: %w", err)
	}
	return &boltStore{db: db}, nil
}

func (s *boltStore) Load() ([]*Task, error) {
	var tasks []*Task
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("tasks"))
		return bucket.ForEach(func(key, value []byte) error {
			var task Task
			if err := json.Unmarshal(value, &task); err != nil {
				return fmt.Errorf("taskmgr: decode task %s: %w", key, err)
			}
			if task.ID == "" {
				return fmt.Errorf("taskmgr: stored task has no id")
			}
			tasks = append(tasks, &task)
			return nil
		})
	})
	return tasks, err
}

func (s *boltStore) Save(task *Task) error {
	if task == nil || task.ID == "" {
		return fmt.Errorf("taskmgr: task id is required")
	}
	encoded, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("taskmgr: encode task %s: %w", task.ID, err)
	}
	return s.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("tasks")).Put([]byte(task.ID), encoded)
	})
}

func (s *boltStore) SaveCheckpoint(checkpoint *Checkpoint) error {
	if checkpoint == nil || checkpoint.ID == "" || checkpoint.TaskID == "" {
		return fmt.Errorf("taskmgr: checkpoint id and task id are required")
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("taskmgr: encode checkpoint %s: %w", checkpoint.ID, err)
	}
	return s.db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("checkpoints"))
		if err := bucket.Put([]byte(checkpoint.ID), encoded); err != nil {
			return err
		}
		return bucket.Put([]byte("latest:"+checkpoint.TaskID), encoded)
	})
}

func (s *boltStore) SaveTaskAndCheckpoint(task *Task, checkpoint *Checkpoint) error {
	if task == nil || task.ID == "" {
		return fmt.Errorf("taskmgr: task id is required")
	}
	if checkpoint == nil || checkpoint.ID == "" || checkpoint.TaskID != task.ID {
		return fmt.Errorf("taskmgr: checkpoint id and matching task id are required")
	}
	encodedTask, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("taskmgr: encode task %s: %w", task.ID, err)
	}
	encodedCheckpoint, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("taskmgr: encode checkpoint %s: %w", checkpoint.ID, err)
	}
	return s.db.Batch(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte("tasks")).Put([]byte(task.ID), encodedTask); err != nil {
			return err
		}
		bucket := tx.Bucket([]byte("checkpoints"))
		if err := bucket.Put([]byte(checkpoint.ID), encodedCheckpoint); err != nil {
			return err
		}
		return bucket.Put([]byte("latest:"+task.ID), encodedCheckpoint)
	})
}

func (s *boltStore) Checkpoints(taskID string) ([]Checkpoint, error) {
	var checkpoints []Checkpoint
	prefix := []byte("cp-" + taskID + "-")
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("checkpoints"))
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(prefix); key != nil && strings.HasPrefix(string(key), string(prefix)); key, value = cursor.Next() {
			var checkpoint Checkpoint
			if err := json.Unmarshal(value, &checkpoint); err == nil {
				checkpoints = append(checkpoints, checkpoint)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].Sequence < checkpoints[j].Sequence
	})
	return checkpoints, nil
}

func (s *boltStore) LatestCheckpoint(taskID string) (*Checkpoint, error) {
	var checkpoint Checkpoint
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("checkpoints"))
		value := bucket.Get([]byte("latest:" + taskID))
		if value == nil {
			return fmt.Errorf("taskmgr: no checkpoint found for task %s", taskID)
		}
		return json.Unmarshal(value, &checkpoint)
	})
	if err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func (s *boltStore) Close() error {
	return s.db.Close()
}
