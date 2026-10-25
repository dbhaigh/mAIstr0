package consensus

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
)

var (
	raftLogsBucket      = []byte("raft-logs")
	raftStableBucket    = []byte("raft-stable")
	raftMetadataBucket  = []byte("raft-metadata")
	raftAppliedIndexKey = []byte("applied-index")
)

type boltStore struct {
	db *bbolt.DB
}

func openBoltStore(path string) (*boltStore, error) {
	if path == "" {
		return nil, errors.New("consensus: Raft store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("consensus: create Raft store directory: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("consensus: open Raft BoltDB: %w", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{raftLogsBucket, raftStableBucket, raftMetadataBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, errors.Join(fmt.Errorf("consensus: initialize Raft BoltDB: %w", err), db.Close())
	}
	return &boltStore{db: db}, nil
}

func (s *boltStore) FirstIndex() (uint64, error) {
	var index uint64
	err := s.db.View(func(tx *bbolt.Tx) error {
		key, _ := tx.Bucket(raftLogsBucket).Cursor().First()
		if len(key) != 0 {
			index = binary.BigEndian.Uint64(key)
		}
		return nil
	})
	return index, err
}

func (s *boltStore) LastIndex() (uint64, error) {
	var index uint64
	err := s.db.View(func(tx *bbolt.Tx) error {
		key, _ := tx.Bucket(raftLogsBucket).Cursor().Last()
		if len(key) != 0 {
			index = binary.BigEndian.Uint64(key)
		}
		return nil
	})
	return index, err
}

func (s *boltStore) GetLog(index uint64, target *raft.Log) error {
	var raw []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		value := tx.Bucket(raftLogsBucket).Get(indexKey(index))
		if value == nil {
			return raft.ErrLogNotFound
		}
		raw = bytes.Clone(value)
		return nil
	})
	if err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(target); err != nil {
		return fmt.Errorf("consensus: decode Raft log %d: %w", index, err)
	}
	return nil
}

func (s *boltStore) StoreLog(entry *raft.Log) error {
	return s.StoreLogs([]*raft.Log{entry})
}

func (s *boltStore) StoreLogs(entries []*raft.Log) error {
	if len(entries) == 0 {
		return nil
	}
	encoded := make([][]byte, len(entries))
	for index, entry := range entries {
		var buffer bytes.Buffer
		if err := gob.NewEncoder(&buffer).Encode(entry); err != nil {
			return fmt.Errorf("consensus: encode Raft log %d: %w", entry.Index, err)
		}
		encoded[index] = buffer.Bytes()
	}

	err := s.db.Update(func(tx *bbolt.Tx) error {
		logs := tx.Bucket(raftLogsBucket)
		for index, entry := range entries {
			if err := logs.Put(indexKey(entry.Index), encoded[index]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("consensus: store Raft logs: %w", err)
	}
	return nil
}

func (s *boltStore) DeleteRange(min, max uint64) error {
	if min > max {
		return errors.New("consensus: invalid Raft log deletion range")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		logs := tx.Bucket(raftLogsBucket)
		cursor := logs.Cursor()
		for key, _ := cursor.Seek(indexKey(min)); len(key) != 0; key, _ = cursor.Next() {
			index := binary.BigEndian.Uint64(key)
			if index > max {
				break
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *boltStore) RecordAppliedIndex(index uint64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		metadata := tx.Bucket(raftMetadataBucket)
		raw := metadata.Get(raftAppliedIndexKey)
		if len(raw) != 0 {
			if len(raw) != 8 {
				return errors.New("consensus: invalid persisted applied index")
			}
			if binary.BigEndian.Uint64(raw) >= index {
				return nil
			}
		}
		return metadata.Put(raftAppliedIndexKey, indexKey(index))
	})
}

func (s *boltStore) GetAppliedIndex() (uint64, error) {
	var appliedIndex uint64
	err := s.db.View(func(tx *bbolt.Tx) error {
		raw := tx.Bucket(raftMetadataBucket).Get(raftAppliedIndexKey)
		if len(raw) != 0 {
			if len(raw) != 8 {
				return errors.New("consensus: invalid persisted applied index")
			}
			appliedIndex = binary.BigEndian.Uint64(raw)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		return 0, err
	}
	if appliedIndex > lastIndex {
		appliedIndex = lastIndex
	}
	return appliedIndex, nil
}

func (s *boltStore) Set(key, value []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(raftStableBucket).Put(key, value)
	})
}

func (s *boltStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		value = bytes.Clone(tx.Bucket(raftStableBucket).Get(key))
		return nil
	})
	return value, err
}

func (s *boltStore) SetUint64(key []byte, value uint64) error {
	return s.Set(key, indexKey(value))
}

func (s *boltStore) GetUint64(key []byte) (uint64, error) {
	raw, err := s.Get(key)
	if err != nil || len(raw) == 0 {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, errors.New("consensus: invalid persisted Raft uint64")
	}
	return binary.BigEndian.Uint64(raw), nil
}

func (s *boltStore) Close() error { return s.db.Close() }

func indexKey(index uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, index)
	return key
}
