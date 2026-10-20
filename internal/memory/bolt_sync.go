package memory

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	bolt "go.etcd.io/bbolt"
)

var localOriginKey = []byte("local-origin")

func originSequenceKey(origin string) []byte {
	return []byte("origin-sequence:" + origin)
}

func originWatermarkKey(origin string) []byte {
	return []byte("origin-watermark:" + origin)
}

func peerWatermarkKey(peer string) []byte {
	return []byte("peer-watermarks:" + peer)
}

func resolveLocalOrigin(tx *bolt.Tx, owner, path string) (string, error) {
	if owner != "" {
		return owner, nil
	}
	meta := tx.Bucket(bucketMeta)
	if saved := meta.Get(localOriginKey); len(saved) > 0 {
		return string(saved), nil
	}
	origin := "store:" + path
	if err := meta.Put(localOriginKey, []byte(origin)); err != nil {
		return "", err
	}
	return origin, nil
}

func watermarkValue(meta *bolt.Bucket, origin string) uint64 {
	value := meta.Get(originWatermarkKey(origin))
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

func saveWatermark(meta *bolt.Bucket, origin string, sequence uint64) error {
	current := watermarkValue(meta, origin)
	if sequence <= current {
		return nil
	}
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], sequence)
	return meta.Put(originWatermarkKey(origin), value[:])
}

func nextOriginSequence(tx *bolt.Tx, origin string) (uint64, error) {
	meta := tx.Bucket(bucketMeta)
	key := originSequenceKey(origin)
	var sequence uint64
	if value := meta.Get(key); len(value) == 8 {
		sequence = binary.BigEndian.Uint64(value)
	}
	if watermark := watermarkValue(meta, origin); watermark > sequence {
		sequence = watermark
	}
	sequence++
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], sequence)
	if err := meta.Put(key, value[:]); err != nil {
		return 0, err
	}
	if err := saveWatermark(meta, origin, sequence); err != nil {
		return 0, err
	}
	return sequence, nil
}

func updateRecordWatermark(tx *bolt.Tx, origin string, sequence uint64) error {
	if origin == "" || sequence == 0 {
		return nil
	}
	return saveWatermark(tx.Bucket(bucketMeta), origin, sequence)
}

func (s *boltStore) Watermarks() map[string]uint64 {
	s.mu.RLock()
	if s.watermarksCacheValid {
		cached := cloneWatermarks(s.watermarksCache)
		s.mu.RUnlock()
		return cached
	}
	generation := s.watermarksCacheGeneration
	s.mu.RUnlock()

	watermarks := make(map[string]uint64)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).ForEach(func(key, value []byte) error {
			if !bytes.HasPrefix(key, []byte("origin-watermark:")) {
				return nil
			}
			if len(value) != 8 {
				return fmt.Errorf("memory: invalid watermark value for %s", key)
			}
			origin := strings.TrimPrefix(string(key), "origin-watermark:")
			watermarks[origin] = binary.BigEndian.Uint64(value)
			return nil
		})
	})
	if err != nil {
		log.Printf("FILE_ONLY: memory: read watermarks: %v", err)
		return watermarks
	}
	s.mu.Lock()
	if generation == s.watermarksCacheGeneration {
		s.watermarksCache = cloneWatermarks(watermarks)
		s.watermarksCacheValid = true
	}
	s.mu.Unlock()
	return watermarks
}

func (s *boltStore) PeerWatermarks(peer string) map[string]uint64 {
	if peer == "" {
		return map[string]uint64{}
	}
	s.mu.RLock()
	if s.peerWatermarksLoaded[peer] {
		cached := cloneWatermarks(s.peerWatermarksCache[peer])
		s.mu.RUnlock()
		return cached
	}
	generation := s.peerWatermarksGeneration[peer]
	s.mu.RUnlock()

	watermarks := make(map[string]uint64)
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(bucketMeta).Get(peerWatermarkKey(peer))
		if value == nil {
			return nil
		}
		if err := json.Unmarshal(value, &watermarks); err != nil {
			return fmt.Errorf("memory: decode watermarks for peer %s: %w", peer, err)
		}
		return nil
	})
	if err != nil {
		log.Printf("FILE_ONLY: memory: read watermarks for peer %s: %v", peer, err)
		return map[string]uint64{}
	}
	s.mu.Lock()
	if generation == s.peerWatermarksGeneration[peer] {
		s.peerWatermarksCache[peer] = cloneWatermarks(watermarks)
		s.peerWatermarksLoaded[peer] = true
	}
	s.mu.Unlock()
	return watermarks
}

func (s *boltStore) SetPeerWatermarks(peer string, watermarks map[string]uint64) error {
	if strings.TrimSpace(peer) == "" {
		return errors.New("memory: peer id is required for watermarks")
	}
	var merged map[string]uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		merged, err = mergePeerWatermarksTx(tx, peer, watermarks)
		return err
	})
	if err != nil {
		return err
	}
	s.cachePeerWatermarks(peer, merged)
	return nil
}

func mergePeerWatermarksTx(tx *bolt.Tx, peer string, watermarks map[string]uint64) (map[string]uint64, error) {
	meta := tx.Bucket(bucketMeta)
	merged := make(map[string]uint64)
	if value := meta.Get(peerWatermarkKey(peer)); value != nil {
		if err := json.Unmarshal(value, &merged); err != nil {
			return nil, fmt.Errorf("memory: decode watermarks for peer %s: %w", peer, err)
		}
	}
	merged = mergeWatermarks(merged, watermarks)
	encoded, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}
	if err := meta.Put(peerWatermarkKey(peer), encoded); err != nil {
		return nil, err
	}
	return merged, nil
}

func (s *boltStore) cachePeerWatermarks(peer string, merged map[string]uint64) {
	s.mu.Lock()
	s.peerWatermarksCache[peer] = cloneWatermarks(merged)
	s.peerWatermarksLoaded[peer] = true
	s.peerWatermarksGeneration[peer]++
	s.mu.Unlock()
}

func (s *boltStore) backfillRecordMetadata() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := backfillExperiences(tx, s.owner); err != nil {
			return err
		}
		if err := backfillFacts(tx, s.owner); err != nil {
			return err
		}
		if err := backfillPerformances(tx, s.owner); err != nil {
			return err
		}
		return backfillDialogues(tx, s.owner)
	})
}

func backfillExperiences(tx *bolt.Tx, origin string) error {
	bucket := tx.Bucket(bucketExperiences)
	return backfillBucket[Experience](tx, bucket, origin, func(record *Experience) {
		record.Origin, record.Sequence = origin, 0
	}, func(record *Experience, sequence uint64) {
		record.Origin, record.Sequence = origin, sequence
	})
}

func backfillFacts(tx *bolt.Tx, origin string) error {
	bucket := tx.Bucket(bucketFacts)
	return backfillBucket[Fact](tx, bucket, origin, func(record *Fact) {
		record.Origin, record.Sequence = origin, 0
	}, func(record *Fact, sequence uint64) {
		record.Origin, record.Sequence = origin, sequence
	})
}

func backfillPerformances(tx *bolt.Tx, origin string) error {
	bucket := tx.Bucket(bucketPerf)
	return backfillBucket[Performance](tx, bucket, origin, func(record *Performance) {
		record.Origin, record.Sequence = origin, 0
	}, func(record *Performance, sequence uint64) {
		record.Origin, record.Sequence = origin, sequence
	})
}

func backfillDialogues(tx *bolt.Tx, origin string) error {
	bucket := tx.Bucket(bucketDialogues)
	return backfillBucket[DialogueRecord](tx, bucket, origin, func(record *DialogueRecord) {
		record.Origin, record.Sequence = origin, 0
	}, func(record *DialogueRecord, sequence uint64) {
		record.Origin, record.Sequence = origin, sequence
	})
}

func backfillBucket[T any](
	tx *bolt.Tx,
	bucket *bolt.Bucket,
	origin string,
	clearMetadata func(*T),
	setMetadata func(*T, uint64),
) error {
	type entry struct {
		key   []byte
		value []byte
	}
	var entries []entry
	if err := bucket.ForEach(func(key, value []byte) error {
		entries = append(entries, entry{
			key:   append([]byte(nil), key...),
			value: append([]byte(nil), value...),
		})
		return nil
	}); err != nil {
		return err
	}
	for _, entry := range entries {
		var record T
		if err := json.Unmarshal(entry.value, &record); err != nil {
			return fmt.Errorf("memory: decode legacy record %s: %w", entry.key, err)
		}
		originValue, sequenceValue := recordMetadata(&record)
		if originValue == "" || sequenceValue == 0 {
			clearMetadata(&record)
			sequence, err := nextOriginSequence(tx, origin)
			if err != nil {
				return err
			}
			setMetadata(&record, sequence)
			encoded, err := json.Marshal(&record)
			if err != nil {
				return err
			}
			if err := bucket.Put(entry.key, encoded); err != nil {
				return err
			}
			continue
		}
		if err := updateRecordWatermark(tx, originValue, sequenceValue); err != nil {
			return err
		}
	}
	return nil
}

func recordMetadata(record any) (string, uint64) {
	switch value := record.(type) {
	case *Experience:
		return value.Origin, value.Sequence
	case *Fact:
		return value.Origin, value.Sequence
	case *Performance:
		return value.Origin, value.Sequence
	case *DialogueRecord:
		return value.Origin, value.Sequence
	default:
		return "", 0
	}
}

func updateSnapshotWatermarks(tx *bolt.Tx, watermarks map[string]uint64) error {
	for origin, sequence := range watermarks {
		if err := updateRecordWatermark(tx, origin, sequence); err != nil {
			return err
		}
	}
	return nil
}
