package syncpage

import (
	"fmt"
	"sort"
)

type Record[T any] struct {
	Value    T
	Origin   string
	Sequence uint64
	Key      string
}

func Next[T any](
	records []Record[T],
	cursor, upper map[string]uint64,
	maxBytes int64,
	baseBytes int,
	encode func(T) ([]byte, error),
) ([]T, map[string]uint64, bool, error) {
	eligible := make([]Record[T], 0, len(records))
	legacyRecords := make([]Record[T], 0)
	for _, record := range records {
		if record.Origin == "" || record.Sequence == 0 {
			legacyRecords = append(legacyRecords, record)
		} else {
			eligible = append(eligible, record)
		}
	}
	sort.Slice(legacyRecords, func(i, j int) bool {
		return legacyRecords[i].Key < legacyRecords[j].Key
	})
	legacyUpper := upper["legacy"]
	for index, record := range legacyRecords {
		record.Origin = "legacy"
		record.Sequence = uint64(index + 1)
		if record.Sequence > legacyUpper {
			legacyUpper = record.Sequence
		}
		eligible = append(eligible, record)
	}
	sort.Slice(eligible, func(i, j int) bool {
		left, right := eligible[i], eligible[j]
		if left.Origin != right.Origin {
			return left.Origin < right.Origin
		}
		if left.Sequence != right.Sequence {
			return left.Sequence < right.Sequence
		}
		return left.Key < right.Key
	})

	nextCursor := make(map[string]uint64, len(cursor)+len(upper))
	for origin, sequence := range cursor {
		nextCursor[origin] = sequence
	}
	page := make([]T, 0)
	size := int64(baseBytes)
	for index, record := range eligible {
		if record.Sequence <= nextCursor[record.Origin] {
			continue
		}
		originUpper := upper[record.Origin]
		if record.Origin == "legacy" {
			originUpper = legacyUpper
		}
		if record.Sequence > originUpper {
			continue
		}
		data, err := encode(record.Value)
		if err != nil {
			return nil, nil, false, err
		}
		recordSize := int64(len(data) + 1)
		if size+recordSize > maxBytes {
			if len(page) == 0 {
				return nil, nil, false, fmt.Errorf("sync record %q from %q exceeds the %d-byte page limit", record.Key, record.Origin, maxBytes)
			}
			return page, nextCursor, true, nil
		}
		page = append(page, record.Value)
		size += recordSize
		if record.Sequence > nextCursor[record.Origin] {
			nextCursor[record.Origin] = record.Sequence
		}
		if index == len(eligible)-1 {
			return page, nextCursor, false, nil
		}
	}
	return page, nextCursor, false, nil
}
