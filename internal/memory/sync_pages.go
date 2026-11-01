package memory

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/maistr0/maistr0/internal/syncpage"
)

func PageSnapshot(snapshot Snapshot, cursor, upper map[string]uint64, maxBytes int64, envelopeBytes int) (Snapshot, map[string]uint64, bool, error) {
	records := make([]syncpage.Record[any], 0, len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Performances)+len(snapshot.Dialogues))
	for i := range snapshot.Experiences {
		experience := &snapshot.Experiences[i]
		records = append(records, syncpage.Record[any]{
			Value: experience, Origin: experience.Origin, Sequence: experience.Sequence,
		})
	}
	for i := range snapshot.Facts {
		fact := &snapshot.Facts[i]
		records = append(records, syncpage.Record[any]{
			Value: fact, Origin: fact.Origin, Sequence: fact.Sequence,
		})
	}
	for i := range snapshot.Performances {
		performance := &snapshot.Performances[i]
		records = append(records, syncpage.Record[any]{
			Value: performance, Origin: performance.Origin, Sequence: performance.Sequence,
		})
	}
	for i := range snapshot.Dialogues {
		dialogue := &snapshot.Dialogues[i]
		records = append(records, syncpage.Record[any]{
			Value: dialogue, Origin: dialogue.Origin, Sequence: dialogue.Sequence,
		})
	}

	base := Snapshot{
		Experiences: make([]Experience, 0), Facts: make([]Fact, 0),
		Performances: make([]Performance, 0), Dialogues: make([]DialogueRecord, 0),
		SyncVersion: SyncProtocolVersion, Watermarks: cloneWatermarks(upper),
	}
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return Snapshot{}, nil, false, err
	}
	values, nextCursor, more, err := syncpage.NextWithKey(
		records, cursor, upper, maxBytes, len(baseJSON)+envelopeBytes,
		func(value any) ([]byte, error) {
			return json.Marshal(value)
		},
		syncpage.KeyFuncs[any]{Compare: compareSnapshotKeys, Format: formatSnapshotKey},
	)
	if err != nil {
		return Snapshot{}, nil, false, err
	}

	page := Snapshot{
		Experiences: make([]Experience, 0), Facts: make([]Fact, 0),
		Performances: make([]Performance, 0), Dialogues: make([]DialogueRecord, 0),
		SyncVersion: SyncProtocolVersion,
	}
	for _, value := range values {
		switch record := value.(type) {
		case *Experience:
			page.Experiences = append(page.Experiences, *record)
		case *Fact:
			page.Facts = append(page.Facts, *record)
		case *Performance:
			page.Performances = append(page.Performances, *record)
		case *DialogueRecord:
			page.Dialogues = append(page.Dialogues, *record)
		default:
			return Snapshot{}, nil, false, fmt.Errorf("memory: unsupported sync record type %T", value)
		}
	}
	if !more {
		page.Watermarks = cloneWatermarks(upper)
	}
	return page, nextCursor, more, nil
}

// Kind values follow the lexical order of the prefixes used in memory sort keys.
const (
	snapshotKeyDialogue = iota
	snapshotKeyExperience
	snapshotKeyFact
	snapshotKeyPerformance
)

type snapshotSortKey struct {
	kind      int
	number    uint64
	parts     [5]string
	partCount int
}

func compareSnapshotKeys(left, right any) int {
	leftKey, rightKey := snapshotSortKeyFor(left), snapshotSortKeyFor(right)
	if leftKey.kind < rightKey.kind {
		return -1
	}
	if leftKey.kind > rightKey.kind {
		return 1
	}
	if leftKey.kind == snapshotKeyExperience {
		if leftKey.number < rightKey.number {
			return -1
		}
		if leftKey.number > rightKey.number {
			return 1
		}
		return 0
	}
	return compareSnapshotKeyParts(leftKey, rightKey)
}

func snapshotSortKeyFor(value any) snapshotSortKey {
	switch record := value.(type) {
	case *DialogueRecord:
		return snapshotSortKey{kind: snapshotKeyDialogue, parts: [5]string{record.ID}, partCount: 1}
	case *Experience:
		return snapshotSortKey{kind: snapshotKeyExperience, number: record.ID}
	case *Fact:
		return snapshotSortKey{kind: snapshotKeyFact, parts: [5]string{record.Scope, "\x00", record.Key}, partCount: 3}
	case *Performance:
		return snapshotSortKey{kind: snapshotKeyPerformance, parts: [5]string{
			record.NodeID, "\x00", record.Model, "\x00", record.TaskType,
		}, partCount: 5}
	default:
		return snapshotSortKey{kind: 255, parts: [5]string{fmt.Sprintf("%T", value)}, partCount: 1}
	}
}

func compareSnapshotKeyParts(left, right snapshotSortKey) int {
	leftPart, rightPart := 0, 0
	leftOffset, rightOffset := 0, 0
	for {
		for leftPart < left.partCount && leftOffset == len(left.parts[leftPart]) {
			leftPart++
			leftOffset = 0
		}
		for rightPart < right.partCount && rightOffset == len(right.parts[rightPart]) {
			rightPart++
			rightOffset = 0
		}
		if leftPart == left.partCount {
			if rightPart == right.partCount {
				return 0
			}
			return -1
		}
		if rightPart == right.partCount {
			return 1
		}
		leftByte, rightByte := left.parts[leftPart][leftOffset], right.parts[rightPart][rightOffset]
		if leftByte < rightByte {
			return -1
		}
		if leftByte > rightByte {
			return 1
		}
		leftOffset++
		rightOffset++
	}
}

func formatSnapshotKey(value any) string {
	switch record := value.(type) {
	case *DialogueRecord:
		return "dialogue:" + record.ID
	case *Experience:
		return experiencePageKey(record.ID)
	case *Fact:
		return "fact:" + record.Scope + "\x00" + record.Key
	case *Performance:
		return "performance:" + record.NodeID + "\x00" + record.Model + "\x00" + record.TaskType
	default:
		return fmt.Sprintf("unsupported sync record type %T", value)
	}
}

func experiencePageKey(id uint64) string {
	const prefix = "experience:"
	var digits [20]byte
	number := strconv.AppendUint(digits[:0], id, 10)
	var key [len(prefix) + 20]byte
	copy(key[:], prefix)
	padding := 20 - len(number)
	for i := 0; i < padding; i++ {
		key[len(prefix)+i] = '0'
	}
	copy(key[len(prefix)+padding:], number)
	return string(key[:])
}

func SplitSnapshot(snapshot Snapshot, maxBytes int64, envelopeBytes int) ([]Snapshot, error) {
	upper := cloneWatermarks(snapshot.Watermarks)
	if len(upper) == 0 {
		upper = snapshotWatermarks(snapshot)
	}
	cursor := make(map[string]uint64)
	pages := make([]Snapshot, 0, 1)
	for {
		page, nextCursor, more, err := PageSnapshot(snapshot, cursor, upper, maxBytes, envelopeBytes)
		if err != nil {
			return nil, err
		}
		pages = append(pages, page)
		if !more {
			return pages, nil
		}
		if len(page.Experiences)+len(page.Facts)+len(page.Performances)+len(page.Dialogues) == 0 {
			return nil, fmt.Errorf("memory: sync pagination made no progress")
		}
		cursor = nextCursor
	}
}

func snapshotWatermarks(snapshot Snapshot) map[string]uint64 {
	watermarks := make(map[string]uint64)
	add := func(origin string, sequence uint64) {
		if origin != "" && sequence > watermarks[origin] {
			watermarks[origin] = sequence
		}
	}
	for _, record := range snapshot.Experiences {
		add(record.Origin, record.Sequence)
	}
	for _, record := range snapshot.Facts {
		add(record.Origin, record.Sequence)
	}
	for _, record := range snapshot.Performances {
		add(record.Origin, record.Sequence)
	}
	for _, record := range snapshot.Dialogues {
		add(record.Origin, record.Sequence)
	}
	return watermarks
}
