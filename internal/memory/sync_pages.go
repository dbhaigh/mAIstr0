package memory

import (
	"encoding/json"
	"fmt"

	"github.com/maistr0/maistr0/internal/syncpage"
)

func PageSnapshot(snapshot Snapshot, cursor, upper map[string]uint64, maxBytes int64, envelopeBytes int) (Snapshot, map[string]uint64, bool, error) {
	records := make([]syncpage.Record[any], 0, len(snapshot.Experiences)+len(snapshot.Facts)+len(snapshot.Performances)+len(snapshot.Dialogues))
	for i := range snapshot.Experiences {
		experience := &snapshot.Experiences[i]
		records = append(records, syncpage.Record[any]{
			Value: experience, Origin: experience.Origin, Sequence: experience.Sequence, Key: fmt.Sprintf("experience:%020d", experience.ID),
		})
	}
	for i := range snapshot.Facts {
		fact := &snapshot.Facts[i]
		records = append(records, syncpage.Record[any]{
			Value: fact, Origin: fact.Origin, Sequence: fact.Sequence, Key: "fact:" + fact.Scope + "\x00" + fact.Key,
		})
	}
	for i := range snapshot.Performances {
		performance := &snapshot.Performances[i]
		records = append(records, syncpage.Record[any]{
			Value: performance, Origin: performance.Origin, Sequence: performance.Sequence, Key: "performance:" + performance.NodeID + "\x00" + performance.Model + "\x00" + performance.TaskType,
		})
	}
	for i := range snapshot.Dialogues {
		dialogue := &snapshot.Dialogues[i]
		records = append(records, syncpage.Record[any]{
			Value: dialogue, Origin: dialogue.Origin, Sequence: dialogue.Sequence, Key: "dialogue:" + dialogue.ID,
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
	values, nextCursor, more, err := syncpage.Next(records, cursor, upper, maxBytes, len(baseJSON)+envelopeBytes, func(value any) ([]byte, error) {
		return json.Marshal(value)
	})
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
