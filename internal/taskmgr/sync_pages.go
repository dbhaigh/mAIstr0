package taskmgr

import (
	"encoding/json"
	"fmt"

	"github.com/maistr0/maistr0/internal/syncpage"
)

func PageTasks(tasks []*Task, cursor, upper map[string]uint64, maxBytes int64) ([]*Task, map[string]uint64, bool, error) {
	records := make([]syncpage.Record[*Task], 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		records = append(records, syncpage.Record[*Task]{
			Value: task, Origin: task.Origin, Sequence: task.Sequence, Key: task.ID,
		})
	}
	page, nextCursor, more, err := syncpage.Next(records, cursor, upper, maxBytes, 2, func(task *Task) ([]byte, error) {
		return json.Marshal(task)
	})
	if err != nil {
		return nil, nil, false, err
	}
	return page, nextCursor, more, nil
}

func SplitTasks(tasks []*Task, maxBytes int64) ([][]*Task, error) {
	upper := SnapshotWatermarks(tasks)
	cursor := make(map[string]uint64)
	pages := make([][]*Task, 0, 1)
	for {
		page, nextCursor, more, err := PageTasks(tasks, cursor, upper, maxBytes)
		if err != nil {
			return nil, err
		}
		pages = append(pages, page)
		if !more {
			return pages, nil
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("taskmgr: sync pagination made no progress")
		}
		cursor = nextCursor
	}
}
