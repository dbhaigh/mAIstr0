package taskmgr

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
)

var ErrPageRecordTooLarge = errors.New("taskmgr: one task exceeds the page response limit")

type PageCursor struct {
	UpdatedAt time.Time
	ID        string
}

type TaskPage struct {
	Items      []*Task
	NextCursor PageCursor
	HasMore    bool
}

// PageRecent returns a stable, filtered slice of task history using the
// recent-task cache and bounds serialized output before cloning task details.
func (m *Manager) PageRecent(cursor *PageCursor, query string, status Status, limit int, maxBytes int64) (TaskPage, error) {
	if limit <= 0 {
		limit = 50
	}
	query = strings.ToLower(strings.TrimSpace(query))

	m.mu.RLock()
	valid := m.recentTasksValid
	m.mu.RUnlock()
	if !valid {
		m.mu.Lock()
		if !m.recentTasksValid {
			m.recentTasks = make([]*Task, 0, len(m.tasks))
			for _, task := range m.tasks {
				m.recentTasks = append(m.recentTasks, task)
			}
			sort.Slice(m.recentTasks, func(i, j int) bool {
				if m.recentTasks[i].UpdatedAt.Equal(m.recentTasks[j].UpdatedAt) {
					return m.recentTasks[i].ID > m.recentTasks[j].ID
				}
				return m.recentTasks[i].UpdatedAt.After(m.recentTasks[j].UpdatedAt)
			})
			m.recentTasksValid = true
		}
		m.mu.Unlock()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	var page TaskPage
	var usedBytes int64
	for _, task := range m.recentTasks {
		if cursor != nil && !taskFollowsCursor(task, *cursor) {
			continue
		}
		if status != "" && task.Status != status {
			continue
		}
		if query != "" && !taskMatchesQuery(task, query) {
			continue
		}
		if len(page.Items) == limit {
			page.HasMore = true
			break
		}

		encoded, err := json.Marshal(task)
		if err != nil {
			return TaskPage{}, err
		}
		itemBytes := int64(len(encoded))
		required := itemBytes
		if len(page.Items) > 0 {
			required++
		}
		if maxBytes > 0 && itemBytes > maxBytes {
			if len(page.Items) == 0 {
				return TaskPage{}, ErrPageRecordTooLarge
			}
			page.HasMore = true
			break
		}
		if maxBytes > 0 && usedBytes+required > maxBytes {
			page.HasMore = true
			break
		}
		page.Items = append(page.Items, cloneTask(task))
		usedBytes += required
		page.NextCursor = PageCursor{UpdatedAt: task.UpdatedAt, ID: task.ID}
	}
	if page.Items == nil {
		page.Items = []*Task{}
	}
	return page, nil
}

func taskFollowsCursor(task *Task, cursor PageCursor) bool {
	if task.UpdatedAt.Equal(cursor.UpdatedAt) {
		return task.ID < cursor.ID
	}
	return task.UpdatedAt.Before(cursor.UpdatedAt)
}

func taskMatchesQuery(task *Task, query string) bool {
	matches := func(value string) bool {
		return strings.Contains(strings.ToLower(value), query)
	}
	if matches(task.ID) || matches(task.Description) || matches(string(task.Status)) ||
		matches(task.Result) || matches(task.Error) {
		return true
	}
	for _, item := range task.Subtasks {
		if matches(item.Subtask.ID) || matches(item.Subtask.Description) ||
			matches(item.Subtask.Role) || matches(item.Subtask.TaskType) ||
			matches(item.NodeID) || matches(item.Model) ||
			matches(item.Output) || matches(item.Error) {
			return true
		}
	}
	return false
}
