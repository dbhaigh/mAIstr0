package orchestrator

import (
	"container/list"
	"strings"
	"sync"

	"github.com/maistr0/maistr0/internal/scheduler"
)

const taskPlanCacheCapacity = 128

type cachedTaskPlan struct {
	key      string
	graph    *scheduler.TaskGraph
	subtasks []scheduler.Subtask
}

type taskPlanCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	recent  list.List
}

func (c *taskPlanCache) get(key string) (*scheduler.TaskGraph, []scheduler.Subtask, bool) {
	if key == "" {
		return nil, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[key]
	if element == nil {
		return nil, nil, false
	}
	c.recent.MoveToFront(element)
	entry := element.Value.(cachedTaskPlan)
	graph, subtasks := cloneTaskPlan(entry.graph, entry.subtasks)
	return graph, subtasks, true
}

func (c *taskPlanCache) put(key string, graph *scheduler.TaskGraph, subtasks []scheduler.Subtask) {
	if key == "" || graph == nil || len(subtasks) == 0 {
		return
	}
	graphCopy, subtaskCopies := cloneTaskPlan(graph, subtasks)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]*list.Element)
	}
	if element := c.entries[key]; element != nil {
		element.Value = cachedTaskPlan{key: key, graph: graphCopy, subtasks: subtaskCopies}
		c.recent.MoveToFront(element)
		return
	}
	c.entries[key] = c.recent.PushFront(cachedTaskPlan{
		key: key, graph: graphCopy, subtasks: subtaskCopies,
	})
	if c.recent.Len() > taskPlanCacheCapacity {
		oldest := c.recent.Back()
		entry := oldest.Value.(cachedTaskPlan)
		delete(c.entries, entry.key)
		c.recent.Remove(oldest)
	}
}

func cloneTaskPlan(graph *scheduler.TaskGraph, subtasks []scheduler.Subtask) (*scheduler.TaskGraph, []scheduler.Subtask) {
	var graphCopy *scheduler.TaskGraph
	if graph != nil {
		graphCopy = graph.Clone()
	}
	subtaskCopies := make([]scheduler.Subtask, len(subtasks))
	for i, subtask := range subtasks {
		subtaskCopies[i] = subtask.Clone()
	}
	return graphCopy, subtaskCopies
}

func normalizedPlanCacheKey(description string) string {
	return strings.Join(strings.Fields(strings.ToLower(description)), " ")
}
