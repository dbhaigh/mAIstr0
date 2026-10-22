package taskmgr

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type memoryStore struct {
	// mu protects task and checkpoint maps; this store takes no nested locks.
	mu          sync.RWMutex
	tasks       map[string]*Task
	checkpoints map[string]*Checkpoint
	latest      map[string]*Checkpoint
}

func NewMemoryStore() Store {
	return &memoryStore{
		tasks:       make(map[string]*Task),
		checkpoints: make(map[string]*Checkpoint),
		latest:      make(map[string]*Checkpoint),
	}
}

func (s *memoryStore) Load() ([]*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.tasks))
	for key := range s.tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	tasks := make([]*Task, 0, len(keys))
	for _, key := range keys {
		tasks = append(tasks, cloneTask(s.tasks[key]))
	}
	return tasks, nil
}

func (s *memoryStore) Save(task *Task) error {
	if task == nil || task.ID == "" {
		return fmt.Errorf("taskmgr: task id is required")
	}
	s.mu.Lock()
	s.tasks[task.ID] = cloneTask(task)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) SaveCheckpoint(checkpoint *Checkpoint) error {
	if checkpoint == nil || checkpoint.ID == "" || checkpoint.TaskID == "" {
		return fmt.Errorf("taskmgr: checkpoint id and task id are required")
	}
	copyOf := cloneCheckpoint(checkpoint)
	s.mu.Lock()
	s.checkpoints[checkpoint.ID] = copyOf
	s.latest[checkpoint.TaskID] = cloneCheckpoint(checkpoint)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) SaveTaskAndCheckpoint(task *Task, checkpoint *Checkpoint) error {
	if task == nil || task.ID == "" {
		return fmt.Errorf("taskmgr: task id is required")
	}
	if checkpoint == nil || checkpoint.ID == "" || checkpoint.TaskID != task.ID {
		return fmt.Errorf("taskmgr: checkpoint id and matching task id are required")
	}
	taskCopy := cloneTask(task)
	checkpointCopy := cloneCheckpoint(checkpoint)
	s.mu.Lock()
	s.tasks[task.ID] = taskCopy
	s.checkpoints[checkpoint.ID] = checkpointCopy
	s.latest[task.ID] = cloneCheckpoint(checkpoint)
	s.mu.Unlock()
	return nil
}

func (s *memoryStore) Checkpoints(taskID string) ([]Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := "cp-" + taskID + "-"
	checkpoints := make([]Checkpoint, 0)
	for key, checkpoint := range s.checkpoints {
		if strings.HasPrefix(key, prefix) {
			checkpoints = append(checkpoints, *cloneCheckpoint(checkpoint))
		}
	}
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].Sequence < checkpoints[j].Sequence
	})
	return checkpoints, nil
}

func (s *memoryStore) LatestCheckpoint(taskID string) (*Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	checkpoint, ok := s.latest[taskID]
	if !ok {
		return nil, fmt.Errorf("taskmgr: no checkpoint found for task %s", taskID)
	}
	return cloneCheckpoint(checkpoint), nil
}

func (s *memoryStore) Close() error { return nil }

func cloneCheckpoint(checkpoint *Checkpoint) *Checkpoint {
	copyOf := *checkpoint
	if checkpoint.Graph != nil {
		copyOf.Graph = checkpoint.Graph.Clone()
	}
	copyOf.Subtasks = append([]SubtaskResult(nil), checkpoint.Subtasks...)
	for i := range copyOf.Subtasks {
		copyOf.Subtasks[i].Subtask = cloneSubtask(checkpoint.Subtasks[i].Subtask)
	}
	copyOf.Outputs = make(map[string]string, len(checkpoint.Outputs))
	for key, value := range checkpoint.Outputs {
		copyOf.Outputs[key] = value
	}
	copyOf.Frontier = append([]string(nil), checkpoint.Frontier...)
	return &copyOf
}
