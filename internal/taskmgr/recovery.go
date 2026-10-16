package taskmgr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// managerSyncID allocates process-local origins without taking a manager lock.
var managerSyncID atomic.Uint64

func NewManager() *Manager {
	return newManager(nil, nil)
}

func NewManagerWithFinalizer(finalizer Finalizer) *Manager {
	return newManager(nil, finalizer)
}

func NewManagerWithStore(store Store, finalizer Finalizer) (*Manager, error) {
	if store == nil {
		return nil, errors.New("taskmgr: store is required")
	}
	manager := newManager(store, finalizer)
	if err := manager.load(); err != nil {
		return nil, err
	}
	return manager, nil
}

func newManager(store Store, finalizer Finalizer) *Manager {
	origin := fmt.Sprintf("taskmgr-%d", managerSyncID.Add(1))
	return &Manager{
		tasks:      make(map[string]*Task),
		syncOrigin: origin,
		syncSeq:    make(map[string]uint64),
		finalizer:  finalizer,
		store:      store,
	}
}

func (m *Manager) SetSyncOrigin(origin string) {
	if m == nil || origin == "" {
		return
	}
	m.persistGate.Lock()
	defer m.persistGate.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncOrigin = origin
	for _, task := range m.tasks {
		if task.Origin == origin && task.Sequence > m.syncSeq[origin] {
			m.syncSeq[origin] = task.Sequence
		}
	}
}

// DefaultPath returns the per-user task history database path.
func DefaultPath(owner string) string {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		base = "."
	}
	return filepath.Join(base, "maistr0", "tasks-"+sanitize(owner)+".db")
}

func sanitize(value string) string {
	var out strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out.WriteRune(r)
		default:
			out.WriteByte('-')
		}
	}
	return out.String()
}

// OpenManager opens a persistent task store and marks interrupted work as
// failed rather than presenting an incomplete task as still running.
func OpenManager(path string, finalizer Finalizer) (*Manager, error) {
	return OpenManagerWithOrigin(path, finalizer, "")
}

func OpenManagerWithOrigin(path string, finalizer Finalizer, origin string) (*Manager, error) {
	if path == "" {
		return nil, fmt.Errorf("taskmgr: persistence path is required")
	}
	store, err := openBoltStore(path)
	if err != nil {
		return nil, err
	}

	manager := newManager(store, finalizer)
	if origin != "" {
		manager.syncOrigin = origin
	}
	if err := manager.load(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return manager, nil
}

func (m *Manager) load() error {
	tasks, err := m.store.Load()
	if err != nil {
		return err
	}
	interrupted := make(map[string]bool)
	metadataChanged := make(map[string]bool)
	for _, task := range tasks {
		if task == nil || task.ID == "" {
			return fmt.Errorf("taskmgr: stored task has no id")
		}
		ctx, cancel := context.WithCancel(context.Background())
		task.ctx = ctx
		task.cancel = cancel
		task.changed = make(chan struct{})
		task.persistMu = &sync.Mutex{}
		if task.Origin == "" || task.Sequence == 0 {
			task.Origin = m.syncOrigin
			m.syncSeq[task.Origin]++
			task.Sequence = m.syncSeq[task.Origin]
			metadataChanged[task.ID] = true
		} else if task.Sequence > m.syncSeq[task.Origin] {
			m.syncSeq[task.Origin] = task.Sequence
		}
		if task.Status == StatusPending || task.Status == StatusRunning || task.Status == StatusSynthesis {
			task.Status = StatusFailed
			task.Error = interruptedTaskError
			task.Resumable = true
			for i := range task.Subtasks {
				if task.Subtasks[i].Status == StatusPending || task.Subtasks[i].Status == StatusRunning {
					task.Subtasks[i].Status = StatusFailed
					task.Subtasks[i].Error = task.Error
				}
			}
			task.UpdatedAt = time.Now()
			task.Origin = m.syncOrigin
			task.Sequence = m.nextSyncSequenceLocked(task.Origin)
			interrupted[task.ID] = true
			cancel()
		} else if task.Status == StatusAwaitingApproval {
			task.Resumable = true
		} else if task.Status == StatusCancelled || task.Status == StatusFailed {
			for _, st := range task.Subtasks {
				if st.Status == StatusCompleted {
					task.Resumable = true
					break
				}
			}
			cancel()
		} else {
			cancel()
		}
		m.tasks[task.ID] = task
		if sequence := taskSequence(task.ID); sequence > m.seq {
			m.seq = sequence
		}
	}
	for _, task := range tasks {
		if interrupted[task.ID] {
			if err := m.store.Save(task); err != nil {
				return err
			}
			checkpoint := checkpointForTask(task)
			if err := m.store.SaveCheckpoint(checkpoint); err != nil {
				return err
			}
		} else if metadataChanged[task.ID] {
			if err := m.store.Save(task); err != nil {
				return err
			}
		}
	}
	return nil
}

func taskSequence(id string) int {
	value, ok := strings.CutPrefix(id, "task-")
	if !ok {
		return 0
	}
	sequence, err := strconv.ParseInt(value, 36, 32)
	if err != nil {
		return 0
	}
	return int(sequence)
}

// Close flushes and releases the persistent task database.
func (m *Manager) Close() error {
	m.persistGate.Lock()
	defer m.persistGate.Unlock()
	if m.store == nil {
		return nil
	}
	return m.store.Close()
}
