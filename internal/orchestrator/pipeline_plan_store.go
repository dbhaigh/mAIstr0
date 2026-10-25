package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/maistr0/maistr0/internal/config"
)

type pipelinePlanStore struct {
	mu        sync.RWMutex
	persistMu sync.Mutex
	path      string
	plans     map[string]config.PipelinePlan
}

func openPipelinePlanStore(path string) (*pipelinePlanStore, error) {
	store := &pipelinePlanStore{path: path, plans: make(map[string]config.PipelinePlan)}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pipeline plans: %w", err)
	}
	if err := json.Unmarshal(data, &store.plans); err != nil {
		return nil, fmt.Errorf("decode pipeline plans: %w", err)
	}
	if store.plans == nil {
		store.plans = make(map[string]config.PipelinePlan)
	}
	for logicalID, plan := range store.plans {
		plan.Normalize()
		if plan.Model.LogicalID != logicalID {
			return nil, fmt.Errorf("pipeline plan key %q does not match its logical model ID %q", logicalID, plan.Model.LogicalID)
		}
		if err := plan.Validate(); err != nil {
			return nil, fmt.Errorf("invalid persisted pipeline plan for %q: %w", logicalID, err)
		}
		store.plans[logicalID] = clonePipelinePlan(plan)
	}
	return store, nil
}

func (store *pipelinePlanStore) get(logicalID string) (config.PipelinePlan, bool) {
	store.mu.RLock()
	plan, ok := store.plans[logicalID]
	store.mu.RUnlock()
	return clonePipelinePlan(plan), ok
}

func (store *pipelinePlanStore) set(plan config.PipelinePlan) error {
	plan.Normalize()
	if err := plan.Validate(); err != nil {
		return err
	}
	store.persistMu.Lock()
	defer store.persistMu.Unlock()
	store.mu.Lock()
	previous, existed := store.plans[plan.Model.LogicalID]
	store.plans[plan.Model.LogicalID] = clonePipelinePlan(plan)
	snapshot := clonePipelinePlans(store.plans)
	store.mu.Unlock()
	if err := store.persist(snapshot); err != nil {
		store.mu.Lock()
		if existed {
			store.plans[plan.Model.LogicalID] = previous
		} else {
			delete(store.plans, plan.Model.LogicalID)
		}
		store.mu.Unlock()
		return err
	}
	return nil
}

func (store *pipelinePlanStore) remove(logicalID string) error {
	store.persistMu.Lock()
	defer store.persistMu.Unlock()
	store.mu.Lock()
	previous, existed := store.plans[logicalID]
	if !existed {
		store.mu.Unlock()
		return nil
	}
	delete(store.plans, logicalID)
	snapshot := clonePipelinePlans(store.plans)
	store.mu.Unlock()
	if err := store.persist(snapshot); err != nil {
		store.mu.Lock()
		store.plans[logicalID] = previous
		store.mu.Unlock()
		return err
	}
	return nil
}

func (store *pipelinePlanStore) persist(plans map[string]config.PipelinePlan) error {
	if err := os.MkdirAll(filepath.Dir(store.path), 0o750); err != nil {
		return fmt.Errorf("create pipeline-plan directory: %w", err)
	}
	data, err := json.MarshalIndent(plans, "", "  ")
	if err != nil {
		return fmt.Errorf("encode pipeline plans: %w", err)
	}
	data = append(data, '\n')
	file, err := os.CreateTemp(filepath.Dir(store.path), filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create pipeline-plan temporary file: %w", err)
	}
	tempPath := file.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("set pipeline-plan file permissions: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write pipeline plans: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync pipeline plans: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close pipeline plans: %w", err)
	}
	if err := os.Rename(tempPath, store.path); err != nil {
		return fmt.Errorf("replace pipeline plans: %w", err)
	}
	return nil
}

func clonePipelinePlan(plan config.PipelinePlan) config.PipelinePlan {
	plan.Stages = append([]config.StageSpec(nil), plan.Stages...)
	return plan
}

func clonePipelinePlans(plans map[string]config.PipelinePlan) map[string]config.PipelinePlan {
	cloned := make(map[string]config.PipelinePlan, len(plans))
	for logicalID, plan := range plans {
		cloned[logicalID] = clonePipelinePlan(plan)
	}
	return cloned
}
