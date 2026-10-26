package consensus

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/maistr0/maistr0/internal/closeutil"
)

const (
	MutationPut    = "put"
	MutationDelete = "delete"
	maxSnapshot    = 256 << 20
)

var ErrOperationIDReuse = errors.New("consensus: operation ID was reused with a different mutation")

type Mutation struct {
	OperationID string `json:"operation_id"`
	Kind        string `json:"kind"`
	Key         string `json:"key"`
	Value       []byte `json:"value,omitempty"`
}

type MutationResult struct {
	OperationID string `json:"operation_id"`
	Index       uint64 `json:"index"`
	Duplicate   bool   `json:"duplicate,omitempty"`
}

type appliedOperation struct {
	Fingerprint [sha256.Size]byte
	Index       uint64
}

type fsmResponse struct {
	Result MutationResult
	Error  string
}

type stateMachine struct {
	mu                  sync.RWMutex
	values              map[string][]byte
	operations          map[string]appliedOperation
	persistAppliedIndex func(uint64) error
	storageErr          error
}

func newStateMachine() *stateMachine {
	return &stateMachine{
		values:     make(map[string][]byte),
		operations: make(map[string]appliedOperation),
	}
}

func (f *stateMachine) Apply(entry *raft.Log) interface{} {
	if entry.Type != raft.LogCommand {
		return nil
	}
	var mutation Mutation
	if err := json.Unmarshal(entry.Data, &mutation); err != nil {
		return fsmResponse{Error: fmt.Sprintf("consensus: decode mutation: %v", err)}
	}
	if err := validateMutation(mutation); err != nil {
		return fsmResponse{Error: err.Error()}
	}
	raw, err := json.Marshal(mutation)
	if err != nil {
		return fsmResponse{Error: fmt.Sprintf("consensus: encode mutation: %v", err)}
	}
	fingerprint := sha256.Sum256(raw)

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.storageErr != nil {
		return fsmResponse{Error: f.storageErr.Error()}
	}
	if f.persistAppliedIndex != nil {
		if err := f.persistAppliedIndex(entry.Index); err != nil {
			f.storageErr = fmt.Errorf("consensus: persist applied index %d: %w", entry.Index, err)
			return fsmResponse{Error: f.storageErr.Error()}
		}
	}
	if previous, exists := f.operations[mutation.OperationID]; exists {
		if previous.Fingerprint != fingerprint {
			return fsmResponse{Error: ErrOperationIDReuse.Error()}
		}
		return fsmResponse{Result: MutationResult{
			OperationID: mutation.OperationID,
			Index:       previous.Index,
			Duplicate:   true,
		}}
	}

	switch mutation.Kind {
	case MutationPut:
		f.values[mutation.Key] = bytes.Clone(mutation.Value)
	case MutationDelete:
		delete(f.values, mutation.Key)
	}
	f.operations[mutation.OperationID] = appliedOperation{Fingerprint: fingerprint, Index: entry.Index}
	return fsmResponse{Result: MutationResult{OperationID: mutation.OperationID, Index: entry.Index}}
}

func validateMutation(mutation Mutation) error {
	if err := validateIdentifier("operation ID", mutation.OperationID); err != nil {
		return err
	}
	if err := validateIdentifier("mutation key", mutation.Key); err != nil {
		return err
	}
	switch mutation.Kind {
	case MutationPut:
	case MutationDelete:
		if len(mutation.Value) != 0 {
			return errors.New("consensus: delete mutation cannot contain a value")
		}
	default:
		return fmt.Errorf("consensus: unsupported mutation kind %q", mutation.Kind)
	}
	return nil
}

func (f *stateMachine) Read(key string) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	value, ok := f.values[key]
	return bytes.Clone(value), ok
}

func (f *stateMachine) StorageError() error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.storageErr
}

func (f *stateMachine) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	snapshot := stateSnapshot{
		Values:     make(map[string][]byte, len(f.values)),
		Operations: make(map[string]appliedOperation, len(f.operations)),
	}
	for key, value := range f.values {
		snapshot.Values[key] = bytes.Clone(value)
	}
	for operationID, operation := range f.operations {
		snapshot.Operations[operationID] = operation
	}
	f.mu.RUnlock()
	return snapshot, nil
}

func (f *stateMachine) Restore(reader io.ReadCloser) error {
	defer closeutil.Discard(reader)
	raw, err := io.ReadAll(io.LimitReader(reader, maxSnapshot+1))
	if err != nil {
		return fmt.Errorf("consensus: read state snapshot: %w", err)
	}
	if len(raw) > maxSnapshot {
		return fmt.Errorf("consensus: state snapshot exceeds %d bytes", maxSnapshot)
	}
	var snapshot stateSnapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("consensus: decode state snapshot: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("consensus: state snapshot contains trailing data")
		}
		return fmt.Errorf("consensus: decode state snapshot trailing data: %w", err)
	}
	if snapshot.Values == nil {
		snapshot.Values = make(map[string][]byte)
	}
	if snapshot.Operations == nil {
		snapshot.Operations = make(map[string]appliedOperation)
	}
	f.mu.Lock()
	f.values = snapshot.Values
	f.operations = snapshot.Operations
	f.mu.Unlock()
	return nil
}

type stateSnapshot struct {
	Values     map[string][]byte           `json:"values"`
	Operations map[string]appliedOperation `json:"operations"`
}

func (s stateSnapshot) Persist(sink raft.SnapshotSink) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return errors.Join(fmt.Errorf("consensus: encode state snapshot: %w", err), sink.Cancel())
	}
	if _, err := sink.Write(raw); err != nil {
		return errors.Join(fmt.Errorf("consensus: write state snapshot: %w", err), sink.Cancel())
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("consensus: close state snapshot: %w", err)
	}
	return nil
}

func (stateSnapshot) Release() {}
