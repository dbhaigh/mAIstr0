package consensus

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/maistr0/maistr0/internal/closeutil"
)

func TestBoltStorePersistsLogsAndAppliedIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	store, err := openBoltStore(path)
	if err != nil {
		t.Fatal(err)
	}
	entry := &raft.Log{Index: 4, Term: 2, Type: raft.LogCommand, Data: []byte("committed")}
	if err := store.StoreLogs([]*raft.Log{entry}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAppliedIndex(entry.Index); err != nil {
		t.Fatal(err)
	}
	appliedIndex, err := store.GetAppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if appliedIndex != entry.Index {
		t.Fatalf("applied index = %d, want %d", appliedIndex, entry.Index)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openBoltStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(reopened)
	var restored raft.Log
	if err := reopened.GetLog(entry.Index, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Index != entry.Index || restored.Term != entry.Term || string(restored.Data) != "committed" {
		t.Fatalf("restored log = %+v", restored)
	}
	appliedIndex, err = reopened.GetAppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if appliedIndex != entry.Index {
		t.Fatalf("reopened applied index = %d, want %d", appliedIndex, entry.Index)
	}
	if err := reopened.DeleteRange(entry.Index, entry.Index); err != nil {
		t.Fatal(err)
	}
	appliedIndex, err = reopened.GetAppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if appliedIndex != 0 {
		t.Fatalf("applied index after deleting all logs = %d, want 0", appliedIndex)
	}
}

func TestBoltStoreRejectsMissingLogAndInvalidRange(t *testing.T) {
	store, err := openBoltStore(filepath.Join(t.TempDir(), "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(store)
	var entry raft.Log
	if err := store.GetLog(1, &entry); !errors.Is(err, raft.ErrLogNotFound) {
		t.Fatalf("missing log error = %v, want %v", err, raft.ErrLogNotFound)
	}
	if err := store.DeleteRange(2, 1); err == nil {
		t.Fatal("invalid deletion range succeeded")
	}
}
