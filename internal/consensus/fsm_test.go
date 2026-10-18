package consensus

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

func TestStateMachineAppliesAndDeduplicatesMutations(t *testing.T) {
	fsm := newStateMachine()
	first := fsm.Apply(&raft.Log{
		Type:  raft.LogCommand,
		Index: 7,
		Data:  mustEncodeMutation(t, Mutation{OperationID: "request-1", Kind: MutationPut, Key: "tasks/t1", Value: []byte("running")}),
	}).(fsmResponse)
	if first.Error != "" || first.Result.Index != 7 || first.Result.Duplicate {
		t.Fatalf("first mutation response = %+v", first)
	}
	if value, ok := fsm.Read("tasks/t1"); !ok || string(value) != "running" {
		t.Fatalf("stored value = %q, present %t", value, ok)
	}

	retry := fsm.Apply(&raft.Log{
		Type:  raft.LogCommand,
		Index: 8,
		Data:  mustEncodeMutation(t, Mutation{OperationID: "request-1", Kind: MutationPut, Key: "tasks/t1", Value: []byte("running")}),
	}).(fsmResponse)
	if retry.Error != "" || !retry.Result.Duplicate || retry.Result.Index != 7 {
		t.Fatalf("duplicate mutation response = %+v", retry)
	}
	conflict := fsm.Apply(&raft.Log{
		Type:  raft.LogCommand,
		Index: 9,
		Data:  mustEncodeMutation(t, Mutation{OperationID: "request-1", Kind: MutationPut, Key: "tasks/t1", Value: []byte("completed")}),
	}).(fsmResponse)
	if conflict.Error != ErrOperationIDReuse.Error() {
		t.Fatalf("reused operation response error = %q", conflict.Error)
	}
	if value, _ := fsm.Read("tasks/t1"); string(value) != "running" {
		t.Fatalf("conflicting retry changed value to %q", value)
	}
}

func TestStateMachineSnapshotRestoresValuesAndOperationIDs(t *testing.T) {
	fsm := newStateMachine()
	fsm.Apply(&raft.Log{
		Type:  raft.LogCommand,
		Index: 11,
		Data:  mustEncodeMutation(t, Mutation{OperationID: "request-1", Kind: MutationPut, Key: "tasks/t1", Value: []byte("completed")}),
	})
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &testSnapshotSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	if !sink.closed || sink.cancelled {
		t.Fatalf("snapshot sink state: closed=%t canceled=%t", sink.closed, sink.cancelled)
	}

	restored := newStateMachine()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if value, ok := restored.Read("tasks/t1"); !ok || string(value) != "completed" {
		t.Fatalf("restored value = %q, present %t", value, ok)
	}
	retry := restored.Apply(&raft.Log{
		Type:  raft.LogCommand,
		Index: 12,
		Data:  mustEncodeMutation(t, Mutation{OperationID: "request-1", Kind: MutationPut, Key: "tasks/t1", Value: []byte("completed")}),
	}).(fsmResponse)
	if retry.Error != "" || !retry.Result.Duplicate || retry.Result.Index != 11 {
		t.Fatalf("restored duplicate response = %+v", retry)
	}
}

func TestStateMachineRejectsInvalidMutations(t *testing.T) {
	fsm := newStateMachine()
	response := fsm.Apply(&raft.Log{
		Type:  raft.LogCommand,
		Index: 1,
		Data:  mustEncodeMutation(t, Mutation{OperationID: "request-1", Kind: "unknown", Key: "key"}),
	}).(fsmResponse)
	if response.Error == "" {
		t.Fatal("invalid mutation was accepted")
	}
	if _, ok := fsm.Read("key"); ok {
		t.Fatal("invalid mutation changed state")
	}
	if err := validateMutation(Mutation{OperationID: "request-1", Kind: MutationDelete, Key: "key", Value: []byte("unexpected")}); err == nil {
		t.Fatal("delete mutation with a value passed validation")
	}
}

type testSnapshotSink struct {
	bytes.Buffer
	closed    bool
	cancelled bool
}

func (s *testSnapshotSink) ID() string { return "snapshot-test" }
func (s *testSnapshotSink) Cancel() error {
	s.cancelled = true
	return nil
}
func (s *testSnapshotSink) Close() error {
	s.closed = true
	return nil
}

func mustEncodeMutation(t *testing.T, mutation Mutation) []byte {
	t.Helper()
	encoded, err := json.Marshal(mutation)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
