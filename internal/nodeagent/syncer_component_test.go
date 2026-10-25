package nodeagent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
)

func TestFlushToClusterBatchesAndDeduplicatesPendingMemory(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	var received memory.Snapshot
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var snapshot memory.Snapshot
		if err := json.NewDecoder(r.Body).Decode(&snapshot); err != nil {
			t.Errorf("decode memory batch: %v", err)
			http.Error(w, "invalid snapshot", http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests++
		received = snapshot
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	syncer := newSyncerComponent(
		"leader",
		nil,
		memory.NewInMemoryStore(),
		nil,
		func() *http.Client {
			return server.Client()
		},
		func() []cluster.NodeStatus {
			return []cluster.NodeStatus{{ID: "peer", Address: server.URL, Healthy: true}}
		},
		0,
		nil,
	)

	if err := syncer.FlushToCluster(); err != nil {
		t.Fatalf("flush empty buffer: %v", err)
	}
	mu.Lock()
	if requests != 0 {
		t.Fatalf("empty flush sent %d requests, want none", requests)
	}
	mu.Unlock()

	first := memory.Snapshot{Facts: []memory.Fact{{
		Key: "shared", Scope: "cluster", Value: "same", Origin: "node-a", Sequence: 1,
	}}}
	second := memory.Snapshot{Facts: []memory.Fact{
		{Key: "shared", Scope: "cluster", Value: "same", Origin: "node-a", Sequence: 1},
		{Key: "other", Scope: "cluster", Value: "new", Origin: "node-b", Sequence: 1},
	}}
	firstPayload, err := memory.PayloadFromSnapshot(first)
	if err != nil {
		t.Fatal(err)
	}
	secondPayload, err := memory.PayloadFromSnapshot(second)
	if err != nil {
		t.Fatal(err)
	}

	var writers sync.WaitGroup
	for range 4 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			syncer.bufferMemoryPayload(firstPayload)
		}()
	}
	syncer.bufferMemoryPayload(secondPayload)
	writers.Wait()

	if err := syncer.FlushToCluster(); err != nil {
		t.Fatalf("flush buffered memory: %v", err)
	}
	mu.Lock()
	if requests != 1 {
		t.Fatalf("batch sent %d requests, want one", requests)
	}
	if len(received.Facts) != 2 {
		t.Fatalf("batch contained %d facts, want two unique records", len(received.Facts))
	}
	mu.Unlock()

	if err := syncer.FlushToCluster(); err != nil {
		t.Fatalf("flush after successful batch: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("empty follow-up flush sent another request; total %d", requests)
	}
}

func TestFlushToClusterDeduplicatesUpdatesQueuedDuringSend(t *testing.T) {
	requestStarted := make(chan memory.Snapshot, 2)
	releaseFirst := make(chan struct{})
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var snapshot memory.Snapshot
		if err := json.NewDecoder(r.Body).Decode(&snapshot); err != nil {
			t.Errorf("decode memory batch: %v", err)
			http.Error(w, "invalid snapshot", http.StatusBadRequest)
			return
		}
		requestStarted <- snapshot
		if requestCount.Add(1) == 1 {
			<-releaseFirst
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	syncer := newSyncerComponent(
		"leader",
		nil,
		memory.NewInMemoryStore(),
		nil,
		func() *http.Client {
			return server.Client()
		},
		func() []cluster.NodeStatus {
			return []cluster.NodeStatus{{ID: "peer", Address: server.URL, Healthy: true}}
		},
		0,
		nil,
	)
	first := memory.Snapshot{Facts: []memory.Fact{{
		Key: "shared", Scope: "cluster", Value: "same", Origin: "node-a", Sequence: 1,
	}}}
	firstPayload, err := memory.PayloadFromSnapshot(first)
	if err != nil {
		t.Fatal(err)
	}
	syncer.bufferMemoryPayload(firstPayload)

	firstFlush := make(chan error, 1)
	go func() {
		firstFlush <- syncer.FlushToCluster()
	}()
	initialBatch := <-requestStarted
	if len(initialBatch.Facts) != 1 {
		t.Fatalf("first batch facts = %d, want 1", len(initialBatch.Facts))
	}

	duringSend, err := memory.PayloadFromSnapshot(memory.Snapshot{Facts: []memory.Fact{
		first.Facts[0],
		{Key: "new", Scope: "cluster", Value: "next", Origin: "node-b", Sequence: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	syncer.bufferMemoryPayload(duringSend)
	close(releaseFirst)
	if err := <-firstFlush; err != nil {
		t.Fatalf("first flush: %v", err)
	}

	if err := syncer.FlushToCluster(); err != nil {
		t.Fatalf("flush update queued during send: %v", err)
	}
	nextBatch := <-requestStarted
	if len(nextBatch.Facts) != 1 || nextBatch.Facts[0].Key != "new" {
		t.Fatalf("follow-up batch = %+v, want only the newly queued record", nextBatch.Facts)
	}
}
