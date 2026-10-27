package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type syncTrafficRoundTripper struct {
	requestBytes  atomic.Int64
	responseBytes atomic.Int64
	peerVersion   int
}

func (t *syncTrafficRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var requestBody []byte
	if req.Body != nil {
		var err error
		requestBody, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		if err := req.Body.Close(); err != nil {
			return nil, err
		}
	}
	t.requestBytes.Add(syncTrafficRequestSize(req, len(requestBody)))

	version, _ := strconv.Atoi(req.Header.Get(memory.SyncVersionHeader))
	if version <= 0 {
		version = memory.FirstSyncProtocolVersion
	}
	if t.peerVersion > 0 && t.peerVersion < version {
		version = t.peerVersion
	}
	headers := make(http.Header)
	headers.Set("Date", "Wed, 21 Oct 2026 00:00:00 GMT")

	statusCode := http.StatusOK
	var responseBody []byte
	switch {
	case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/memory/sync"):
		if err := memory.SetSyncHeadersVersion(headers, map[string]uint64{}, version); err != nil {
			return nil, err
		}
		if memory.SupportsEmptySync(version) {
			statusCode = http.StatusNoContent
		} else {
			responseBody, _ = json.Marshal(memory.Snapshot{
				Experiences:  []memory.Experience{},
				Facts:        []memory.Fact{},
				Performances: []memory.Performance{},
				Dialogues:    []memory.DialogueRecord{},
				SyncVersion:  version,
				Watermarks:   map[string]uint64{},
			})
			headers.Set("Content-Type", "application/json")
			headers.Set("Content-Length", strconv.Itoa(len(responseBody)))
		}
	case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/tasks"):
		if err := memory.SetSyncHeadersVersion(headers, map[string]uint64{}, version); err != nil {
			return nil, err
		}
		if memory.SupportsEmptySync(version) {
			statusCode = http.StatusNoContent
		} else {
			responseBody = []byte("[]")
			headers.Set("Content-Type", "application/json")
			headers.Set("Content-Length", strconv.Itoa(len(responseBody)))
		}
	case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/tasks/replica"):
		statusCode = http.StatusNoContent
	default:
		return nil, fmt.Errorf("unexpected sync request: %s %s", req.Method, req.URL.Path)
	}

	status := fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode))
	t.responseBytes.Add(syncTrafficResponseSize(status, headers, len(responseBody)))
	body := io.NopCloser(bytes.NewReader(responseBody))
	if statusCode == http.StatusNoContent {
		body = http.NoBody
	}
	return &http.Response{
		Status:        status,
		StatusCode:    statusCode,
		Header:        headers,
		Body:          body,
		ContentLength: int64(len(responseBody)),
		Request:       req,
	}, nil
}

func syncTrafficRequestSize(req *http.Request, bodyBytes int) int64 {
	size := int64(len(req.Method) + 1 + len(req.URL.RequestURI()) + 1 + len(req.Proto) + 2)
	size += syncTrafficHeaderSize(req.Header)
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	size += int64(len("Host: ") + len(host) + 2)
	if req.ContentLength > 0 {
		size += int64(len("Content-Length: ") + len(strconv.FormatInt(req.ContentLength, 10)) + 2)
	}
	return size + 2 + int64(bodyBytes)
}

func syncTrafficResponseSize(status string, headers http.Header, bodyBytes int) int64 {
	size := int64(len("HTTP/1.1 ") + len(status) + 2)
	return size + syncTrafficHeaderSize(headers) + 2 + int64(bodyBytes)
}

func syncTrafficHeaderSize(headers http.Header) int64 {
	var size int64
	for name, values := range headers {
		for _, value := range values {
			size += int64(len(name) + 2 + len(value) + 2)
		}
	}
	return size
}

func BenchmarkIdleSyncTraffic5Node(b *testing.B) {
	benchmarkIdleSyncTraffic(b, 5, memory.SyncProtocolVersion)
}

func BenchmarkIdleSyncTraffic3Node(b *testing.B) {
	for _, peerVersion := range []int{memory.SyncPageProtocolVersion, memory.SyncProtocolVersion} {
		b.Run(fmt.Sprintf("peer-v%d", peerVersion), func(b *testing.B) {
			benchmarkIdleSyncTraffic(b, 3, peerVersion)
		})
	}
}

func benchmarkIdleSyncTraffic(b *testing.B, nodeCount, peerVersion int) {
	oldLogOutput := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(oldLogOutput) })

	tasks, err := taskmgr.NewManagerWithStore(taskmgr.NewMemoryStore(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer closeutil.Discard(tasks)

	transport := &syncTrafficRoundTripper{peerVersion: peerVersion}
	client := &http.Client{Transport: transport}
	peers := make([]cluster.NodeStatus, nodeCount-1)
	for i := range peers {
		peers[i] = cluster.NodeStatus{
			ID:      fmt.Sprintf("peer-%d", i),
			Address: fmt.Sprintf("http://peer-%d.invalid", i),
			Healthy: true,
		}
	}
	syncer := newSyncerComponent(
		"leader",
		tasks,
		memory.NewInMemoryStore(),
		nil,
		func() *http.Client { return client },
		func() []cluster.NodeStatus { return peers },
		1<<20,
		func() bool { return true },
	)

	var memoryBytes, taskBytes int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := transport.requestBytes.Load() + transport.responseBytes.Load()
		syncer.syncPeerMemory()
		memoryBytes += transport.requestBytes.Load() + transport.responseBytes.Load() - before

		before = transport.requestBytes.Load() + transport.responseBytes.Load()
		syncer.syncPeerTaskHistory()
		taskBytes += transport.requestBytes.Load() + transport.responseBytes.Load() - before
	}
	b.StopTimer()

	memoryPerCycle := float64(memoryBytes) / float64(b.N)
	taskPerCycle := float64(taskBytes) / float64(b.N)
	workPerCycle := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
	b.ReportMetric(memoryPerCycle, "memory-bytes/cycle")
	b.ReportMetric(taskPerCycle, "task-bytes/cycle")
	b.ReportMetric((memoryPerCycle+taskPerCycle)/60, "bytes/s-at-60s-backstop")
	b.ReportMetric(workPerCycle/1e6, "cpu-ms/min-at-60s-backstop")
}

func BenchmarkTaskHistoryTransfer10K(b *testing.B) {
	tasks := make([]*taskmgr.Task, 10_000)
	createdAt := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	description := strings.Repeat("Representative completed task history record. ", 3)
	for i := range tasks {
		tasks[i] = &taskmgr.Task{
			ID:          fmt.Sprintf("task-%05d", i+1),
			Origin:      "task:worker-a",
			Sequence:    uint64(i + 1),
			Description: description,
			Status:      taskmgr.StatusCompleted,
			CreatedAt:   createdAt,
			UpdatedAt:   createdAt,
		}
	}
	jsonBody, err := json.Marshal(tasks)
	if err != nil {
		b.Fatal(err)
	}
	watermarks := taskmgr.SnapshotWatermarks(tasks)

	for _, peerVersion := range []int{memory.SyncPageProtocolVersion, memory.SyncProtocolVersion} {
		b.Run(fmt.Sprintf("peer-v%d", peerVersion), func(b *testing.B) {
			var requestBytes atomic.Int64
			var bodyBytes atomic.Int64
			handlerErrors := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n, err := io.Copy(io.Discard, r.Body)
				if err != nil {
					select {
					case handlerErrors <- err:
					default:
					}
				} else {
					bodyBytes.Add(n)
					requestBytes.Add(syncTrafficRequestSize(r, int(n)))
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client := server.Client()
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := postTaskSnapshotPagesWithContext(
					ctx,
					client,
					server.URL+"/tasks/replica",
					tasks,
					watermarks,
					maxTaskSyncBodyBytes,
					peerVersion,
				); err != nil {
					b.Fatal(err)
				}
				select {
				case err := <-handlerErrors:
					b.Fatal(err)
				default:
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(jsonBody)), "json-bytes")
			b.ReportMetric(float64(bodyBytes.Load())/float64(b.N), "body-bytes/op")
			b.ReportMetric(float64(requestBytes.Load())/float64(b.N), "request-bytes/op")
		})
	}
}
