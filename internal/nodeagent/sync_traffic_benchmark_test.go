package nodeagent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type syncTrafficRoundTripper struct {
	requestBytes  atomic.Int64
	responseBytes atomic.Int64
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
	headers := make(http.Header)
	headers.Set("Date", "Wed, 21 Oct 2026 00:00:00 GMT")

	statusCode := http.StatusOK
	var responseBody []byte
	switch {
	case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/memory/sync"):
		if err := memory.SetSyncHeaders(headers, map[string]uint64{}); err != nil {
			return nil, err
		}
		if version >= 3 {
			statusCode = http.StatusNoContent
		} else {
			responseBody, _ = json.Marshal(memory.Snapshot{
				Experiences:  []memory.Experience{},
				Facts:        []memory.Fact{},
				Performances: []memory.Performance{},
				Dialogues:    []memory.DialogueRecord{},
				SyncVersion:  memory.SyncProtocolVersion,
				Watermarks:   map[string]uint64{},
			})
			headers.Set("Content-Type", "application/json")
			headers.Set("Content-Length", strconv.Itoa(len(responseBody)))
		}
	case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/tasks"):
		if err := memory.SetSyncHeaders(headers, map[string]uint64{}); err != nil {
			return nil, err
		}
		if version >= 3 {
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
	oldLogOutput := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(oldLogOutput) })

	tasks, err := taskmgr.NewManagerWithStore(taskmgr.NewMemoryStore(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer tasks.Close()

	transport := &syncTrafficRoundTripper{}
	client := &http.Client{Transport: transport}
	peers := make([]cluster.NodeStatus, 4)
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
	b.ReportMetric(memoryPerCycle, "memory-bytes/cycle")
	b.ReportMetric(taskPerCycle, "task-bytes/cycle")
	b.ReportMetric(memoryPerCycle/30+taskPerCycle/15, "bytes/s-30s-memory")
	b.ReportMetric(memoryPerCycle/15+taskPerCycle/15, "bytes/s-15s-memory")
}
