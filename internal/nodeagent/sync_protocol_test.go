package nodeagent

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type syncProtocolRoundTripper func(*http.Request) (*http.Response, error)

func (f syncProtocolRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestSyncProtocolV2PeerUsesFullJSONAndUncompressedPosts(t *testing.T) {
	remote := memory.Snapshot{
		Experiences: []memory.Experience{{
			ID:          1,
			Origin:      "remote",
			Sequence:    1,
			Description: strings.Repeat("full snapshot record ", 100),
		}},
		Facts:        []memory.Fact{},
		Performances: []memory.Performance{},
		Dialogues:    []memory.DialogueRecord{},
		SyncVersion:  2,
		Watermarks:   map[string]uint64{"remote": 1},
	}
	var getRequests, postRequests int
	client := &http.Client{Transport: syncProtocolRoundTripper(func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodGet:
			getRequests++
			if got := request.Header.Get(memory.SyncVersionHeader); got != "3" {
				t.Errorf("GET protocol version = %q, want 3", got)
			}
			if got := request.Header.Get("Accept-Encoding"); got != "gzip" {
				t.Errorf("GET Accept-Encoding = %q, want gzip", got)
			}
			body, err := json.Marshal(remote)
			if err != nil {
				return nil, err
			}
			headers := make(http.Header)
			if err := memory.SetSyncHeadersVersion(headers, remote.Watermarks, 2); err != nil {
				return nil, err
			}
			headers.Set("Content-Length", fmt.Sprint(len(body)))
			headers.Set("Content-Type", "application/json")
			return syncProtocolResponse(request, http.StatusOK, headers, body), nil
		case http.MethodPost:
			postRequests++
			if got := request.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("version 2 peer received Content-Encoding %q", got)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			var snapshot memory.Snapshot
			if err := json.Unmarshal(body, &snapshot); err != nil {
				return nil, err
			}
			if len(snapshot.Experiences) != 1 {
				t.Errorf("posted experiences = %d, want 1", len(snapshot.Experiences))
			}
			return syncProtocolResponse(request, http.StatusNoContent, make(http.Header), nil), nil
		default:
			return nil, fmt.Errorf("unexpected sync method %s", request.Method)
		}
	})}

	got, _, status, err := fetchMemorySyncSnapshot(client, "https://peer.example/memory/sync", map[string]uint64{"local": 99}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK || got.SyncVersion != 2 || len(got.Experiences) != 1 || got.Watermarks["remote"] != 1 {
		t.Fatalf("legacy full snapshot = %+v", got)
	}
	body, err := json.Marshal(remote)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postMemorySnapshotPages(client, "https://peer.example/memory/sync", [][]byte{body}, map[string]uint64{"remote": 1}, 2); err != nil {
		t.Fatal(err)
	}
	if getRequests != 1 || postRequests != 1 {
		t.Fatalf("requests = GET %d, POST %d; want one each", getRequests, postRequests)
	}
}

func TestSyncProtocolEmptyResponsesPreserveWatermarks(t *testing.T) {
	watermarks := map[string]uint64{"remote": 7}
	client := &http.Client{Transport: syncProtocolRoundTripper(func(request *http.Request) (*http.Response, error) {
		headers := make(http.Header)
		if err := memory.SetSyncHeadersVersion(headers, watermarks, memory.SyncProtocolVersion); err != nil {
			return nil, err
		}
		return syncProtocolResponse(request, http.StatusNoContent, headers, nil), nil
	})}

	snapshot, _, status, err := fetchMemorySyncSnapshot(client, "https://peer.example/memory/sync", nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusNoContent || snapshot.SyncVersion != memory.SyncProtocolVersion || len(snapshot.Experiences) != 0 || snapshot.Watermarks["remote"] != 7 {
		t.Fatalf("empty memory exchange = %+v", snapshot)
	}
	tasks, version, taskWatermarks, err := fetchTaskSnapshot(client, "https://peer.example/tasks", nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if version != memory.SyncProtocolVersion || len(tasks) != 0 || taskWatermarks["remote"] != 7 {
		t.Fatalf("empty task exchange = tasks %v, version %d, watermarks %v", tasks, version, taskWatermarks)
	}
}

func TestEmptyTaskSnapshotDoesNotSendPost(t *testing.T) {
	var requests int
	client := &http.Client{Transport: syncProtocolRoundTripper(func(request *http.Request) (*http.Response, error) {
		requests++
		return nil, fmt.Errorf("unexpected request %s %s", request.Method, request.URL)
	})}
	if err := postTaskSnapshotPages(client, "https://peer.example/tasks/replica", []*taskmgr.Task{}, nil, 1<<20, memory.SyncProtocolVersion); err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatalf("empty task snapshot sent %d requests, want none", requests)
	}
	if _, err := postMemorySnapshotPages(client, "https://peer.example/memory/sync", nil, nil, memory.SyncProtocolVersion); err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatalf("empty memory snapshot sent %d requests, want none", requests)
	}
}

func TestTaskSyncResponseNegotiatesEmptyAndGzip(t *testing.T) {
	a := newTestAgent(t, "node-sync-test")
	for _, test := range []struct {
		name        string
		version     int
		wantStatus  int
		wantVersion int
	}{
		{name: "version 2 keeps JSON", version: 2, wantStatus: http.StatusOK, wantVersion: 2},
		{name: "version 3 uses empty response", version: 3, wantStatus: http.StatusNoContent, wantVersion: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/tasks", nil)
			if err := memory.SetSyncHeadersVersion(request.Header, nil, test.version); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			a.handleTasksList(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("task sync status = %d, want %d", response.Code, test.wantStatus)
			}
			version, _, err := memory.ReadSyncHeaders(response.Header())
			if err != nil {
				t.Fatal(err)
			}
			if version != test.wantVersion {
				t.Fatalf("task sync response version = %d, want %d", version, test.wantVersion)
			}
			if test.version == 2 && response.Body.String() != "[]" {
				t.Fatalf("legacy empty task response body = %q, want []", response.Body.String())
			}
		})
	}

	if _, err := a.orchestratorLink.tasks.CreateChecked(strings.Repeat("task-data ", 300), nil); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	if err := memory.SetSyncHeadersVersion(request.Header, nil, memory.SyncProtocolVersion); err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", "gzip")
	response := httptest.NewRecorder()
	a.handleTasksList(response, request)
	if got := response.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("task sync Content-Encoding = %q, want gzip", got)
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	var tasks []taskmgr.Task
	if err := json.Unmarshal(decoded, &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("decoded task count = %d, want 1", len(tasks))
	}
}

func TestSyncHandlersDecodeNegotiatedGzipBodies(t *testing.T) {
	snapshot := memory.Snapshot{
		Experiences: []memory.Experience{{
			ID:          1,
			Origin:      "sender",
			Sequence:    1,
			Description: strings.Repeat("memory-data ", 200),
		}},
		Facts:        []memory.Fact{},
		Performances: []memory.Performance{},
		Dialogues:    []memory.DialogueRecord{},
	}
	memoryBody, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	encodedMemory, compressed, err := memory.EncodeSyncBody(memoryBody, memory.SyncProtocolVersion, true)
	if err != nil || !compressed {
		t.Fatalf("memory request compression = %t, err %v", compressed, err)
	}
	memoryRequest := httptest.NewRequest(http.MethodPost, "/memory/sync", bytes.NewReader(encodedMemory))
	if err := memory.SetSyncHeadersVersion(memoryRequest.Header, nil, memory.SyncProtocolVersion); err != nil {
		t.Fatal(err)
	}
	memoryRequest.Header.Set("Content-Encoding", "gzip")
	decodedMemory, _, err := decodeMemorySyncRequest(httptest.NewRecorder(), memoryRequest, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(decodedMemory.Experiences) != 1 || decodedMemory.Experiences[0].Description != snapshot.Experiences[0].Description {
		t.Fatalf("decoded memory body = %+v", decodedMemory)
	}

	taskBody, err := json.Marshal([]*taskmgr.Task{{ID: "task:compressed", Description: strings.Repeat("task-data ", 200)}})
	if err != nil {
		t.Fatal(err)
	}
	encodedTasks, compressed, err := memory.EncodeSyncBody(taskBody, memory.SyncProtocolVersion, true)
	if err != nil || !compressed {
		t.Fatalf("task request compression = %t, err %v", compressed, err)
	}
	taskRequest := httptest.NewRequest(http.MethodPost, "/tasks/replica", bytes.NewReader(encodedTasks))
	if err := memory.SetSyncHeadersVersion(taskRequest.Header, nil, memory.SyncProtocolVersion); err != nil {
		t.Fatal(err)
	}
	taskRequest.Header.Set("Content-Encoding", "gzip")
	decodedTasks, err := decodeTaskSync(httptest.NewRecorder(), taskRequest, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(decodedTasks) != 1 || decodedTasks[0].ID != "task:compressed" {
		t.Fatalf("decoded tasks = %+v", decodedTasks)
	}
}

func TestSyncPostsCompressForProtocolV3Peers(t *testing.T) {
	requests := 0
	client := &http.Client{Transport: syncProtocolRoundTripper(func(request *http.Request) (*http.Response, error) {
		requests++
		if request.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("%s request Content-Encoding = %q, want gzip", request.Method, request.Header.Get("Content-Encoding"))
		}
		body, err := memory.DecodeSyncBody(request.Body, request.Header.Get("Content-Encoding"), memory.SyncProtocolVersion, 1<<20)
		if err != nil {
			return nil, err
		}
		switch request.URL.Path {
		case "/memory/sync":
			var snapshot memory.Snapshot
			if err := json.Unmarshal(body, &snapshot); err != nil {
				return nil, err
			}
			if len(snapshot.Experiences) != 1 {
				t.Errorf("posted memory experiences = %d, want 1", len(snapshot.Experiences))
			}
		case "/tasks/replica":
			var tasks []*taskmgr.Task
			if err := json.Unmarshal(body, &tasks); err != nil {
				return nil, err
			}
			if len(tasks) != 1 {
				t.Errorf("posted task count = %d, want 1", len(tasks))
			}
		default:
			return nil, fmt.Errorf("unexpected sync path %s", request.URL.Path)
		}
		return syncProtocolResponse(request, http.StatusNoContent, make(http.Header), nil), nil
	})}

	snapshotBody, err := json.Marshal(memory.Snapshot{
		Experiences:  []memory.Experience{{ID: 1, Description: strings.Repeat("memory-data ", 200)}},
		Facts:        []memory.Fact{},
		Performances: []memory.Performance{},
		Dialogues:    []memory.DialogueRecord{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postMemorySnapshotPages(
		client, "https://peer.example/memory/sync", [][]byte{snapshotBody}, nil, memory.SyncProtocolVersion,
	); err != nil {
		t.Fatal(err)
	}
	if err := postTaskSnapshotPages(
		client,
		"https://peer.example/tasks/replica",
		[]*taskmgr.Task{{ID: "task:compressed", Description: strings.Repeat("task-data ", 200)}},
		nil,
		1<<20,
		memory.SyncProtocolVersion,
	); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("compressed sync requests = %d, want 2", requests)
	}
}

func syncProtocolResponse(request *http.Request, status int, headers http.Header, body []byte) *http.Response {
	responseBody := io.NopCloser(bytes.NewReader(body))
	if status == http.StatusNoContent {
		responseBody = http.NoBody
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
		Header:        headers,
		Body:          responseBody,
		ContentLength: int64(len(body)),
		Request:       request,
	}
}
