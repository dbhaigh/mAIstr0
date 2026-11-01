package tasksync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type Snapshot struct {
	Version    int
	Watermarks map[string]uint64
}

type SnapshotSource interface {
	SnapshotAndWatermarksForPeer(int, map[string]uint64) ([]*taskmgr.Task, map[string]uint64)
}

type PageResult struct {
	Records  int
	More     bool
	Duration time.Duration
}

type SnapshotRequest struct {
	HadCursor  bool
	Version    int
	Watermarks map[string]uint64
	Cursor     map[string]uint64
	Upper      map[string]uint64
}

type Transfer struct {
	MaxBytes   int64
	RemoteName string
}

func ParseSnapshotRequest(r *http.Request) (SnapshotRequest, error) {
	request := SnapshotRequest{HadCursor: r.Header.Get(memory.SyncPageCursorHeader) != ""}
	version, watermarks, err := memory.ReadSyncRequestHeaders(r)
	if err != nil {
		return SnapshotRequest{}, fmt.Errorf("invalid task sync headers: %w", err)
	}
	cursor, upper, err := memory.ReadSyncPageRequestHeaders(r)
	if err != nil {
		return SnapshotRequest{}, fmt.Errorf("invalid task sync page headers: %w", err)
	}
	if cursor != nil && !memory.SupportsSyncPages(version) {
		return SnapshotRequest{}, errors.New("task sync pagination requires protocol version 2")
	}
	request.Version = version
	request.Watermarks = watermarks
	request.Cursor = cursor
	request.Upper = upper
	return request, nil
}

func (transfer Transfer) Fetch(
	ctx context.Context,
	client Client,
	endpoint string,
	requestWatermarks map[string]uint64,
) ([]*taskmgr.Task, Snapshot, error) {
	tasks := make([]*taskmgr.Task, 0)
	var cursor, upper map[string]uint64
	var snapshot Snapshot
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, Snapshot{}, err
		}
		if err := memory.SetSyncRequestHeaders(request, requestWatermarks); err != nil {
			return nil, Snapshot{}, err
		}
		memory.SetSyncCompressionRequestHeader(request)
		if cursor != nil {
			if err := memory.SetSyncPageRequestHeaders(request, cursor, upper); err != nil {
				return nil, Snapshot{}, err
			}
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, Snapshot{}, err
		}
		page, version, watermarks, nextCursor, more, err := transfer.decodeResponse(response)
		if err != nil {
			return nil, Snapshot{}, err
		}
		if more && !memory.SupportsSyncPages(version) {
			return nil, Snapshot{}, fmt.Errorf("%s paginated task history without protocol version 2", transfer.remoteName())
		}
		if more && len(nextCursor) == 0 {
			return nil, Snapshot{}, fmt.Errorf("%s returned an empty task sync page cursor", transfer.remoteName())
		}
		if more && len(watermarks) == 0 {
			return nil, Snapshot{}, fmt.Errorf("%s omitted task sync watermarks during pagination", transfer.remoteName())
		}
		tasks = append(tasks, page...)
		snapshot = Snapshot{Version: version, Watermarks: watermarks}
		if !more {
			return tasks, snapshot, nil
		}
		cursor = nextCursor
		upper = watermarks
	}
}

func (transfer Transfer) Dispatch(
	ctx context.Context,
	client Client,
	endpoint string,
	tasks []*taskmgr.Task,
	watermarks map[string]uint64,
	peerVersion int,
) error {
	if len(tasks) == 0 {
		return nil
	}
	pages, err := taskmgr.SplitTasks(tasks, transfer.MaxBytes)
	if err != nil {
		return err
	}
	for _, page := range pages {
		body, err := json.Marshal(page)
		if err != nil {
			return err
		}
		if int64(len(body)) > transfer.MaxBytes {
			return fmt.Errorf("task history page exceeds the %d-byte sync limit", transfer.MaxBytes)
		}
		body, compressed, err := memory.EncodeSyncBody(body, peerVersion, true)
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		if compressed {
			request.Header.Set("Content-Encoding", "gzip")
		}
		if err := memory.SetSyncRequestHeaders(request, watermarks); err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			_ = response.Body.Close()
			return fmt.Errorf("%s returned %s: %s", transfer.remoteName(), response.Status, strings.TrimSpace(string(message)))
		}
		_, copyErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (transfer Transfer) ServeSnapshot(
	w http.ResponseWriter,
	r *http.Request,
	source SnapshotSource,
	onPage func(PageResult),
) error {
	request, err := ParseSnapshotRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}
	return transfer.ServeSnapshotRequest(w, r, request, source, onPage)
}

func (transfer Transfer) ServeSnapshotRequest(
	w http.ResponseWriter,
	r *http.Request,
	request SnapshotRequest,
	source SnapshotSource,
	onPage func(PageResult),
) error {
	version := request.Version
	cursor := request.Cursor
	upper := request.Upper
	if cursor != nil && !memory.SupportsSyncPages(version) {
		http.Error(w, "task sync pagination requires protocol version 2", http.StatusBadRequest)
		return nil
	}
	tasks, currentWatermarks := source.SnapshotAndWatermarksForPeer(
		version, taskmgr.FilterTaskWatermarks(request.Watermarks),
	)
	if memory.SupportsSyncPages(version) {
		pageStarted := time.Now()
		if cursor == nil {
			cursor = make(map[string]uint64)
			upper = currentWatermarks
		}
		page, nextCursor, more, err := taskmgr.PageTasks(tasks, cursor, upper, transfer.MaxBytes)
		if err != nil {
			http.Error(w, "could not page task history: "+err.Error(), http.StatusRequestEntityTooLarge)
			return nil
		}
		tasks = page
		currentWatermarks = upper
		if more {
			if err := memory.SetSyncPageResponseHeader(w, nextCursor); err != nil {
				http.Error(w, "could not encode task sync page cursor: "+err.Error(), http.StatusInternalServerError)
				return nil
			}
		}
		if onPage != nil {
			onPage(PageResult{Records: len(tasks), More: more, Duration: time.Since(pageStarted)})
		}
	}
	if err := memory.SetSyncResponseHeadersForPeer(w, currentWatermarks, version); err != nil {
		http.Error(w, "could not encode task sync watermarks: "+err.Error(), http.StatusInternalServerError)
		return nil
	}
	if memory.SupportsEmptySync(version) && !request.HadCursor && len(tasks) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	_, err := memory.WriteSyncJSON(w, http.StatusOK, tasks, version, memory.AcceptsSyncCompression(r))
	return err
}

func (transfer Transfer) decodeResponse(
	response *http.Response,
) ([]*taskmgr.Task, int, map[string]uint64, map[string]uint64, bool, error) {
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, 0, nil, nil, false, fmt.Errorf(
			"%s returned %s: %s", transfer.remoteName(), response.Status, strings.TrimSpace(string(body)),
		)
	}
	version, watermarks, err := memory.ReadSyncResponseHeaders(response)
	if err != nil {
		return nil, 0, nil, nil, false, fmt.Errorf("read task sync response headers: %w", err)
	}
	cursor, more, err := memory.ReadSyncPageResponseHeader(response)
	if err != nil {
		return nil, 0, nil, nil, false, fmt.Errorf("read task sync page header: %w", err)
	}
	if response.StatusCode == http.StatusNoContent {
		if !memory.SupportsEmptySync(version) {
			return nil, 0, nil, nil, false, fmt.Errorf(
				"%s returned an empty task response without protocol version %d",
				transfer.remoteName(), memory.SyncCompressionProtocolVersion,
			)
		}
		return []*taskmgr.Task{}, version, watermarks, cursor, false, nil
	}
	body, err := memory.DecodeSyncBody(response.Body, response.Header.Get("Content-Encoding"), version, transfer.MaxBytes)
	if err != nil {
		return nil, 0, nil, nil, false, err
	}
	var tasks []*taskmgr.Task
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, 0, nil, nil, false, err
	}
	return tasks, version, watermarks, cursor, more, nil
}

func (transfer Transfer) remoteName() string {
	if transfer.RemoteName == "" {
		return "remote"
	}
	return transfer.RemoteName
}
