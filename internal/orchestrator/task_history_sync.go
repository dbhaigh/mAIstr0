package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/syncinterval"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

const maxReplicatedTaskHistoryBytes int64 = config.DefaultSyncMaxBytes

func (s *syncerComponent) taskHistorySyncLoop(ctx context.Context) {
	var backoff syncinterval.Backoff
	if syncinterval.WaitDebouncedContext(
		ctx,
		syncinterval.Jitter(3*time.Second),
		syncinterval.DebounceInterval,
		s.tasks.Changes(),
		s.tasks.Changes,
	) {
		backoff.Reset()
	}
	for ctx.Err() == nil {
		changes := s.tasks.Changes()
		if !s.shouldSync() {
			backoff.Reset()
			syncinterval.WaitDebouncedContext(
				ctx,
				syncinterval.Jitter(5*time.Second),
				syncinterval.DebounceInterval,
				changes,
				s.tasks.Changes,
			)
			continue
		}
		before := s.tasks.Watermarks()
		s.syncTaskHistoryWithContext(ctx)
		backoff.Observe(syncinterval.WatermarksAdvanced(before, s.tasks.Watermarks()))
		if syncinterval.WaitDebouncedContext(
			ctx,
			syncinterval.Jitter(backoff.Current()),
			syncinterval.DebounceInterval,
			changes,
			s.tasks.Changes,
		) {
			backoff.Reset()
		}
	}
}

func (s *syncerComponent) syncTaskHistory() {
	s.syncTaskHistoryWithContext(context.Background())
}

func (s *syncerComponent) syncTaskHistoryWithContext(ctx context.Context) {
	if ctx.Err() != nil || !s.shouldSync() {
		return
	}
	if !s.syncing.CompareAndSwap(false, true) {
		return
	}
	defer s.syncing.Store(false)
	if ctx.Err() != nil || !s.shouldSync() {
		return
	}

	nodes := s.registry.Active()
	remoteSnapshots := make(map[string]taskSyncSnapshot)
	for _, node := range nodes {
		if ctx.Err() != nil || !s.shouldSync() {
			return
		}
		if !node.Healthy || node.Address == "" {
			continue
		}
		var watermarks map[string]uint64
		if s.memory != nil {
			watermarks = taskmgr.FilterTaskWatermarks(s.memory.PeerWatermarks(node.ID))
		}
		requestID := syncLogRequestID(nil)
		started := time.Now()
		tasks, version, watermarks, err := fetchReplicatedTaskHistoryWithContext(
			ctx,
			s.client,
			strings.TrimRight(node.Address, "/")+"/tasks",
			watermarks,
			s.maxBytes,
		)
		if err != nil {
			logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=fetch_failed %s error=%v",
				syncLogFields(requestID, node.ID, time.Since(started), "fetch"), err)
			continue
		}
		if err := s.tasks.MergeFromReplicaFrom(tasks, "task:"+node.ID); err != nil {
			logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=import_failed %s error=%v",
				syncLogFields(requestID, node.ID, time.Since(started), "import"), err)
			continue
		}
		logging.FileInfof("orchestrator", "FILE_ONLY: orchestrator: task history sync event=fetch_complete %s records=%d version=%d",
			syncLogFields(requestID, node.ID, time.Since(started), "none"), len(tasks), version)
		remoteSnapshots[node.ID] = taskSyncSnapshot{version: version, watermarks: watermarks}
		if s.memory != nil {
			if err := s.memory.SetPeerWatermarks(node.ID, watermarks); err != nil {
				logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=watermark_persist_failed %s error=%v",
					syncLogFields(requestID, node.ID, time.Since(started), "persistence"), err)
			}
		}
	}

	for _, node := range nodes {
		if ctx.Err() != nil || !s.shouldSync() {
			return
		}
		if !node.Healthy || node.Address == "" {
			continue
		}
		remote, fetched := remoteSnapshots[node.ID]
		version := 0
		var watermarks map[string]uint64
		if fetched {
			version = remote.version
			watermarks = remote.watermarks
		}
		tasks := s.tasks.SnapshotForPeer(version, watermarks)
		requestID := syncLogRequestID(nil)
		started := time.Now()
		err := postTaskHistoryPagesWithContext(
			ctx,
			s.client,
			strings.TrimRight(node.Address, "/")+"/tasks/replica",
			tasks,
			taskmgr.SnapshotWatermarks(s.tasks.All()),
			s.maxBytes,
			version,
		)
		if err != nil {
			logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=replicate_failed %s error=%v",
				syncLogFields(requestID, node.ID, time.Since(started), "replication"), err)
			continue
		}
		logging.FileInfof("orchestrator", "FILE_ONLY: orchestrator: task history sync event=replicate_complete %s records=%d",
			syncLogFields(requestID, node.ID, time.Since(started), "none"), len(tasks))
		if s.memory != nil && fetched && memory.SupportsIncrementalSync(version) {
			if err := s.memory.SetPeerWatermarks(node.ID, taskmgr.SnapshotWatermarks(s.tasks.All())); err != nil {
				logging.FileErrorf("orchestrator", "FILE_ONLY: orchestrator: task history sync event=watermark_persist_failed %s error=%v",
					syncLogFields(requestID, node.ID, time.Since(started), "persistence"), err)
			}
		}
	}
}

func (s *syncerComponent) recoverTaskHistory() {
	if s.runRecovery == nil || !s.shouldSync() || len(s.registry.Active()) == 0 {
		return
	}
	s.recoveryOnce.Do(func() {
		s.runRecovery(s.syncTaskHistoryWithContext)
	})
}

type taskSyncSnapshot struct {
	version    int
	watermarks map[string]uint64
}

func fetchReplicatedTaskHistory(client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, requestWatermarks map[string]uint64, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	return fetchReplicatedTaskHistoryWithContext(context.Background(), client, endpoint, requestWatermarks, maxBytes)
}

func fetchReplicatedTaskHistoryWithContext(ctx context.Context, client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, requestWatermarks map[string]uint64, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, error) {
	tasks := make([]*taskmgr.Task, 0)
	var cursor, upper map[string]uint64
	var responseWatermarks map[string]uint64
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, 0, nil, err
		}
		if err := memory.SetSyncRequestHeaders(request, requestWatermarks); err != nil {
			return nil, 0, nil, err
		}
		memory.SetSyncCompressionRequestHeader(request)
		if cursor != nil {
			if err := memory.SetSyncPageRequestHeaders(request, cursor, upper); err != nil {
				return nil, 0, nil, err
			}
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, 0, nil, err
		}
		page, pageVersion, watermarks, nextCursor, more, err := decodeReplicatedTaskHistory(response, maxBytes)
		if err != nil {
			return nil, 0, nil, err
		}
		if more && !memory.SupportsSyncPages(pageVersion) {
			return nil, 0, nil, fmt.Errorf("node paginated task history without protocol version 2")
		}
		if more && len(nextCursor) == 0 {
			return nil, 0, nil, fmt.Errorf("node returned an empty task sync page cursor")
		}
		if more && len(watermarks) == 0 {
			return nil, 0, nil, fmt.Errorf("node omitted task sync watermarks during pagination")
		}
		tasks = append(tasks, page...)
		responseWatermarks = watermarks
		if !more {
			return tasks, pageVersion, responseWatermarks, nil
		}
		cursor = nextCursor
		upper = watermarks
	}
}

func decodeReplicatedTaskHistory(resp *http.Response, maxBytes int64) ([]*taskmgr.Task, int, map[string]uint64, map[string]uint64, bool, error) {
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		response, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, 0, nil, nil, false, fmt.Errorf("node returned %s: %s", resp.Status, strings.TrimSpace(string(response)))
	}
	version, watermarks, err := memory.ReadSyncResponseHeaders(resp)
	if err != nil {
		return nil, 0, nil, nil, false, fmt.Errorf("read task sync response headers: %w", err)
	}
	cursor, more, err := memory.ReadSyncPageResponseHeader(resp)
	if err != nil {
		return nil, 0, nil, nil, false, fmt.Errorf("read task sync page header: %w", err)
	}
	if resp.StatusCode == http.StatusNoContent {
		if !memory.SupportsEmptySync(version) {
			return nil, 0, nil, nil, false, fmt.Errorf("node returned an empty task response without protocol version %d", memory.SyncCompressionProtocolVersion)
		}
		return []*taskmgr.Task{}, version, watermarks, cursor, false, nil
	}
	var tasks []*taskmgr.Task
	body, err := memory.DecodeSyncBody(resp.Body, resp.Header.Get("Content-Encoding"), version, maxBytes)
	if err != nil {
		return nil, 0, nil, nil, false, err
	}
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, 0, nil, nil, false, err
	}
	return tasks, version, watermarks, cursor, more, nil
}

func postTaskHistoryPages(client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, tasks []*taskmgr.Task, watermarks map[string]uint64, maxBytes int64, peerVersion int) error {
	return postTaskHistoryPagesWithContext(context.Background(), client, endpoint, tasks, watermarks, maxBytes, peerVersion)
}

func postTaskHistoryPagesWithContext(ctx context.Context, client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, tasks []*taskmgr.Task, watermarks map[string]uint64, maxBytes int64, peerVersion int) error {
	if len(tasks) == 0 {
		return nil
	}
	pages, err := taskmgr.SplitTasks(tasks, maxBytes)
	if err != nil {
		return err
	}
	for _, page := range pages {
		body, err := json.Marshal(page)
		if err != nil {
			return err
		}
		if int64(len(body)) > maxBytes {
			return fmt.Errorf("task history page exceeds the %d-byte sync limit", maxBytes)
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
			response.Body.Close()
			return fmt.Errorf("node returned %s: %s", response.Status, strings.TrimSpace(string(message)))
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
