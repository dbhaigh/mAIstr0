package orchestrator

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/maistr0/maistr0/internal/memory"
)

const (
	maxHistoryPageBytes     int64 = 64 << 20
	historyPageEnvelopeRoom int64 = 2048
	defaultHistoryPageSize        = 50
	maxHistoryPageSize            = 100
)

type historyPageCursor struct {
	Version   int       `json:"v"`
	Resource  string    `json:"r"`
	Filter    string    `json:"f"`
	ID        uint64    `json:"i,omitempty"`
	Key       string    `json:"k,omitempty"`
	UpdatedAt time.Time `json:"t,omitempty"`
	Score     float64   `json:"s,omitempty"`
	AsOf      time.Time `json:"a,omitempty"`
}

type historyPageRequest struct {
	Active   bool
	Limit    int
	Resource string
	Filter   string
	Cursor   historyPageCursor
}

type historyPageEnvelope struct {
	Items      json.RawMessage `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
	HasMore    bool            `json:"has_more"`
}

func parseHistoryPageRequest(r *http.Request, resource string, filterKeys ...string) (historyPageRequest, error) {
	query := r.URL.Query()
	_, hasPageSize := query["page_size"]
	_, hasCursor := query["cursor"]
	request := historyPageRequest{
		Active:   hasPageSize || hasCursor,
		Limit:    defaultHistoryPageSize,
		Resource: resource,
		Filter:   historyFilterFingerprint(query, filterKeys),
	}
	if !request.Active {
		return request, nil
	}
	if hasPageSize {
		limit, err := strconv.Atoi(query.Get("page_size"))
		if err != nil || limit < 1 || limit > maxHistoryPageSize {
			return historyPageRequest{}, fmt.Errorf("page_size must be between 1 and %d", maxHistoryPageSize)
		}
		request.Limit = limit
	}
	if !hasCursor {
		return request, nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(query.Get("cursor"))
	if err != nil {
		return historyPageRequest{}, errors.New("cursor is invalid")
	}
	if err := json.Unmarshal(encoded, &request.Cursor); err != nil {
		return historyPageRequest{}, errors.New("cursor is invalid")
	}
	if request.Cursor.Version != 1 || request.Cursor.Resource != resource || request.Cursor.Filter != request.Filter {
		return historyPageRequest{}, errors.New("cursor does not match this list or its filters")
	}
	return request, nil
}

func historyFilterFingerprint(query url.Values, keys []string) string {
	values := make(url.Values, len(keys))
	for _, key := range keys {
		values.Set(key, query.Get(key))
	}
	sum := sha256.Sum256([]byte(values.Encode()))
	return hex.EncodeToString(sum[:])
}

func (request historyPageRequest) encodeCursor(cursor memory.HistoryCursor) (string, error) {
	encoded, err := json.Marshal(historyPageCursor{
		Version: 1, Resource: request.Resource, Filter: request.Filter,
		ID: cursor.ID, Key: cursor.Key, UpdatedAt: cursor.UpdatedAt,
		Score: cursor.Score, AsOf: cursor.AsOf,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func (request historyPageRequest) memoryCursor() memory.HistoryCursor {
	return memory.HistoryCursor{
		ID: request.Cursor.ID, Key: request.Cursor.Key,
		UpdatedAt: request.Cursor.UpdatedAt, Score: request.Cursor.Score,
		AsOf: request.Cursor.AsOf,
	}
}

func writeHistoryPage(w http.ResponseWriter, request historyPageRequest, items any, cursor memory.HistoryCursor, hasMore bool) {
	rawItems, err := json.Marshal(items)
	if err != nil {
		http.Error(w, "could not encode history page: "+err.Error(), http.StatusInternalServerError)
		return
	}
	response := historyPageEnvelope{Items: rawItems, HasMore: hasMore}
	if hasMore {
		response.NextCursor, err = request.encodeCursor(cursor)
		if err != nil {
			http.Error(w, "could not encode history page cursor: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	body, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "could not encode history page: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if int64(len(body)) > maxHistoryPageBytes {
		http.Error(w, "history page exceeds the 64 MiB response limit", http.StatusRequestEntityTooLarge)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func isTaskSyncRequest(r *http.Request) bool {
	return r.Header.Get(memory.SyncVersionHeader) != "" ||
		r.Header.Get(memory.SyncWatermarksHeader) != "" ||
		r.Header.Get(memory.SyncPageCursorHeader) != "" ||
		r.Header.Get(memory.SyncPageUpperHeader) != ""
}
