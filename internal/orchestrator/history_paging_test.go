package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

type historyPageResponse[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

func TestTaskHistoryCursorPagesAndLegacyShape(t *testing.T) {
	srv := newTestServer(t)
	for _, description := range []string{"first task", "second task", "third task"} {
		_, err := srv.tasks.CreateChecked(description, []scheduler.Assignment{{
			Subtask: scheduler.Subtask{ID: "work", Description: description},
			NodeID:  "node-a", Model: "model-a",
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	first := httptest.NewRecorder()
	srv.Mux().ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/tasks?page_size=2", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first page returned %d: %s", first.Code, first.Body.String())
	}
	var firstPage historyPageResponse[taskmgr.Task]
	if err := json.NewDecoder(first.Body).Decode(&firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Items) != 2 || !firstPage.HasMore || firstPage.NextCursor == "" {
		t.Fatalf("unexpected first task page: %+v", firstPage)
	}

	last := httptest.NewRecorder()
	path := "/api/tasks?page_size=2&cursor=" + url.QueryEscape(firstPage.NextCursor)
	srv.Mux().ServeHTTP(last, httptest.NewRequest(http.MethodGet, path, nil))
	if last.Code != http.StatusOK {
		t.Fatalf("last page returned %d: %s", last.Code, last.Body.String())
	}
	var lastPage historyPageResponse[taskmgr.Task]
	if err := json.NewDecoder(last.Body).Decode(&lastPage); err != nil {
		t.Fatal(err)
	}
	if len(lastPage.Items) != 1 || lastPage.HasMore || lastPage.NextCursor != "" {
		t.Fatalf("unexpected last task page: %+v", lastPage)
	}

	empty := httptest.NewRecorder()
	srv.Mux().ServeHTTP(empty, httptest.NewRequest(http.MethodGet, "/api/tasks?page_size=2&q=missing", nil))
	var emptyPage historyPageResponse[taskmgr.Task]
	if empty.Code != http.StatusOK || json.NewDecoder(empty.Body).Decode(&emptyPage) != nil ||
		len(emptyPage.Items) != 0 || emptyPage.HasMore {
		t.Fatalf("unexpected empty task page (%d): %s", empty.Code, empty.Body.String())
	}

	legacy := httptest.NewRecorder()
	srv.Mux().ServeHTTP(legacy, httptest.NewRequest(http.MethodGet, "/api/tasks", nil))
	var tasks []taskmgr.Task
	if legacy.Code != http.StatusOK || json.NewDecoder(legacy.Body).Decode(&tasks) != nil || len(tasks) != 3 {
		t.Fatalf("legacy task response changed (%d): %s", legacy.Code, legacy.Body.String())
	}

	invalid := httptest.NewRecorder()
	srv.Mux().ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, "/api/tasks?page_size=0", nil))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid page_size returned %d, want 400", invalid.Code)
	}
}

func TestMemoryCursorPagesAndOversizedRecordResponse(t *testing.T) {
	srv := newTestServer(t)
	for i := 0; i < 3; i++ {
		if _, err := srv.mem.Record(memory.Experience{Description: "websocket memory", Success: true}); err != nil {
			t.Fatal(err)
		}
	}
	first := httptest.NewRecorder()
	srv.Mux().ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/memory/experiences?page_size=2&q=websocket", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("first memory page returned %d: %s", first.Code, first.Body.String())
	}
	var firstPage historyPageResponse[memory.Experience]
	if err := json.NewDecoder(first.Body).Decode(&firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Items) != 2 || !firstPage.HasMore || firstPage.NextCursor == "" {
		t.Fatalf("unexpected first experience page: %+v", firstPage)
	}

	last := httptest.NewRecorder()
	path := "/api/memory/experiences?page_size=2&q=websocket&cursor=" + url.QueryEscape(firstPage.NextCursor)
	srv.Mux().ServeHTTP(last, httptest.NewRequest(http.MethodGet, path, nil))
	var lastPage historyPageResponse[memory.Experience]
	if last.Code != http.StatusOK || json.NewDecoder(last.Body).Decode(&lastPage) != nil ||
		len(lastPage.Items) != 1 || lastPage.HasMore {
		t.Fatalf("unexpected final experience page (%d): %s", last.Code, last.Body.String())
	}

	mismatch := httptest.NewRecorder()
	path = "/api/memory/experiences?page_size=2&q=other&cursor=" + url.QueryEscape(firstPage.NextCursor)
	srv.Mux().ServeHTTP(mismatch, httptest.NewRequest(http.MethodGet, path, nil))
	if mismatch.Code != http.StatusBadRequest {
		t.Fatalf("filter-mismatched cursor returned %d, want 400", mismatch.Code)
	}

	empty := httptest.NewRecorder()
	srv.Mux().ServeHTTP(empty, httptest.NewRequest(http.MethodGet, "/api/memory/dialogues?page_size=5", nil))
	var emptyPage historyPageResponse[memory.DialogueRecord]
	if empty.Code != http.StatusOK || json.NewDecoder(empty.Body).Decode(&emptyPage) != nil ||
		len(emptyPage.Items) != 0 || emptyPage.HasMore {
		t.Fatalf("unexpected empty dialogue page (%d): %s", empty.Code, empty.Body.String())
	}
	for _, key := range []string{"first", "second"} {
		if _, err := srv.mem.PutFact(memory.Fact{Key: key, Value: key}); err != nil {
			t.Fatal(err)
		}
		if err := srv.mem.SaveDialogue(memory.DialogueRecord{ID: key, Topic: key}); err != nil {
			t.Fatal(err)
		}
	}
	for endpoint, target := range map[string]any{
		"/api/memory/facts?page_size=1":     &historyPageResponse[memory.Fact]{},
		"/api/memory/dialogues?page_size=1": &historyPageResponse[memory.DialogueRecord]{},
	} {
		response := httptest.NewRecorder()
		srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, endpoint, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("paged %s returned %d: %s", endpoint, response.Code, response.Body.String())
		}
		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			t.Fatal(err)
		}
		switch page := target.(type) {
		case *historyPageResponse[memory.Fact]:
			if len(page.Items) != 1 || !page.HasMore {
				t.Fatalf("unexpected facts page: %+v", page)
			}
		case *historyPageResponse[memory.DialogueRecord]:
			if len(page.Items) != 1 || !page.HasMore {
				t.Fatalf("unexpected dialogue page: %+v", page)
			}
		}
	}

	srv.mem = pageErrorStore{Store: srv.mem}
	oversized := httptest.NewRecorder()
	srv.Mux().ServeHTTP(oversized, httptest.NewRequest(http.MethodGet, "/api/memory/experiences?page_size=1", nil))
	if oversized.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized memory record returned %d, want 413: %s", oversized.Code, oversized.Body.String())
	}
}

type pageErrorStore struct {
	memory.Store
}

func (pageErrorStore) ExperiencesPage(memory.SearchFilter, memory.PageOptions) (memory.HistoryPage[memory.Experience], error) {
	return memory.HistoryPage[memory.Experience]{}, memory.ErrPageRecordTooLarge
}

func (s pageErrorStore) DialoguesPage(options memory.PageOptions) (memory.HistoryPage[memory.DialogueRecord], error) {
	return s.Store.(memory.HistoryPager).DialoguesPage(options)
}

func (s pageErrorStore) FactsPage(scope string, options memory.PageOptions) (memory.HistoryPage[memory.Fact], error) {
	return s.Store.(memory.HistoryPager).FactsPage(scope, options)
}

var _ memory.HistoryPager = pageErrorStore{}

func TestPageCursorRejectsMalformedToken(t *testing.T) {
	srv := newTestServer(t)
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/tasks?cursor=not-valid", nil))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("malformed cursor returned %d, want 400", response.Code)
	}
}

func TestMemoryPagingUnsupportedStoreIsExplicit(t *testing.T) {
	srv := newTestServer(t)
	srv.mem = unsupportedPageStore{Store: srv.mem}
	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/memory/facts?page_size=1", nil))
	if response.Code != http.StatusNotImplemented || !strings.Contains(response.Body.String(), "not supported") {
		t.Fatalf("unsupported paging response = %d %q", response.Code, response.Body.String())
	}
}

type unsupportedPageStore struct {
	memory.Store
}
