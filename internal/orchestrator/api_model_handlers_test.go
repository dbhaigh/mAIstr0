package orchestrator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

type modelListPageTest struct {
	Items      []json.RawMessage `json:"items"`
	NextCursor string            `json:"next_cursor"`
	HasMore    bool              `json:"has_more"`
}

func TestModelListsSupportFilteredCursorPagingAndLegacyShapes(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: "http://node-a",
		Models: []engine.Model{
			{Name: "alpha", Engine: "ollama"},
			{Name: "beta", Engine: "ollama"},
			{Name: "shared", Engine: "vllm"},
		},
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-b", Address: "http://node-b",
		Models: []engine.Model{
			{Name: "alpha", Engine: "ollama"},
			{Name: "delta", Engine: "vllm"},
		},
	})

	legacyCases := []struct {
		path string
		key  string
	}{
		{path: "/api/models", key: ""},
		{path: "/api/agent/models", key: ""},
		{path: "/v1/models", key: "data"},
		{path: "/api/tags", key: "models"},
	}
	for _, test := range legacyCases {
		response := httptest.NewRecorder()
		srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s returned %d: %s", test.path, response.Code, response.Body.String())
		}
		var payload any
		if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode legacy %s response: %v", test.path, err)
		}
		switch value := payload.(type) {
		case []any:
			if test.key != "" || len(value) == 0 {
				t.Fatalf("legacy %s response has unexpected array shape: %T", test.path, payload)
			}
		case map[string]any:
			if test.key == "" {
				t.Fatalf("legacy %s response changed from array to object", test.path)
			}
			if _, ok := value[test.key].([]any); !ok {
				t.Fatalf("legacy %s response is missing array field %q", test.path, test.key)
			}
		default:
			t.Fatalf("legacy %s response has unexpected shape %T", test.path, payload)
		}
	}

	for _, test := range legacyCases {
		response := httptest.NewRecorder()
		path := test.path + "?source=node-a&engine=OLLAMA"
		srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("filtered GET %s returned %d: %s", test.path, response.Code, response.Body.String())
		}
		var items []json.RawMessage
		if test.key == "" {
			if err := json.NewDecoder(response.Body).Decode(&items); err != nil {
				t.Fatalf("decode filtered %s response: %v", test.path, err)
			}
		} else {
			var payload map[string]json.RawMessage
			if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
				t.Fatalf("decode filtered %s response: %v", test.path, err)
			}
			if err := json.Unmarshal(payload[test.key], &items); err != nil {
				t.Fatalf("decode filtered %s field %q: %v", test.path, test.key, err)
			}
		}
		if len(items) != 2 {
			t.Fatalf("filtered %s returned %d items, want 2", test.path, len(items))
		}
	}

	pageCases := []struct {
		path string
		key  string
	}{
		{path: "/api/models", key: "name"},
		{path: "/api/agent/models", key: "name"},
		{path: "/v1/models", key: "id"},
		{path: "/api/tags", key: "name"},
	}
	for _, test := range pageCases {
		query := "?source=node-a&engine=OLLAMA&page_size=1"
		first := httptest.NewRecorder()
		srv.Mux().ServeHTTP(first, httptest.NewRequest(http.MethodGet, test.path+query, nil))
		if first.Code != http.StatusOK {
			t.Fatalf("first page %s returned %d: %s", test.path, first.Code, first.Body.String())
		}
		var firstPage modelListPageTest
		if err := json.NewDecoder(first.Body).Decode(&firstPage); err != nil {
			t.Fatalf("decode first page %s: %v", test.path, err)
		}
		if len(firstPage.Items) != 1 || !firstPage.HasMore || firstPage.NextCursor == "" {
			t.Fatalf("unexpected first page %s: %+v", test.path, firstPage)
		}
		if got := modelListItemName(t, firstPage.Items[0], test.key); got != "alpha" {
			t.Fatalf("first page %s item = %q, want alpha", test.path, got)
		}

		nextPath := test.path + query + "&cursor=" + url.QueryEscape(firstPage.NextCursor)
		next := httptest.NewRecorder()
		srv.Mux().ServeHTTP(next, httptest.NewRequest(http.MethodGet, nextPath, nil))
		if next.Code != http.StatusOK {
			t.Fatalf("next page %s returned %d: %s", test.path, next.Code, next.Body.String())
		}
		var nextPage modelListPageTest
		if err := json.NewDecoder(next.Body).Decode(&nextPage); err != nil {
			t.Fatalf("decode next page %s: %v", test.path, err)
		}
		if len(nextPage.Items) != 1 || nextPage.HasMore || nextPage.NextCursor != "" {
			t.Fatalf("unexpected next page %s: %+v", test.path, nextPage)
		}
		if got := modelListItemName(t, nextPage.Items[0], test.key); got != "beta" {
			t.Fatalf("next page %s item = %q, want beta", test.path, got)
		}
	}
}

func TestModelListPagingRejectsInvalidAndMismatchedCursors(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: "http://node-a",
		Models: []engine.Model{{Name: "alpha", Engine: "ollama"}, {Name: "beta", Engine: "ollama"}},
	})

	invalidSize := httptest.NewRecorder()
	srv.Mux().ServeHTTP(invalidSize, httptest.NewRequest(http.MethodGet, "/api/models?page_size=0", nil))
	if invalidSize.Code != http.StatusBadRequest {
		t.Fatalf("invalid page_size returned %d, want 400", invalidSize.Code)
	}

	first := httptest.NewRecorder()
	srv.Mux().ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/models?page_size=1&source=node-a", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("could not obtain model cursor (%d): %s", first.Code, first.Body.String())
	}
	var firstPage modelListPageTest
	if err := json.NewDecoder(first.Body).Decode(&firstPage); err != nil {
		t.Fatalf("decode first model page: %v", err)
	}
	if firstPage.NextCursor == "" {
		t.Fatal("first model page is missing continuation cursor")
	}

	mismatched := httptest.NewRecorder()
	path := "/api/models?page_size=1&source=node-b&cursor=" + url.QueryEscape(firstPage.NextCursor)
	srv.Mux().ServeHTTP(mismatched, httptest.NewRequest(http.MethodGet, path, nil))
	if mismatched.Code != http.StatusBadRequest {
		t.Fatalf("filter-mismatched cursor returned %d, want 400", mismatched.Code)
	}

	invalidCursor := httptest.NewRecorder()
	srv.Mux().ServeHTTP(invalidCursor, httptest.NewRequest(http.MethodGet,
		"/api/models?page_size=1&source=node-a&cursor=not-a-cursor", nil))
	if invalidCursor.Code != http.StatusBadRequest {
		t.Fatalf("invalid cursor returned %d, want 400", invalidCursor.Code)
	}

	srv.modelPageCache.mu.Lock()
	for key, entry := range srv.modelPageCache.entries {
		entry.expiresAt = time.Now().Add(-time.Second)
		srv.modelPageCache.entries[key] = entry
	}
	srv.modelPageCache.mu.Unlock()
	afterCacheExpiry := httptest.NewRecorder()
	path = "/api/models?page_size=1&source=node-a&cursor=" + url.QueryEscape(firstPage.NextCursor)
	srv.Mux().ServeHTTP(afterCacheExpiry, httptest.NewRequest(http.MethodGet, path, nil))
	if afterCacheExpiry.Code != http.StatusOK {
		t.Fatalf("cursor stopped working after page cache expiry (%d): %s", afterCacheExpiry.Code, afterCacheExpiry.Body.String())
	}
	var nextPage modelListPageTest
	if err := json.NewDecoder(afterCacheExpiry.Body).Decode(&nextPage); err != nil {
		t.Fatalf("decode page after cache expiry: %v", err)
	}
	if len(nextPage.Items) != 1 || modelListItemName(t, nextPage.Items[0], "name") != "beta" {
		t.Fatalf("page after cache expiry = %+v, want beta", nextPage)
	}
}

func TestModelInventoryCacheReusesAndExpiresSnapshots(t *testing.T) {
	cache := modelInventoryCache{}
	loads := 0
	load := func() []agent.ModelInfo {
		loads++
		return []agent.ModelInfo{{Name: fmt.Sprintf("cached-%d", loads)}}
	}

	first := cache.get("deepseek", load)
	second := cache.get("deepseek", load)
	if loads != 1 || first.createdAt != second.createdAt || first.models[0].Name != "cached-1" {
		t.Fatalf("cache did not reuse inventory: loads=%d first=%+v second=%+v", loads, first, second)
	}

	cache.mu.Lock()
	cache.entry.expiresAt = time.Now().Add(-time.Second)
	cache.mu.Unlock()
	third := cache.get("deepseek", load)
	if loads != 2 || third.models[0].Name != "cached-2" {
		t.Fatalf("expired cache was not refreshed: loads=%d first=%v third=%v", loads, first.createdAt, third.createdAt)
	}

	_ = cache.get("pi", load)
	if loads != 3 {
		t.Fatalf("backend change reused cached inventory: loads=%d, want 3", loads)
	}
}

func TestModelPageCacheIsBoundedAndExpiresEntries(t *testing.T) {
	cache := modelPageCache{}
	loads := 0
	load := func() (modelListPage, error) {
		loads++
		return modelListPage{models: []agent.ModelInfo{{Name: "cached"}}}, nil
	}

	first, err := cache.get("page-0", load)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.get("page-0", load)
	if err != nil {
		t.Fatal(err)
	}
	if loads != 1 || first.models[0].Name != second.models[0].Name {
		t.Fatalf("page cache did not reuse page: loads=%d", loads)
	}

	cache.mu.Lock()
	entry := cache.entries["page-0"]
	entry.expiresAt = time.Now().Add(-time.Second)
	cache.entries["page-0"] = entry
	cache.mu.Unlock()
	if _, err := cache.get("page-0", load); err != nil {
		t.Fatal(err)
	}
	if loads != 2 {
		t.Fatalf("expired page was not loaded again: loads=%d, want 2", loads)
	}

	for index := 1; index < modelPageCacheMaxEntries+2; index++ {
		if _, err := cache.get(fmt.Sprintf("page-%d", index), load); err != nil {
			t.Fatal(err)
		}
	}
	cache.mu.Lock()
	entries := len(cache.entries)
	cache.mu.Unlock()
	if entries > modelPageCacheMaxEntries {
		t.Fatalf("page cache has %d entries, limit is %d", entries, modelPageCacheMaxEntries)
	}
}

func TestPagedModelListingsIncludeShardsAndConfiguredLogicalModels(t *testing.T) {
	srv := newTestServer(t)
	srv.shardedModels["logical-shard"] = config.ShardedModelConfig{
		LogicalID: "logical-shard", SourceModelID: "source-model",
	}
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "node-a", Address: "http://node-a",
		Models: []engine.Model{
			{Name: "source-model", Engine: "ollama"},
			{Name: "cluster-model", Engine: "ollama"},
			{Name: "ordinary", Engine: "vllm"},
		},
	})
	addTaskTestShardGroup(srv, "http://head")

	clusterModels := httptest.NewRecorder()
	srv.Mux().ServeHTTP(clusterModels, httptest.NewRequest(http.MethodGet, "/api/models?page_size=10", nil))
	if clusterModels.Code != http.StatusOK {
		t.Fatalf("GET /api/models returned %d: %s", clusterModels.Code, clusterModels.Body.String())
	}
	var clusterPage modelListPageTest
	if err := json.NewDecoder(clusterModels.Body).Decode(&clusterPage); err != nil {
		t.Fatalf("decode cluster model page: %v", err)
	}
	if len(clusterPage.Items) != 3 {
		t.Fatalf("cluster model page has %d entries, want 3: %s", len(clusterPage.Items), clusterPage.Items)
	}

	openAIModels := httptest.NewRecorder()
	srv.Mux().ServeHTTP(openAIModels, httptest.NewRequest(http.MethodGet, "/v1/models?page_size=10", nil))
	if openAIModels.Code != http.StatusOK {
		t.Fatalf("GET /v1/models returned %d: %s", openAIModels.Code, openAIModels.Body.String())
	}
	var openAIPage modelListPageTest
	if err := json.NewDecoder(openAIModels.Body).Decode(&openAIPage); err != nil {
		t.Fatalf("decode OpenAI model page: %v", err)
	}
	names := make(map[string]bool, len(openAIPage.Items))
	owners := make(map[string]string, len(openAIPage.Items))
	for _, item := range openAIPage.Items {
		var model struct {
			ID      string `json:"id"`
			OwnedBy string `json:"owned_by"`
		}
		if err := json.Unmarshal(item, &model); err != nil {
			t.Fatalf("decode OpenAI model item: %v", err)
		}
		names[model.ID] = true
		owners[model.ID] = model.OwnedBy
	}
	if names["source-model"] || !names["logical-shard"] || !names["cluster-model"] {
		t.Fatalf("unexpected OpenAI model names: %v", names)
	}
	if owners["cluster-model"] != "head" {
		t.Fatalf("sharded model owner = %q, want head", owners["cluster-model"])
	}
}

func modelListItemName(t *testing.T, item json.RawMessage, field string) string {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(item, &value); err != nil {
		t.Fatalf("decode model list item: %v", err)
	}
	name, ok := value[field].(string)
	if !ok {
		t.Fatalf("model list item is missing %q: %s", field, item)
	}
	return name
}

func BenchmarkModelListPageCold(b *testing.B) {
	b.Setenv("PATH", "")
	srv, err := NewWithMemoryAndHarness(filepath.Join(b.TempDir(), "model-list-benchmark.db"), "deepseek")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := srv.Close(); err != nil {
			b.Errorf("close benchmark server: %v", err)
		}
	})

	const nodeCount = 512
	const modelsPerNode = 32
	for nodeIndex := 0; nodeIndex < nodeCount; nodeIndex++ {
		models := make([]engine.Model, modelsPerNode)
		for modelIndex := range models {
			models[modelIndex] = engine.Model{
				Name:   fmt.Sprintf("model-%03d-%03d", nodeIndex, modelIndex),
				Engine: "benchmark",
			}
		}
		srv.registry.Upsert(cluster.NodeStatus{
			ID: fmt.Sprintf("node-%03d", nodeIndex), Models: models,
		})
	}
	mux := srv.Mux()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.modelCache.mu.Lock()
		srv.modelCache.entry.expiresAt = time.Time{}
		srv.modelCache.mu.Unlock()

		nodeID := fmt.Sprintf("node-%03d", i%nodeCount)
		path := "/api/models?page_size=20&source=" + url.QueryEscape(nodeID)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			b.Fatalf("GET %s returned %d: %s", path, response.Code, response.Body.String())
		}
	}
}
