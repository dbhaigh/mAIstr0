package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/agent"
	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/handler_utils"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
)

const modelListCacheTTL = 10 * time.Second

const (
	modelPageCacheMaxEntries = 128
	modelListCursorID        = 1
)

type modelInventorySnapshot struct {
	backendKey string
	createdAt  time.Time
	expiresAt  time.Time
	models     []agent.ModelInfo
}

type modelInventoryCache struct {
	// mu protects the current immutable inventory snapshot. Inventory loading
	// happens without the lock so model and shard owners are never called under it.
	mu    sync.Mutex
	entry modelInventorySnapshot
}

func (c *modelInventoryCache) get(backendKey string, load func() []agent.ModelInfo) modelInventorySnapshot {
	c.mu.Lock()
	if c.entry.backendKey == backendKey && time.Now().Before(c.entry.expiresAt) {
		entry := c.entry
		c.mu.Unlock()
		return entry
	}
	c.mu.Unlock()

	models := load()
	createdAt := time.Now()
	candidate := modelInventorySnapshot{
		backendKey: backendKey,
		createdAt:  createdAt,
		expiresAt:  createdAt.Add(modelListCacheTTL),
		models:     models,
	}

	c.mu.Lock()
	if c.entry.backendKey == backendKey && time.Now().Before(c.entry.expiresAt) {
		entry := c.entry
		c.mu.Unlock()
		return entry
	}
	c.entry = candidate
	c.mu.Unlock()
	return candidate
}

type modelListPage struct {
	models  []agent.ModelInfo
	next    cluster.ModelListingPosition
	hasMore bool
}

type modelPageCacheEntry struct {
	page      modelListPage
	expiresAt time.Time
	lastUsed  uint64
}

type modelPageCache struct {
	// mu protects the bounded page cache; page generation runs outside the lock.
	mu      sync.Mutex
	entries map[string]modelPageCacheEntry
	clock   uint64
}

func (c *modelPageCache) get(key string, load func() (modelListPage, error)) (modelListPage, error) {
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok {
		if time.Now().Before(entry.expiresAt) {
			c.clock++
			entry.lastUsed = c.clock
			c.entries[key] = entry
			c.mu.Unlock()
			return entry.page, nil
		}
		delete(c.entries, key)
	}
	c.mu.Unlock()

	page, err := load()
	if err != nil {
		return modelListPage{}, err
	}

	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && time.Now().Before(entry.expiresAt) {
		c.clock++
		entry.lastUsed = c.clock
		c.entries[key] = entry
		c.mu.Unlock()
		return entry.page, nil
	}
	if c.entries == nil {
		c.entries = make(map[string]modelPageCacheEntry)
	}
	now := time.Now()
	for cachedKey, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, cachedKey)
		}
	}
	if len(c.entries) >= modelPageCacheMaxEntries {
		oldestKey := ""
		var oldestUse uint64
		for cachedKey, entry := range c.entries {
			if oldestKey == "" || entry.lastUsed < oldestUse {
				oldestKey = cachedKey
				oldestUse = entry.lastUsed
			}
		}
		delete(c.entries, oldestKey)
	}
	c.clock++
	c.entries[key] = modelPageCacheEntry{
		page: page, expiresAt: now.Add(modelListCacheTTL), lastUsed: c.clock,
	}
	c.mu.Unlock()
	return page, nil
}

type modelListFilters struct {
	source string
	engine string
}

type modelListRequest struct {
	Page         historyPageRequest
	Filters      modelListFilters
	Position     cluster.ModelListingPosition
	HasPosition  bool
	UniqueByName bool
}

func (s *Server) handleListAllModels(w http.ResponseWriter, r *http.Request) {
	_, backend := s.currentAgent()
	s.handleClusterModelInfoList(w, r, backend, "cluster-models")
}

func (s *Server) handleClusterModelInfoList(w http.ResponseWriter, r *http.Request, backend agent.Backend, resource string) {
	request, err := parseModelListRequest(r, resource, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Page.Active {
		page, err := s.modelListingsPage(request, resource, false)
		if err != nil {
			http.Error(w, "could not list model page: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeModelListResponse(w, request, page.models, page.next, page.hasMore, nil)
		return
	}
	snapshot := s.modelInventorySnapshot(backend)
	models := filterModelInfos(snapshot.models, request.Filters)
	writeModelListResponse(w, request, models, cluster.ModelListingPosition{}, false, models)
}

func parseModelListRequest(r *http.Request, resource string, uniqueByName bool) (modelListRequest, error) {
	pageRequest, err := parseHistoryPageRequest(r, resource, "source", "engine")
	if err != nil {
		return modelListRequest{}, err
	}
	request := modelListRequest{
		Page: pageRequest,
		Filters: modelListFilters{
			source: strings.TrimSpace(r.URL.Query().Get("source")),
			engine: strings.TrimSpace(r.URL.Query().Get("engine")),
		},
		UniqueByName: uniqueByName,
	}
	if !r.URL.Query().Has("cursor") {
		return request, nil
	}
	if pageRequest.Cursor.ID != modelListCursorID || pageRequest.Cursor.Key == "" {
		return modelListRequest{}, fmt.Errorf("cursor is missing its model position")
	}
	if err := json.Unmarshal([]byte(pageRequest.Cursor.Key), &request.Position); err != nil {
		return modelListRequest{}, fmt.Errorf("cursor model position is invalid")
	}
	if request.Position.Unique != uniqueByName ||
		request.Position.Kind < cluster.ModelListingNode ||
		request.Position.Kind > cluster.ModelListingLogical ||
		request.Position.Index < 0 ||
		(!uniqueByName && request.Position.Kind == cluster.ModelListingNode && request.Position.NodeID == "") {
		return modelListRequest{}, fmt.Errorf("cursor model position is invalid")
	}
	request.HasPosition = true
	return request, nil
}

func writeModelListResponse[T any](
	w http.ResponseWriter,
	request modelListRequest,
	items []T,
	next cluster.ModelListingPosition,
	hasMore bool,
	legacy any,
) {
	if !request.Page.Active {
		writeJSON(w, http.StatusOK, legacy)
		return
	}

	var cursorKey string
	if hasMore {
		encoded, err := json.Marshal(next)
		if err != nil {
			http.Error(w, "could not encode model list cursor: "+err.Error(), http.StatusInternalServerError)
			return
		}
		cursorKey = string(encoded)
	}
	writeHistoryPage(w, request.Page, items, memory.HistoryCursor{
		ID: modelListCursorID, Key: cursorKey,
	}, hasMore, maxHistoryPageBytes)
}

func filterModelInfos(models []agent.ModelInfo, filters modelListFilters) []agent.ModelInfo {
	if filters.source == "" && filters.engine == "" {
		return models
	}
	filtered := make([]agent.ModelInfo, 0, len(models))
	for _, model := range models {
		if filters.source != "" && !strings.EqualFold(model.NodeID, filters.source) {
			continue
		}
		if filters.engine != "" && !strings.EqualFold(model.Engine, filters.engine) {
			continue
		}
		filtered = append(filtered, model)
	}
	return filtered
}

func (s *Server) modelInventorySnapshot(backend agent.Backend) modelInventorySnapshot {
	return s.modelCache.get(fmt.Sprintf("%T", backend), func() []agent.ModelInfo {
		return s.buildClusterModels(backend)
	})
}

// listClusterModels returns the cached inventory. Callers must treat the
// returned models and their nested slices as immutable.
func (s *Server) listClusterModels(backend agent.Backend) []agent.ModelInfo {
	return s.modelInventorySnapshot(backend).models
}

func (s *Server) modelListingsPage(request modelListRequest, resource string, includeConfigured bool) (modelListPage, error) {
	if s.registry == nil {
		return modelListPage{}, fmt.Errorf("cluster model registry is unavailable")
	}
	excluded, excludedAll, additional := s.modelListingDecorations(includeConfigured, request.Filters)
	cacheKey, err := modelPageCacheKey(request, resource, excluded, excludedAll, additional)
	if err != nil {
		return modelListPage{}, err
	}
	return s.modelPageCache.get(cacheKey, func() (modelListPage, error) {
		page, err := s.registry.ListModelListingsPage(cluster.ModelListingPageOptions{
			Source: request.Filters.source, Engine: request.Filters.engine,
			Limit: request.Page.Limit, UniqueByName: request.UniqueByName,
			HasPosition: request.HasPosition, Position: request.Position,
			Excluded: excluded, ExcludedAll: excludedAll, Additional: additional,
		})
		if err != nil {
			return modelListPage{}, err
		}
		models := make([]agent.ModelInfo, len(page.Items))
		for index, item := range page.Items {
			models[index] = agent.ModelInfo{
				NodeID: item.NodeID, NodeAddr: item.NodeAddress, Name: item.Name,
				Engine: item.Engine, Tags: item.Tags, SizeGB: item.SizeGB,
				ContextLength: item.ContextLength, Members: item.Members,
				Default: item.Default, Healthy: item.Healthy,
			}
		}
		return modelListPage{models: models, next: page.Next, hasMore: page.HasMore}, nil
	})
}

func (s *Server) modelListingDecorations(
	includeConfigured bool,
	filters modelListFilters,
) ([]string, []string, []cluster.ModelListing) {
	excludedNodeNames := make(map[string]struct{})
	excludedAllNames := make(map[string]struct{})
	additional := make([]cluster.ModelListing, 0)
	addExcludedNode := func(name string) {
		excludedNodeNames[strings.ToLower(name)] = struct{}{}
	}
	addExcludedAll := func(name string) {
		excludedAllNames[strings.ToLower(name)] = struct{}{}
	}
	if s.shards != nil {
		groups := s.shards.List()
		sort.Slice(groups, func(i, j int) bool {
			if groups[i].Model.Name != groups[j].Model.Name {
				return groups[i].Model.Name < groups[j].Model.Name
			}
			return groups[i].ID < groups[j].ID
		})
		for index, group := range groups {
			if group.State != ShardGroupReady {
				continue
			}
			addExcludedNode(group.Model.Name)
			headID, headAddress := "", ""
			members := make([]string, 0, len(group.Members))
			seen := make(map[string]bool, len(group.Members))
			for _, member := range group.Members {
				if !seen[member.NodeID] {
					members = append(members, member.NodeID)
					seen[member.NodeID] = true
				}
				if member.Role == "head" {
					headID, headAddress = member.NodeID, member.Address
				}
			}
			additional = append(additional, cluster.ModelListing{
				NodeID: headID, NodeAddress: headAddress, Name: group.Model.Name,
				Engine: "shard/" + string(group.Mode), Tags: []string{"sharded"},
				ContextLength: group.Model.RequestedContext, Members: members,
				Healthy: true, Kind: cluster.ModelListingShard, Index: index,
			})
		}
	}
	if includeConfigured {
		logicalIDs := make([]string, 0, len(s.shardedModels))
		for logicalID, model := range s.shardedModels {
			addExcludedAll(model.SourceModelID)
			logicalIDs = append(logicalIDs, logicalID)
		}
		sort.Strings(logicalIDs)
		if filters.source == "" && filters.engine == "" {
			for index, logicalID := range logicalIDs {
				additional = append(additional, cluster.ModelListing{
					NodeID: "maistr0", Name: logicalID, Engine: "sharded",
					Healthy: true, Kind: cluster.ModelListingLogical, Index: index,
				})
			}
		}
	}
	excludedNode := make([]string, 0, len(excludedNodeNames))
	for name := range excludedNodeNames {
		excludedNode = append(excludedNode, name)
	}
	sort.Strings(excludedNode)
	excludedAll := make([]string, 0, len(excludedAllNames))
	for name := range excludedAllNames {
		excludedAll = append(excludedAll, name)
	}
	sort.Strings(excludedAll)
	return excludedNode, excludedAll, additional
}

func modelPageCacheKey(
	request modelListRequest,
	resource string,
	excluded []string,
	excludedAll []string,
	additional []cluster.ModelListing,
) (string, error) {
	encoded, err := json.Marshal(struct {
		Resource     string
		Source       string
		Engine       string
		Limit        int
		UniqueByName bool
		HasPosition  bool
		Position     cluster.ModelListingPosition
		Excluded     []string
		ExcludedAll  []string
		Additional   []cluster.ModelListing
	}{
		Resource: resource, Source: strings.ToLower(request.Filters.source),
		Engine: strings.ToLower(request.Filters.engine), Limit: request.Page.Limit,
		UniqueByName: request.UniqueByName, HasPosition: request.HasPosition,
		Position: request.Position, Excluded: excluded, ExcludedAll: excludedAll,
		Additional: additional,
	})
	if err != nil {
		return "", fmt.Errorf("encode model page cache key: %w", err)
	}
	return string(encoded), nil
}

func (s *Server) buildClusterModels(backend agent.Backend) []agent.ModelInfo {
	models := backend.ListClusterModels()
	if s.shards == nil {
		return models
	}
	groups := s.shards.List()
	groupNames := make(map[string]bool)
	for _, group := range groups {
		if group.State == ShardGroupReady {
			groupNames[strings.ToLower(group.Model.Name)] = true
		}
	}
	filtered := make([]agent.ModelInfo, 0, len(models)+len(groupNames))
	for _, model := range models {
		if !groupNames[strings.ToLower(model.Name)] {
			filtered = append(filtered, model)
		}
	}
	for _, group := range groups {
		if group.State != ShardGroupReady {
			continue
		}
		headID, headAddress := "", ""
		members := make([]string, 0, len(group.Members))
		seen := make(map[string]bool, len(group.Members))
		for _, member := range group.Members {
			if !seen[member.NodeID] {
				members = append(members, member.NodeID)
				seen[member.NodeID] = true
			}
			if member.Role == "head" {
				headID, headAddress = member.NodeID, member.Address
			}
		}
		filtered = append(filtered, agent.ModelInfo{
			NodeID: headID, NodeAddr: headAddress, Name: group.Model.Name,
			Engine: "shard/" + string(group.Mode), Tags: []string{"sharded"},
			ContextLength: group.Model.RequestedContext,
			Members:       members, Healthy: true,
		})
	}
	return filtered
}

func (s *Server) handleGenerate(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get(inferroute.HeaderIdempotencyKey)
	if err := inferroute.ValidateIdempotencyKey(idempotencyKey); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateAPIResponse{Error: err.Error()})
		return
	}
	streaming := strings.EqualFold(r.URL.Query().Get("stream"), "true")
	if idempotencyKey != "" && streaming {
		err := apperr.Invalid(errors.New("Idempotency-Key is supported only for non-streaming generate requests"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	req, ok := handler_utils.DecodeJSONWithResponder[generateAPIRequest](
		w, r, httpx.InferenceJSONBodyBytes, respondGenerateDecodeError,
	)
	if !ok {
		return
	}
	s.handleGenerateRequest(w, r, req, streaming, idempotencyKey)
}

func respondGenerateDecodeError(w http.ResponseWriter, err error) {
	writeJSONError(w, apperr.Invalid(err), generateAPIResponse{Error: err.Error()})
}

func (s *Server) handleGenerateRequest(w http.ResponseWriter, r *http.Request, req generateAPIRequest, streaming bool, idempotencyKey string) {
	if strings.TrimSpace(req.Prompt) == "" {
		err := apperr.Invalid(errors.New("prompt is required"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}

	nodes := s.schedulerNodes()
	if len(nodes) == 0 {
		err := apperr.UnavailableError(errors.New("no healthy nodes in cluster"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}

	candidates := nodes
	if req.NodeID != "" {
		n, ok := s.registry.Get(req.NodeID)
		if !ok || !n.Healthy {
			err := apperr.NotFoundError(fmt.Errorf("node not found or offline: %s", req.NodeID))
			writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
			return
		}
		for _, candidate := range nodes {
			if candidate.ID == req.NodeID {
				candidates = []cluster.NodeStatus{candidate}
				break
			}
		}
	}
	if req.Model != "" {
		hasModel := false
		for i := range candidates {
			var compatible []engine.Model
			for _, model := range candidates[i].Models {
				if strings.EqualFold(model.Name, req.Model) {
					compatible = append(compatible, model)
					hasModel = true
				}
			}
			candidates[i].Models = compatible
		}
		if !hasModel {
			err := apperr.NotFoundError(fmt.Errorf("model not found on any cluster node: %s", req.Model))
			writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
			return
		}
	}

	taskType := strings.TrimSpace(req.TaskType)
	if taskType == "" {
		taskType = "general"
	}
	assignments, err := scheduler.AssignWithExperience([]scheduler.Subtask{{
		ID: "generate", Description: req.Prompt, TaskType: taskType,
	}}, candidates, s.experience())
	if err != nil {
		err := apperr.UnavailableError(errors.New("could not assign request to a healthy node"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	if len(assignments) != 1 {
		err := apperr.UnavailableError(errors.New("scheduler returned no assignment"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	assignment := assignments[0]
	targetNode, ok := s.registry.Get(assignment.NodeID)
	if !ok || !targetNode.Healthy || targetNode.Address != assignment.Address {
		err := apperr.UnavailableError(errors.New("selected node is no longer healthy"))
		writeJSONError(w, err, generateAPIResponse{Error: err.Error()})
		return
	}
	targetModel := assignment.Model

	fullPrompt := req.Prompt
	if req.System != "" {
		fullPrompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", req.System, req.Prompt)
	}

	s.registry.IncrementLoad(targetNode.ID, 1)
	defer s.registry.IncrementLoad(targetNode.ID, -1)
	if streaming {
		s.streamNodeGenerate(w, r, targetNode.Address, targetModel, req, fullPrompt)
		return
	}

	start := time.Now()
	var output string
	if idempotencyKey != "" {
		output, err = s.callNodeGenerateWithOptionsContextAndIdempotency(
			r.Context(), targetNode.Address, targetModel, fullPrompt,
			req.Temperature, req.MaxTokens, idempotencyKey,
		)
	} else {
		output, err = s.callNodeGenerateWithOptions(targetNode.Address, targetModel, fullPrompt, req.Temperature, req.MaxTokens)
	}
	duration := time.Since(start).Milliseconds()

	if err != nil {
		var nodeErr *nodeResponseError
		if idempotencyKey != "" && errors.As(err, &nodeErr) {
			message := nodeErr.body
			var backend generateAPIResponse
			if json.Unmarshal([]byte(nodeErr.body), &backend) == nil && backend.Error != "" {
				message = backend.Error
			}
			writeJSON(w, nodeErr.statusCode, generateAPIResponse{
				NodeID: targetNode.ID, NodeAddress: targetNode.Address, Model: targetModel,
				DurationMs: duration, Error: message,
			})
			return
		}
		appErr := apperr.InternalError(err)
		writeJSONError(w, appErr, generateAPIResponse{
			NodeID:      targetNode.ID,
			NodeAddress: targetNode.Address,
			Model:       targetModel,
			DurationMs:  duration,
			Error:       appErr.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, generateAPIResponse{
		NodeID:      targetNode.ID,
		NodeAddress: targetNode.Address,
		Model:       targetModel,
		Output:      output,
		DurationMs:  duration,
	})
}

func (s *Server) handleNodeGenerate(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	req, ok := handler_utils.DecodeJSONWithResponder[generateAPIRequest](
		w, r, httpx.InferenceJSONBodyBytes, respondGenerateDecodeError,
	)
	if !ok {
		return
	}
	req.NodeID = nodeID
	body, _ := json.Marshal(req)
	r.Body = io.NopCloser(bytes.NewReader(body))
	s.handleGenerate(w, r)
}

func (s *Server) handleNodeModelGenerate(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	model := r.PathValue("model")
	req, ok := handler_utils.DecodeJSONWithResponder[generateAPIRequest](
		w, r, httpx.InferenceJSONBodyBytes, respondGenerateDecodeError,
	)
	if !ok {
		return
	}
	req.NodeID = nodeID
	req.Model = model
	body, _ := json.Marshal(req)
	r.Body = io.NopCloser(bytes.NewReader(body))
	s.handleGenerate(w, r)
}

func (s *Server) handleNodeModelTest(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	model := r.PathValue("model")
	n, ok := s.registry.Get(nodeID)
	if !ok || !n.Healthy {
		writeJSONError(w, apperr.NotFoundError(errors.New("node offline")), map[string]any{"healthy": false, "error": "node offline"})
		return
	}

	testPrompt := "Respond with OK in 3 words."
	start := time.Now()
	s.registry.IncrementLoad(n.ID, 1)
	out, err := s.callNodeGenerate(n.Address, model, testPrompt)
	s.registry.IncrementLoad(n.ID, -1)
	elapsed := time.Since(start).Milliseconds()

	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"node_id":     n.ID,
			"model":       model,
			"healthy":     false,
			"error":       err.Error(),
			"duration_ms": elapsed,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":     n.ID,
		"model":       model,
		"healthy":     true,
		"output":      out,
		"duration_ms": elapsed,
	})
}

func (s *Server) handleOpenAIModels(w http.ResponseWriter, r *http.Request) {
	request, err := parseModelListRequest(r, "openai-models", true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Page.Active {
		page, err := s.modelListingsPage(request, "openai-models", true)
		if err != nil {
			http.Error(w, "could not list model page: "+err.Error(), http.StatusInternalServerError)
			return
		}
		now := time.Now().Unix()
		list := make([]openAIModelObj, len(page.models))
		for index, model := range page.models {
			list[index] = openAIModelObj{
				ID: model.Name, Object: "model", Created: now, OwnedBy: model.NodeID,
			}
		}
		writeModelListResponse(w, request, list, page.next, page.hasMore, nil)
		return
	}
	_, backend := s.currentAgent()
	snapshot := s.modelInventorySnapshot(backend)
	models := filterModelInfos(snapshot.models, request.Filters)
	list := make([]openAIModelObj, 0, len(models)+len(s.shardedModels))
	seen := make(map[string]bool)
	shardedSources := make(map[string]struct{}, len(s.shardedModels))
	for _, model := range s.shardedModels {
		shardedSources[strings.ToLower(model.SourceModelID)] = struct{}{}
	}
	now := time.Now().Unix()

	for _, m := range models {
		if _, hidden := shardedSources[strings.ToLower(m.Name)]; hidden {
			continue
		}
		if !seen[m.Name] {
			seen[m.Name] = true
			list = append(list, openAIModelObj{
				ID:      m.Name,
				Object:  "model",
				Created: now,
				OwnedBy: m.NodeID,
			})
		}
	}
	for logicalID := range s.shardedModels {
		if seen[logicalID] {
			continue
		}
		if request.Filters.source != "" || request.Filters.engine != "" {
			continue
		}
		list = append(list, openAIModelObj{
			ID: logicalID, Object: "model", Created: now, OwnedBy: "maistr0",
		})
	}

	legacy := map[string]any{
		"object": "list",
		"data":   list,
	}
	writeModelListResponse(w, request, list, cluster.ModelListingPosition{}, false, legacy)
}

func (s *Server) handleOpenAIChatCompletions(w http.ResponseWriter, r *http.Request) {
	req, ok := handler_utils.DecodeJSON[openAIChatRequest](
		w, r, httpx.InferenceJSONBodyBytes,
	)
	if !ok {
		return
	}

	if len(req.Messages) == 0 {
		writeError(w, apperr.Invalid(errors.New("messages array is required")))
		return
	}
	if s.shards != nil {
		if _, found := s.shards.GroupForModel(req.Model); found {
			body, err := json.Marshal(req)
			if err != nil {
				writeError(w, apperr.InternalError(fmt.Errorf("encode shard inference request: %w", err)))
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.SetPathValue("path", "v1/chat/completions")
			s.handleRouterRequest(w, r)
			return
		}
	}

	// Preserve every message role supported by the engine prompt format.
	var sb strings.Builder
	for _, m := range req.Messages {
		switch m.Role {
		case "system", "user", "assistant", "tool":
			sb.WriteString("<|im_start|>" + m.Role + "\n" + m.Content + "<|im_end|>\n")
		}
	}
	sb.WriteString("<|im_start|>assistant\n")

	genReq := generateAPIRequest{
		Model:       req.Model,
		Prompt:      sb.String(),
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}
	if model, ok := s.shardedModel(req.Model); ok {
		s.handleShardedOpenAIChatCompletion(w, r, model, genReq.Prompt, engine.GenerationOptions{
			Temperature: req.Temperature, MaxTokens: req.MaxTokens,
		}, req.Stream)
		return
	}
	if model, ok := s.shardedSourceModel(req.Model); ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": map[string]string{
				"message": fmt.Sprintf("use logical model ID %q for this sharded model", model.LogicalID),
				"type":    "invalid_request_error",
			},
		})
		return
	}

	if req.Stream {
		streamWriter := newOpenAIStreamWriter(w, req.Model)
		s.handleGenerateRequest(streamWriter, r, genReq, true, "")
		streamWriter.finish()
		return
	}
	rec := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	s.handleGenerateRequest(rec, r, genReq, false, "")

	if rec.status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
		return
	}

	var genResp generateAPIResponse
	_ = json.Unmarshal(rec.body.Bytes(), &genResp)

	writeJSON(w, http.StatusOK, openAIChatCompletionResponse(
		genResp.Model, genResp.Output, genReq.Prompt, genResp.NodeID,
	))
}

func (s *Server) handleOpenAICompletions(w http.ResponseWriter, r *http.Request) {
	req, ok := handler_utils.DecodeJSON[openAICompletionRequest](
		w, r, httpx.InferenceJSONBodyBytes,
	)
	if !ok {
		return
	}
	if s.shards != nil {
		if _, found := s.shards.GroupForModel(req.Model); found {
			body, err := json.Marshal(req)
			if err != nil {
				writeError(w, apperr.InternalError(fmt.Errorf("encode shard inference request: %w", err)))
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.SetPathValue("path", "v1/completions")
			s.handleRouterRequest(w, r)
			return
		}
	}

	genReq := generateAPIRequest{
		Model:       req.Model,
		Prompt:      req.Prompt,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}

	rec := &responseCapture{header: make(http.Header), body: new(bytes.Buffer)}
	s.handleGenerateRequest(rec, r, genReq, false, "")

	if rec.status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
		return
	}

	var genResp generateAPIResponse
	_ = json.Unmarshal(rec.body.Bytes(), &genResp)

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "cmpl-" + genResp.NodeID,
		"object":  "text_completion",
		"created": time.Now().Unix(),
		"model":   genResp.Model,
		"choices": []map[string]any{
			{
				"text":          genResp.Output,
				"index":         0,
				"finish_reason": "stop",
			},
		},
	})
}
