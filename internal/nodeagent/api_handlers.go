package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func (a *Agent) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.status())
}

func (a *Agent) handleSystemStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"role":        "node",
		"persistence": memory.AvailabilityFor(a.mem != nil),
	})
}

func (a *Agent) handleExecute(w http.ResponseWriter, r *http.Request) {
	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), executeResponse{Error: err.Error()})
		return
	}

	eng := a.engineFor(req.Model)
	if eng == nil {
		err := apperr.NotFoundError(fmt.Errorf("model not found on this node: %s", req.Model))
		writeJSONError(w, err, executeResponse{Error: err.Error()})
		return
	}

	release, err := a.executorComponent.beginExecution(r.Context())
	if err != nil {
		writeJSON(w, http.StatusRequestTimeout, executeResponse{Error: "execution interrupted while waiting for capacity"})
		return
	}
	defer release()

	start := time.Now()
	output, err := generateWithOptions(r.Context(), eng, req.Model, req.Prompt, engine.GenerationOptions{})
	duration := time.Since(start).Milliseconds()

	a.remember(memory.Experience{
		Kind:        "subtask",
		TaskType:    req.TaskType,
		Description: truncateText(req.Prompt, 400),
		Prompt:      req.Prompt,
		Output:      output,
		Model:       req.Model,
		Success:     err == nil,
		Error:       errText(err),
		DurationMs:  duration,
		Tags:        []string{"execute", req.SubtaskID},
	})

	if err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			writeJSON(w, http.StatusRequestTimeout, executeResponse{Error: "execution interrupted"})
			return
		}
		writeJSONError(w, apperr.InternalError(err), executeResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, executeResponse{Output: output})
}

func (a *Agent) handleGenerate(w http.ResponseWriter, r *http.Request) {
	var req generateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), generateResponse{Error: err.Error()})
		return
	}

	modelName := req.Model
	if modelName == "" {
		modelName = a.defaultModelName()
	}
	if modelName == "" {
		enabled := a.enabledModels()
		if len(enabled) > 0 {
			modelName = enabled[0].Name
		}
	}

	eng := a.engineFor(modelName)
	if eng == nil {
		err := apperr.NotFoundError(fmt.Errorf("model not found on this node: %s", modelName))
		writeJSONError(w, err, generateResponse{Error: err.Error()})
		return
	}

	prompt := req.Prompt
	if req.System != "" {
		prompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", req.System, req.Prompt)
	}

	release := a.executorComponent.beginActiveTask()
	defer release()

	start := time.Now()
	options := engine.GenerationOptions{Temperature: req.Temperature, MaxTokens: req.MaxTokens}
	if strings.EqualFold(r.URL.Query().Get("stream"), "true") {
		a.streamGenerate(w, r, eng, modelName, prompt, options)
		return
	}
	output, err := generateWithOptions(r.Context(), eng, modelName, prompt, options)
	duration := time.Since(start).Milliseconds()

	a.remember(memory.Experience{
		Kind:        "generate",
		Description: truncateText(req.Prompt, 400),
		Prompt:      req.Prompt,
		Output:      output,
		Model:       modelName,
		Success:     err == nil,
		Error:       errText(err),
		DurationMs:  duration,
	})

	if err != nil {
		writeJSONError(w, apperr.InternalError(err), generateResponse{Model: modelName, DurationMs: duration, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, generateResponse{Model: modelName, Output: output, DurationMs: duration})
}

func (a *Agent) handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), chatResponse{Error: err.Error()})
		return
	}

	modelName := req.Model
	if modelName == "" {
		modelName = a.defaultModelName()
	}
	if modelName == "" {
		enabled := a.enabledModels()
		if len(enabled) > 0 {
			modelName = enabled[0].Name
		}
	}

	eng := a.engineFor(modelName)
	if eng == nil {
		err := apperr.NotFoundError(fmt.Errorf("model not found on this node: %s", modelName))
		writeJSONError(w, err, chatResponse{Error: err.Error()})
		return
	}

	release := a.executorComponent.beginActiveTask()
	defer release()

	start := time.Now()
	reply, err := chatWithContext(r.Context(), eng, modelName, req.Messages)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			writeJSON(w, http.StatusRequestTimeout, chatResponse{Model: modelName, DurationMs: duration, Error: "chat interrupted"})
			return
		}
		// Fallback to Generate if Chat returned an error
		var sb strings.Builder
		for _, m := range req.Messages {
			sb.WriteString(fmt.Sprintf("<|im_start|>%s\n%s<|im_end|>\n", m.Role, m.Content))
		}
		sb.WriteString("<|im_start|>assistant\n")
		var genErr error
		reply, genErr = generateWithOptions(r.Context(), eng, modelName, sb.String(), engine.GenerationOptions{})
		if genErr != nil {
			writeJSONError(w, apperr.InternalError(fmt.Errorf("%s; fallback error: %w", err, genErr)),
				chatResponse{Model: modelName, DurationMs: duration, Error: err.Error() + "; fallback error: " + genErr.Error()})
			return
		}
	}

	writeJSON(w, http.StatusOK, chatResponse{
		Model: modelName,
		Message: engine.ChatMessage{
			Role:    "assistant",
			Content: reply,
		},
		DurationMs: duration,
	})
}

func (a *Agent) handleDialogueCreate(w http.ResponseWriter, r *http.Request) {
	var req createDialogueRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	id := req.ID
	if id == "" {
		id = fmt.Sprintf("dlg-%s-%d", a.cfg.NodeID, time.Now().UnixNano())
	}
	dlg := a.dialogueComponent.create(id, req.Model, req.System)
	writeJSON(w, http.StatusCreated, dlg)
}

func (a *Agent) handleDialogueList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.dialogueComponent.list())
}

func (a *Agent) handleDialogueGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	dlg, ok := a.dialogueComponent.get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("dialogue not found")))
		return
	}
	writeJSON(w, http.StatusOK, dlg)
}

func (a *Agent) handleDialogueDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !a.dialogueComponent.delete(id) {
		writeError(w, apperr.NotFoundError(errors.New("dialogue not found")))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleDialogueMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req dialogueMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, apperr.Invalid(err), dialogueMessageResponse{Error: err.Error()})
		return
	}

	content := strings.TrimSpace(req.Content)
	if content == "" {
		content = strings.TrimSpace(req.Message)
	}
	if content == "" {
		err := apperr.Invalid(errors.New("content is required"))
		writeJSONError(w, err, dialogueMessageResponse{Error: err.Error()})
		return
	}

	role := req.Role
	if role == "" {
		role = "user"
	}

	res, err := a.dialogueTurnContext(r.Context(), id, "", "", role, content, "dialogue_turn")
	if err != nil {
		writeJSONError(w, apperr.InternalError(err), dialogueMessageResponse{
			DialogueID: id,
			Model:      res.Model,
			DurationMs: res.DurationMs,
			Error:      err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (a *Agent) handleDialogueInterrupt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, apperr.Invalid(errors.New("id is required")), map[string]any{"error": "id is required"})
		return
	}
	interrupted := a.InterruptDialogue(id)
	writeJSON(w, http.StatusOK, map[string]any{
		"dialogue_id": id,
		"interrupted": interrupted,
	})
}

func (a *Agent) handlePairingStatus(w http.ResponseWriter, _ *http.Request) {
	cfg := a.configSnapshot()
	orchestratorID := a.getOrchestratorID()
	trusted := orchestratorID != "" && a.identityComponent.IsTrustedID(orchestratorID)
	joined := a.isJoined()
	orchestratorAddr := a.getOrchestratorAddr()
	pairingRequired := false
	autoJoinAvailable := false
	if !joined {
		candidates := a.orchestratorCandidates()
		if len(candidates) > 0 {
			orchestratorAddr = candidates[0].address
			orchestratorID = candidates[0].id
			trusted = a.identityComponent.IsTrustedID(orchestratorID)
		}
		for _, candidate := range candidates {
			if discovery.IsSameSubnetAddress(candidate.address) {
				autoJoinAvailable = true
			}
			if !a.identityComponent.IsTrustedID(candidate.id) {
				pairingRequired = true
				break
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"role":                "node",
		"node_id":             cfg.NodeID,
		"node_addr":           cfg.AdvertiseAddr,
		"orchestrator_addr":   orchestratorAddr,
		"orchestrator_id":     orchestratorID,
		"trusted":             trusted,
		"joined":              joined,
		"pairing_required":    pairingRequired,
		"auto_join_available": autoJoinAvailable,
	})
}

func (a *Agent) handlePairingConnect(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Address string `json:"address"`
		PIN     string `json:"pin"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&request); err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing request")))
		return
	}
	if err := a.PairWithOrchestrator(request.Address, request.PIN); err != nil {
		writeError(w, apperr.UpstreamError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"orchestrator_addr": a.getOrchestratorAddr(),
		"orchestrator_id":   a.getOrchestratorID(),
	})
}

func (a *Agent) handlePairingInvitation(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil {
		http.Error(w, "pairing invitations require HTTPS", http.StatusUpgradeRequired)
		return
	}
	if len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "pairing invitation must include an orchestrator identity", http.StatusForbidden)
		return
	}
	var request pairingInvitationRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&request); err != nil {
		writeError(w, apperr.Invalid(errors.New("invalid pairing invitation")))
		return
	}
	address, err := normalizeOrchestratorAddress(request.Address)
	if err != nil || request.NodeID != a.cfg.NodeID || request.OrchestratorID == "" ||
		r.TLS.PeerCertificates[0].Subject.CommonName != request.OrchestratorID || !validPairingPIN(request.PIN) {
		writeError(w, apperr.Invalid(errors.New("pairing invitation identity or details are invalid")))
		return
	}
	now := time.Now()
	expiresAt := request.ExpiresAt
	if expiresAt.IsZero() || expiresAt.After(now.Add(10*time.Minute)) {
		expiresAt = now.Add(10 * time.Minute)
	}
	if !expiresAt.After(now) {
		http.Error(w, "pairing invitation has expired", http.StatusGone)
		return
	}

	invitation := &pairingInvitation{
		OrchestratorID: request.OrchestratorID,
		Address:        address,
		PIN:            request.PIN,
		ExpiresAt:      expiresAt,
	}
	stored, err := a.orchestratorLink.storePairingInvitation(invitation, now)
	if err != nil {
		writeError(w, apperr.ConflictError(err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "expires_at": stored.ExpiresAt})
}

func (a *Agent) handlePairingInvitationStatus(w http.ResponseWriter, _ *http.Request) {
	invitation := a.orchestratorLink.pairingInvitationStatus(time.Now())
	if invitation == nil {
		writeJSON(w, http.StatusOK, map[string]any{"pending": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pending":         true,
		"orchestrator_id": invitation.OrchestratorID,
		"address":         invitation.Address,
		"expires_at":      invitation.ExpiresAt,
	})
}

func (a *Agent) handlePairingInvitationAccept(w http.ResponseWriter, _ *http.Request) {
	invitation, err := a.orchestratorLink.beginPairingInvitationAcceptance(time.Now())
	if errors.Is(err, errPairingInvitationAlreadyAccepting) {
		writeError(w, apperr.ConflictError(err))
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusGone)
		return
	}

	err = a.pairWithAddress(invitation.Address, invitation.OrchestratorID, invitation.PIN)

	a.orchestratorLink.finishPairingInvitationAcceptance(invitation, err)
	if err != nil {
		writeError(w, apperr.UpstreamError(fmt.Errorf("could not join cluster: %w", err)))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"orchestrator_id":   a.getOrchestratorID(),
		"orchestrator_addr": a.getOrchestratorAddr(),
	})
}

func (a *Agent) handlePairingInvitationReject(w http.ResponseWriter, _ *http.Request) {
	if err := a.orchestratorLink.rejectPairingInvitation(); err != nil {
		writeError(w, apperr.ConflictError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleProperties(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.refreshDetectedEngines()
		writeJSON(w, http.StatusOK, a.propertiesSnapshot())
		return
	}
	var req nodeProperties
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	a.mu.Lock()
	if strings.TrimSpace(req.AdvertiseAddr) != "" {
		a.cfg.AdvertiseAddr = strings.TrimRight(req.AdvertiseAddr, "/")
	}
	a.cfg.DiscoveryEnabled = req.DiscoveryEnabled
	a.cfg.AutoOpenBrowser = req.AutoOpenBrowser
	if req.TrayMode != "" {
		a.cfg.TrayMode = req.TrayMode
	}
	if req.MemoryPath != "" {
		a.cfg.MemoryPath = req.MemoryPath
	}
	if req.DefaultModel != "" {
		a.defaultModel = req.DefaultModel
	}
	a.mu.Unlock()
	a.setOrchestratorAddr(strings.TrimRight(strings.TrimSpace(req.OrchestratorAddr), "/"))
	if req.DisabledModels != nil {
		disabled := make(map[string]bool, len(req.DisabledModels))
		for _, model := range req.DisabledModels {
			disabled[strings.TrimSpace(model)] = true
		}
		a.engineComponent.configureModels(disabled, "")
	}
	if req.EngineName != "" && req.EngineURL != "" {
		if configured := engine.NewConfigured(strings.ToLower(req.EngineName), req.EngineURL); configured != nil {
			a.engineComponent.replaceConfiguredEngine(configured)
		}
	}
	a.refreshDetectedEngines()
	writeJSON(w, http.StatusOK, a.propertiesSnapshot())
}

func (a *Agent) propertiesSnapshot() nodeProperties {
	a.mu.Lock()
	props := nodeProperties{
		ID:               a.cfg.NodeID,
		ListenAddr:       a.cfg.ListenAddr,
		AdvertiseAddr:    a.cfg.AdvertiseAddr,
		DefaultModel:     a.defaultModel,
		DiscoveryEnabled: a.cfg.DiscoveryEnabled,
		AutoOpenBrowser:  a.cfg.AutoOpenBrowser,
		TrayMode:         a.cfg.TrayMode,
		MemoryPath:       a.cfg.MemoryPath,
	}
	for model, disabled := range a.disabledModels {
		if disabled {
			props.DisabledModels = append(props.DisabledModels, model)
		}
	}
	a.mu.Unlock()

	sort.Strings(props.DisabledModels)
	props.OrchestratorAddr = a.getOrchestratorAddr()

	if engines := a.engineSnapshot(); len(engines) > 0 {
		props.EngineName = engines[0].Name()
		props.EngineURL = engineURL(engines[0])
	}
	return props
}

func engineURL(e engine.Engine) string {
	if provider, ok := e.(interface{ URL() string }); ok {
		return provider.URL()
	}
	return ""
}

func (a *Agent) handleEngineInfo(w http.ResponseWriter, r *http.Request) {
	engines := a.engineSnapshot()
	result := make([]engineInfo, 0, len(engines))
	for _, e := range engines {
		url := engineURL(e)
		_, canPull := e.(engine.ModelManager)
		result = append(result, engineInfo{Name: e.Name(), URL: url, CanPull: canPull})
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *Agent) handleModelPull(w http.ResponseWriter, r *http.Request) {
	a.refreshDetectedEngines()
	var req struct {
		Model  string `json:"model"`
		Engine string `json:"engine,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(fmt.Errorf("invalid model pull request: %w", err)))
		return
	}
	req.Model = strings.TrimSpace(req.Model)
	req.Engine = strings.ToLower(strings.TrimSpace(req.Engine))
	if req.Model == "" {
		writeError(w, apperr.Invalid(errors.New("model is required")))
		return
	}
	foundEngine := false
	for _, e := range a.engineSnapshot() {
		if req.Engine != "" && !strings.EqualFold(e.Name(), req.Engine) {
			continue
		}
		foundEngine = true
		if manager, ok := e.(engine.ModelManager); ok {
			if err := manager.Pull(req.Model); err != nil {
				writeJSONError(w, apperr.UpstreamError(err), map[string]any{"error": err.Error()})
				return
			}
			a.engineComponent.invalidateReportedModels()
			writeJSON(w, http.StatusOK, map[string]any{"model": req.Model, "engine": e.Name()})
			return
		}
		if req.Engine != "" {
			http.Error(w, "configured engine does not support model pulls: "+e.Name(), http.StatusNotImplemented)
			return
		}
	}
	if req.Engine != "" && !foundEngine {
		writeError(w, apperr.Invalid(fmt.Errorf("engine is not configured on this node: %s", req.Engine)))
		return
	}
	http.Error(w, "no configured engine supports model pulls", http.StatusNotImplemented)
}

func (a *Agent) handleModelsGet(w http.ResponseWriter, r *http.Request) {
	a.refreshDetectedEngines()
	writeJSON(w, http.StatusOK, a.enabledModels())
}

func (a *Agent) handleModelsAllGet(w http.ResponseWriter, r *http.Request) {
	a.refreshDetectedEngines()
	writeJSON(w, http.StatusOK, a.allModelsWithState())
}

func (a *Agent) handleModelsRefresh(w http.ResponseWriter, r *http.Request) {
	a.rescanDetectedEngines()
	a.engineComponent.invalidateReportedModels()
	writeJSON(w, http.StatusOK, a.allModelsWithState())
}

func (a *Agent) handleModelsPatch(w http.ResponseWriter, r *http.Request) {
	var req modelsPatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	disabled := make(map[string]bool, len(req.DisabledModels))
	for _, m := range req.DisabledModels {
		disabled[m] = true
	}
	if req.DefaultModel != "" && (disabled[req.DefaultModel] || a.engineFor(req.DefaultModel) == nil) {
		writeError(w, apperr.Invalid(errors.New("default model must be an installed enabled model")))
		return
	}
	a.engineComponent.configureModels(disabled, req.DefaultModel)
	writeJSON(w, http.StatusOK, a.enabledModels())
}

func (a *Agent) handlePeersGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.Peers())
}

// handleSubmit lets this node accept a task directly and share the work
// across every other node it knows about (via the peer cache), dispatching
// each subtask straight to the chosen peer's /execute endpoint rather than
// routing everything back through the orchestrator.
func (a *Agent) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}

	subtasks := scheduler.Decompose(req.Description)
	if len(subtasks) == 0 {
		writeError(w, apperr.Invalid(errors.New("description produced no subtasks")))
		return
	}

	assignments, err := scheduler.AssignWithExperience(subtasks, a.Peers(), a.experience())
	if err != nil {
		if errors.Is(err, scheduler.ErrNoHealthyNodes) {
			writeError(w, apperr.UnavailableError(err))
			return
		}
		var unassignable *scheduler.UnassignableSubtasksError
		if errors.As(err, &unassignable) {
			writeError(w, apperr.UnavailableError(err))
			return
		}
		writeError(w, apperr.InternalError(err))
		return
	}
	if len(assignments) != len(subtasks) {
		writeError(w, apperr.UnavailableError(errors.New("scheduler returned an incomplete assignment")))
		return
	}

	task, err := a.tasks.CreateChecked(req.Description, assignments)
	if err != nil {
		writeError(w, apperr.InternalError(fmt.Errorf("could not persist task: %w", err)))
		return
	}
	taskCtx, ok := a.tasks.Context(task.ID)
	if !ok {
		writeError(w, apperr.InternalError(errors.New("created task is no longer available")))
		return
	}
	for _, asg := range assignments {
		if err := a.peerComponent.dispatchPool.Submit(taskCtx, peerDispatchJob{taskID: task.ID, assignment: asg}); err != nil {
			a.tasks.CompleteSubtask(task.ID, asg.Subtask.ID, "", err)
		}
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (a *Agent) handleTasksList(w http.ResponseWriter, r *http.Request) {
	version, watermarks, err := memory.ReadSyncRequestHeaders(r)
	if err != nil {
		http.Error(w, "invalid task sync headers: "+err.Error(), http.StatusBadRequest)
		return
	}
	tasks, currentWatermarks := a.tasks.SnapshotAndWatermarksForPeer(version, taskmgr.FilterTaskWatermarks(watermarks))
	if err := memory.SetSyncResponseHeaders(w, currentWatermarks); err != nil {
		http.Error(w, "could not encode task sync watermarks: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (a *Agent) handleTaskGet(w http.ResponseWriter, r *http.Request) {
	t, ok := a.tasks.Get(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("task not found")))
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// handleAPINodesGet mirrors the orchestrator's GET /api/nodes so the same
// GUI works unmodified when loaded from any node: every node's peer cache
// is refreshed from the same orchestrator registry, so this list is
// identical across the whole cluster.
func (a *Agent) handleAPINodesGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.Peers())
}

func (a *Agent) handleAPIDiscovery(w http.ResponseWriter, r *http.Request) {
	if a.discoveryListener == nil {
		writeJSON(w, http.StatusOK, []discovery.Peer{})
		return
	}
	writeJSON(w, http.StatusOK, a.discoveryListener.Snapshot())
}

func (a *Agent) handleAPIDiscoveryPairingInvite(w http.ResponseWriter, r *http.Request) {
	orchestratorAddr := a.getOrchestratorAddr()
	if orchestratorAddr == "" {
		writeError(w, apperr.UnavailableError(errors.New("pairing invitations must be sent from a joined cluster member")))
		return
	}
	path := "/api/discovery/" + url.PathEscape(r.PathValue("id")) + "/pairing/invite"
	a.proxy(w, r, orchestratorAddr, path)
}

// handleAPITasksPost sends joined-cluster submissions to the persistent
// orchestrator task manager. Standalone nodes fall back to their elected peer.
func (a *Agent) handleAPITasksPost(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, "/api/tasks")
		return
	}
	if a.isMostCapable() {
		a.handleSubmit(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/submit")
}

func (a *Agent) handleAPITasksList(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, "/api/tasks")
		return
	}
	if a.isMostCapable() {
		a.handleTasksList(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks")
}

func (a *Agent) handleAPITaskGet(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, "/api/tasks/"+url.PathEscape(r.PathValue("id")))
		return
	}
	if a.isMostCapable() {
		a.handleTaskGet(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks/"+r.PathValue("id"))
}

func (a *Agent) handleAPITaskCancel(w http.ResponseWriter, r *http.Request) {
	taskPath := "/api/tasks/" + url.PathEscape(r.PathValue("id")) + "/cancel"
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, taskPath)
		return
	}
	if a.isMostCapable() {
		a.handleTaskCancel(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks/"+url.PathEscape(r.PathValue("id"))+"/cancel")
}

func (a *Agent) handleAPITaskApprove(w http.ResponseWriter, r *http.Request) {
	a.proxyTaskAction(w, r, "/approve")
}

func (a *Agent) handleAPITaskResume(w http.ResponseWriter, r *http.Request) {
	a.proxyTaskAction(w, r, "/resume")
}

func (a *Agent) handleAPITaskCheckpoints(w http.ResponseWriter, r *http.Request) {
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, "/api/tasks/"+url.PathEscape(r.PathValue("id"))+"/checkpoints")
		return
	}
	writeError(w, apperr.UnavailableError(errors.New("task checkpoints are unavailable without an orchestrator")))
}

func (a *Agent) proxyTaskAction(w http.ResponseWriter, r *http.Request, action string) {
	taskPath := "/api/tasks/" + url.PathEscape(r.PathValue("id")) + action
	if addr := a.getOrchestratorAddr(); addr != "" {
		a.proxy(w, r, addr, taskPath)
		return
	}
	writeError(w, apperr.UnavailableError(errors.New("task action is unavailable without an orchestrator")))
}

func (a *Agent) handleAPIMemorySync(w http.ResponseWriter, r *http.Request) {
	addr := a.getOrchestratorAddr()
	if addr == "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator unavailable")))
		return
	}
	a.proxy(w, r, addr, "/api/memory/sync")
}

// handleAPINodeModelsGet/Patch serve local models directly and proxy remote
// models to their owner so the same GUI works from any node.
func (a *Agent) handleAPINodeModelsGet(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("id") == a.cfg.NodeID {
		a.handleModelsAllGet(w, r)
		return
	}
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	a.proxy(w, r, addr, "/models/all")
}

func (a *Agent) handleAPINodeModelsPatch(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("id") == a.cfg.NodeID {
		a.handleModelsPatch(w, r)
		return
	}
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	a.proxy(w, r, addr, "/models")
}

func (a *Agent) handleAPINodeControl(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("id")
	path := "/properties"
	if strings.HasSuffix(r.URL.Path, "/engines") {
		path = "/engine"
	}
	if strings.HasSuffix(r.URL.Path, "/pull") {
		path = "/models/pull"
	}
	if strings.HasSuffix(r.URL.Path, "/refresh") {
		path = "/models/refresh"
	}
	if strings.HasSuffix(r.URL.Path, "/pairing/connect") {
		path = "/api/pairing/connect"
	}
	if nodeID == a.cfg.NodeID {
		switch path {
		case "/engine":
			a.handleEngineInfo(w, r)
		case "/models/pull":
			a.handleModelPull(w, r)
		case "/models/refresh":
			a.handleModelsRefresh(w, r)
		case "/api/pairing/connect":
			a.handlePairingConnect(w, r)
		default:
			a.handleProperties(w, r)
		}
		return
	}
	if _, ok := a.peerAddress(nodeID); !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	orchestratorAddr := a.getOrchestratorAddr()
	if orchestratorAddr == "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator unavailable")))
		return
	}
	a.proxy(w, r, orchestratorAddr, r.URL.RequestURI())
}

func (a *Agent) handleAPINodeDialogue(w http.ResponseWriter, r *http.Request) {
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("node not found")))
		return
	}
	prefix := "/api/nodes/" + url.PathEscape(r.PathValue("id"))
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if path == "" {
		path = "/dialogues"
	} else if strings.HasPrefix(path, "/dialogues") {
		path = strings.TrimPrefix(path, "/dialogues")
		path = "/dialogues" + path
	}
	a.proxy(w, r, addr, path)
}

func (a *Agent) handleAPIAgentProxy(w http.ResponseWriter, r *http.Request) {
	orch := a.getOrchestratorAddr()
	if orch == "" {
		writeError(w, apperr.UnavailableError(errors.New("orchestrator unavailable; harness requests cannot run on a worker node")))
		return
	}
	destPath := r.URL.Path
	if r.URL.RawQuery != "" {
		destPath += "?" + r.URL.RawQuery
	}
	a.proxy(w, r, orch, destPath)
}
