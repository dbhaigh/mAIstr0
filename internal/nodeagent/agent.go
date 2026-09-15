// Package nodeagent implements the per-node HTTP service: it detects local
// LLM engines/models and hardware, reports itself to the orchestrator, and
// executes subtasks dispatched to it.
package nodeagent

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
	"github.com/maistr0/maistr0/internal/version"
	"github.com/maistr0/maistr0/web"
)

type Agent struct {
	cfg               config.NodeConfig
	engines           []engine.Engine
	enginesConfigured bool
	hw                hardware.Info

	discoveryListener *discovery.Listener
	events            *hub.Hub

	peerClient     *http.Client // short timeout, for polling the orchestrator's node list
	dispatchClient *http.Client // long timeout, peer-to-peer subtask execution can cold-load a model
	streamClient   *http.Client // no timeout, kept open for the live /api/events subscription
	tasks          *taskmgr.Manager

	orchMu           sync.RWMutex
	orchestratorAddr string

	mu             sync.Mutex
	disabledModels map[string]bool
	defaultModel   string
	activeTasks    int
	peers          []cluster.NodeStatus
	modelMu        sync.Mutex
	reportedModels []engine.Model
	reportedAt     time.Time
	joinedMu       sync.Mutex
	joined         bool

	dialogues   map[string]*Dialogue
	dialoguesMu sync.RWMutex

	mem *memory.Store
}

// Dialogue represents an ongoing conversational session with an LLM on this node.
type Dialogue struct {
	ID        string               `json:"id"`
	Model     string               `json:"model"`
	System    string               `json:"system,omitempty"`
	Messages  []engine.ChatMessage `json:"messages"`
	CreatedAt time.Time            `json:"created_at"`
	UpdatedAt time.Time            `json:"updated_at"`
}

func New(cfg config.NodeConfig) *Agent {
	if cfg.NodeID == "" {
		cfg.NodeID = DefaultNodeID()
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = "http://" + discovery.OutboundIP() + portSuffix(cfg.ListenAddr)
	}
	disabled := make(map[string]bool, len(cfg.DisabledModels))
	for _, m := range cfg.DisabledModels {
		disabled[m] = true
	}
	engines := engine.DetectAll()
	enginesConfigured := len(cfg.Engines) > 0
	if len(cfg.Engines) > 0 {
		engines = nil
		for _, configured := range cfg.Engines {
			if e := engine.NewConfigured(configured.Name, configured.URL); e != nil {
				engines = append(engines, e)
			}
		}
	}
	hw := hardware.Detect()
	if !enginesConfigured && hw.HasNPU {
		engines = prioritizeNPUEngines(engines)
	}
	a := &Agent{
		cfg:               cfg,
		engines:           engines,
		enginesConfigured: enginesConfigured,
		hw:                hw,
		disabledModels:    disabled,
		defaultModel:      cfg.DefaultModel,
		peerClient:        &http.Client{Timeout: 5 * time.Second},
		dispatchClient:    &http.Client{Timeout: 10 * time.Minute},
		streamClient:      &http.Client{},
		tasks:             taskmgr.NewManager(),
		orchestratorAddr:  cfg.OrchestratorAddr,
		events:            hub.New(),
		dialogues:         make(map[string]*Dialogue),
	}
	if store, err := memory.Open(cfg.MemoryPath, cfg.NodeID); err != nil {
		log.Printf("nodeagent: memory unavailable, running stateless: %v", err)
	} else {
		a.mem = store
		log.Printf("nodeagent: memory store at %s", store.Path())
	}
	return a
}

func prioritizeNPUEngines(engines []engine.Engine) []engine.Engine {
	ordered := make([]engine.Engine, 0, len(engines))
	for _, e := range engines {
		if e.Name() == "fastflowlm" {
			ordered = append(ordered, e)
		}
	}
	for _, e := range engines {
		if e.Name() != "fastflowlm" {
			ordered = append(ordered, e)
		}
	}
	return ordered
}

// Memory exposes this node's persistent knowledge store (nil if the
// database could not be opened).
func (a *Agent) Memory() *memory.Store { return a.mem }

// remember persists one unit of local LLM work so the cluster can learn
// which models on this node actually perform well.
func (a *Agent) remember(e memory.Experience) {
	if a.mem == nil {
		return
	}
	if e.NodeID == "" {
		e.NodeID = a.cfg.NodeID
	}
	if _, err := a.mem.Record(e); err != nil {
		log.Printf("nodeagent: memory write failed: %v", err)
	}
}

// DefaultNodeID generates a stable node ID from the hostname,
// exported so callers (main.go) can compute it up front, e.g. to include it
// in a shared discovery listener's self-exclusion set.
func DefaultNodeID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node"
	}
	return hostname
}

// SetDiscovery attaches a (possibly shared) discovery listener the agent
// will use to auto-resolve the orchestrator's address when none is
// configured. Must be called before Run.
func (a *Agent) SetDiscovery(l *discovery.Listener) {
	a.discoveryListener = l
}

// ID returns this agent's node ID (auto-generated if not configured).
func (a *Agent) ID() string { return a.cfg.NodeID }

type executeRequest struct {
	SubtaskID string `json:"subtask_id"`
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	TaskType  string `json:"task_type,omitempty"`
}

type executeResponse struct {
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

type statusResponse struct {
	ID           string         `json:"id"`
	Version      string         `json:"version"`
	Address      string         `json:"address"`
	Hardware     hardware.Info  `json:"hardware"`
	Models       []engine.Model `json:"models"`
	DefaultModel string         `json:"default_model,omitempty"`
	FastScore    float64        `json:"fast_score"`
	ActiveTasks  int            `json:"active_tasks"`
}

func fastScore(hw hardware.Info) float64 {
	return cluster.FastScore(hw)
}

func (a *Agent) enabledModels() []engine.Model {
	a.refreshDetectedEngines()
	var out []engine.Model
	for _, e := range a.engines {
		models, err := e.ListModels()
		if err != nil {
			log.Printf("nodeagent: list models for engine %s failed: %v", e.Name(), err)
			continue
		}
		for _, m := range models {
			if !a.disabledModels[m.Name] {
				out = append(out, m)
			}
		}
	}
	return out
}

// refreshDetectedEngines handles Ollama starting after the node agent. An
// explicitly configured engine is authoritative and must not be replaced by
// automatic localhost detection.
func (a *Agent) refreshDetectedEngines() {
	if a.enginesConfigured {
		return
	}
	if len(a.engines) == 0 {
		a.engines = engine.DetectAll()
		if a.hw.HasNPU {
			a.engines = prioritizeNPUEngines(a.engines)
		}
	}
}

// ModelState pairs a discovered model with whether it is currently enabled
// for scheduling, used by the GUI's per-node model configuration panel.
type ModelState struct {
	engine.Model
	Enabled bool `json:"enabled"`
	Default bool `json:"default"`
}

func (a *Agent) allModelsWithState() []ModelState {
	a.refreshDetectedEngines()
	a.mu.Lock()
	disabled := make(map[string]bool, len(a.disabledModels))
	for k, v := range a.disabledModels {
		disabled[k] = v
	}
	a.mu.Unlock()

	var out []ModelState
	for _, e := range a.engines {
		models, err := e.ListModels()
		if err != nil {
			log.Printf("nodeagent: list models for engine %s failed: %v", e.Name(), err)
			continue
		}
		for _, m := range models {
			out = append(out, ModelState{Model: m, Enabled: !disabled[m.Name], Default: m.Name == a.defaultModelName()})
		}
	}
	return out
}

func (a *Agent) status() statusResponse {
	a.mu.Lock()
	active := a.activeTasks
	a.mu.Unlock()
	return statusResponse{
		ID:           a.cfg.NodeID,
		Version:      version.String(),
		Address:      a.cfg.AdvertiseAddr,
		Hardware:     a.hw,
		Models:       a.reportedModelList(),
		DefaultModel: a.defaultModelName(),
		FastScore:    fastScore(a.hw),
		ActiveTasks:  active,
	}
}

func (a *Agent) reportedModelList() []engine.Model {
	a.modelMu.Lock()
	defer a.modelMu.Unlock()
	if time.Since(a.reportedAt) >= 10*time.Second || a.reportedModels == nil {
		a.reportedModels = a.enabledModels()
		a.reportedAt = time.Now()
	}
	return append([]engine.Model(nil), a.reportedModels...)
}

func (a *Agent) defaultModelName() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.defaultModel
}

func (a *Agent) engineFor(model string) engine.Engine {
	a.refreshDetectedEngines()
	for _, e := range a.engines {
		models, err := e.ListModels()
		if err != nil {
			continue
		}
		for _, m := range models {
			if m.Name == model {
				return e
			}
		}
	}
	return nil
}

func (a *Agent) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.status())
}

func (a *Agent) handleExecute(w http.ResponseWriter, r *http.Request) {
	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, executeResponse{Error: err.Error()})
		return
	}

	eng := a.engineFor(req.Model)
	if eng == nil {
		writeJSON(w, http.StatusNotFound, executeResponse{Error: "model not found on this node: " + req.Model})
		return
	}

	a.mu.Lock()
	a.activeTasks++
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.activeTasks--
		a.mu.Unlock()
	}()

	start := time.Now()
	output, err := eng.Generate(req.Model, req.Prompt)
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
		writeJSON(w, http.StatusInternalServerError, executeResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, executeResponse{Output: output})
}

type generateRequest struct {
	Model       string  `json:"model"`
	Prompt      string  `json:"prompt"`
	System      string  `json:"system,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
}

type generateResponse struct {
	Model      string `json:"model"`
	Output     string `json:"output"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

func (a *Agent) handleGenerate(w http.ResponseWriter, r *http.Request) {
	var req generateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, generateResponse{Error: err.Error()})
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
		writeJSON(w, http.StatusNotFound, generateResponse{Error: "model not found on this node: " + modelName})
		return
	}

	prompt := req.Prompt
	if req.System != "" {
		prompt = fmt.Sprintf("<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n", req.System, req.Prompt)
	}

	a.mu.Lock()
	a.activeTasks++
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.activeTasks--
		a.mu.Unlock()
	}()

	start := time.Now()
	output, err := eng.Generate(modelName, prompt)
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
		writeJSON(w, http.StatusInternalServerError, generateResponse{Model: modelName, DurationMs: duration, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, generateResponse{Model: modelName, Output: output, DurationMs: duration})
}

type chatRequest struct {
	Model       string               `json:"model"`
	Messages    []engine.ChatMessage `json:"messages"`
	Temperature float64              `json:"temperature,omitempty"`
}

type chatResponse struct {
	Model      string             `json:"model"`
	Message    engine.ChatMessage `json:"message"`
	DurationMs int64              `json:"duration_ms"`
	Error      string             `json:"error,omitempty"`
}

func (a *Agent) handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, chatResponse{Error: err.Error()})
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
		writeJSON(w, http.StatusNotFound, chatResponse{Error: "model not found on this node: " + modelName})
		return
	}

	a.mu.Lock()
	a.activeTasks++
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.activeTasks--
		a.mu.Unlock()
	}()

	start := time.Now()
	reply, err := eng.Chat(modelName, req.Messages)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		// Fallback to Generate if Chat returned an error
		var sb strings.Builder
		for _, m := range req.Messages {
			sb.WriteString(fmt.Sprintf("<|im_start|>%s\n%s<|im_end|>\n", m.Role, m.Content))
		}
		sb.WriteString("<|im_start|>assistant\n")
		var genErr error
		reply, genErr = eng.Generate(modelName, sb.String())
		if genErr != nil {
			writeJSON(w, http.StatusInternalServerError, chatResponse{Model: modelName, DurationMs: duration, Error: err.Error() + "; fallback error: " + genErr.Error()})
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

// --- Node-level Multi-Turn Dialogue Management ---

type createDialogueRequest struct {
	ID     string `json:"id,omitempty"`
	Model  string `json:"model,omitempty"`
	System string `json:"system,omitempty"`
}

type dialogueMessageRequest struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content"`
	Message string `json:"message,omitempty"`
}

type dialogueMessageResponse struct {
	DialogueID string               `json:"dialogue_id"`
	Model      string               `json:"model"`
	Turn       int                  `json:"turn"`
	Reply      string               `json:"reply"`
	Messages   []engine.ChatMessage `json:"messages"`
	DurationMs int64                `json:"duration_ms"`
	Error      string               `json:"error,omitempty"`
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
	model := req.Model
	if model == "" {
		model = a.defaultModelName()
	}
	if model == "" {
		enabled := a.enabledModels()
		if len(enabled) > 0 {
			model = enabled[0].Name
		}
	}

	dlg := &Dialogue{
		ID:        id,
		Model:     model,
		System:    req.System,
		Messages:  make([]engine.ChatMessage, 0),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if req.System != "" {
		dlg.Messages = append(dlg.Messages, engine.ChatMessage{
			Role:    "system",
			Content: req.System,
		})
	}

	a.dialoguesMu.Lock()
	a.dialogues[id] = dlg
	a.dialoguesMu.Unlock()

	writeJSON(w, http.StatusCreated, dlg)
}

func (a *Agent) handleDialogueList(w http.ResponseWriter, r *http.Request) {
	a.dialoguesMu.RLock()
	defer a.dialoguesMu.RUnlock()
	list := make([]*Dialogue, 0, len(a.dialogues))
	for _, d := range a.dialogues {
		list = append(list, d)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].UpdatedAt.After(list[j].UpdatedAt)
	})
	writeJSON(w, http.StatusOK, list)
}

func (a *Agent) handleDialogueGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a.dialoguesMu.RLock()
	dlg, ok := a.dialogues[id]
	a.dialoguesMu.RUnlock()
	if !ok {
		http.Error(w, "dialogue not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, dlg)
}

func (a *Agent) handleDialogueDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a.dialoguesMu.Lock()
	_, ok := a.dialogues[id]
	if ok {
		delete(a.dialogues, id)
	}
	a.dialoguesMu.Unlock()
	if !ok {
		http.Error(w, "dialogue not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleDialogueMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req dialogueMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, dialogueMessageResponse{Error: err.Error()})
		return
	}

	content := strings.TrimSpace(req.Content)
	if content == "" {
		content = strings.TrimSpace(req.Message)
	}
	if content == "" {
		writeJSON(w, http.StatusBadRequest, dialogueMessageResponse{Error: "content is required"})
		return
	}

	role := req.Role
	if role == "" {
		role = "user"
	}

	res, err := a.dialogueTurn(id, "", "", role, content, "dialogue_turn")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, dialogueMessageResponse{
			DialogueID: id,
			Model:      res.Model,
			DurationMs: res.DurationMs,
			Error:      err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type modelsPatchRequest struct {
	DisabledModels []string `json:"disabled_models"`
	DefaultModel   string   `json:"default_model"`
}

type nodeProperties struct {
	ID            string `json:"id"`
	AdvertiseAddr string `json:"advertise_addr"`
	DefaultModel  string `json:"default_model"`
	EngineName    string `json:"engine_name,omitempty"`
	EngineURL     string `json:"engine_url,omitempty"`
}

type engineInfo struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	CanPull bool   `json:"can_pull"`
}

func (a *Agent) handleProperties(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.mu.Lock()
		props := nodeProperties{ID: a.cfg.NodeID, AdvertiseAddr: a.cfg.AdvertiseAddr, DefaultModel: a.defaultModel}
		a.mu.Unlock()
		if len(a.engines) > 0 {
			props.EngineName = a.engines[0].Name()
			switch typed := a.engines[0].(type) {
			case *engine.Ollama:
				props.EngineURL = typed.URL()
			case *engine.VLLM:
				props.EngineURL = typed.URL()
			}
		}
		writeJSON(w, http.StatusOK, props)
		return
	}
	var req nodeProperties
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	if strings.TrimSpace(req.AdvertiseAddr) != "" {
		a.cfg.AdvertiseAddr = strings.TrimRight(req.AdvertiseAddr, "/")
	}
	if req.DefaultModel != "" {
		a.defaultModel = req.DefaultModel
	}
	props := nodeProperties{ID: a.cfg.NodeID, AdvertiseAddr: a.cfg.AdvertiseAddr, DefaultModel: a.defaultModel}
	a.mu.Unlock()
	if req.EngineName != "" && req.EngineURL != "" {
		if configured := engine.NewConfigured(strings.ToLower(req.EngineName), req.EngineURL); configured != nil {
			a.engines = []engine.Engine{configured}
			a.modelMu.Lock()
			a.reportedModels = nil
			a.reportedAt = time.Time{}
			a.modelMu.Unlock()
		}
	}
	writeJSON(w, http.StatusOK, props)
}

func (a *Agent) handleEngineInfo(w http.ResponseWriter, r *http.Request) {
	result := make([]engineInfo, 0, len(a.engines))
	for _, e := range a.engines {
		url := ""
		switch typed := e.(type) {
		case *engine.Ollama:
			url = typed.URL()
		case *engine.VLLM:
			url = typed.URL()
		}
		_, canPull := e.(engine.ModelManager)
		result = append(result, engineInfo{Name: e.Name(), URL: url, CanPull: canPull})
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *Agent) handleModelPull(w http.ResponseWriter, r *http.Request) {
	a.refreshDetectedEngines()
	var req struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Model) == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}
	for _, e := range a.engines {
		if manager, ok := e.(engine.ModelManager); ok {
			if err := manager.Pull(req.Model); err != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
				return
			}
			a.modelMu.Lock()
			a.reportedModels = nil
			a.reportedAt = time.Time{}
			a.modelMu.Unlock()
			writeJSON(w, http.StatusOK, map[string]any{"model": req.Model, "engine": e.Name()})
			return
		}
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
	if !a.enginesConfigured {
		a.engines = engine.DetectAll()
	}
	a.modelMu.Lock()
	a.reportedModels = nil
	a.reportedAt = time.Time{}
	a.modelMu.Unlock()
	writeJSON(w, http.StatusOK, a.allModelsWithState())
}

func (a *Agent) handleModelsPatch(w http.ResponseWriter, r *http.Request) {
	var req modelsPatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	disabled := make(map[string]bool, len(req.DisabledModels))
	for _, m := range req.DisabledModels {
		disabled[m] = true
	}
	if req.DefaultModel != "" && (disabled[req.DefaultModel] || a.engineFor(req.DefaultModel) == nil) {
		http.Error(w, "default model must be an installed enabled model", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	a.disabledModels = disabled
	if req.DefaultModel != "" {
		a.defaultModel = req.DefaultModel
	}
	a.mu.Unlock()
	a.modelMu.Lock()
	a.reportedModels = nil
	a.reportedAt = time.Time{}
	a.modelMu.Unlock()
	writeJSON(w, http.StatusOK, a.enabledModels())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// portSuffix extracts ":port" from a listen address like ":7451" or
// "0.0.0.0:7451" so it can be appended to a discovered/outbound IP.
func portSuffix(listenAddr string) string {
	if i := strings.LastIndex(listenAddr, ":"); i >= 0 {
		return listenAddr[i:]
	}
	return listenAddr
}

// Run starts the node's HTTP server and its background registration loop
// with the orchestrator. It blocks until the server stops.
func (a *Agent) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", a.handleStatus)
	mux.HandleFunc("GET /api/version", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": version.String()})
	})
	mux.HandleFunc("/execute", a.handleExecute)
	mux.HandleFunc("POST /generate", a.handleGenerate)
	mux.HandleFunc("POST /chat", a.handleChat)
	mux.HandleFunc("POST /dialogues", a.handleDialogueCreate)
	mux.HandleFunc("GET /dialogues", a.handleDialogueList)
	mux.HandleFunc("GET /dialogues/{id}", a.handleDialogueGet)
	mux.HandleFunc("DELETE /dialogues/{id}", a.handleDialogueDelete)
	mux.HandleFunc("POST /dialogues/{id}/messages", a.handleDialogueMessage)
	mux.HandleFunc("POST /dialogues/{id}/chat", a.handleDialogueMessage)

	mux.HandleFunc("/models", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			a.handleModelsPatch(w, r)
			return
		}
		a.handleModelsGet(w, r)
	})
	mux.HandleFunc("/models/all", a.handleModelsAllGet)
	mux.HandleFunc("POST /models/refresh", a.handleModelsRefresh)
	mux.HandleFunc("/models/pull", a.handleModelPull)
	mux.HandleFunc("/engine", a.handleEngineInfo)
	mux.HandleFunc("/properties", a.handleProperties)
	mux.HandleFunc("GET /peers", a.handlePeersGet)
	mux.HandleFunc("POST /submit", a.handleSubmit)
	mux.HandleFunc("GET /tasks", a.handleTasksList)
	mux.HandleFunc("GET /tasks/{id}", a.handleTaskGet)

	// Node-to-node LLM conversation: /peer/message answers an inbound
	// utterance from another node's model, /peer/converse drives a full
	// multi-round exchange between this node's LLM and a peer's LLM.
	mux.HandleFunc("POST /peer/message", a.handlePeerMessage)
	mux.HandleFunc("POST /peer/converse", a.handlePeerConverse)

	mux.HandleFunc("GET /memory/experiences", a.handleMemoryExperiences)
	mux.HandleFunc("GET /memory/insights", a.handleMemoryInsights)
	mux.HandleFunc("GET /memory/facts", a.handleMemoryFacts)
	mux.HandleFunc("POST /memory/facts", a.handleMemoryFacts)

	// Every node serves the exact same control-plane GUI as the
	// orchestrator, backed by /api/* aliases below, so any node shows the
	// same cluster information and can submit work on its members' behalf.
	mux.HandleFunc("GET /api/nodes", a.handleAPINodesGet)
	mux.HandleFunc("GET /api/nodes/{id}/models", a.handleAPINodeModelsGet)
	mux.HandleFunc("PATCH /api/nodes/{id}/models", a.handleAPINodeModelsPatch)
	mux.HandleFunc("GET /api/nodes/{id}/properties", a.handleAPINodeControl)
	mux.HandleFunc("PATCH /api/nodes/{id}/properties", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/nodes/{id}/engines", a.handleAPINodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/refresh", a.handleAPINodeControl)
	mux.HandleFunc("POST /api/nodes/{id}/models/pull", a.handleAPINodeControl)
	mux.HandleFunc("GET /api/discovery", a.handleAPIDiscovery)
	mux.HandleFunc("GET /api/events", a.events.ServeHTTP)
	mux.HandleFunc("POST /api/tasks", a.handleAPITasksPost)
	mux.HandleFunc("GET /api/tasks", a.handleAPITasksList)
	mux.HandleFunc("GET /api/tasks/{id}", a.handleAPITaskGet)
	mux.HandleFunc("POST /api/generate", a.handleGenerate)
	mux.HandleFunc("POST /api/chat", a.handleChat)
	mux.HandleFunc("POST /api/nodes/{id}/dialogues", a.handleAPINodeDialogue)
	mux.HandleFunc("GET /api/nodes/{id}/dialogues", a.handleAPINodeDialogue)
	mux.HandleFunc("/api/nodes/{id}/dialogues/", a.handleAPINodeDialogue)
	mux.HandleFunc("POST /api/dialogues", a.handleDialogueCreate)
	mux.HandleFunc("GET /api/dialogues", a.handleDialogueList)
	mux.HandleFunc("GET /api/dialogues/{id}", a.handleDialogueGet)
	mux.HandleFunc("DELETE /api/dialogues/{id}", a.handleDialogueDelete)
	mux.HandleFunc("POST /api/dialogues/{id}/messages", a.handleDialogueMessage)
	mux.HandleFunc("POST /api/peer/converse", a.handlePeerConverse)
	mux.HandleFunc("GET /api/memory/experiences", a.handleMemoryExperiences)
	mux.HandleFunc("GET /api/memory/sync", a.handleAPIMemorySync)
	mux.HandleFunc("GET /api/memory/insights", a.handleMemoryInsights)
	mux.HandleFunc("/api/agent/", a.handleAPIAgentProxy)

	staticFS, err := fs.Sub(web.StaticFiles, "static")
	if err != nil {
		log.Fatalf("nodeagent: embedded web assets missing: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	if a.cfg.DiscoveryEnabled {
		if a.discoveryListener == nil {
			a.discoveryListener = discovery.Listen(a.cfg.NodeID)
		}
		// Announce continuously: before joining this is how the cluster finds
		// us, and after joining it keeps us visible in every LAN panel.
		go discovery.Beacon(func() discovery.Announcement {
			return discovery.Announcement{Role: "node", ID: a.cfg.NodeID, HTTPAddr: a.cfg.AdvertiseAddr, Score: cluster.SelfScore(a.hw)}
		}, 5*time.Second, nil)
	}
	go a.resolveOrchestratorAndRegister()
	go a.peerPollLoop()
	go a.sseSubscribeLoop()
	go a.broadcastLoop()
	go a.syncMemoryToOrchestrator()

	log.Printf("node agent %q listening on %s (advertised as %s)", a.cfg.NodeID, a.cfg.ListenAddr, a.cfg.AdvertiseAddr)
	return http.ListenAndServe(a.cfg.ListenAddr, mux)
}

func (a *Agent) getOrchestratorAddr() string {
	a.orchMu.RLock()
	defer a.orchMu.RUnlock()
	return a.orchestratorAddr
}

func (a *Agent) setOrchestratorAddr(addr string) {
	a.orchMu.Lock()
	a.orchestratorAddr = addr
	a.orchMu.Unlock()
}

func (a *Agent) markJoined() {
	a.joinedMu.Lock()
	a.joined = true
	a.joinedMu.Unlock()
}

func (a *Agent) isJoined() bool {
	a.joinedMu.Lock()
	defer a.joinedMu.Unlock()
	return a.joined
}

// resolveOrchestratorAndRegister starts registration without blocking discovery.
func (a *Agent) resolveOrchestratorAndRegister() {
	if a.getOrchestratorAddr() == "" && a.discoveryListener == nil {
		log.Printf("nodeagent: discovery disabled and no orchestrator configured; registration will retry")
	}
	a.registrationLoop()
}

func (a *Agent) registrationLoop() {
	client := &http.Client{Timeout: 1500 * time.Millisecond}
	register := func() {
		candidates := a.orchestratorCandidates()
		if len(candidates) == 0 {
			if a.discoveryListener != nil {
				a.setOrchestratorAddr("")
			}
			a.joinedMu.Lock()
			a.joined = false
			a.joinedMu.Unlock()
			return
		}
		body, err := json.Marshal(a.status())
		if err != nil {
			return
		}
		for _, candidate := range candidates {
			resp, postErr := client.Post(candidate.address+"/api/nodes/register", "application/json", bytes.NewReader(body))
			if postErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				resp.Body.Close()
				previous := a.getOrchestratorAddr()
				a.setOrchestratorAddr(candidate.address)
				a.markJoined()
				if previous != candidate.address {
					log.Printf("nodeagent: election winner is %s at %s, switching orchestrator", candidate.id, candidate.address)
				}
				return
			}
			if resp != nil {
				log.Printf("nodeagent: registration candidate %s rejected or unreachable: %v (status %s)", candidate.address, postErr, resp.Status)
				resp.Body.Close()
			} else if postErr != nil {
				log.Printf("nodeagent: registration candidate %s unreachable: %v", candidate.address, postErr)
			}
		}
		a.joinedMu.Lock()
		a.joined = false
		a.joinedMu.Unlock()
		if a.discoveryListener != nil {
			a.setOrchestratorAddr("")
		}
		log.Printf("nodeagent: no elected orchestrator accepted registration; discovery remains active")
	}

	// Give the orchestrator (often starting concurrently in the same
	// process for --role=both) a moment to bind its listener before the
	// first registration attempt, to avoid a noisy connection-refused log.
	time.Sleep(1500 * time.Millisecond)
	register()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		register()
	}
}

type orchestratorCandidate struct {
	id      string
	address string
	score   float64
	members int
	elected bool
}

func (a *Agent) orchestratorCandidates() []orchestratorCandidate {
	byAddress := make(map[string]orchestratorCandidate)
	current := a.getOrchestratorAddr()
	if current != "" {
		host := strings.TrimPrefix(strings.TrimPrefix(current, "http://"), "https://")
		if colon := strings.LastIndex(host, ":"); colon >= 0 {
			host = host[:colon]
		}
		// A configured/local address is only a fallback until discovery proves
		// which orchestrator currently owns the cluster.
		byAddress[current] = orchestratorCandidate{id: "orchestrator-" + host, address: current, score: -1}
	}
	if a.discoveryListener != nil {
		for _, peer := range a.discoveryListener.Snapshot() {
			if peer.Role != "orchestrator" || peer.HTTPAddr == "" {
				continue
			}
			candidate := orchestratorCandidate{id: peer.ID, address: peer.HTTPAddr, score: peer.Score, members: peer.Members, elected: peer.Elected}
			if existing, ok := byAddress[peer.HTTPAddr]; !ok || candidate.score > existing.score || (candidate.score == existing.score && candidate.id < existing.id) {
				byAddress[peer.HTTPAddr] = candidate
			}
		}
	}
	candidates := make([]orchestratorCandidate, 0, len(byAddress))
	for _, candidate := range byAddress {
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].elected != candidates[j].elected {
			return candidates[i].elected
		}
		if candidates[i].members != candidates[j].members {
			return candidates[i].members > candidates[j].members
		}
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].id < candidates[j].id
	})
	return candidates
}

// peerPollLoop keeps a local cache of every node in the cluster (fetched
// from the orchestrator's registry) so this node can display the status of
// every other node and dispatch work directly to them, without every
// request needing to go through the orchestrator.
func (a *Agent) peerPollLoop() {
	poll := func() {
		addr := a.getOrchestratorAddr()
		if addr == "" {
			return
		}
		resp, err := a.peerClient.Get(addr + "/api/nodes")
		if err != nil {
			log.Printf("nodeagent: peer poll failed: %v", err)
			return
		}
		defer resp.Body.Close()
		var nodes []cluster.NodeStatus
		if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
			return
		}
		a.mu.Lock()
		a.peers = nodes
		a.mu.Unlock()
	}

	// Give the orchestrator (often starting concurrently in the same
	// process for --role=both) a moment to bind its listener before the
	// first poll attempt, to avoid a noisy connection-refused log.
	time.Sleep(1500 * time.Millisecond)
	poll()
	// This is a slow fallback only; sseSubscribeLoop below is the primary,
	// real-time path and normally keeps a.peers fresh well within a second.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		poll()
	}
}

// sseSubscribeLoop keeps a live connection to the orchestrator's event
// stream so this node's peer cache (and therefore its dashboard) updates in
// real time as other nodes come online, instead of waiting on the slow
// polling fallback above. It reconnects automatically if the stream drops.
func (a *Agent) sseSubscribeLoop() {
	time.Sleep(1500 * time.Millisecond)
	for {
		addr := a.getOrchestratorAddr()
		if addr == "" {
			time.Sleep(1 * time.Second)
			continue
		}
		if err := a.subscribeOnce(addr); err != nil {
			log.Printf("nodeagent: event stream to orchestrator lost, retrying: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func (a *Agent) subscribeOnce(addr string) error {
	req, err := http.NewRequest(http.MethodGet, addr+"/api/events", nil)
	if err != nil {
		return err
	}
	resp, err := a.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		if a.getOrchestratorAddr() != addr {
			return errors.New("orchestrator changed, resubscribing")
		}
		line := scanner.Text()
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var snap struct {
			Nodes       []cluster.NodeStatus `json:"nodes"`
			MemberCount int                  `json:"member_count"`
		}
		if err := json.Unmarshal([]byte(data), &snap); err != nil {
			continue
		}
		a.mu.Lock()
		// The list is authoritative: an ejected node is removed here as soon
		// as the orchestrator broadcasts the post-ejection snapshot.
		a.peers = append([]cluster.NodeStatus(nil), snap.Nodes...)
		a.mu.Unlock()
	}
	return scanner.Err()
}

// broadcastLoop pushes this node's view of the cluster (peers, its own
// task tracking, and LAN discovery) to every locally-connected dashboard in
// real time, so the GUI never has to poll.
func (a *Agent) broadcastLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		a.publishSnapshot()
	}
}

func (a *Agent) publishSnapshot() {
	var disc []discovery.Peer
	if a.discoveryListener != nil {
		disc = a.discoveryListener.Snapshot()
	}
	snap := struct {
		Nodes        []cluster.NodeStatus `json:"nodes"`
		Tasks        []*taskmgr.Task      `json:"tasks"`
		Discovery    []discovery.Peer     `json:"discovery"`
		BuildVersion string               `json:"build_version"`
		MemberCount  int                  `json:"member_count"`
	}{
		Nodes:        a.Peers(),
		Tasks:        a.currentTasks(),
		Discovery:    disc,
		BuildVersion: version.String(),
	}
	snap.MemberCount = len(snap.Nodes)
	data, err := json.Marshal(snap)
	if err != nil {
		return
	}
	a.events.Broadcast(data)
}

// currentTasks returns this node's task list if it is the current cluster
// coordinator (the most capable live node), otherwise it fetches the
// authoritative list from that coordinator.
func (a *Agent) currentTasks() []*taskmgr.Task {
	if a.isMostCapable() {
		return a.tasks.All()
	}
	resp, err := a.peerClient.Get(a.mostCapableNode().Address + "/tasks")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var tasks []*taskmgr.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil
	}
	return tasks
}

// Peers returns the last known status of every node in the cluster.
func (a *Agent) Peers() []cluster.NodeStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]cluster.NodeStatus, len(a.peers))
	copy(out, a.peers)
	return out
}

func (a *Agent) handlePeersGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.Peers())
}

type submitRequest struct {
	Description string `json:"description"`
}

// handleSubmit lets this node accept a task directly and share the work
// across every other node it knows about (via the peer cache), dispatching
// each subtask straight to the chosen peer's /execute endpoint rather than
// routing everything back through the orchestrator.
func (a *Agent) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	subtasks := scheduler.Decompose(req.Description)
	if len(subtasks) == 0 {
		http.Error(w, "description produced no subtasks", http.StatusBadRequest)
		return
	}

	assignments, err := scheduler.AssignWithExperience(subtasks, a.Peers(), a.experience())
	if err != nil {
		if errors.Is(err, scheduler.ErrNoHealthyNodes) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	task := a.tasks.Create(req.Description, assignments)
	for _, asg := range assignments {
		go a.dispatchToPeer(task.ID, asg)
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (a *Agent) dispatchToPeer(taskID string, asg scheduler.Assignment) {
	a.tasks.StartSubtask(taskID, asg.Subtask.ID)
	available := false
	for _, peer := range a.Peers() {
		if peer.ID == asg.NodeID && peer.Healthy && peer.Address == asg.Address {
			available = true
			break
		}
	}
	if !available && asg.NodeID != a.cfg.NodeID {
		a.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New("node was ejected before dispatch"))
		return
	}
	body, _ := json.Marshal(executeRequest{
		SubtaskID: asg.Subtask.ID,
		Model:     asg.Model,
		Prompt:    asg.Subtask.Description,
	})

	resp, err := a.dispatchClient.Post(asg.Address+"/execute", "application/json", bytes.NewReader(body))
	if err != nil {
		a.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		a.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New("node returned "+resp.Status+": "+string(errBody)))
		return
	}

	var out executeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		a.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", err)
		return
	}
	if out.Error != "" {
		a.tasks.CompleteSubtask(taskID, asg.Subtask.ID, "", errors.New(out.Error))
		return
	}
	a.tasks.CompleteSubtask(taskID, asg.Subtask.ID, out.Output, nil)
}

func (a *Agent) handleTasksList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.tasks.All())
}

func (a *Agent) handleTaskGet(w http.ResponseWriter, r *http.Request) {
	t, ok := a.tasks.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// mostCapableNode returns the cluster's elected leader. The orchestrator's
// registry enforces the election and marks the winner on every snapshot, so
// a peer with the leader flag is authoritative; local scoring is only the
// bootstrap fallback before the first snapshot arrives.
func (a *Agent) mostCapableNode() cluster.NodeStatus {
	self := cluster.NodeStatus{
		ID:           a.cfg.NodeID,
		Address:      a.cfg.AdvertiseAddr,
		Hardware:     a.hw,
		FastScore:    fastScore(a.hw),
		DefaultModel: a.defaultModelName(),
		ActiveTasks:  a.activeTaskCount(),
		Healthy:      true,
	}
	best := self
	for _, n := range a.Peers() {
		if !n.Healthy {
			continue
		}
		if n.Leader {
			return n
		}
		if n.ID == self.ID {
			continue
		}
		if cluster.CapabilityScore(n) > cluster.CapabilityScore(best) ||
			(cluster.CapabilityScore(n) == cluster.CapabilityScore(best) && n.ID < best.ID) {
			best = n
		}
	}
	return best
}

func (a *Agent) fastestNode() cluster.NodeStatus {
	self := cluster.NodeStatus{
		ID:           a.cfg.NodeID,
		Address:      a.cfg.AdvertiseAddr,
		Hardware:     a.hw,
		FastScore:    fastScore(a.hw),
		DefaultModel: a.defaultModelName(),
		ActiveTasks:  a.activeTaskCount(),
		Healthy:      true,
	}
	best := self
	for _, n := range a.Peers() {
		if !n.Healthy || n.ID == self.ID {
			continue
		}
		if n.FastScore > best.FastScore ||
			(n.FastScore == best.FastScore && n.ID < best.ID) {
			best = n
		}
	}
	return best
}

func (a *Agent) isMostCapable() bool { return a.mostCapableNode().ID == a.cfg.NodeID }

func (a *Agent) activeTaskCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.activeTasks
}

func (a *Agent) isFastest() bool { return a.fastestNode().ID == a.cfg.NodeID }

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

// handleAPITasksPost accepts a submission from this node's GUI. The most
// capable live node is the coordinator for the cluster, so all submissions are
// routed there automatically instead of whichever node happens to be first.
func (a *Agent) handleAPITasksPost(w http.ResponseWriter, r *http.Request) {
	if a.isMostCapable() {
		a.handleSubmit(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/submit")
}

func (a *Agent) handleAPITasksList(w http.ResponseWriter, r *http.Request) {
	if a.isMostCapable() {
		a.handleTasksList(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks")
}

func (a *Agent) handleAPITaskGet(w http.ResponseWriter, r *http.Request) {
	if a.isMostCapable() {
		a.handleTaskGet(w, r)
		return
	}
	a.proxy(w, r, a.mostCapableNode().Address, "/tasks/"+r.PathValue("id"))
}

func (a *Agent) handleAPIMemorySync(w http.ResponseWriter, r *http.Request) {
	addr := a.getOrchestratorAddr()
	if addr == "" {
		http.Error(w, "orchestrator unavailable", http.StatusServiceUnavailable)
		return
	}
	a.proxy(w, r, addr, "/api/memory/sync")
}

// handleAPINodeModelsGet/Patch proxy straight to whichever peer owns the
// requested node ID, mirroring the orchestrator's node-model-config proxy
// so the same GUI's "configure models" panel works from any node.
func (a *Agent) handleAPINodeModelsGet(w http.ResponseWriter, r *http.Request) {
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	a.proxy(w, r, addr, "/models/all")
}

func (a *Agent) handleAPINodeModelsPatch(w http.ResponseWriter, r *http.Request) {
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	a.proxy(w, r, addr, "/models")
}

func (a *Agent) handleAPINodeControl(w http.ResponseWriter, r *http.Request) {
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
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
	a.proxy(w, r, addr, path)
}

func (a *Agent) handleAPINodeDialogue(w http.ResponseWriter, r *http.Request) {
	addr, ok := a.peerAddress(r.PathValue("id"))
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
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

func (a *Agent) peerAddress(id string) (string, bool) {
	if id == a.cfg.NodeID {
		return a.cfg.AdvertiseAddr, true
	}
	for _, n := range a.Peers() {
		if n.ID == id {
			return n.Address, true
		}
	}
	return "", false
}

func (a *Agent) handleAPIAgentProxy(w http.ResponseWriter, r *http.Request) {
	orch := a.getOrchestratorAddr()
	if orch == "" {
		if a.isMostCapable() {
			http.Error(w, "orchestrator unavailable", http.StatusServiceUnavailable)
			return
		}
		orch = a.mostCapableNode().Address
	}
	if orch == "" {
		http.Error(w, "no orchestrator or coordinator found", http.StatusServiceUnavailable)
		return
	}
	destPath := r.URL.Path
	if r.URL.RawQuery != "" {
		destPath += "?" + r.URL.RawQuery
	}
	a.proxy(w, r, orch, destPath)
}

// proxy forwards the incoming request's method/body to addr+path and
// copies the response straight back to the caller.
func (a *Agent) proxy(w http.ResponseWriter, r *http.Request, addr, path string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := http.NewRequest(r.Method, addr+path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, v := range r.Header {
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := a.peerClient
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") || r.URL.Query().Get("stream") == "true" {
		client = a.dispatchClient
	}

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "peer unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		for _, val := range v {
			w.Header().Add(k, val)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if flusher, ok := w.(http.Flusher); ok {
		buf := make([]byte, 4096)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
				flusher.Flush()
			}
			if readErr != nil {
				break
			}
		}
		return
	}

	_, _ = io.Copy(w, resp.Body)
}
