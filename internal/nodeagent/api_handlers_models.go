package nodeagent

import (
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
	"net/http"
	"sort"
	"strings"
)

func (a *Agent) handleProperties(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.refreshDetectedEngines()
		writeJSON(w, http.StatusOK, a.propertiesSnapshot())
		return
	}
	req, err := httpx.DecodeJSON[nodeProperties](
		w, r, httpx.ControlJSONBodyBytes,
	)
	if err != nil {
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
	a.mu.Unlock()
	a.setOrchestratorAddr(strings.TrimRight(strings.TrimSpace(req.OrchestratorAddr), "/"))
	var disabled map[string]bool
	if req.DisabledModels != nil {
		disabled = make(map[string]bool, len(req.DisabledModels))
		for _, model := range req.DisabledModels {
			disabled[strings.TrimSpace(model)] = true
		}
	}
	if disabled != nil || req.DefaultModel != "" {
		a.engineComponent.configureModels(disabled, req.DefaultModel)
	}
	if req.EngineName != "" && req.EngineURL != "" {
		unchanged := false
		for _, configured := range a.engineSnapshot() {
			if strings.EqualFold(configured.Name(), req.EngineName) && publicEngineURL(configured) == req.EngineURL {
				unchanged = true
				break
			}
		}
		if !unchanged {
			if configured := engine.NewConfigured(strings.ToLower(req.EngineName), req.EngineURL); configured != nil {
				a.engineComponent.replaceConfiguredEngine(configured)
			}
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
		DiscoveryEnabled: a.cfg.DiscoveryEnabled,
		AutoOpenBrowser:  a.cfg.AutoOpenBrowser,
		TrayMode:         a.cfg.TrayMode,
		MemoryPath:       a.cfg.MemoryPath,
	}
	a.mu.Unlock()

	props.DisabledModels, props.DefaultModel = a.engineComponent.modelSelectionSnapshot()
	sort.Strings(props.DisabledModels)
	props.OrchestratorAddr = a.getOrchestratorAddr()

	if engines := a.engineSnapshot(); len(engines) > 0 {
		props.EngineName = engines[0].Name()
		props.EngineURL = publicEngineURL(engines[0])
	}
	return props
}

func engineURL(e engine.Engine) string {
	if provider, ok := e.(interface{ URL() string }); ok {
		return provider.URL()
	}
	return ""
}

func publicEngineURL(e engine.Engine) string {
	endpoint := engineURL(e)
	if endpoint == "" {
		return ""
	}
	redacted, err := httpx.RedactedURL(endpoint)
	if err != nil {
		logging.Errorf("nodeagent", "nodeagent: cannot expose invalid engine endpoint: %v", err)
		return ""
	}
	return redacted
}

func (a *Agent) handleEngineInfo(w http.ResponseWriter, r *http.Request) {
	statusByName := make(map[string]engine.Status)
	for _, status := range a.reportedEngineList() {
		statusByName[strings.ToLower(status.Name)] = status
	}
	engines := a.engineSnapshot()
	result := make([]engineInfo, 0, len(engines))
	for _, e := range engines {
		url := publicEngineURL(e)
		_, canPull := e.(engine.ModelManager)
		status, ok := statusByName[strings.ToLower(e.Name())]
		if !ok || status.InventoryState == "" {
			status.InventoryState = engine.ModelInventoryUnknown
		}
		result = append(result, engineInfo{
			Name: e.Name(), URL: url, CanPull: canPull,
			Ready: status.Ready, Protocols: status.Protocols,
			InventoryState: status.InventoryState, ModelCount: status.ModelCount,
			LastCheckedAt: status.LastCheckedAt, LastSuccessfulAt: status.LastSuccessfulAt,
			LastError: status.LastError,
		})
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *Agent) handleModelPull(w http.ResponseWriter, r *http.Request) {
	a.refreshDetectedEngines()
	type modelPullRequest struct {
		Model  string `json:"model"`
		Engine string `json:"engine,omitempty"`
	}
	req, err := httpx.DecodeJSON[modelPullRequest](w, r, 1<<20)
	if err != nil {
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
			var err error
			if contextManager, ok := manager.(engine.ContextModelManager); ok {
				err = contextManager.PullContext(r.Context(), req.Model)
			} else {
				err = manager.Pull(req.Model)
			}
			if err != nil {
				writeJSONError(w, apperr.UpstreamError(err), map[string]any{"error": err.Error()})
				return
			}
			a.engineComponent.refreshReportedModels()
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
	writeJSON(w, http.StatusOK, a.allModelsWithState())
}

func (a *Agent) handleModelsPatch(w http.ResponseWriter, r *http.Request) {
	req, err := httpx.DecodeJSON[modelsPatchRequest](
		w, r, httpx.ControlJSONBodyBytes,
	)
	if err != nil {
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
