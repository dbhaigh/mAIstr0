package nodeagent

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/httpx"
)

func (a *Agent) SetRuntimeSettings(path string, settings config.RuntimeSettings) {
	a.runtimeSettingsMu.Lock()
	a.runtimeSettingsPath = path
	a.runtimeSettings = settings
	a.runtimeSettingsMu.Unlock()
	required := settings.RequirePairingVerification != nil && *settings.RequirePairingVerification
	a.orchestratorLink.setPairingVerificationRequired(required)
	a.orchestratorLink.applyPairingPolicy = a.SetPairingVerificationRequired
}

func (a *Agent) PairingVerificationRequired() bool {
	a.runtimeSettingsMu.RLock()
	defer a.runtimeSettingsMu.RUnlock()
	return a.runtimeSettings.RequirePairingVerification != nil &&
		*a.runtimeSettings.RequirePairingVerification
}

func (a *Agent) SetPairingVerificationRequired(required bool) error {
	a.runtimeSettingsMu.RLock()
	settings := a.runtimeSettings
	path := a.runtimeSettingsPath
	a.runtimeSettingsMu.RUnlock()
	if path == "" {
		return errors.New("runtime settings are not persisted on this process")
	}
	settings.RequirePairingVerification = &required
	if err := config.SaveRuntimeSettings(path, settings); err != nil {
		return fmt.Errorf("save cluster pairing policy: %w", err)
	}
	a.runtimeSettingsMu.Lock()
	a.runtimeSettings.RequirePairingVerification = &required
	a.runtimeSettingsMu.Unlock()
	a.orchestratorLink.setPairingVerificationRequired(required)
	return nil
}

func (a *Agent) handleRuntimeSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		a.runtimeSettingsMu.RLock()
		settings := a.runtimeSettings
		a.runtimeSettingsMu.RUnlock()
		writeJSON(w, http.StatusOK, settings)
		return
	}
	settings, err := httpx.DecodeJSON[config.RuntimeSettings](w, r, httpx.ControlJSONBodyBytes)
	if err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	if err := settings.Validate(); err != nil {
		writeError(w, apperr.Invalid(err))
		return
	}
	a.runtimeSettingsMu.RLock()
	path := a.runtimeSettingsPath
	currentPolicy := a.runtimeSettings.RequirePairingVerification
	a.runtimeSettingsMu.RUnlock()
	settings.RequirePairingVerification = currentPolicy
	if path == "" {
		writeError(w, apperr.UnavailableError(errors.New("runtime settings are not persisted on this process")))
		return
	}
	if err := config.SaveRuntimeSettings(path, settings); err != nil {
		writeError(w, apperr.InternalError(err))
		return
	}
	a.runtimeSettingsMu.Lock()
	a.runtimeSettings = settings
	a.runtimeSettingsMu.Unlock()
	writeJSON(w, http.StatusOK, settings)
}

func (a *Agent) runtimeSettingsMutation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			next.ServeHTTP(w, r)
			return
		}
		a.requireManagedEngineOrchestrator(next).ServeHTTP(w, r)
	})
}
