package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// LlamaCPP is an OpenAI-compatible llama.cpp server. It shares the vLLM wire
// protocol while retaining its own identity in node status and model records.
type LlamaCPP struct {
	*VLLM
	capabilitiesMu    sync.RWMutex
	capabilities      Capabilities
	capabilitiesKnown bool
}

func NewLlamaCPP(baseURL string) *LlamaCPP {
	return &LlamaCPP{VLLM: newOpenAICompatibleEngine(baseURL, "llamacpp")}
}

func (l *LlamaCPP) Name() string { return "llamacpp" }

func (l *LlamaCPP) Capabilities() Capabilities {
	l.capabilitiesMu.RLock()
	if l.capabilitiesKnown {
		capabilities := CloneCapabilities(l.capabilities)
		l.capabilitiesMu.RUnlock()
		return capabilities
	}
	l.capabilitiesMu.RUnlock()
	return l.VLLM.Capabilities()
}

func (l *LlamaCPP) ProbeCapabilities(ctx context.Context) (Capabilities, error) {
	l.capabilitiesMu.RLock()
	known := l.capabilitiesKnown
	l.capabilitiesMu.RUnlock()
	if known {
		return l.Capabilities(), nil
	}
	properties, _, err := l.fetchProperties(ctx)
	if err != nil {
		return Capabilities{}, err
	}
	l.updateCapabilities(properties)
	return l.Capabilities(), nil
}

type llamaCPPProperties struct {
	ContextLength             int      `json:"context_length"`
	NCtx                      int      `json:"n_ctx"`
	ModelSizeGB               float64  `json:"model_size_gb"`
	SizeGB                    float64  `json:"size_gb"`
	ModelSize                 float64  `json:"model_size"`
	ModelSizeBytes            int64    `json:"model_size_bytes"`
	SizeBytes                 int64    `json:"size_bytes"`
	Device                    string   `json:"device"`
	Devices                   []string `json:"devices"`
	NGPULayers                *int     `json:"n_gpu_layers"`
	TotalSlots                int      `json:"total_slots"`
	MaxParallelSeqs           int      `json:"max_parallel_seqs"`
	Backends                  []string `json:"backends"`
	Quantizations             []string `json:"quantizations"`
	RequiresContextCap        bool     `json:"requires_context_cap"`
	SupportsSharding          []string `json:"supports_sharding"`
	HomogeneousOnly           bool     `json:"homogeneous_only"`
	OS                        []string `json:"os"`
	ValidatedRPC              bool     `json:"validated_rpc"`
	RPCServers                []string `json:"rpc_servers"`
	DefaultGenerationSettings struct {
		ContextLength int `json:"context_length"`
		NCtx          int `json:"n_ctx"`
	} `json:"default_generation_settings"`
}

func (l *LlamaCPP) ListModels() ([]Model, error) {
	models, err := l.VLLM.ListModels()
	if err != nil || len(models) == 0 {
		return models, err
	}
	properties, found, err := l.fetchProperties(context.Background())
	if err != nil {
		return nil, err
	}
	if !found {
		l.updateCapabilities(properties)
		return models, nil
	}

	contextLength := properties.DefaultGenerationSettings.NCtx
	if contextLength == 0 {
		contextLength = properties.DefaultGenerationSettings.ContextLength
	}
	if contextLength == 0 {
		contextLength = properties.NCtx
	}
	if contextLength == 0 {
		contextLength = properties.ContextLength
	}
	sizeGB := properties.ModelSizeGB
	if sizeGB == 0 {
		sizeGB = properties.SizeGB
	}
	if sizeGB == 0 {
		sizeBytes := properties.ModelSizeBytes
		if sizeBytes == 0 {
			sizeBytes = properties.SizeBytes
		}
		if sizeBytes == 0 {
			sizeBytes = int64(properties.ModelSize)
		}
		sizeGB = float64(sizeBytes) / (1024 * 1024 * 1024)
	}
	for index := range models {
		if contextLength > 0 {
			models[index].ContextLength = contextLength
		}
		if sizeGB > 0 {
			models[index].SizeGB = sizeGB
		}
		models[index].Device = llamaCPPModelDevice(properties)
	}
	l.updateCapabilities(properties)
	return models, nil
}

func (l *LlamaCPP) fetchProperties(ctx context.Context) (llamaCPPProperties, bool, error) {
	req, err := l.VLLM.newRequest(ctx, http.MethodGet, l.baseURL+"/props", nil, "")
	if err != nil {
		return llamaCPPProperties{}, false, fmt.Errorf("llamacpp build properties request: %w", err)
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return llamaCPPProperties{}, false, fmt.Errorf("llamacpp get properties: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed ||
		resp.StatusCode == http.StatusNoContent {
		resp.Body.Close()
		return llamaCPPProperties{}, false, nil
	}
	if resp.StatusCode >= 300 {
		return llamaCPPProperties{}, false, l.VLLM.responseError("get properties", resp)
	}
	defer resp.Body.Close()
	var properties llamaCPPProperties
	if err := json.NewDecoder(resp.Body).Decode(&properties); err != nil {
		return llamaCPPProperties{}, false, fmt.Errorf("llamacpp decode properties: %w", err)
	}
	return properties, true, nil
}

func llamaCPPModelDevice(properties llamaCPPProperties) string {
	if device := NormalizeDeviceKind(properties.Device); device != "" {
		return device
	}
	if properties.NGPULayers != nil {
		if *properties.NGPULayers == 0 {
			return "cpu"
		}
		return "gpu"
	}
	return ""
}

func (l *LlamaCPP) updateCapabilities(properties llamaCPPProperties) {
	capabilities := l.VLLM.Capabilities()
	if device := llamaCPPModelDevice(properties); device != "" {
		capabilities.Devices = uniqueCapabilityValues(capabilities.Devices, []string{device})
	}
	maxParallelSeqs := properties.MaxParallelSeqs
	if maxParallelSeqs == 0 {
		maxParallelSeqs = properties.TotalSlots
	}
	capabilities = MergeCapabilities(capabilities, Capabilities{
		Devices: properties.Devices, Backends: properties.Backends, Quantizations: properties.Quantizations,
		MaxParallelSeqs: maxParallelSeqs, RequiresContextCap: properties.RequiresContextCap,
		SupportsSharding: properties.SupportsSharding,
		HomogeneousOnly:  properties.HomogeneousOnly,
		OS:               properties.OS, ValidatedRPC: properties.ValidatedRPC,
	})
	if len(properties.RPCServers) > 0 {
		capabilities.SupportsSharding = uniqueCapabilityValues(
			capabilities.SupportsSharding, []string{"llamacpp-rpc"},
		)
	}
	l.capabilitiesMu.Lock()
	l.capabilities = capabilities
	l.capabilitiesKnown = true
	l.capabilitiesMu.Unlock()
}
