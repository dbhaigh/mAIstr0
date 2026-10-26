package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/maistr0/maistr0/internal/closeutil"
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

func (l *LlamaCPP) Name() string { return l.VLLM.Name() }

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
	NPUVendors                []string `json:"npu_vendors"`
	NGPULayers                *int     `json:"n_gpu_layers"`
	TotalSlots                int      `json:"total_slots"`
	MaxParallelSeqs           int      `json:"max_parallel_seqs"`
	MaxContextLength          int      `json:"max_context_length"`
	Backends                  []string `json:"backends"`
	Quantizations             []string `json:"quantizations"`
	PrimaryQuantization       string   `json:"primary_quantization"`
	Modalities                []string `json:"modalities"`
	Quantization              string   `json:"quantization"`
	RequiresContextCap        bool     `json:"requires_context_cap"`
	SupportsSharding          []string `json:"supports_sharding"`
	HomogeneousOnly           bool     `json:"homogeneous_only"`
	OS                        []string `json:"os"`
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
		for index := range models {
			models[index].Device = l.modelDevice(properties)
		}
		return models, nil
	}

	contextLength := llamaCPPContextLength(properties)
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
	quantization := strings.ToLower(strings.TrimSpace(properties.Quantization))
	for index := range models {
		if contextLength > 0 {
			models[index].ContextLength = contextLength
		}
		if sizeGB > 0 {
			models[index].SizeGB = sizeGB
		}
		if quantization != "" {
			models[index].Quantization = quantization
		}
		models[index].Device = l.modelDevice(properties)
	}
	l.updateCapabilities(properties)
	return models, nil
}

func (l *LlamaCPP) modelDevice(properties llamaCPPProperties) string {
	if l.Name() == TemplateLlamaCPPXDNA2NPU {
		return ""
	}
	return llamaCPPModelDevice(properties)
}

func (l *LlamaCPP) fetchProperties(ctx context.Context) (llamaCPPProperties, bool, error) {
	req, err := l.VLLM.newRequest(ctx, http.MethodGet, l.baseURL+"/props", nil, "")
	if err != nil {
		return llamaCPPProperties{}, false, fmt.Errorf("%s build properties request: %w", l.Name(), err)
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return llamaCPPProperties{}, false, fmt.Errorf("%s get properties: %w", l.Name(), err)
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed ||
		resp.StatusCode == http.StatusNoContent {
		_ = resp.Body.Close()
		return llamaCPPProperties{}, false, nil
	}
	if resp.StatusCode >= 300 {
		return llamaCPPProperties{}, false, l.VLLM.responseError("get properties", resp)
	}
	defer closeutil.Discard(resp.Body)
	var properties llamaCPPProperties
	if err := json.NewDecoder(resp.Body).Decode(&properties); err != nil {
		return llamaCPPProperties{}, false, fmt.Errorf("%s decode properties: %w", l.Name(), err)
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

func llamaCPPContextLength(properties llamaCPPProperties) int {
	for _, length := range []int{
		properties.DefaultGenerationSettings.NCtx,
		properties.DefaultGenerationSettings.ContextLength,
		properties.NCtx,
		properties.ContextLength,
		properties.MaxContextLength,
	} {
		if length > 0 {
			return length
		}
	}
	return 0
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
		Devices: properties.Devices, Backends: properties.Backends,
		NPUVendors: properties.NPUVendors, Quantizations: properties.Quantizations,
		PrimaryQuantization: properties.PrimaryQuantization, Modalities: properties.Modalities,
		MaxParallelSeqs: maxParallelSeqs, RequiresContextCap: properties.RequiresContextCap,
		MaxContextLength: llamaCPPContextLength(properties),
		SupportsSharding: properties.SupportsSharding,
		HomogeneousOnly:  properties.HomogeneousOnly,
		OS:               properties.OS,
	})
	if template, ok := ManagedEngineTemplateFor(l.Name()); ok {
		capabilities = applyTemplateConstraints(capabilities, template.Capabilities)
	}
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
