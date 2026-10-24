package engine

import (
	"context"
	"strings"
)

// Capabilities describes verified engine support and runtime limits.
type Capabilities struct {
	Devices             []string `json:"devices,omitempty"`
	Backends            []string `json:"backends,omitempty"`
	NPUVendors          []string `json:"npu_vendors,omitempty"`
	Quantizations       []string `json:"quantizations,omitempty"`
	PrimaryQuantization string   `json:"primary_quantization,omitempty"`
	Modalities          []string `json:"modalities,omitempty"`
	MaxParallelSeqs     int      `json:"max_parallel_seqs,omitempty"`
	MaxContextLength    int      `json:"max_context_length,omitempty"`
	RequiresContextCap  bool     `json:"requires_context_cap,omitempty"`
	SupportsSharding    []string `json:"supports_sharding,omitempty"`
	HomogeneousOnly     bool     `json:"homogeneous_only,omitempty"`
	OS                  []string `json:"os,omitempty"`
	ValidatedRPC        bool     `json:"validated_rpc,omitempty"`
}

// CapabilityProvider exposes the engine's conservative baseline capabilities.
type CapabilityProvider interface {
	Capabilities() Capabilities
}

// CapabilityProber reports capabilities that a live engine endpoint confirms.
type CapabilityProber interface {
	ProbeCapabilities(context.Context) (Capabilities, error)
}

// CapabilitiesFor returns a defensive copy of the engine's known capabilities.
func CapabilitiesFor(backend Engine) Capabilities {
	provider, ok := backend.(CapabilityProvider)
	if !ok {
		return Capabilities{}
	}
	return CloneCapabilities(provider.Capabilities())
}

// CloneCapabilities returns a defensive copy of every capability slice.
func CloneCapabilities(capabilities Capabilities) Capabilities {
	capabilities.Devices = append([]string(nil), capabilities.Devices...)
	capabilities.Backends = append([]string(nil), capabilities.Backends...)
	capabilities.NPUVendors = append([]string(nil), capabilities.NPUVendors...)
	capabilities.Quantizations = append([]string(nil), capabilities.Quantizations...)
	capabilities.Modalities = append([]string(nil), capabilities.Modalities...)
	capabilities.SupportsSharding = append([]string(nil), capabilities.SupportsSharding...)
	capabilities.OS = append([]string(nil), capabilities.OS...)
	return capabilities
}

// MergeCapabilities combines baseline capabilities with runtime-confirmed data.
func MergeCapabilities(base, observed Capabilities) Capabilities {
	merged := Capabilities{
		Devices:             uniqueDeviceKinds(base.Devices, observed.Devices),
		Backends:            uniqueCapabilityValues(base.Backends, observed.Backends),
		NPUVendors:          uniqueCapabilityValues(base.NPUVendors, observed.NPUVendors),
		Quantizations:       uniqueCapabilityValues(base.Quantizations, observed.Quantizations),
		PrimaryQuantization: base.PrimaryQuantization,
		Modalities:          uniqueCapabilityValues(base.Modalities, observed.Modalities),
		MaxParallelSeqs:     base.MaxParallelSeqs,
		MaxContextLength:    base.MaxContextLength,
		RequiresContextCap:  base.RequiresContextCap || observed.RequiresContextCap,
		SupportsSharding:    uniqueCapabilityValues(base.SupportsSharding, observed.SupportsSharding),
		HomogeneousOnly:     base.HomogeneousOnly || observed.HomogeneousOnly,
		OS:                  uniqueCapabilityValues(base.OS, observed.OS),
		ValidatedRPC:        base.ValidatedRPC || observed.ValidatedRPC,
	}
	if observed.MaxParallelSeqs > 0 {
		merged.MaxParallelSeqs = observed.MaxParallelSeqs
	}
	if observed.MaxContextLength > 0 {
		merged.MaxContextLength = observed.MaxContextLength
	}
	if merged.PrimaryQuantization == "" {
		merged.PrimaryQuantization = strings.TrimSpace(observed.PrimaryQuantization)
	}
	return merged
}

func uniqueDeviceKinds(values ...[]string) []string {
	var devices []string
	seen := make(map[string]struct{})
	for _, group := range values {
		for _, value := range group {
			kind := NormalizeDeviceKind(value)
			if kind == "" {
				continue
			}
			if _, ok := seen[kind]; ok {
				continue
			}
			seen[kind] = struct{}{}
			devices = append(devices, kind)
		}
	}
	return devices
}

// SupportsDeviceKind reports whether an engine capability includes a device
// kind, accepting recognized backend aliases as device identifiers.
func SupportsDeviceKind(devices []string, kind string) bool {
	for _, device := range devices {
		if NormalizeDeviceKind(device) == kind {
			return true
		}
	}
	return false
}

// BackendsOverlap accepts unknown backend inventories but rejects a known
// mismatch between engine and device backends.
func BackendsOverlap(engineBackends, deviceBackends []string) bool {
	if len(engineBackends) == 0 || len(deviceBackends) == 0 {
		return true
	}
	for _, engineBackend := range engineBackends {
		for _, deviceBackend := range deviceBackends {
			if strings.EqualFold(strings.TrimSpace(engineBackend), strings.TrimSpace(deviceBackend)) {
				return true
			}
		}
	}
	return false
}

// SupportsNPUVendor reports whether an engine's vendor restriction matches a
// node NPU. Empty restrictions accept any vendor; an unknown node vendor does
// not match a vendor-specific engine.
func SupportsNPUVendor(engineVendors []string, nodeVendor string) bool {
	if len(engineVendors) == 0 {
		return true
	}
	nodeVendor = normalizeCapabilityVendor(nodeVendor)
	if nodeVendor == "" {
		return false
	}
	for _, vendor := range engineVendors {
		if normalizeCapabilityVendor(vendor) == nodeVendor {
			return true
		}
	}
	return false
}

func normalizeCapabilityVendor(vendor string) string {
	return strings.ToLower(strings.TrimSpace(vendor))
}

func uniqueCapabilityValues(values ...[]string) []string {
	var unique []string
	seen := make(map[string]struct{})
	for _, group := range values {
		for _, value := range group {
			value = strings.TrimSpace(value)
			key := strings.ToLower(value)
			if value == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			unique = append(unique, value)
		}
	}
	return unique
}

// NormalizeDeviceKind accepts only an explicit device kind or a well-known
// accelerator identifier; ambiguous backend names remain unknown.
func NormalizeDeviceKind(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, kind := range []string{"cpu", "gpu", "npu"} {
		if value == kind || strings.HasPrefix(value, kind+":") {
			return kind
		}
	}
	switch value {
	case "cuda", "rocm", "metal", "vulkan", "sycl":
		return "gpu"
	default:
		return ""
	}
}
