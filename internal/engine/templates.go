package engine

import "strings"

const (
	TemplateLlamaCPPOpenVINONPU = "llamacpp-openvino-npu"
	TemplateLlamaCPPHexagonNPU  = "llamacpp-hexagon-npu"
	TemplateLlamaCPPXDNA2NPU    = "llamacpp-xdna2-npu"
	TemplateFastFlowLMXDNA2     = "fastflowlm-xdna2"
)

type TemplateEnvironment struct {
	Name  string
	Value string
}

// ManagedEngineTemplate is a typed, conservative profile for a documented
// engine/backend combination. Process settings are fixed values, not shell
// fragments or operator-provided environment strings.
type ManagedEngineTemplate struct {
	ID                      string
	EngineName              string
	ManagedEngineKind       string
	LlamaServerDevice       string
	LlamaServerParallelSeqs int
	LlamaServerNoKVOffload  bool
	Environment             []TemplateEnvironment
	Capabilities            Capabilities
	DocumentationURL        string
}

var managedEngineTemplates = []ManagedEngineTemplate{
	{
		ID:                      TemplateLlamaCPPOpenVINONPU,
		EngineName:              TemplateLlamaCPPOpenVINONPU,
		ManagedEngineKind:       "llama-server",
		LlamaServerParallelSeqs: 1,
		Environment: []TemplateEnvironment{
			{Name: "GGML_OPENVINO_DEVICE", Value: "NPU"},
		},
		Capabilities: Capabilities{
			Devices:             []string{"npu"},
			Backends:            []string{"openvino"},
			NPUVendors:          []string{"intel-npu"},
			Quantizations:       []string{"q4_0"},
			PrimaryQuantization: "q4_0",
			Modalities:          []string{"text"},
			MaxParallelSeqs:     1,
			RequiresContextCap:  true,
		},
		DocumentationURL: "https://github.com/ggml-org/llama.cpp/blob/master/docs/backend/OPENVINO.md",
	},
	{
		ID:                TemplateLlamaCPPHexagonNPU,
		EngineName:        TemplateLlamaCPPHexagonNPU,
		ManagedEngineKind: "llama-server",
		LlamaServerDevice: "HTP0",
		Capabilities: Capabilities{
			Devices:       []string{"npu"},
			Backends:      []string{"hexagon"},
			NPUVendors:    []string{"qualcomm-npu"},
			Quantizations: []string{"q4_0"},
		},
		DocumentationURL: "https://github.com/ggml-org/llama.cpp/blob/master/docs/backend/snapdragon/README.md",
	},
	{
		ID:                      TemplateLlamaCPPXDNA2NPU,
		EngineName:              TemplateLlamaCPPXDNA2NPU,
		ManagedEngineKind:       "llama-server",
		LlamaServerDevice:       "XDNA0",
		LlamaServerParallelSeqs: 1,
		LlamaServerNoKVOffload:  true,
		Capabilities: Capabilities{
			Devices:             []string{"npu"},
			Backends:            []string{"xdna"},
			NPUVendors:          []string{"amd-npu"},
			Quantizations:       []string{"q4_0", "q4nx"},
			PrimaryQuantization: "q4_0",
			Modalities:          []string{"text"},
			MaxParallelSeqs:     1,
			RequiresContextCap:  true,
		},
		DocumentationURL: "https://github.com/dbhaigh/llama.cpp",
	},
	{
		ID:         TemplateFastFlowLMXDNA2,
		EngineName: TemplateFastFlowLMXDNA2,
		Capabilities: Capabilities{
			Devices:          []string{"npu"},
			Backends:         []string{"xdna"},
			NPUVendors:       []string{"amd-npu"},
			MaxContextLength: 262144,
		},
		DocumentationURL: "https://github.com/ROCm/FastFlowLM",
	},
}

// ManagedEngineTemplates returns a copy of the built-in typed profiles.
func ManagedEngineTemplates() []ManagedEngineTemplate {
	templates := make([]ManagedEngineTemplate, len(managedEngineTemplates))
	for i, template := range managedEngineTemplates {
		templates[i] = cloneManagedEngineTemplate(template)
	}
	return templates
}

// ManagedEngineTemplateFor returns a named profile and a defensive copy.
func ManagedEngineTemplateFor(id string) (ManagedEngineTemplate, bool) {
	id = strings.TrimSpace(id)
	for _, template := range managedEngineTemplates {
		if strings.EqualFold(template.ID, id) {
			return cloneManagedEngineTemplate(template), true
		}
	}
	return ManagedEngineTemplate{}, false
}

func cloneManagedEngineTemplate(template ManagedEngineTemplate) ManagedEngineTemplate {
	template.Environment = append([]TemplateEnvironment(nil), template.Environment...)
	template.Capabilities = CloneCapabilities(template.Capabilities)
	return template
}

func applyTemplateConstraints(observed, template Capabilities) Capabilities {
	if len(template.Devices) > 0 {
		observed.Devices = append([]string(nil), template.Devices...)
	}
	if len(template.Backends) > 0 {
		observed.Backends = append([]string(nil), template.Backends...)
	}
	if len(template.NPUVendors) > 0 {
		observed.NPUVendors = append([]string(nil), template.NPUVendors...)
	}
	if len(template.Quantizations) > 0 {
		observed.Quantizations = append([]string(nil), template.Quantizations...)
	}
	if template.PrimaryQuantization != "" {
		observed.PrimaryQuantization = template.PrimaryQuantization
	}
	if len(template.Modalities) > 0 {
		observed.Modalities = append([]string(nil), template.Modalities...)
	}
	if template.MaxParallelSeqs > 0 {
		observed.MaxParallelSeqs = template.MaxParallelSeqs
	}
	if template.MaxContextLength > 0 {
		observed.MaxContextLength = template.MaxContextLength
	}
	observed.RequiresContextCap = observed.RequiresContextCap || template.RequiresContextCap
	return observed
}
