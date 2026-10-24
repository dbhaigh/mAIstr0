package engine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestManagedEngineTemplatesDeclareDocumentedCapabilities(t *testing.T) {
	tests := []struct {
		id      string
		backend string
		vendor  string
	}{
		{id: TemplateLlamaCPPOpenVINONPU, backend: "openvino", vendor: "intel-npu"},
		{id: TemplateLlamaCPPHexagonNPU, backend: "hexagon", vendor: "qualcomm-npu"},
		{id: TemplateLlamaCPPXDNA2NPU, backend: "xdna", vendor: "amd-npu"},
		{id: TemplateFastFlowLMXDNA2, backend: "xdna", vendor: "amd-npu"},
	}
	for _, test := range tests {
		t.Run(test.id, func(t *testing.T) {
			template, ok := ManagedEngineTemplateFor(test.id)
			if !ok {
				t.Fatal("template not found")
			}
			if template.EngineName != test.id ||
				!reflect.DeepEqual(template.Capabilities.Devices, []string{"npu"}) ||
				!reflect.DeepEqual(template.Capabilities.Backends, []string{test.backend}) ||
				!reflect.DeepEqual(template.Capabilities.NPUVendors, []string{test.vendor}) ||
				template.DocumentationURL == "" {
				t.Fatalf("template = %+v", template)
			}
			configured := NewConfigured(test.id, "http://127.0.0.1:8080")
			if configured == nil || configured.Name() != test.id ||
				!SupportsProtocol(configured.Name(), "openai") {
				t.Fatalf("configured engine = %v, want OpenAI-compatible profile %q", configured, test.id)
			}
			if got := CapabilitiesFor(configured); !reflect.DeepEqual(got, template.Capabilities) {
				t.Fatalf("configured capabilities = %+v, want %+v", got, template.Capabilities)
			}
		})
	}

	openVINO, _ := ManagedEngineTemplateFor(TemplateLlamaCPPOpenVINONPU)
	if openVINO.Capabilities.MaxParallelSeqs != 1 ||
		!openVINO.Capabilities.RequiresContextCap ||
		openVINO.Capabilities.PrimaryQuantization != "q4_0" ||
		!reflect.DeepEqual(openVINO.Capabilities.Modalities, []string{"text"}) {
		t.Fatalf("OpenVINO NPU profile omits documented restrictions: %+v", openVINO.Capabilities)
	}
	hexagon, _ := ManagedEngineTemplateFor(TemplateLlamaCPPHexagonNPU)
	if hexagon.LlamaServerDevice != "HTP0" || hexagon.Capabilities.MaxParallelSeqs != 0 ||
		hexagon.Capabilities.RequiresContextCap {
		t.Fatalf("Hexagon profile contains undocumented launch/capability limits: %+v", hexagon)
	}
	xdna, _ := ManagedEngineTemplateFor(TemplateLlamaCPPXDNA2NPU)
	if xdna.LlamaServerDevice != "XDNA0" || !xdna.LlamaServerNoKVOffload ||
		xdna.Capabilities.MaxParallelSeqs != 1 || !xdna.Capabilities.RequiresContextCap ||
		xdna.Capabilities.PrimaryQuantization != "q4_0" ||
		!reflect.DeepEqual(xdna.Capabilities.Quantizations, []string{"q4_0", "q4nx"}) ||
		len(xdna.Capabilities.SupportsSharding) != 0 {
		t.Fatalf("XDNA profile omits its launch settings or conservative limits: %+v", xdna)
	}
}

func TestManagedEngineTemplateCatalogReturnsDefensiveCopies(t *testing.T) {
	templates := ManagedEngineTemplates()
	templates[0].Capabilities.Devices[0] = "cpu"
	templates[0].Environment[0].Value = "CPU"
	template, ok := ManagedEngineTemplateFor(TemplateLlamaCPPOpenVINONPU)
	if !ok || template.Capabilities.Devices[0] != "npu" || template.Environment[0].Value != "NPU" {
		t.Fatalf("mutating returned template changed catalog: %+v", template)
	}
}

func TestFastFlowLMXDNAProfileReportsItsDocumentedNPUDevice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `{"data":[{"id":"fastflow-model","size_gb":2,"max_model_len":8192,"quantization":"q4_0"}]}`)
	}))
	defer server.Close()

	backend := NewConfigured(TemplateFastFlowLMXDNA2, server.URL)
	models, err := backend.ListModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Device != "npu" ||
		models[0].SizeGB != 2 || models[0].ContextLength != 8192 ||
		models[0].Quantization != "q4_0" {
		t.Fatalf("FastFlowLM XDNA model metadata = %+v", models)
	}
}

func TestLlamaCPPXDNAProfileReportsMixedExecutionConservatively(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			_, _ = fmt.Fprint(w, `{"data":[{"id":"qwen-q4nx","device":"npu","quantization":"q4nx"}]}`)
		case "/props":
			_, _ = fmt.Fprint(w, `{"n_ctx":4096,"n_gpu_layers":32,"backends":["cpu"],"quantizations":["q8_0"],"rpc_servers":["127.0.0.1:50052"]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend := NewConfigured(TemplateLlamaCPPXDNA2NPU, server.URL)
	models, err := backend.ListModels()
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Quantization != "q4nx" || models[0].Device != "" {
		t.Fatalf("XDNA model metadata = %+v, want source quantization and unspecified mixed device", models)
	}
	capabilities := CapabilitiesFor(backend)
	if !reflect.DeepEqual(capabilities.Devices, []string{"npu"}) ||
		!reflect.DeepEqual(capabilities.Backends, []string{"xdna"}) ||
		!reflect.DeepEqual(capabilities.NPUVendors, []string{"amd-npu"}) ||
		!reflect.DeepEqual(capabilities.Quantizations, []string{"q4_0", "q4nx"}) ||
		capabilities.PrimaryQuantization != "q4_0" ||
		!reflect.DeepEqual(capabilities.Modalities, []string{"text"}) ||
		capabilities.MaxParallelSeqs != 1 || !capabilities.RequiresContextCap ||
		capabilities.MaxContextLength != 4096 ||
		!reflect.DeepEqual(capabilities.SupportsSharding, []string{"llamacpp-rpc"}) {
		t.Fatalf("XDNA template capabilities = %+v", capabilities)
	}
}

func TestOpenVINOTemplateKeepsDocumentedLimitsAgainstServerSelfReport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			_, _ = fmt.Fprint(w, `{"data":[{"id":"model"}]}`)
		case "/props":
			_, _ = fmt.Fprint(w, `{"n_ctx":8192,"total_slots":8,"backends":["cpu"],"quantizations":["q5_k"],"primary_quantization":"q5_k","modalities":["audio"]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend := NewConfigured(TemplateLlamaCPPOpenVINONPU, server.URL)
	if _, err := backend.ListModels(); err != nil {
		t.Fatal(err)
	}
	capabilities := CapabilitiesFor(backend)
	if !reflect.DeepEqual(capabilities.Devices, []string{"npu"}) ||
		!reflect.DeepEqual(capabilities.Backends, []string{"openvino"}) ||
		!reflect.DeepEqual(capabilities.NPUVendors, []string{"intel-npu"}) ||
		!reflect.DeepEqual(capabilities.Quantizations, []string{"q4_0"}) ||
		capabilities.PrimaryQuantization != "q4_0" ||
		!reflect.DeepEqual(capabilities.Modalities, []string{"text"}) ||
		capabilities.MaxParallelSeqs != 1 || !capabilities.RequiresContextCap ||
		capabilities.MaxContextLength != 8192 {
		t.Fatalf("OpenVINO template capabilities were widened by server metadata: %+v", capabilities)
	}
}
