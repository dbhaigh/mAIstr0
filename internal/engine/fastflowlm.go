package engine

// FastFlowLM is an OpenAI-compatible FastFlowLM server. It shares the vLLM
// wire protocol while retaining its own engine identity in node status.
type FastFlowLM struct {
	*VLLM
}

func NewFastFlowLM(baseURL string) *FastFlowLM {
	return &FastFlowLM{VLLM: newOpenAICompatibleEngine(baseURL, "fastflowlm")}
}

func (f *FastFlowLM) ListModels() ([]Model, error) {
	models, err := f.VLLM.ListModels()
	if err != nil || f.Name() != TemplateFastFlowLMXDNA2 {
		return models, err
	}
	for index := range models {
		if models[index].Device == "" {
			models[index].Device = "npu"
		}
	}
	return models, nil
}
