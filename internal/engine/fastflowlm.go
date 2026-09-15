package engine

// FastFlowLM is an OpenAI-compatible FastFlowLM server. It shares the vLLM
// wire protocol while retaining its own engine identity in node status.
type FastFlowLM struct {
	*VLLM
}

func NewFastFlowLM(baseURL string) *FastFlowLM {
	return &FastFlowLM{VLLM: NewVLLM(baseURL)}
}

func (f *FastFlowLM) Name() string { return "fastflowlm" }

func (f *FastFlowLM) ListModels() ([]Model, error) {
	models, err := f.VLLM.ListModels()
	if err != nil {
		return nil, err
	}
	for i := range models {
		models[i].Engine = f.Name()
	}
	return models, nil
}
