package engine

// FastFlowLM is an OpenAI-compatible FastFlowLM server. It shares the vLLM
// wire protocol while retaining its own engine identity in node status.
type FastFlowLM struct {
	*VLLM
}

func NewFastFlowLM(baseURL string) *FastFlowLM {
	return &FastFlowLM{VLLM: newOpenAICompatibleEngine(baseURL, "fastflowlm")}
}

func (f *FastFlowLM) Name() string { return "fastflowlm" }

func (f *FastFlowLM) ListModels() ([]Model, error) {
	return f.VLLM.ListModels()
}
