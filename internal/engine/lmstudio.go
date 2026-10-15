package engine

// LMStudio is an OpenAI-compatible LM Studio server. It uses the same API
// protocol as vLLM while retaining its own identity in node status.
type LMStudio struct {
	*VLLM
}

func NewLMStudio(baseURL string) *LMStudio {
	return &LMStudio{VLLM: newOpenAICompatibleEngine(baseURL, "lmstudio")}
}

func (l *LMStudio) Name() string { return "lmstudio" }

func (l *LMStudio) ListModels() ([]Model, error) {
	return l.VLLM.ListModels()
}
