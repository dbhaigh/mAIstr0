package engine

// LlamaCPP is an OpenAI-compatible llama.cpp server. It shares the vLLM wire
// protocol while retaining its own identity in node status and model records.
type LlamaCPP struct {
	*VLLM
}

func NewLlamaCPP(baseURL string) *LlamaCPP {
	return &LlamaCPP{VLLM: newOpenAICompatibleEngine(baseURL, "llamacpp")}
}

func (l *LlamaCPP) Name() string { return "llamacpp" }
