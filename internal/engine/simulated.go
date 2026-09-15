package engine

import "fmt"

// Simulated is a no-runtime-required fallback engine used when no real LLM
// server (e.g. Ollama) is detected on the host. It lets the cluster be
// exercised end-to-end (registration, scheduling, dispatch) without
// requiring model weights to be installed on every test machine.
type Simulated struct{}

func NewSimulated() *Simulated { return &Simulated{} }

func (s *Simulated) Name() string { return "simulated" }

func (s *Simulated) Available() bool { return true }

func (s *Simulated) ListModels() ([]Model, error) {
	return []Model{
		{Name: "simulated-general", Engine: "simulated", Tags: []string{"chat", "general"}},
		{Name: "simulated-code", Engine: "simulated", Tags: []string{"chat", "code", "fast"}},
	}, nil
}

func (s *Simulated) Generate(model, prompt string) (string, error) {
	return fmt.Sprintf("[simulated response from %s] processed prompt of %d chars", model, len(prompt)), nil
}

func (s *Simulated) Chat(model string, messages []ChatMessage) (string, error) {
	lastMsg := ""
	if len(messages) > 0 {
		lastMsg = messages[len(messages)-1].Content
	}
	return fmt.Sprintf("[simulated chat turn %d from %s]: acknowledged '%s'", len(messages), model, lastMsg), nil
}
