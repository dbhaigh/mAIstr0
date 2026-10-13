package engine

import (
	"context"
	"fmt"
)

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
	return s.GenerateWithOptions(context.Background(), model, prompt, GenerationOptions{})
}

func (s *Simulated) GenerateWithOptions(ctx context.Context, model, prompt string, _ GenerationOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return fmt.Sprintf("[simulated response from %s] processed prompt of %d chars", model, len(prompt)), nil
}

func (s *Simulated) Chat(model string, messages []ChatMessage) (string, error) {
	return s.ChatWithContext(context.Background(), model, messages)
}

func (s *Simulated) ChatWithContext(ctx context.Context, model string, messages []ChatMessage) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	lastMsg := ""
	if len(messages) > 0 {
		lastMsg = messages[len(messages)-1].Content
	}
	return fmt.Sprintf("[simulated chat turn %d from %s]: acknowledged '%s'", len(messages), model, lastMsg), nil
}
