// Package engine abstracts local LLM server backends (e.g. Ollama) so the
// node agent can discover installed models and dispatch generation requests
// to whichever engine is actually available on the host.
package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Model describes an LLM available on a node, tagged with the task
// categories it is well suited for so the scheduler can match work to it.
type Model struct {
	Name   string   `json:"name"`
	Engine string   `json:"engine"`
	Tags   []string `json:"tags"`
	SizeGB float64  `json:"size_gb,omitempty"`
}

// ChatMessage represents a single message in a multi-turn conversation.
type ChatMessage struct {
	Role    string `json:"role"` // "system", "user", "assistant", "tool"
	Content string `json:"content"`
}

// Engine is a local LLM serving backend (Ollama, llama.cpp server, ...).
type Engine interface {
	Name() string
	// Available reports whether this engine is reachable on the host.
	Available() bool
	// ListModels returns the models currently served by this engine.
	ListModels() ([]Model, error)
	// Generate runs a prompt against the given model and returns the response text.
	Generate(model, prompt string) (string, error)
	// Chat runs a multi-turn conversation against the given model and returns the assistant response.
	Chat(model string, messages []ChatMessage) (string, error)
}

// ModelManager is implemented by engines that can download models.
type ModelManager interface {
	Engine
	Pull(model string) error
}

// DetectAll probes the host for every known engine implementation and returns
// only engines that respond. A real node must never advertise a simulated
// model as locally servable.
func DetectAll() []Engine {
	candidates := []Engine{
		NewOllama("http://127.0.0.1:11434"),
		NewFastFlowLM("http://127.0.0.1:8000"),
	}
	var found []Engine
	for _, e := range candidates {
		if e.Available() {
			found = append(found, e)
		}
	}
	return found
}

func NewConfigured(name, baseURL string) Engine {
	switch name {
	case "ollama":
		return NewOllama(baseURL)
	case "vllm":
		return NewVLLM(baseURL)
	case "fastflowlm":
		return NewFastFlowLM(baseURL)
	default:
		return nil
	}
}

// httpClient is shared with a short timeout since these are local calls.
var httpClient = &http.Client{Timeout: 5 * time.Second}

func postJSON(url string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("request to %s failed: %s", url, resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
