package nodeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
	"net/http"
	"strings"
	"time"
)

func generateWithOptions(ctx context.Context, eng engine.Engine, model, prompt string, options engine.GenerationOptions) (string, error) {
	if engineWithOptions, ok := eng.(engine.EngineWithOptions); ok {
		return engineWithOptions.GenerateWithOptions(ctx, model, prompt, options)
	}
	return eng.Generate(model, prompt)
}

func (a *Agent) streamGenerate(w http.ResponseWriter, r *http.Request, eng engine.Engine, model, prompt string, options engine.GenerationOptions) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	start := time.Now()
	var output strings.Builder
	writeDelta := func(delta string) error {
		output.WriteString(delta)
		data, err := json.Marshal(map[string]string{"output": delta})
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}

	var err error
	if streaming, ok := eng.(engine.StreamingEngine); ok {
		err = streaming.GenerateStream(r.Context(), model, prompt, options, writeDelta)
	} else {
		var text string
		text, err = generateWithOptions(r.Context(), eng, model, prompt, options)
		if err == nil {
			err = writeDelta(text)
		}
	}
	duration := time.Since(start).Milliseconds()
	a.remember(memory.Experience{
		Kind: "generate", Description: truncateText(prompt, 400), Prompt: prompt,
		Output: output.String(), Model: model, Success: err == nil,
		Error: errText(err), DurationMs: duration,
	})
	if err != nil {
		data, marshalErr := json.Marshal(map[string]string{"error": err.Error()})
		if marshalErr == nil {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		}
	}
	if flusher != nil {
		flusher.Flush()
	}
}

func chatWithContext(ctx context.Context, eng engine.Engine, model string, messages []engine.ChatMessage) (string, error) {
	if withChat, ok := eng.(engine.EngineWithChatContext); ok {
		return withChat.ChatWithContext(ctx, model, messages)
	}
	return eng.Chat(model, messages)
}
