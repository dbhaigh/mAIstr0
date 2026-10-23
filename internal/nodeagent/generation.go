package nodeagent

import (
	"context"
	"encoding/json"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/logging"
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
	stream := httpx.NewStreamWriter(w)
	start := time.Now()
	var output strings.Builder
	writeDelta := func(delta string) error {
		output.WriteString(delta)
		data, err := json.Marshal(map[string]string{"output": delta})
		if err != nil {
			return err
		}
		if err := stream.WriteSSEData(data); err != nil {
			return err
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
			if writeErr := stream.WriteSSEData(data); writeErr != nil {
				logging.Errorf("nodeagent", "nodeagent: write generation stream error: %v", writeErr)
			}
		}
	}
	stream.Flush()
}

func chatWithContext(ctx context.Context, eng engine.Engine, model string, messages []engine.ChatMessage, options engine.GenerationOptions) (string, error) {
	if withOptions, ok := eng.(engine.EngineWithChatOptions); ok {
		return withOptions.ChatWithOptions(ctx, model, messages, options)
	}
	if withChat, ok := eng.(engine.EngineWithChatContext); ok {
		return withChat.ChatWithContext(ctx, model, messages)
	}
	return eng.Chat(model, messages)
}
