package nodeagent

import (
	"context"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"sort"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/memory"
)

func (c *dialogueComponent) create(id, model, system string) *Dialogue {
	if model == "" {
		model = c.models.defaultModelName()
	}
	if model == "" {
		if enabled := c.models.enabledModels(); len(enabled) > 0 {
			model = enabled[0].Name
		}
	}
	dlg := &Dialogue{
		ID: id, Model: model, System: system,
		Messages:  make([]engine.ChatMessage, 0),
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if system != "" {
		dlg.Messages = append(dlg.Messages, engine.ChatMessage{Role: "system", Content: system})
	}
	c.dialoguesMu.Lock()
	c.dialogues[id] = dlg
	snapshot := cloneDialogue(dlg)
	c.dialoguesMu.Unlock()
	return snapshot
}

func (c *dialogueComponent) list() []*Dialogue {
	c.dialoguesMu.RLock()
	defer c.dialoguesMu.RUnlock()
	list := make([]*Dialogue, 0, len(c.dialogues))
	for _, dialogue := range c.dialogues {
		list = append(list, cloneDialogue(dialogue))
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].UpdatedAt.After(list[j].UpdatedAt)
	})
	return list
}

func (c *dialogueComponent) get(id string) (*Dialogue, bool) {
	c.dialoguesMu.RLock()
	defer c.dialoguesMu.RUnlock()
	dialogue, ok := c.dialogues[id]
	if !ok {
		return nil, false
	}
	return cloneDialogue(dialogue), true
}

func cloneDialogue(dialogue *Dialogue) *Dialogue {
	if dialogue == nil {
		return nil
	}
	cloned := *dialogue
	cloned.Messages = append([]engine.ChatMessage(nil), dialogue.Messages...)
	return &cloned
}

func (c *dialogueComponent) delete(id string) bool {
	c.dialoguesMu.Lock()
	defer c.dialoguesMu.Unlock()
	if _, ok := c.dialogues[id]; !ok {
		return false
	}
	delete(c.dialogues, id)
	return true
}

func (c *dialogueComponent) interrupt(id string) bool {
	c.dialogueCancelsMu.Lock()
	cancel, ok := c.dialogueCancels[id]
	c.dialogueCancelsMu.Unlock()
	if ok && cancel != nil {
		cancel()
		return true
	}
	return false
}

func (c *dialogueComponent) turn(id, model, system, role, content, kind string) (dialogueMessageResponse, error) {
	return c.turnContext(context.Background(), id, model, system, role, content, kind)
}

func (c *dialogueComponent) turnContext(ctx context.Context, id, model, system, role, content, kind string) (dialogueMessageResponse, error) {
	if role == "" {
		role = "user"
	}
	if kind == "" {
		kind = "dialogue_turn"
	}

	turnCtx, turnCancel := context.WithCancel(ctx)
	c.dialogueCancelsMu.Lock()
	c.dialogueCancels[id] = turnCancel
	c.dialogueCancelsMu.Unlock()
	defer func() {
		c.dialogueCancelsMu.Lock()
		delete(c.dialogueCancels, id)
		c.dialogueCancelsMu.Unlock()
		turnCancel()
	}()

	c.dialoguesMu.Lock()
	dlg, ok := c.dialogues[id]
	if !ok {
		resolved := model
		if resolved == "" {
			resolved = c.models.defaultModelName()
		}
		if resolved == "" {
			if enabled := c.models.enabledModels(); len(enabled) > 0 {
				resolved = enabled[0].Name
			}
		}
		dlg = &Dialogue{
			ID: id, Model: resolved, System: system,
			Messages:  make([]engine.ChatMessage, 0),
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
		if system != "" {
			dlg.Messages = append(dlg.Messages, engine.ChatMessage{Role: "system", Content: system})
		}
		c.dialogues[id] = dlg
	}
	if model != "" {
		dlg.Model = model
	}
	dlg.Messages = append(dlg.Messages, engine.ChatMessage{Role: role, Content: content})
	dlg.UpdatedAt = time.Now()
	modelName := dlg.Model
	history := append([]engine.ChatMessage(nil), dlg.Messages...)
	c.dialoguesMu.Unlock()

	if brief := c.memoryBrief(content); brief != "" {
		history = append([]engine.ChatMessage{{Role: "system", Content: brief}}, history...)
	}

	eng := c.models.engineFor(modelName)
	if eng == nil {
		return dialogueMessageResponse{DialogueID: id, Model: modelName},
			fmt.Errorf("model not found on node %s: %s", c.nodeID, modelName)
	}

	release := c.executor.beginActiveTask()
	defer release()

	start := time.Now()
	reply, err := chatWithContext(turnCtx, eng, modelName, history, engine.GenerationOptions{})
	if err != nil && turnCtx.Err() == nil {
		reply, err = generateWithOptions(turnCtx, eng, modelName, flattenChatML(history), engine.GenerationOptions{})
	}
	duration := time.Since(start).Milliseconds()

	if turnCtx.Err() != nil {
		c.remember(memory.Experience{
			Kind: kind, Description: truncateText(content, 400), Prompt: content,
			Model: modelName, Success: false, Error: "interrupted", DurationMs: duration,
			Tags: []string{"dialogue", id, "interrupted"},
		})
		return dialogueMessageResponse{DialogueID: id, Model: modelName, DurationMs: duration, Error: "dialogue turn interrupted"}, turnCtx.Err()
	}

	c.remember(memory.Experience{
		Kind: kind, Description: truncateText(content, 400), Prompt: content,
		Output: reply, Model: modelName, Success: err == nil,
		Error: errText(err), DurationMs: duration, Tags: []string{"dialogue", id},
	})

	if err != nil {
		return dialogueMessageResponse{DialogueID: id, Model: modelName, DurationMs: duration}, err
	}

	c.dialoguesMu.Lock()
	dlg.Messages = append(dlg.Messages, engine.ChatMessage{Role: "assistant", Content: reply})
	dlg.UpdatedAt = time.Now()
	all := append([]engine.ChatMessage(nil), dlg.Messages...)
	c.dialoguesMu.Unlock()

	if c.mem != nil {
		participants := []string{"node:" + c.nodeID}
		turns := []memory.Turn{
			{Speaker: role, Content: content, At: time.Now()},
			{
				Speaker: "node:" + c.nodeID, NodeID: c.nodeID, Model: modelName,
				Content: reply, DurationMs: duration, At: time.Now(),
			},
		}
		if appender, ok := c.mem.(memory.DialogueBatchAppender); ok {
			if err := appender.AppendDialogueTurns(id, participants, "", turns); err != nil {
				logging.Errorf("nodeagent", "nodeagent: persist dialogue %s: %v", id, err)
			}
		} else {
			for _, turn := range turns {
				if err := c.mem.AppendDialogueTurn(id, participants, "", turn); err != nil {
					logging.Errorf("nodeagent", "nodeagent: persist dialogue %s turn: %v", id, err)
				}
			}
		}
	}

	return dialogueMessageResponse{
		DialogueID: id, Model: modelName, Turn: len(all) / 2,
		Reply: reply, Messages: all, DurationMs: duration,
	}, nil
}

func (c *dialogueComponent) memoryBrief(query string) string {
	if c.mem == nil {
		return ""
	}
	return c.mem.ContextBrief(query, "", 4)
}

func (a *Agent) InterruptDialogue(id string) bool {
	return a.dialogueComponent.interrupt(id)
}

func (a *Agent) dialogueTurn(id, model, system, role, content, kind string) (dialogueMessageResponse, error) {
	return a.dialogueComponent.turn(id, model, system, role, content, kind)
}

func (a *Agent) dialogueTurnContext(ctx context.Context, id, model, system, role, content, kind string) (dialogueMessageResponse, error) {
	return a.dialogueComponent.turnContext(ctx, id, model, system, role, content, kind)
}
