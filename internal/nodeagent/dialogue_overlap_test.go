package nodeagent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
)

// gatedChatEngine hands each chat call to the test, which decides when it
// completes; calls also end when their context is canceled.
type gatedChatEngine struct {
	calls chan chan string
}

func (g *gatedChatEngine) Name() string    { return "gated" }
func (g *gatedChatEngine) Available() bool { return true }
func (g *gatedChatEngine) ListModels() ([]engine.Model, error) {
	return []engine.Model{{Name: "gated"}}, nil
}
func (g *gatedChatEngine) Generate(string, string) (string, error) { return "", errors.New("unused") }
func (g *gatedChatEngine) Chat(string, []engine.ChatMessage) (string, error) {
	return "", errors.New("unused")
}
func (g *gatedChatEngine) ChatWithContext(ctx context.Context, _ string, _ []engine.ChatMessage) (string, error) {
	reply := make(chan string)
	select {
	case g.calls <- reply:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	select {
	case content := <-reply:
		return content, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestOverlappingDialogueTurnsKeepTheirOwnCancellation(t *testing.T) {
	a := newTestAgent(t, "node-overlap")
	eng := &gatedChatEngine{calls: make(chan chan string)}
	a.engines = []engine.Engine{eng}

	type result struct {
		response dialogueMessageResponse
		err      error
	}
	startTurn := func(content string) (chan string, chan result) {
		done := make(chan result, 1)
		go func() {
			response, err := a.dialogueTurn("dlg", "gated", "", "user", content, "dialogue_turn")
			done <- result{response, err}
		}()
		select {
		case reply := <-eng.calls:
			return reply, done
		case <-time.After(5 * time.Second):
			t.Fatal("dialogue turn did not reach the engine")
			return nil, nil
		}
	}

	firstReply, firstDone := startTurn("first")
	_, secondDone := startTurn("second")

	firstReply <- "answer one"
	first := <-firstDone
	if first.err != nil || first.response.Reply != "answer one" {
		t.Fatalf("first turn = %+v, %v", first.response, first.err)
	}

	// The first turn's completion must not discard the second turn's handle.
	if !a.InterruptDialogue("dlg") {
		t.Fatal("in-flight second turn could not be interrupted after the first completed")
	}
	second := <-secondDone
	if !errors.Is(second.err, context.Canceled) {
		t.Fatalf("second turn error = %v, want cancellation", second.err)
	}
	if a.InterruptDialogue("dlg") {
		t.Fatal("cancellation handles leaked after both turns finished")
	}
	a.dialogueComponent.dialogueCancelsMu.Lock()
	leaked := len(a.dialogueComponent.dialogueCancels)
	a.dialogueComponent.dialogueCancelsMu.Unlock()
	if leaked != 0 {
		t.Fatalf("cancellation map retains %d dialogues", leaked)
	}
}
