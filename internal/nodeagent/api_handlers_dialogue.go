package nodeagent

import (
	"errors"
	"fmt"
	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
	"io"
	"net/http"
	"strings"
	"time"
)

func (a *Agent) handleDialogueCreate(w http.ResponseWriter, r *http.Request) {
	var req createDialogueRequest
	if r.Body != nil {
		decoded, err := httpx.DecodeJSON[createDialogueRequest](w, r, httpx.SmallJSONBodyBytes)
		if err != nil && !errors.Is(err, io.EOF) {
			writeError(w, apperr.Invalid(err))
			return
		}
		req = decoded
	}

	id := req.ID
	if id == "" {
		id = fmt.Sprintf("dlg-%s-%d", a.cfg.NodeID, time.Now().UnixNano())
	}
	dlg := a.dialogueComponent.create(id, req.Model, req.System)
	writeJSON(w, http.StatusCreated, dlg)
}

func (a *Agent) handleDialogueList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.dialogueComponent.list())
}

func (a *Agent) handleDialogueGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	dlg, ok := a.dialogueComponent.get(id)
	if !ok {
		writeError(w, apperr.NotFoundError(errors.New("dialogue not found")))
		return
	}
	writeJSON(w, http.StatusOK, dlg)
}

func (a *Agent) handleDialogueDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !a.dialogueComponent.delete(id) {
		writeError(w, apperr.NotFoundError(errors.New("dialogue not found")))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *Agent) handleDialogueMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	req, err := httpx.DecodeJSON[dialogueMessageRequest](
		w, r, httpx.InferenceJSONBodyBytes,
	)
	if err != nil {
		writeJSONError(w, apperr.Invalid(err), dialogueMessageResponse{Error: err.Error()})
		return
	}

	content := strings.TrimSpace(req.Content)
	if content == "" {
		content = strings.TrimSpace(req.Message)
	}
	if content == "" {
		err := apperr.Invalid(errors.New("content is required"))
		writeJSONError(w, err, dialogueMessageResponse{Error: err.Error()})
		return
	}

	role := req.Role
	if role == "" {
		role = "user"
	}

	res, err := a.dialogueTurnContext(r.Context(), id, "", "", role, content, "dialogue_turn")
	if err != nil {
		writeJSONError(w, apperr.InternalError(err), dialogueMessageResponse{
			DialogueID: id,
			Model:      res.Model,
			DurationMs: res.DurationMs,
			Error:      err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (a *Agent) handleDialogueInterrupt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, apperr.Invalid(errors.New("id is required")), map[string]any{"error": "id is required"})
		return
	}
	interrupted := a.InterruptDialogue(id)
	writeJSON(w, http.StatusOK, map[string]any{
		"dialogue_id": id,
		"interrupted": interrupted,
	})
}
