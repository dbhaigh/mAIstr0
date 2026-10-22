package orchestrator

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	_ = httpx.WriteJSON(w, status, v)
}

func writeError(w http.ResponseWriter, err error) {
	httpx.WriteError(w, err)
}

func writeJSONError(w http.ResponseWriter, err error, body any) {
	_ = httpx.WriteJSON(w, apperr.StatusCode(err), body)
}
