package orchestrator

import (
	"encoding/json"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), apperr.StatusCode(err))
}

func writeJSONError(w http.ResponseWriter, err error, body any) {
	writeJSON(w, apperr.StatusCode(err), body)
}
