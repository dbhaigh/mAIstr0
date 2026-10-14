package orchestrator

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
)

func writeError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), apperr.StatusCode(err))
}

func writeJSONError(w http.ResponseWriter, err error, body any) {
	writeJSON(w, apperr.StatusCode(err), body)
}
