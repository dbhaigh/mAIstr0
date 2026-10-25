package nodeagent

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
)

func writeError(w http.ResponseWriter, err error) {
	httpx.WriteError(w, err)
}

func writeJSONError(w http.ResponseWriter, err error, body any) {
	writeJSON(w, apperr.StatusCode(err), body)
}
