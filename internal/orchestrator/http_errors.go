package orchestrator

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/handler_utils"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	_ = handler_utils.WriteJSON(w, status, v)
}

func writeError(w http.ResponseWriter, err error) {
	handler_utils.WriteError(w, err)
}

func writeJSONError(w http.ResponseWriter, err error, body any) {
	_ = handler_utils.WriteJSONError(w, err, body)
}
