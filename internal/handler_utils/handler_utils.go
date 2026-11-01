package handler_utils

import (
	"errors"
	"net/http"

	"github.com/maistr0/maistr0/internal/apperr"
	"github.com/maistr0/maistr0/internal/httpx"
)

type ErrorResponder func(http.ResponseWriter, error)

func WriteJSON(w http.ResponseWriter, status int, value any) error {
	return httpx.WriteJSON(w, status, value)
}

func WriteError(w http.ResponseWriter, err error) {
	httpx.WriteError(w, err)
}

func WriteJSONError(w http.ResponseWriter, err error, body any) error {
	return httpx.WriteJSONError(w, err, body)
}

func RespondInvalid(w http.ResponseWriter, err error) {
	WriteError(w, apperr.Invalid(err))
}

func DecodeJSON[T any](w http.ResponseWriter, r *http.Request, maxBytes int64) (T, bool) {
	return DecodeJSONWithResponder[T](w, r, maxBytes, RespondInvalid)
}

func DecodeJSONWithResponder[T any](
	w http.ResponseWriter,
	r *http.Request,
	maxBytes int64,
	respond ErrorResponder,
) (T, bool) {
	value, err := httpx.DecodeJSON[T](w, r, maxBytes)
	if err != nil {
		respond(w, err)
		return value, false
	}
	return value, true
}

func RequirePathValue(
	w http.ResponseWriter,
	r *http.Request,
	key string,
	missingMessage string,
	respond ErrorResponder,
) (string, bool) {
	value := r.PathValue(key)
	if value == "" {
		respond(w, errors.New(missingMessage))
		return "", false
	}
	return value, true
}
