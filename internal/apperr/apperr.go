package apperr

import (
	"errors"
	"fmt"
	"net/http"
)

type Kind uint8

const (
	InvalidInput Kind = iota + 1
	NotFound
	Conflict
	Unavailable
	Upstream
	Internal
)

func (k Kind) Error() string {
	switch k {
	case InvalidInput:
		return "invalid input"
	case NotFound:
		return "not found"
	case Conflict:
		return "conflict"
	case Unavailable:
		return "unavailable"
	case Upstream:
		return "upstream failure"
	case Internal:
		return "internal error"
	default:
		return "unknown error"
	}
}

type Error struct {
	Kind Kind
	Err  error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Kind.Error()
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	if kind, ok := target.(Kind); ok && kind == e.Kind {
		return true
	}
	return errors.Is(e.Err, target)
}

func (e *Error) As(target any) bool {
	if kind, ok := target.(*Kind); ok {
		*kind = e.Kind
		return true
	}
	return false
}

func Invalid(err error) error          { return wrap(InvalidInput, err) }
func NotFoundError(err error) error    { return wrap(NotFound, err) }
func ConflictError(err error) error    { return wrap(Conflict, err) }
func UnavailableError(err error) error { return wrap(Unavailable, err) }
func UpstreamError(err error) error    { return wrap(Upstream, err) }
func InternalError(err error) error    { return wrap(Internal, err) }

func wrap(kind Kind, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w", &Error{Kind: kind, Err: err})
}

func StatusCode(err error) int {
	switch {
	case errors.Is(err, InvalidInput):
		return http.StatusBadRequest
	case errors.Is(err, NotFound):
		return http.StatusNotFound
	case errors.Is(err, Conflict):
		return http.StatusConflict
	case errors.Is(err, Unavailable):
		return http.StatusServiceUnavailable
	case errors.Is(err, Upstream):
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

func FromStatusCode(status int, err error) error {
	switch status {
	case http.StatusBadRequest:
		return Invalid(err)
	case http.StatusNotFound:
		return NotFoundError(err)
	case http.StatusConflict:
		return ConflictError(err)
	case http.StatusServiceUnavailable:
		return UnavailableError(err)
	case http.StatusBadGateway:
		return UpstreamError(err)
	default:
		return InternalError(err)
	}
}
