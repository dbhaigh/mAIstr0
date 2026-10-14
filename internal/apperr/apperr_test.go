package apperr

import (
	"errors"
	"net/http"
	"testing"
)

func TestErrorKindsAndStatusCodes(t *testing.T) {
	cause := errors.New("root cause")
	tests := []struct {
		name   string
		wrap   func(error) error
		kind   Kind
		status int
	}{
		{"invalid input", Invalid, InvalidInput, http.StatusBadRequest},
		{"not found", NotFoundError, NotFound, http.StatusNotFound},
		{"conflict", ConflictError, Conflict, http.StatusConflict},
		{"unavailable", UnavailableError, Unavailable, http.StatusServiceUnavailable},
		{"upstream", UpstreamError, Upstream, http.StatusBadGateway},
		{"internal", InternalError, Internal, http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.wrap(cause)
			if !errors.Is(err, test.kind) {
				t.Fatalf("errors.Is(%v, %v) = false", err, test.kind)
			}
			if !errors.Is(err, cause) {
				t.Fatalf("wrapped error %v lost its cause", err)
			}
			var gotKind Kind
			if !errors.As(err, &gotKind) || gotKind != test.kind {
				t.Fatalf("errors.As kind = %v, want %v", gotKind, test.kind)
			}
			var typed *Error
			if !errors.As(err, &typed) || typed.Kind != test.kind {
				t.Fatalf("errors.As typed error = %#v, want kind %v", typed, test.kind)
			}
			if err.Error() != cause.Error() {
				t.Fatalf("wrapped message = %q, want unchanged cause %q", err.Error(), cause.Error())
			}
			if got := StatusCode(err); got != test.status {
				t.Fatalf("status = %d, want %d", got, test.status)
			}
		})
	}
	if got := StatusCode(cause); got != http.StatusInternalServerError {
		t.Fatalf("unclassified error status = %d, want %d", got, http.StatusInternalServerError)
	}
	if Invalid(nil) != nil {
		t.Fatal("wrapping a nil error should return nil")
	}
}
