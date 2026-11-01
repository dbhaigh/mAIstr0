package apperr

import (
	"errors"
	"net/http"
	"testing"
)

func TestErrorKindsAndStatusCodes(t *testing.T) {
	cause := errors.New("root cause")
	tests := []struct {
		name      string
		wrap      func(error) error
		kind      Kind
		status    int
		retryable bool
	}{
		{"invalid input", Invalid, InvalidInput, http.StatusBadRequest, false},
		{"not found", NotFoundError, NotFound, http.StatusNotFound, false},
		{"conflict", ConflictError, Conflict, http.StatusConflict, false},
		{"unavailable", UnavailableError, Unavailable, http.StatusServiceUnavailable, true},
		{"upstream", UpstreamError, Upstream, http.StatusBadGateway, true},
		{"internal", InternalError, Internal, http.StatusInternalServerError, false},
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
			if got := IsRetryable(err); got != test.retryable {
				t.Fatalf("retryable = %t, want %t", got, test.retryable)
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

func TestFromStatusCodePreservesHTTPClassification(t *testing.T) {
	tests := []struct {
		status    int
		kind      Kind
		retryable bool
	}{
		{http.StatusUnauthorized, Unauthorized, false},
		{http.StatusForbidden, Forbidden, false},
		{http.StatusMethodNotAllowed, MethodNotAllowed, false},
		{http.StatusRequestTimeout, RequestTimeout, true},
		{http.StatusRequestEntityTooLarge, PayloadTooLarge, false},
		{http.StatusUpgradeRequired, UpgradeRequired, false},
		{http.StatusTooManyRequests, RateLimited, true},
		{http.StatusNotImplemented, NotImplemented, false},
		{http.StatusGatewayTimeout, GatewayTimeout, true},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			err := FromStatusCode(test.status, errors.New("classified failure"))
			if got := StatusCode(err); got != test.status {
				t.Fatalf("status = %d, want %d", got, test.status)
			}
			var kind Kind
			if !errors.As(err, &kind) || kind != test.kind {
				t.Fatalf("kind = %v, want %v", kind, test.kind)
			}
			if got := IsRetryable(err); got != test.retryable {
				t.Fatalf("retryable = %t, want %t", got, test.retryable)
			}
		})
	}
}

func TestTopLevelClassificationControlsStatusAndRetryability(t *testing.T) {
	cause := errors.New("upstream unavailable")
	err := InternalError(UnavailableError(cause))
	if got := StatusCode(err); got != http.StatusInternalServerError {
		t.Fatalf("outer internal status = %d, want %d", got, http.StatusInternalServerError)
	}
	if IsRetryable(err) {
		t.Fatal("outer internal error inherited retryability from its cause")
	}
	if !errors.Is(err, Unavailable) || !errors.Is(err, cause) {
		t.Fatalf("wrapped error lost its cause classifications: %v", err)
	}
}

func TestRetryabilityOverrideAndErrorContext(t *testing.T) {
	cause := errors.New("node request timed out")
	err := WithContext(
		WithRetryable(UpstreamError(cause), false),
		ErrorContext{NodeID: "node-7", Operation: "POST /api/nodes/{id}/generate"},
	)
	response := (ErrorFormatter{}).Format(err, ErrorContext{
		RequestID: "request-123",
		NodeID:    "fallback-node",
		Operation: "POST /fallback",
	})
	if response.Error != cause.Error() ||
		response.Code != "upstream_error" ||
		response.Retryable ||
		response.RequestID != "request-123" ||
		response.NodeID != "node-7" ||
		response.Operation != "POST /api/nodes/{id}/generate" {
		t.Fatalf("formatted error = %+v", response)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("context and retryability wrappers lost the cause: %v", err)
	}
	if WithContext(nil, ErrorContext{}) != nil || WithRetryable(nil, true) != nil {
		t.Fatal("error metadata helpers should preserve nil")
	}
}

func TestErrorFormatterClassifiesUnwrappedErrorsAsInternal(t *testing.T) {
	response := (ErrorFormatter{}).Format(errors.New("unexpected failure"), ErrorContext{})
	if response.Error != "unexpected failure" ||
		response.Code != "internal_error" ||
		response.Retryable {
		t.Fatalf("formatted error = %+v", response)
	}
}
