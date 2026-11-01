package apperr

import (
	"errors"
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
	Unauthorized
	Forbidden
	MethodNotAllowed
	RequestTimeout
	PayloadTooLarge
	UpgradeRequired
	RateLimited
	NotImplemented
	GatewayTimeout
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
	case Unauthorized:
		return "unauthorized"
	case Forbidden:
		return "forbidden"
	case MethodNotAllowed:
		return "method not allowed"
	case RequestTimeout:
		return "request timeout"
	case PayloadTooLarge:
		return "request body too large"
	case UpgradeRequired:
		return "HTTPS required"
	case RateLimited:
		return "too many requests"
	case NotImplemented:
		return "not implemented"
	case GatewayTimeout:
		return "upstream timeout"
	default:
		return "unknown error"
	}
}

func (k Kind) Code() string {
	switch k {
	case InvalidInput:
		return "invalid_input"
	case NotFound:
		return "not_found"
	case Conflict:
		return "conflict"
	case Unavailable:
		return "unavailable"
	case Upstream:
		return "upstream_error"
	case Internal:
		return "internal_error"
	case Unauthorized:
		return "unauthorized"
	case Forbidden:
		return "forbidden"
	case MethodNotAllowed:
		return "method_not_allowed"
	case RequestTimeout:
		return "request_timeout"
	case PayloadTooLarge:
		return "payload_too_large"
	case UpgradeRequired:
		return "upgrade_required"
	case RateLimited:
		return "rate_limited"
	case NotImplemented:
		return "not_implemented"
	case GatewayTimeout:
		return "gateway_timeout"
	default:
		return "internal_error"
	}
}

type ErrorContext struct {
	RequestID string
	NodeID    string
	Operation string
}

type Error struct {
	Kind Kind
	Err  error

	context       ErrorContext
	retryable     bool
	retryableSet  bool
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Kind.Error()
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	if kind, ok := target.(Kind); ok {
		return kind == e.Kind
	}
	return false
}

func (e *Error) As(target any) bool {
	if kind, ok := target.(*Kind); ok {
		*kind = e.Kind
		return true
	}
	return false
}

// ErrorResponse is the common metadata envelope used for HTTP errors.
type ErrorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
	RequestID string `json:"request_id,omitempty"`
	NodeID    string `json:"node_id,omitempty"`
	Operation string `json:"operation,omitempty"`
}

// ErrorFormatter maps domain errors and request context to an HTTP error envelope.
type ErrorFormatter struct{}

func (ErrorFormatter) Format(err error, requestContext ErrorContext) ErrorResponse {
	if err == nil {
		err = errors.New("internal server error")
	}
	kind, errorContext, _, _ := errorPolicy(err)
	errorContext = mergeContext(errorContext, requestContext)
	return ErrorResponse{
		Error:     err.Error(),
		Code:      kind.Code(),
		Retryable: IsRetryable(err),
		RequestID: errorContext.RequestID,
		NodeID:    errorContext.NodeID,
		Operation: errorContext.Operation,
	}
}

func Invalid(err error) error             { return wrap(InvalidInput, err) }
func NotFoundError(err error) error       { return wrap(NotFound, err) }
func ConflictError(err error) error       { return wrap(Conflict, err) }
func UnavailableError(err error) error    { return wrap(Unavailable, err) }
func UpstreamError(err error) error       { return wrap(Upstream, err) }
func InternalError(err error) error       { return wrap(Internal, err) }
func UnauthorizedError(err error) error   { return wrap(Unauthorized, err) }
func ForbiddenError(err error) error      { return wrap(Forbidden, err) }
func MethodNotAllowedError(err error) error {
	return wrap(MethodNotAllowed, err)
}
func RequestTimeoutError(err error) error { return wrap(RequestTimeout, err) }
func PayloadTooLargeError(err error) error {
	return wrap(PayloadTooLarge, err)
}
func UpgradeRequiredError(err error) error { return wrap(UpgradeRequired, err) }
func RateLimitedError(err error) error     { return wrap(RateLimited, err) }
func NotImplementedError(err error) error  { return wrap(NotImplemented, err) }
func GatewayTimeoutError(err error) error { return wrap(GatewayTimeout, err) }

func wrap(kind Kind, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Err: err}
}

// WithContext adds observability metadata while preserving the error classification and cause.
func WithContext(err error, context ErrorContext) error {
	if err == nil {
		return nil
	}
	kind, existingContext, retryable, retryableSet := errorPolicy(err)
	return &Error{
		Kind:          kind,
		Err:           err,
		context:       mergeContext(context, existingContext),
		retryable:     retryable,
		retryableSet:  retryableSet,
	}
}

// WithRetryable overrides the retryability default for an error.
func WithRetryable(err error, retryable bool) error {
	if err == nil {
		return nil
	}
	kind, context, _, _ := errorPolicy(err)
	return &Error{
		Kind:         kind,
		Err:          err,
		context:      context,
		retryable:    retryable,
		retryableSet: true,
	}
}

func IsRetryable(err error) bool {
	kind, _, retryable, retryableSet := errorPolicy(err)
	if retryableSet {
		return retryable
	}
	switch kind {
	case Unavailable, Upstream, RequestTimeout, RateLimited, GatewayTimeout:
		return true
	default:
		return false
	}
}

func StatusCode(err error) int {
	kind, _, _, _ := errorPolicy(err)
	switch kind {
	case InvalidInput:
		return http.StatusBadRequest
	case Unauthorized:
		return http.StatusUnauthorized
	case Forbidden:
		return http.StatusForbidden
	case NotFound:
		return http.StatusNotFound
	case MethodNotAllowed:
		return http.StatusMethodNotAllowed
	case RequestTimeout:
		return http.StatusRequestTimeout
	case Conflict:
		return http.StatusConflict
	case PayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case UpgradeRequired:
		return http.StatusUpgradeRequired
	case RateLimited:
		return http.StatusTooManyRequests
	case NotImplemented:
		return http.StatusNotImplemented
	case Upstream:
		return http.StatusBadGateway
	case Unavailable:
		return http.StatusServiceUnavailable
	case GatewayTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func FromStatusCode(status int, err error) error {
	switch status {
	case http.StatusBadRequest:
		return Invalid(err)
	case http.StatusUnauthorized:
		return UnauthorizedError(err)
	case http.StatusForbidden:
		return ForbiddenError(err)
	case http.StatusNotFound:
		return NotFoundError(err)
	case http.StatusMethodNotAllowed:
		return MethodNotAllowedError(err)
	case http.StatusRequestTimeout:
		return RequestTimeoutError(err)
	case http.StatusConflict:
		return ConflictError(err)
	case http.StatusRequestEntityTooLarge:
		return PayloadTooLargeError(err)
	case http.StatusUpgradeRequired:
		return UpgradeRequiredError(err)
	case http.StatusTooManyRequests:
		return RateLimitedError(err)
	case http.StatusNotImplemented:
		return NotImplementedError(err)
	case http.StatusBadGateway:
		return UpstreamError(err)
	case http.StatusServiceUnavailable:
		return UnavailableError(err)
	case http.StatusGatewayTimeout:
		return GatewayTimeoutError(err)
	default:
		return InternalError(err)
	}
}

func errorPolicy(err error) (Kind, ErrorContext, bool, bool) {
	var typed *Error
	if errors.As(err, &typed) {
		return typed.Kind, typed.context, typed.retryable, typed.retryableSet
	}
	var kind Kind
	if errors.As(err, &kind) {
		return kind, ErrorContext{}, false, false
	}
	return Internal, ErrorContext{}, false, false
}

func mergeContext(preferred, fallback ErrorContext) ErrorContext {
	if preferred.RequestID == "" {
		preferred.RequestID = fallback.RequestID
	}
	if preferred.NodeID == "" {
		preferred.NodeID = fallback.NodeID
	}
	if preferred.Operation == "" {
		preferred.Operation = fallback.Operation
	}
	return preferred
}
