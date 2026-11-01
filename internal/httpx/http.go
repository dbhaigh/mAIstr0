package httpx

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/apperr"
)

const (
	RequestIDHeader                  = "X-Request-ID"
	legacyRequestIDHeader            = "X-Maistr0-Request-ID"
	DefaultMaxRequestBodyBytes int64 = 256 << 20
	SmallJSONBodyBytes         int64 = 64 << 10
	ControlJSONBodyBytes       int64 = 1 << 20
	NodeStatusJSONBodyBytes    int64 = 8 << 20
	InferenceJSONBodyBytes     int64 = 20 << 20
)

type Middleware func(http.Handler) http.Handler

func Chain(handler http.Handler, middleware ...Middleware) http.Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}

func DecodeJSON[T any](w http.ResponseWriter, r *http.Request, maxBytes int64) (T, error) {
	var value T
	decoder, err := jsonDecoder(w, r, maxBytes)
	if err != nil {
		return value, err
	}
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}

func DecodeJSONStrict[T any](w http.ResponseWriter, r *http.Request, maxBytes int64) (T, error) {
	var value T
	decoder, err := jsonDecoder(w, r, maxBytes)
	if err != nil {
		return value, err
	}
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return value, errors.New("request must contain one JSON object")
		}
		return value, err
	}
	return value, nil
}

func ReadBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("request body limit must be greater than zero")
	}
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	return io.ReadAll(http.MaxBytesReader(w, body, maxBytes))
}

func jsonDecoder(w http.ResponseWriter, r *http.Request, maxBytes int64) (*json.Decoder, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("JSON body limit must be greater than zero")
	}
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	r.Body = http.MaxBytesReader(w, body, maxBytes)
	return json.NewDecoder(r.Body), nil
}

// WriteJSON enriches HTTP error responses with the shared error metadata.
func WriteJSON(w http.ResponseWriter, status int, value any) error {
	if status >= http.StatusBadRequest {
		body, err := json.Marshal(value)
		if err != nil {
			return err
		}
		appErr := apperr.FromStatusCode(status, errors.New(errorMessage(body, status)))
		formatted := (apperr.ErrorFormatter{}).Format(appErr, errorContextForWriter(w))
		body, err = mergeErrorJSON(body, formatted)
		if err != nil {
			return err
		}
		return writeJSONRaw(w, status, json.RawMessage(body))
	}
	return writeJSONRaw(w, status, value)
}

// WriteJSONError preserves a route's response fields and adds shared error metadata.
func WriteJSONError(w http.ResponseWriter, err error, body any) error {
	if err == nil {
		err = apperr.InternalError(errors.New("internal server error"))
	}
	encoded, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		return marshalErr
	}
	formatted := (apperr.ErrorFormatter{}).Format(err, errorContextForWriter(w))
	formattedBody, mergeErr := mergeErrorJSON(encoded, formatted)
	if mergeErr != nil {
		return mergeErr
	}
	return writeJSONRaw(w, apperr.StatusCode(err), json.RawMessage(formattedBody))
}

func writeJSONRaw(w http.ResponseWriter, status int, value any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(value)
}

func WriteError(w http.ResponseWriter, err error) {
	if err == nil {
		err = apperr.InternalError(errors.New("internal server error"))
	}
	response := (apperr.ErrorFormatter{}).Format(err, errorContextForWriter(w))
	_ = writeJSONRaw(w, apperr.StatusCode(err), response)
}

// FormatErrors converts plain-text HTTP errors to JSON without rewriting
// provider-specific JSON or streaming error bodies.
func FormatErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &errorFormattingWriter{ResponseWriter: w, request: r}
		next.ServeHTTP(recorder, r)
		recorder.finish()
	})
}

type errorFormattingWriter struct {
	http.ResponseWriter
	request   *http.Request
	status    int
	committed bool
	body      bytes.Buffer
}

func (w *errorFormattingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *errorFormattingWriter) ErrorContext() apperr.ErrorContext {
	context := apperr.ErrorContext{RequestID: RequestIDFromContext(w.request.Context())}
	if context.RequestID == "" {
		context.RequestID = w.Header().Get(RequestIDHeader)
	}
	if context.RequestID == "" {
		context.RequestID = w.request.Header.Get(RequestIDHeader)
	}
	context.Operation = w.request.Pattern
	if context.Operation == "" {
		context.Operation = w.request.Method
	}
	switch {
	case strings.Contains(w.request.Pattern, "/nodes/{id}"):
		context.NodeID = w.request.PathValue("id")
	case strings.Contains(w.request.Pattern, "/nodes/{node_id}"):
		context.NodeID = w.request.PathValue("node_id")
	case strings.Contains(w.request.Pattern, "/api/discovery/{id}"):
		context.NodeID = w.request.PathValue("id")
	}
	staticContext := errorContextFromContext(w.request.Context())
	if context.NodeID == "" {
		context.NodeID = staticContext.NodeID
	}
	if context.Operation == w.request.Method {
		context.Operation = staticContext.Operation
		if context.Operation == "" {
			context.Operation = w.request.Method
		}
	}
	return context
}

func (w *errorFormattingWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	if status >= http.StatusBadRequest && isPlainTextError(w.Header().Get("Content-Type")) {
		return
	}
	w.ResponseWriter.WriteHeader(status)
	w.committed = true
}

func (w *errorFormattingWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= http.StatusBadRequest && !w.committed {
		return w.body.Write(data)
	}
	return w.ResponseWriter.Write(data)
}

func (w *errorFormattingWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= http.StatusBadRequest && !w.committed {
		return
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *errorFormattingWriter) finish() {
	if w.status == 0 || w.committed {
		return
	}
	if w.status < http.StatusBadRequest {
		w.ResponseWriter.WriteHeader(w.status)
		return
	}
	message := strings.TrimSpace(w.body.String())
	if message == "" {
		message = http.StatusText(w.status)
	}
	err := apperr.FromStatusCode(w.status, errors.New(message))
	response := (apperr.ErrorFormatter{}).Format(err, w.ErrorContext())
	w.Header().Del("Content-Length")
	_ = writeJSONRaw(w.ResponseWriter, w.status, response)
}

func isPlainTextError(contentType string) bool {
	contentType, _, _ = strings.Cut(strings.ToLower(strings.TrimSpace(contentType)), ";")
	return contentType == "" || contentType == "text/plain"
}

func errorContextForWriter(w http.ResponseWriter) apperr.ErrorContext {
	if contextual, ok := w.(interface {
		ErrorContext() apperr.ErrorContext
	}); ok {
		return contextual.ErrorContext()
	}
	return apperr.ErrorContext{RequestID: w.Header().Get(RequestIDHeader)}
}

func errorMessage(body []byte, status int) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) == nil && object != nil {
		if raw, ok := object["error"]; ok {
			var message string
			if json.Unmarshal(raw, &message) == nil && strings.TrimSpace(message) != "" {
				return message
			}
			var nested map[string]json.RawMessage
			if json.Unmarshal(raw, &nested) == nil {
				if message := jsonString(nested["message"]); message != "" {
					return message
				}
			}
		}
		if message := jsonString(object["message"]); message != "" {
			return message
		}
		return http.StatusText(status)
	}
	var message string
	if json.Unmarshal(body, &message) == nil && strings.TrimSpace(message) != "" {
		return message
	}
	if json.Valid(body) {
		return http.StatusText(status)
	}
	if message := strings.TrimSpace(string(body)); message != "" {
		return message
	}
	return http.StatusText(status)
}

func jsonString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func mergeErrorJSON(body []byte, response apperr.ErrorResponse) ([]byte, error) {
	var object map[string]json.RawMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &object); err != nil {
			if !json.Valid(body) {
				return nil, err
			}
			object = map[string]json.RawMessage{"details": append(json.RawMessage(nil), body...)}
		}
	}
	if object == nil {
		object = make(map[string]json.RawMessage)
	}
	if !hasErrorValue(object["error"]) {
		if err := setJSONField(object, "error", response.Error); err != nil {
			return nil, err
		}
	}
	fields := map[string]any{
		"code":      response.Code,
		"retryable": response.Retryable,
	}
	if response.RequestID != "" {
		fields["request_id"] = response.RequestID
	}
	if response.NodeID != "" {
		fields["node_id"] = response.NodeID
	}
	if response.Operation != "" {
		fields["operation"] = response.Operation
	}
	for key, value := range fields {
		if err := setJSONField(object, key, value); err != nil {
			return nil, err
		}
	}
	return json.Marshal(object)
}

func hasErrorValue(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	var message string
	if json.Unmarshal(raw, &message) == nil {
		return strings.TrimSpace(message) != ""
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(raw, &object) == nil && object != nil
}

func setJSONField(object map[string]json.RawMessage, key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	object[key] = encoded
	return nil
}

func LimitBodyBytes(maxBytes int64) (Middleware, error) {
	if maxBytes <= 0 {
		return nil, errors.New("request body limit must be greater than zero")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := r.Body
			if body == nil {
				body = http.NoBody
			}
			r.Body = http.MaxBytesReader(w, body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}, nil
}

type requestIDKey struct{}
type errorContextKey struct{}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID(id) {
			id = r.Header.Get(legacyRequestIDHeader)
		}
		if !validRequestID(id) {
			generated, err := NewRequestID()
			if err != nil {
				WriteError(w, apperr.InternalError(errors.New("could not initialize request")))
				return
			}
			id = generated
		}
		if r.Header == nil {
			r.Header = make(http.Header)
		}
		r.Header.Set(RequestIDHeader, id)
		if !validRequestID(r.Header.Get(legacyRequestIDHeader)) {
			r.Header.Set(legacyRequestIDHeader, id)
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// WithErrorContext supplies request-independent context such as the local node ID.
func WithErrorContext(metadata apperr.ErrorContext) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), errorContextKey{}, metadata)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func NewRequestID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func validRequestID(id string) bool {
	if len(id) == 0 || len(id) > 128 || strings.TrimSpace(id) != id {
		return false
	}
	for _, character := range id {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func errorContextFromContext(ctx context.Context) apperr.ErrorContext {
	context, _ := ctx.Value(errorContextKey{}).(apperr.ErrorContext)
	return context
}

func RequestLogger(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "http")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorder := &statusRecorder{ResponseWriter: w}
			started := time.Now()
			next.ServeHTTP(recorder, r)
			status := recorder.status
			if status == 0 {
				status = http.StatusOK
			}
			requestID := recorder.Header().Get(RequestIDHeader)
			if requestID == "" {
				requestID = RequestIDFromContext(r.Context())
			}
			logger.Info("http request",
				"file_only", true,
				"request_id", requestID,
				"method", r.Method,
				"status", status,
				"duration_ms", time.Since(started).Milliseconds(),
			)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *statusRecorder) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
