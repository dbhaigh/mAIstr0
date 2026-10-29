package httpx

import (
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

func WriteJSON(w http.ResponseWriter, status int, value any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(value)
}

func WriteError(w http.ResponseWriter, err error) {
	if err == nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	http.Error(w, err.Error(), apperr.StatusCode(err))
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

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID(id) {
			id = r.Header.Get(legacyRequestIDHeader)
		}
		if !validRequestID(id) {
			generated, err := NewRequestID()
			if err != nil {
				http.Error(w, "could not initialize request", http.StatusInternalServerError)
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
