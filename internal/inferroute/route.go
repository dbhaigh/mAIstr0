package inferroute

import (
	"errors"
	"net/http"
	"strings"
	"unicode"
)

type Protocol string

const (
	ProtocolOpenAI          Protocol = "openai"
	ProtocolOllama          Protocol = "ollama"
	ProtocolAnthropic       Protocol = "anthropic"
	HeaderEngine                     = "X-Maistr0-Engine"
	HeaderRequestID                  = "X-Maistr0-Request-ID"
	HeaderAttemptID                  = "X-Maistr0-Attempt-ID"
	HeaderIdempotencyKey             = "Idempotency-Key"
	MaxIdempotencyKeyLength          = 200
)

func ValidateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if len(key) > MaxIdempotencyKeyLength {
		return errors.New("idempotency key exceeds 200 bytes")
	}
	if strings.TrimSpace(key) == "" {
		return errors.New("idempotency key must not be blank")
	}
	for _, char := range key {
		if unicode.IsControl(char) {
			return errors.New("idempotency key must not contain control characters")
		}
	}
	return nil
}

type Route struct {
	Path     string
	Protocol Protocol
}

func Lookup(method, path string) (Route, bool) {
	if method != http.MethodPost {
		return Route{}, false
	}
	switch path {
	case "/v1/chat/completions", "/v1/completions":
		return Route{Path: path, Protocol: ProtocolOpenAI}, true
	case "/v1/messages":
		return Route{Path: path, Protocol: ProtocolAnthropic}, true
	case "/api/chat", "/api/generate":
		return Route{Path: path, Protocol: ProtocolOllama}, true
	default:
		return Route{}, false
	}
}
