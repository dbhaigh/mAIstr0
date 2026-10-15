package inferroute

import "net/http"

type Protocol string

const (
	ProtocolOpenAI    Protocol = "openai"
	ProtocolOllama    Protocol = "ollama"
	ProtocolAnthropic Protocol = "anthropic"
	HeaderEngine               = "X-Maistr0-Engine"
	HeaderRequestID            = "X-Maistr0-Request-ID"
	HeaderAttemptID            = "X-Maistr0-Attempt-ID"
)

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
