package httpx

import (
	"net/http"
	"time"
)

// Named timeout profiles are durations so callers can also retain a custom
// timeout without an enum lookup or an implicit fallback.
const (
	ProbeTimeout     = 5 * time.Second
	ControlTimeout   = 10 * time.Second
	InferenceTimeout = 10 * time.Minute
	DownloadTimeout  = time.Hour
)

type ClientOptions struct {
	// Timeout is the total request timeout. Zero leaves it unlimited, including
	// for streaming requests whose lifetime is controlled by their context.
	Timeout       time.Duration
	Transport     http.RoundTripper
	CheckRedirect func(*http.Request, []*http.Request) error
}

// NewClient creates an independently configurable client while sharing the
// standard transport's connection pool unless a transport is supplied.
// A supplied transport (including peer pinning) and redirect policy are retained
// unchanged. Negative timeouts are programming errors and panic.
func NewClient(opts ClientOptions) *http.Client {
	if opts.Timeout < 0 {
		panic("httpx: client timeout must not be negative")
	}
	transport := opts.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &http.Client{
		Timeout:       opts.Timeout,
		Transport:     transport,
		CheckRedirect: opts.CheckRedirect,
	}
}
