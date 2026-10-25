package orchestrator

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRouterClientLimiterSeparatesHashedAPIKeysAndAppliesRateLimit(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	limiter := newRouterClientLimiter(2, 0)
	limiter.now = func() time.Time { return now }

	request := httptest.NewRequest("POST", "/router/v1/chat/completions", nil)
	request.RemoteAddr = "192.0.2.10:1234"
	request.Header.Set("Authorization", "Bearer secret-a")
	if strings.Contains(routerClientKey(request), "secret-a") {
		t.Fatal("client key contains the API credential")
	}
	for i := 0; i < 2; i++ {
		result := limiter.acquire(request)
		if result.Reason != "" {
			t.Fatalf("request %d rejected unexpectedly: %+v", i+1, result)
		}
		result.Release()
	}
	if result := limiter.acquire(request); result.Reason != "rate" || result.RetryAfter <= 0 {
		t.Fatalf("rate-limited request result = %+v", result)
	}

	otherKey := request.Clone(request.Context())
	otherKey.Header.Set("Authorization", "Bearer secret-b")
	if result := limiter.acquire(otherKey); result.Reason != "" {
		t.Fatalf("independent API key was rate limited: %+v", result)
	} else {
		result.Release()
	}
}

func TestRouterClientLimiterEnforcesConcurrencyAndIgnoresForwardedAddress(t *testing.T) {
	limiter := newRouterClientLimiter(0, 1)
	request := httptest.NewRequest("POST", "/router/v1/chat/completions", nil)
	request.RemoteAddr = "192.0.2.20:1234"
	request.Header.Set("X-Forwarded-For", "198.51.100.10")

	first := limiter.acquire(request)
	if first.Reason != "" {
		t.Fatalf("first request rejected: %+v", first)
	}
	second := limiter.acquire(request)
	if second.Reason != "concurrency" {
		t.Fatalf("concurrent request result = %+v, want concurrency rejection", second)
	}
	first.Release()
	third := limiter.acquire(request)
	if third.Reason != "" {
		t.Fatalf("request after release rejected: %+v", third)
	}
	third.Release()

	otherForwarded := request.Clone(request.Context())
	otherForwarded.Header.Set("X-Forwarded-For", "203.0.113.99")
	if routerClientKey(request) != routerClientKey(otherForwarded) {
		t.Fatal("untrusted forwarded address changed the limiter identity")
	}
}
