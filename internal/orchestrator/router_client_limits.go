package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	routerClientWindow  = time.Minute
	routerClientIdleTTL = 2 * time.Minute
	routerClientMaxKeys = 10000
)

type routerClientLimit struct {
	windowStart time.Time
	requests    int
	active      int
	lastSeen    time.Time
}

type routerClientLimiter struct {
	mu                sync.Mutex
	clients           map[string]*routerClientLimit
	requestsPerMinute int
	maxConcurrent     int
	now               func() time.Time
}

type routerClientLimitResult struct {
	Release    func()
	Reason     string
	RetryAfter int
}

func newRouterClientLimiter(requestsPerMinute, maxConcurrent int) *routerClientLimiter {
	return &routerClientLimiter{
		clients:           make(map[string]*routerClientLimit),
		requestsPerMinute: requestsPerMinute,
		maxConcurrent:     maxConcurrent,
		now:               time.Now,
	}
}

func (l *routerClientLimiter) acquire(request *http.Request) routerClientLimitResult {
	if l == nil || (l.requestsPerMinute == 0 && l.maxConcurrent == 0) {
		return routerClientLimitResult{Release: func() {}}
	}
	key := routerClientKey(request)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(now)
	client := l.clients[key]
	if client == nil {
		if len(l.clients) >= routerClientMaxKeys && !l.evictOldestIdle() {
			return routerClientLimitResult{Reason: "capacity", RetryAfter: 1}
		}
		client = &routerClientLimit{windowStart: now}
		l.clients[key] = client
	}
	if now.Before(client.windowStart) || now.Sub(client.windowStart) >= routerClientWindow {
		client.windowStart = now
		client.requests = 0
	}
	client.lastSeen = now
	if l.maxConcurrent > 0 && client.active >= l.maxConcurrent {
		return routerClientLimitResult{Reason: "concurrency", RetryAfter: 1}
	}
	if l.requestsPerMinute > 0 && client.requests >= l.requestsPerMinute {
		retryAfter := int((client.windowStart.Add(routerClientWindow).Sub(now) + time.Second - 1) / time.Second)
		return routerClientLimitResult{Reason: "rate", RetryAfter: max(1, retryAfter)}
	}
	client.requests++
	client.active++
	var once sync.Once
	return routerClientLimitResult{Release: func() {
		once.Do(func() {
			l.mu.Lock()
			if current := l.clients[key]; current != nil && current.active > 0 {
				current.active--
				current.lastSeen = l.now()
			}
			l.mu.Unlock()
		})
	}}
}

func (l *routerClientLimiter) prune(now time.Time) {
	for key, client := range l.clients {
		if client.active == 0 && now.Sub(client.lastSeen) >= routerClientIdleTTL {
			delete(l.clients, key)
		}
	}
}

func (l *routerClientLimiter) evictOldestIdle() bool {
	var oldestKey string
	var oldest time.Time
	for key, client := range l.clients {
		if client.active != 0 {
			continue
		}
		if oldestKey == "" || client.lastSeen.Before(oldest) {
			oldestKey = key
			oldest = client.lastSeen
		}
	}
	if oldestKey == "" {
		return false
	}
	delete(l.clients, oldestKey)
	return true
}

func routerClientKey(request *http.Request) string {
	if request != nil {
		credential := strings.TrimSpace(request.Header.Get("X-API-Key"))
		if credential == "" {
			credential = strings.TrimSpace(request.Header.Get("Authorization"))
		}
		if credential != "" {
			digest := sha256.Sum256([]byte(credential))
			return "key:" + hex.EncodeToString(digest[:])
		}
		host, _, err := net.SplitHostPort(request.RemoteAddr)
		if err != nil {
			host = strings.TrimSpace(request.RemoteAddr)
		}
		if host != "" {
			return "addr:" + host
		}
	}
	return "addr:unknown"
}
