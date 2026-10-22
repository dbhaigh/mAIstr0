package orchestrator

import (
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
)

const (
	routerBreakerFailureThreshold = 3
	routerBreakerFailureWindow    = 30 * time.Second
	routerBreakerBaseBackoff      = time.Second
	routerBreakerMaxBackoff       = time.Minute
	routerBreakerMaxEntries       = 4096
)

type routerCircuitBreakerSnapshot struct {
	NodeID         string     `json:"node_id"`
	Model          string     `json:"model"`
	State          string     `json:"state"`
	RecentFailures int        `json:"recent_failures"`
	OpenUntil      *time.Time `json:"open_until,omitempty"`
	ProbeInFlight  bool       `json:"probe_in_flight,omitempty"`
}

type routerCircuitBreaker struct {
	state          string
	recentFailures []time.Time
	openCount      int
	openUntil      time.Time
	probeInFlight  bool
	lastFailure    time.Time
}

type routerCircuitBreakers struct {
	mu      sync.Mutex
	entries map[string]*routerCircuitBreaker
	now     func() time.Time
	jitter  func(time.Duration) time.Duration
}

func newRouterCircuitBreakers() *routerCircuitBreakers {
	return &routerCircuitBreakers{
		entries: make(map[string]*routerCircuitBreaker),
		now:     time.Now,
		jitter: func(delay time.Duration) time.Duration {
			jittered := time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
			return min(jittered, routerBreakerMaxBackoff)
		},
	}
}

func (b *routerCircuitBreakers) eligible(nodeID, model string) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[routerPairKey(nodeID, strings.ToLower(strings.TrimSpace(model)))]
	if entry == nil {
		return true
	}
	b.transitionExpired(entry, b.now())
	return entry.state != "open" && (entry.state != "half_open" || !entry.probeInFlight)
}

func (b *routerCircuitBreakers) acquire(nodeID, model string) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[routerPairKey(nodeID, strings.ToLower(strings.TrimSpace(model)))]
	if entry == nil {
		return true
	}
	b.transitionExpired(entry, b.now())
	if entry.state != "half_open" {
		return entry.state != "open"
	}
	if entry.probeInFlight {
		return false
	}
	entry.probeInFlight = true
	return true
}

func (b *routerCircuitBreakers) releaseProbe(nodeID, model string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[routerPairKey(nodeID, strings.ToLower(strings.TrimSpace(model)))]
	if entry != nil && entry.state == "half_open" {
		entry.probeInFlight = false
	}
}

func (b *routerCircuitBreakers) recordFailure(nodeID, model string) {
	if b == nil {
		return
	}
	key := routerPairKey(nodeID, strings.ToLower(strings.TrimSpace(model)))
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entryForUpdate(key, now)
	if entry == nil {
		return
	}
	b.pruneFailures(entry, now)
	entry.lastFailure = now
	if entry.state == "half_open" {
		entry.recentFailures = append(entry.recentFailures, now)
		b.open(entry, now)
		return
	}
	entry.recentFailures = append(entry.recentFailures, now)
	if entry.state != "open" && len(entry.recentFailures) >= routerBreakerFailureThreshold {
		entry.openCount = 1
		b.open(entry, now)
	}
}

func (b *routerCircuitBreakers) recordSuccess(nodeID, model string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	delete(b.entries, routerPairKey(nodeID, strings.ToLower(strings.TrimSpace(model))))
	b.mu.Unlock()
}

func (b *routerCircuitBreakers) recordHealthyResponse(nodeID, model string) {
	if b == nil {
		return
	}
	key := routerPairKey(nodeID, strings.ToLower(strings.TrimSpace(model)))
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry := b.entries[key]; entry != nil && entry.state == "half_open" {
		delete(b.entries, key)
	}
}

func (b *routerCircuitBreakers) snapshot(nodes []cluster.NodeStatus) []routerCircuitBreakerSnapshot {
	if b == nil {
		return []routerCircuitBreakerSnapshot{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	result := make([]routerCircuitBreakerSnapshot, 0)
	seen := make(map[string]bool)
	for _, node := range nodes {
		for _, model := range node.Models {
			modelName := strings.TrimSpace(model.Name)
			if modelName == "" {
				continue
			}
			key := routerPairKey(node.ID, strings.ToLower(modelName))
			if seen[key] {
				continue
			}
			seen[key] = true
			item := routerCircuitBreakerSnapshot{NodeID: node.ID, Model: modelName, State: "closed"}
			if entry := b.entries[key]; entry != nil {
				b.transitionExpired(entry, now)
				b.pruneFailures(entry, now)
				item.State = entry.state
				item.RecentFailures = len(entry.recentFailures)
				item.ProbeInFlight = entry.probeInFlight
				if entry.state == "open" {
					openUntil := entry.openUntil.UTC()
					item.OpenUntil = &openUntil
				}
			}
			result = append(result, item)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].NodeID != result[j].NodeID {
			return result[i].NodeID < result[j].NodeID
		}
		return strings.ToLower(result[i].Model) < strings.ToLower(result[j].Model)
	})
	return result
}

func (s *Server) routerCircuitBreakerSnapshots() []routerCircuitBreakerSnapshot {
	var nodes []cluster.NodeStatus
	if s.registry != nil {
		nodes = s.registry.All()
	}
	return s.routerBreakers.snapshot(nodes)
}

func isTransientRouterFailure(class string) bool {
	switch class {
	case "node_unavailable", "transport", "timeout", "time_to_first_byte_timeout",
		"idle_stream_timeout", "upstream_5xx", "rate_limit", "stale_model",
		"upstream_read", "stream_interrupted":
		return true
	default:
		return false
	}
}

func (b *routerCircuitBreakers) entryForUpdate(key string, now time.Time) *routerCircuitBreaker {
	if entry := b.entries[key]; entry != nil {
		return entry
	}
	for oldKey, entry := range b.entries {
		b.pruneFailures(entry, now)
		if entry.state == "closed" && len(entry.recentFailures) == 0 {
			delete(b.entries, oldKey)
		}
	}
	if len(b.entries) >= routerBreakerMaxEntries {
		return nil
	}
	entry := &routerCircuitBreaker{state: "closed"}
	b.entries[key] = entry
	return entry
}

func (b *routerCircuitBreakers) transitionExpired(entry *routerCircuitBreaker, now time.Time) {
	if entry.state == "open" && !now.Before(entry.openUntil) {
		entry.state = "half_open"
		entry.probeInFlight = false
	}
}

func (b *routerCircuitBreakers) pruneFailures(entry *routerCircuitBreaker, now time.Time) {
	first := 0
	for first < len(entry.recentFailures) && now.Sub(entry.recentFailures[first]) > routerBreakerFailureWindow {
		first++
	}
	if first > 0 {
		entry.recentFailures = append([]time.Time(nil), entry.recentFailures[first:]...)
	}
	if entry.state == "closed" && len(entry.recentFailures) < routerBreakerFailureThreshold {
		entry.openCount = 0
	}
}

func (b *routerCircuitBreakers) open(entry *routerCircuitBreaker, now time.Time) {
	entry.state = "open"
	entry.probeInFlight = false
	if entry.openCount < 1 {
		entry.openCount = 1
	}
	delay := routerBreakerBaseBackoff
	for i := 1; i < entry.openCount && delay < routerBreakerMaxBackoff; i++ {
		delay = min(delay*2, routerBreakerMaxBackoff)
	}
	if b.jitter != nil {
		delay = b.jitter(delay)
	}
	delay = min(delay, routerBreakerMaxBackoff)
	entry.openUntil = now.Add(delay)
	entry.openCount++
}
