package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/engine"
)

const (
	generateIdempotencyTTL                 = 10 * time.Minute
	generateIdempotencyMaxEntries          = 512
	generateIdempotencyMaxResultBytes      = 1 << 20
	generateIdempotencyMaxTotalResultBytes = 8 << 20
)

var (
	errIdempotencyKeyConflict = errors.New("idempotency key was already used with a different request")
	errIdempotencyCacheBusy   = errors.New("idempotency result cache is full")
)

type generateIdempotencyResult struct {
	output     string
	durationMs int64
}

type generateIdempotencyEntry struct {
	fingerprint [32]byte
	result      generateIdempotencyResult
	err         error
	done        chan struct{}
	expiresAt   time.Time
	cacheBytes  int
	completed   bool
}

type generateIdempotencyCache struct {
	mu          sync.Mutex
	entries     map[string]*generateIdempotencyEntry
	resultBytes int
}

func newGenerateIdempotencyCache() *generateIdempotencyCache {
	return &generateIdempotencyCache{entries: make(map[string]*generateIdempotencyEntry)}
}

func generateRequestFingerprint(model, prompt string, options engine.GenerationOptions) ([32]byte, error) {
	encoded, err := json.Marshal(struct {
		Model       string  `json:"model"`
		Prompt      string  `json:"prompt"`
		Temperature float64 `json:"temperature"`
		MaxTokens   int     `json:"max_tokens"`
	}{
		Model: model, Prompt: prompt,
		Temperature: options.Temperature, MaxTokens: options.MaxTokens,
	})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (c *generateIdempotencyCache) do(
	ctx context.Context,
	key string,
	fingerprint [32]byte,
	generate func() (generateIdempotencyResult, error),
) (generateIdempotencyResult, error) {
	if err := ctx.Err(); err != nil {
		return generateIdempotencyResult{}, err
	}
	now := time.Now()
	c.mu.Lock()
	for existingKey, entry := range c.entries {
		if entry.completed && !entry.expiresAt.After(now) {
			c.resultBytes -= entry.cacheBytes
			delete(c.entries, existingKey)
		}
	}
	if entry, ok := c.entries[key]; ok {
		if entry.fingerprint != fingerprint {
			c.mu.Unlock()
			return generateIdempotencyResult{}, errIdempotencyKeyConflict
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return generateIdempotencyResult{}, ctx.Err()
		case <-entry.done:
			return entry.result, entry.err
		}
	}
	if len(c.entries) >= generateIdempotencyMaxEntries {
		var oldestKey string
		var oldestExpiry time.Time
		for existingKey, entry := range c.entries {
			if entry.completed && (oldestKey == "" || entry.expiresAt.Before(oldestExpiry)) {
				oldestKey, oldestExpiry = existingKey, entry.expiresAt
			}
		}
		if oldestKey == "" {
			c.mu.Unlock()
			return generateIdempotencyResult{}, errIdempotencyCacheBusy
		}
		c.resultBytes -= c.entries[oldestKey].cacheBytes
		delete(c.entries, oldestKey)
	}
	entry := &generateIdempotencyEntry{
		fingerprint: fingerprint,
		done:        make(chan struct{}),
	}
	c.entries[key] = entry
	c.mu.Unlock()

	entry.result, entry.err = generate()

	c.mu.Lock()
	if entry.err == nil && len(entry.result.output) <= generateIdempotencyMaxResultBytes {
		for c.resultBytes+len(entry.result.output) > generateIdempotencyMaxTotalResultBytes {
			var oldestKey string
			var oldestExpiry time.Time
			for existingKey, candidate := range c.entries {
				if candidate != entry && candidate.completed &&
					(oldestKey == "" || candidate.expiresAt.Before(oldestExpiry)) {
					oldestKey, oldestExpiry = existingKey, candidate.expiresAt
				}
			}
			if oldestKey == "" {
				break
			}
			c.resultBytes -= c.entries[oldestKey].cacheBytes
			delete(c.entries, oldestKey)
		}
		if c.entries[key] == entry &&
			c.resultBytes+len(entry.result.output) <= generateIdempotencyMaxTotalResultBytes {
			entry.completed = true
			entry.expiresAt = time.Now().Add(generateIdempotencyTTL)
			entry.cacheBytes = len(entry.result.output)
			c.resultBytes += entry.cacheBytes
		} else if c.entries[key] == entry {
			delete(c.entries, key)
		}
	} else if c.entries[key] == entry {
		delete(c.entries, key)
	}
	close(entry.done)
	c.mu.Unlock()
	return entry.result, entry.err
}
