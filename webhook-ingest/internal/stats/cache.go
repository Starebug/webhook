// Package stats keeps a hot-path, in-memory view of per-account call totals.
//
// The durable copy of these numbers lives in Postgres; this cache exists so
// the stats endpoint does not hit the database on every read.
//
// The cache implements:
// - Read-through: On cache miss, loads from database
// - Write-through: Updates cache on every webhook
// - Warm-up: Can be preloaded from database on startup
package stats

import (
	"context"
	"sync"
)

// AccountStats is a point-in-time view of one account's totals.
type AccountStats struct {
	CallCount        int64
	TotalDurationSec int64
}

// StatsReader provides read access to durable account stats.
// This interface avoids a circular dependency with the store package.
type StatsReader interface {
	AccountStats(ctx context.Context, accountID string) (AccountStats, error)
	AllAccountStats(ctx context.Context) (map[string]AccountStats, error)
}

// Cache holds per-account running totals with database fallback.
type Cache struct {
	mu     sync.RWMutex
	m      map[string]*AccountStats
	reader StatsReader // For read-through on cache miss
}

// NewCache returns an empty cache without database fallback.
// Use NewCacheWithReader for read-through capability.
func NewCache() *Cache {
	return &Cache{m: make(map[string]*AccountStats)}
}

// NewCacheWithReader returns a cache with read-through capability.
// On cache miss, it will load stats from the database.
func NewCacheWithReader(reader StatsReader) *Cache {
	return &Cache{
		m:      make(map[string]*AccountStats),
		reader: reader,
	}
}

// Get returns a snapshot of an account's totals.
// If not in cache and a reader is configured, loads from database (read-through).
// Unknown accounts with no database record read as zero.
func (c *Cache) Get(accountID string) AccountStats {
	// Fast path: check cache with read lock
	c.mu.RLock()
	s, ok := c.m[accountID]
	if ok {
		// Copy while lock is held to avoid racing with concurrent Record writes.
		snapshot := *s
		c.mu.RUnlock()
		return snapshot
	}
	c.mu.RUnlock()

	// Cache miss - try to load from database if reader available
	if c.reader != nil {
		return c.loadFromDatabase(accountID)
	}

	// No reader configured, return zero
	return AccountStats{}
}

// loadFromDatabase fetches stats from the database and populates the cache.
// This implements read-through caching on cache miss.
func (c *Cache) loadFromDatabase(accountID string) AccountStats {
	ctx := context.Background()

	dbStats, err := c.reader.AccountStats(ctx, accountID)
	if err != nil {
		// On error, return zero (account might not exist yet)
		return AccountStats{}
	}

	// Populate cache with database value
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine might have loaded it
	if existing, ok := c.m[accountID]; ok {
		return *existing
	}

	// Store in cache
	stats := &AccountStats{
		CallCount:        dbStats.CallCount,
		TotalDurationSec: dbStats.TotalDurationSec,
	}
	c.m[accountID] = stats

	return *stats
}

// Record folds one completed call into an account's running totals.
func (c *Cache) Record(accountID string, durationSec int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, ok := c.m[accountID]
	if !ok {
		s = &AccountStats{}
		c.m[accountID] = s
	}
	s.CallCount++
	s.TotalDurationSec += int64(durationSec)
}

// WarmUp preloads the cache with all account stats from the database.
// This is typically called once on server startup to ensure the cache
// is populated with existing data before serving requests.
//
// Returns the number of accounts loaded and any error encountered.
func (c *Cache) WarmUp(ctx context.Context) (int, error) {
	if c.reader == nil {
		return 0, nil // No reader configured, nothing to warm up
	}

	allStats, err := c.reader.AllAccountStats(ctx)
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Populate cache with all account stats from database
	count := 0
	for accountID, dbStats := range allStats {
		c.m[accountID] = &AccountStats{
			CallCount:        dbStats.CallCount,
			TotalDurationSec: dbStats.TotalDurationSec,
		}
		count++
	}

	return count, nil
}
