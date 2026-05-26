package repository

import (
	"sort"
	"time"

	gocache "github.com/patrickmn/go-cache"
	"quote-ticker/internal/model"
)

// klineCache is an in-memory kline cache built on go-cache.
//
// Each interval can have its own TTL — 1m data is cached for ~checkpoint interval
// (fresh data arrives every 500ms), while longer intervals can cache longer.
//
// Data layout — each go-cache item is a sorted []*model.Kline keyed by "symbol:interval":
//
//	c.Get("BTCUSDT:1m") → [{t:10:00,...}, {t:10:01,...}, ...]  ← sorted by StartTime ASC
type klineCache struct {
	store       *gocache.Cache
	maxItems    int
	intervalTTL map[string]time.Duration // "1m" → 2s, others → defaultTTL
	defaultTTL  time.Duration
}

// newKlineCache creates a klineCache.
//   - maxItems: max klines per (symbol, interval) key
//   - intervalTTL: per-interval TTL overrides (e.g. "1m" → 2s)
//   - defaultTTL: fallback for intervals not in intervalTTL
func newKlineCache(maxItems int, intervalTTL map[string]time.Duration, defaultTTL time.Duration) *klineCache {
	if defaultTTL <= 0 {
		defaultTTL = 5 * time.Second
	}
	// Use defaultTTL for the go-cache janitor sweep interval.
	return &klineCache{
		store:       gocache.New(defaultTTL, defaultTTL/2),
		maxItems:    maxItems,
		intervalTTL: intervalTTL,
		defaultTTL:  defaultTTL,
	}
}

func cacheKey(symbol, interval string) string { return symbol + ":" + interval }

// ttlFor returns the TTL for a given interval.
func (c *klineCache) ttlFor(interval string) time.Duration {
	if ttl, ok := c.intervalTTL[interval]; ok {
		return ttl
	}
	return c.defaultTTL
}

// put inserts klines into the cache, upserting by StartTime.
// Each interval gets its own TTL (1m → 2s, others → 5s default).
func (c *klineCache) put(symbol, interval string, klines []*model.Kline) {
	if len(klines) == 0 {
		return
	}
	key := cacheKey(symbol, interval)
	ttl := c.ttlFor(interval)

	var entries []*model.Kline
	if existing, found := c.store.Get(key); found {
		entries = existing.([]*model.Kline)
	}
	if entries == nil {
		entries = make([]*model.Kline, 0, c.maxItems)
	}

	for _, k := range klines {
		idx := sort.Search(len(entries), func(i int) bool {
			return entries[i].StartTime >= k.StartTime
		})
		if idx < len(entries) && entries[idx].StartTime == k.StartTime {
			entries[idx] = k
		} else {
			entries = append(entries, nil)
			copy(entries[idx+1:], entries[idx:])
			entries[idx] = k
		}
	}

	if len(entries) > c.maxItems {
		entries = entries[len(entries)-c.maxItems:]
	}

	c.store.Set(key, entries, ttl)
}

// get returns cached klines within [from, to), up to limit.
// Returns nil if the key is missing, expired, or there are no matching rows.
func (c *klineCache) get(symbol, interval string, from, to int64, limit int) []*model.Kline {
	key := cacheKey(symbol, interval)

	raw, found := c.store.Get(key)
	if !found {
		return nil
	}
	entries := raw.([]*model.Kline)
	if len(entries) == 0 {
		return nil
	}

	startIdx := sort.Search(len(entries), func(i int) bool {
		return entries[i].StartTime >= from
	})
	if startIdx == len(entries) {
		return nil
	}

	endIdx := startIdx
	for endIdx < len(entries) && entries[endIdx].StartTime < to && (limit <= 0 || endIdx-startIdx < limit) {
		endIdx++
	}
	if startIdx == endIdx {
		return nil
	}

	out := make([]*model.Kline, endIdx-startIdx)
	copy(out, entries[startIdx:endIdx])
	return out
}
