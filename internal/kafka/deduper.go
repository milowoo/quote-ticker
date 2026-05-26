package kafka

import (
	"strconv"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

// Deduper prevents duplicate trade processing by tracking recent TradeIDs
// with a TTL-based cache. Entries expire after ttl — long enough to cover
// Kafka rebalance windows but short enough to bound memory.
//
// go-cache handles all concurrency, eviction, and background cleanup.
type Deduper struct {
	store *gocache.Cache
}

// NewDeduper creates a deduper. Trade IDs are remembered for ttl duration.
// 5 minutes is a safe default — exceeds typical Kafka rebalance time.
func NewDeduper(ttl time.Duration) *Deduper {
	return &Deduper{
		store: gocache.New(ttl, ttl/2),
	}
}

// Seen checks and records a trade ID. Returns true if already seen (duplicate).
func (d *Deduper) Seen(tradeID int64) bool {
	key := strconv.FormatInt(tradeID, 10)
	if _, found := d.store.Get(key); found {
		return true
	}
	d.store.Set(key, struct{}{}, gocache.DefaultExpiration)
	return false
}
