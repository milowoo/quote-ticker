package kafka

import "sync"

// Deduper prevents duplicate trade processing by tracking recent TradeIDs
// in a sliding-window ring buffer. TradeIDs older than the buffer are
// forgotten — this is safe because Kafka's at-least-once delivery combined
// with the checkpoint/recovery mechanism bounds the duplicate window.
type Deduper struct {
	mu     sync.Mutex
	seen   map[int64]struct{}
	queue  []int64
	maxLen int
}

// NewDeduper creates a deduper that remembers up to maxLen trade IDs.
// 1M entries ≈ 16 MB, enough for ~5 min of high-volume trading.
func NewDeduper(maxLen int) *Deduper {
	return &Deduper{
		seen:   make(map[int64]struct{}, maxLen),
		queue:  make([]int64, 0, maxLen),
		maxLen: maxLen,
	}
}

// Seen checks and records a trade ID. Returns true if already seen (duplicate).
func (d *Deduper) Seen(tradeID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.seen[tradeID]; ok {
		return true
	}

	d.seen[tradeID] = struct{}{}
	d.queue = append(d.queue, tradeID)

	if len(d.queue) > d.maxLen {
		oldest := d.queue[0]
		delete(d.seen, oldest)
		d.queue = d.queue[1:]
	}
	return false
}
