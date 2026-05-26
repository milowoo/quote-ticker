package kline

import (
	"context"
	"log"
	"sync"
	"time"

	"quote-ticker/internal/metrics"
	"quote-ticker/internal/model"
)

// Repository defines the DB operations the aggregator needs.
type Repository interface {
	BatchSave(ctx context.Context, symbol string, klines []*model.Kline) error
	LoadKline(ctx context.Context, symbol, interval string, startTime int64) (*model.Kline, error)
	Query(ctx context.Context, symbol, interval string, startTime, endTime int64, limit int) ([]*model.Kline, error)
}

// symbolBuckets holds all interval buckets for one symbol, with its own mutex.
// Two symbols NEVER share a lock — zero contention regardless of hash collisions.
type symbolBuckets struct {
	mu     sync.Mutex
	symbol string
	data   map[string]*model.Kline // interval → open kline
}

func newSymbolBuckets(symbol string) *symbolBuckets {
	return &symbolBuckets{
		symbol: symbol,
		data:   make(map[string]*model.Kline, len(Intervals)),
	}
}

// Aggregator consumes trades and produces completed klines.
// Each symbol gets its own lock — no shard collisions, no hash conflicts.
type Aggregator struct {
	repo    Repository
	symbols sync.Map // map[string]*symbolBuckets

	checkpointCancel context.CancelFunc
	isLeader         func() bool

	dirtyMu   sync.Mutex
	dirty     map[string]struct{} // symbols updated since last DrainDirty

	completedMu  sync.Mutex
	completedBuf map[string][]*model.Kline // completed klines awaiting flush
}

// NewAggregator creates an aggregator.
func NewAggregator(repo Repository) *Aggregator {
	return &Aggregator{
		repo:         repo,
		dirty:        make(map[string]struct{}),
		completedBuf: make(map[string][]*model.Kline),
	}
}

// DrainDirty returns and clears the set of symbols that have received trades
// since the last call. Used by the continuity checker to avoid scanning
// all symbols — only recently active symbols need checking.
func (a *Aggregator) DrainDirty() []string {
	a.dirtyMu.Lock()
	defer a.dirtyMu.Unlock()
	out := make([]string, 0, len(a.dirty))
	for sym := range a.dirty {
		out = append(out, sym)
	}
	a.dirty = make(map[string]struct{})
	return out
}

// SetLeaderChecker registers a callback to determine if this instance
// should persist klines to the database. Only the leader writes.
func (a *Aggregator) SetLeaderChecker(fn func() bool) { a.isLeader = fn }

func (a *Aggregator) shouldWrite() bool {
	return a.isLeader == nil || a.isLeader()
}

// ProcessTrade updates all kline intervals for the trade's symbol.
// Completed klines are buffered and flushed by the next checkpoint cycle.
// This decouples trade processing from DB write latency — a window close
// no longer blocks the hot path.
func (a *Aggregator) ProcessTrade(ctx context.Context, trade model.Trade) error {
	start := time.Now()
	metrics.TradesTotal.WithLabelValues(trade.Symbol).Inc()

	// Mark symbol as dirty for the continuity checker.
	a.dirtyMu.Lock()
	a.dirty[trade.Symbol] = struct{}{}
	a.dirtyMu.Unlock()

	t := time.UnixMilli(trade.Timestamp).UTC()
	completed := a.updateBuckets(ctx, trade, t)

	metrics.ProcessingDuration.WithLabelValues(trade.Symbol).Observe(time.Since(start).Seconds())

	// Buffer completed klines — they'll be flushed by the next checkpoint cycle.
	if len(completed) > 0 && a.shouldWrite() {
		for _, k := range completed {
			metrics.KlinesWrittenTotal.WithLabelValues(k.Interval).Inc()
		}
		a.completedMu.Lock()
		a.completedBuf[trade.Symbol] = append(a.completedBuf[trade.Symbol], completed...)
		a.completedMu.Unlock()
	}
	return nil
}

func (a *Aggregator) updateBuckets(ctx context.Context, trade model.Trade, t time.Time) []*model.Kline {
	// LoadOrStore — creates a new symbolBuckets on first trade for this symbol.
	sbRaw, _ := a.symbols.LoadOrStore(trade.Symbol, newSymbolBuckets(trade.Symbol))
	sb := sbRaw.(*symbolBuckets)

	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Recovery on first trade (data loaded the first time sb.data is empty).
	if len(sb.data) == 0 {
		a.recoverLocked(ctx, sb, trade.Symbol, t)
	}

	var completed []*model.Kline
	for _, iv := range Intervals {
		start := iv.AlignFn(t)
		startMs := start.UnixMilli()
		closeTime := calcCloseTime(start, iv.Name, iv.Width)

		cur, exists := sb.data[iv.Name]

		if exists && cur.StartTime != startMs {
			completed = append(completed, cur)
			exists = false
		}

		if !exists {
			cur = model.NewKline(iv.Name, startMs, closeTime, trade.Price)
			sb.data[iv.Name] = cur
		}

		cur.Update(trade.Price, trade.Quantity, trade.Amount, trade.TakerBuy)
	}

	return completed
}

// recoverLocked loads checkpoint data from the DB into the symbolBuckets.
// 7 SELECTs run in parallel to cut recovery latency from ~35ms to ~5ms.
// Called once per symbol. sb.mu is held.
func (a *Aggregator) recoverLocked(ctx context.Context, sb *symbolBuckets, symbol string, t time.Time) {
	var wg sync.WaitGroup
	for _, iv := range Intervals {
		wg.Add(1)
		go func(iv Interval) {
			defer wg.Done()
			startMs := iv.AlignFn(t).UnixMilli()
			k, err := a.repo.LoadKline(ctx, symbol, iv.Name, startMs)
			if err != nil {
				log.Printf("recover error: symbol=%s interval=%s err=%v", symbol, iv.Name, err)
				return
			}
			if k != nil {
				sb.mu.Lock()
				sb.data[iv.Name] = k
				sb.mu.Unlock()
				log.Printf("recovered bucket: symbol=%s interval=%s start=%d trades=%d",
					symbol, iv.Name, startMs, k.TradeCount)
			}
		}(iv)
	}
	wg.Wait()
}

// calcCloseTime returns the close timestamp for the window starting at start.
func calcCloseTime(start time.Time, name string, width time.Duration) int64 {
	if width > 0 {
		return start.Add(width - time.Millisecond).UnixMilli()
	}
	if name == "1mon" {
		return time.Date(start.Year(), start.Month()+1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1
	}
	return time.Date(start.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1
}

// StartCheckpoint starts a goroutine that periodically persists all open buckets.
func (a *Aggregator) StartCheckpoint(ctx context.Context, interval time.Duration) {
	ctx, a.checkpointCancel = context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.checkpoint(ctx)
			}
		}
	}()
	log.Printf("checkpoint started: interval=%v", interval)
}

func (a *Aggregator) checkpoint(ctx context.Context) {
	if !a.shouldWrite() {
		return
	}
	start := time.Now()

	type entry struct {
		symbol string
		klines []*model.Kline
	}
	var batch []entry
	totalKlines := 0

	// 1. Drain buffered completed klines (from window closes).
	//    These are merged into the same batch as open-bucket checkpoints
	//    to minimize the number of BatchSave calls.
	a.completedMu.Lock()
	for sym, klines := range a.completedBuf {
		batch = append(batch, entry{sym, klines})
		totalKlines += len(klines)
	}
	a.completedBuf = make(map[string][]*model.Kline)
	a.completedMu.Unlock()

	// 2. Collect open-bucket checkpoints.
	a.symbols.Range(func(key, value interface{}) bool {
		symbol := key.(string)
		sb := value.(*symbolBuckets)

		sb.mu.Lock()
		var toSave []*model.Kline
		for _, k := range sb.data {
			if k != nil && k.TradeCount > 0 {
				toSave = append(toSave, k)
			}
			if k != nil {
				metrics.SymbolBuckets.
					WithLabelValues(symbol, k.Interval).
					Set(float64(k.TradeCount))
			}
		}
		sb.mu.Unlock()

		if len(toSave) > 0 {
			batch = append(batch, entry{symbol, toSave})
			totalKlines += len(toSave)
		}
		return true
	})

	metrics.CheckpointKlines.Observe(float64(totalKlines))

	// Parallel checkpoint writes with bounded concurrency (8 goroutines).
	// Without this, 500 sequential BatchSave calls take ~2.5s, starving
	// the write pool for completed klines and HTTP queries.
	sem := make(chan struct{}, 8)
	var ckptWg sync.WaitGroup
	for _, e := range batch {
		sem <- struct{}{}
		ckptWg.Add(1)
		go func(e entry) {
			defer ckptWg.Done()
			defer func() { <-sem }()
			if err := a.repo.BatchSave(ctx, e.symbol, e.klines); err != nil {
				log.Printf("checkpoint error: symbol=%s err=%v", e.symbol, err)
			}
		}(e)
	}
	ckptWg.Wait()

	metrics.CheckpointDuration.Observe(time.Since(start).Seconds())
}

// Snapshot returns all currently open klines for a symbol (debugging).
func (a *Aggregator) Snapshot(symbol string) map[string]*model.Kline {
	sbRaw, ok := a.symbols.Load(symbol)
	if !ok {
		return nil
	}
	sb := sbRaw.(*symbolBuckets)
	sb.mu.Lock()
	defer sb.mu.Unlock()

	out := make(map[string]*model.Kline, len(sb.data))
	for k, v := range sb.data {
		out[k] = v
	}
	return out
}

// Close flushes all open klines and buffered completed klines before shutdown.
func (a *Aggregator) Close(ctx context.Context) error {
	if a.checkpointCancel != nil {
		a.checkpointCancel()
	}
	if !a.shouldWrite() {
		return nil
	}

	// Flush buffered completed klines first.
	a.completedMu.Lock()
	for sym, klines := range a.completedBuf {
		if err := a.repo.BatchSave(ctx, sym, klines); err != nil {
			log.Printf("flush completed error on close: %v", err)
		}
	}
	a.completedBuf = make(map[string][]*model.Kline)
	a.completedMu.Unlock()

	// Flush all open buckets.
	a.symbols.Range(func(key, value interface{}) bool {
		symbol := key.(string)
		sb := value.(*symbolBuckets)

		sb.mu.Lock()
		for _, k := range sb.data {
			if k != nil && k.TradeCount > 0 {
				if err := a.repo.BatchSave(ctx, symbol, []*model.Kline{k}); err != nil {
					log.Printf("flush error on close: %v", err)
				}
			}
		}
		sb.mu.Unlock()
		return true
	})
	return nil
}
