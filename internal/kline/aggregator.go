package kline

import (
	"context"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"quote-ticker/internal/model"
)

// Repository defines the DB operations the aggregator needs.
type Repository interface {
	BatchSave(ctx context.Context, symbol string, klines []*model.Kline) error
	LoadKline(ctx context.Context, symbol, interval string, startTime int64) (*model.Kline, error)
	Query(ctx context.Context, symbol, interval string, startTime, endTime int64, limit int) ([]*model.Kline, error)
}

const numShards = 64

// shard holds a portion of the symbol→buckets map with its own lock.
type shard struct {
	mu      sync.Mutex
	buckets map[string]*symbolBuckets
}

func newShard() *shard {
	return &shard{buckets: make(map[string]*symbolBuckets)}
}

// symbolBuckets holds all interval buckets for one symbol.
type symbolBuckets struct {
	mu     sync.Mutex
	symbol string
	data   map[string]*model.Kline // interval -> open kline
}

func newSymbolBuckets(symbol string) *symbolBuckets {
	return &symbolBuckets{
		symbol: symbol,
		data:   make(map[string]*model.Kline, len(Intervals)),
	}
}

// shardKey hashes the symbol to a shard index.
func shardKey(symbol string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(symbol))
	return h.Sum32() % numShards
}

// FlushWorker handles async DB writes on a background goroutine.
type FlushWorker struct {
	ch   chan flushTask
	repo Repository
}

type flushTask struct {
	symbol string
	klines []*model.Kline
}

func NewFlushWorker(repo Repository, bufferSize int) *FlushWorker {
	w := &FlushWorker{
		ch:   make(chan flushTask, bufferSize),
		repo: repo,
	}
	go w.loop()
	return w
}

func (w *FlushWorker) loop() {
	for t := range w.ch {
		if len(t.klines) > 0 {
			if err := w.repo.BatchSave(context.Background(), t.symbol, t.klines); err != nil {
				log.Printf("async flush error: symbol=%s err=%v", t.symbol, err)
			}
		}
	}
}

func (w *FlushWorker) Enqueue(symbol string, klines []*model.Kline) {
	select {
	case w.ch <- flushTask{symbol: symbol, klines: klines}:
	default:
		log.Printf("async flush channel full, dropping %d klines for %s", len(klines), symbol)
	}
}

func (w *FlushWorker) Close() { close(w.ch) }

// Aggregator consumes trades and produces completed klines.
type Aggregator struct {
	repo      Repository
	flush     *FlushWorker
	shards    [numShards]*shard

	checkpointCancel context.CancelFunc
	isLeader         func() bool
}

// NewAggregator creates an aggregator.
func NewAggregator(repo Repository) *Aggregator {
	a := &Aggregator{
		repo: repo,
	}
	for i := range a.shards {
		a.shards[i] = newShard()
	}
	return a
}

// SetFlushWorker attaches an async DB writer (nil = synchronous writes).
func (a *Aggregator) SetFlushWorker(w *FlushWorker) { a.flush = w }

// SetLeaderChecker registers a callback to determine if this instance
// should persist klines to the database. Only the leader writes.
func (a *Aggregator) SetLeaderChecker(fn func() bool) { a.isLeader = fn }

func (a *Aggregator) shouldWrite() bool {
	return a.isLeader == nil || a.isLeader()
}

// ProcessTrade updates all kline intervals for the trade's symbol.
// Completed klines are flushed asynchronously.
func (a *Aggregator) ProcessTrade(ctx context.Context, trade model.Trade) error {
	t := time.UnixMilli(trade.Timestamp).UTC()
	completed := a.updateBuckets(ctx, trade, t)
	if a.shouldWrite() && len(completed) > 0 {
		if a.flush != nil {
			a.flush.Enqueue(trade.Symbol, completed)
		} else {
			if err := a.repo.BatchSave(ctx, trade.Symbol, completed); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Aggregator) updateBuckets(ctx context.Context, trade model.Trade, t time.Time) []*model.Kline {
	s := a.shards[shardKey(trade.Symbol)]
	s.mu.Lock()
	defer s.mu.Unlock()

	sb, ok := s.buckets[trade.Symbol]
	if !ok {
		sb = newSymbolBuckets(trade.Symbol)
		s.buckets[trade.Symbol] = sb
		// Try recovery outside symbolBuckets.mu to avoid nested lock.
		a.recoverLocked(ctx, sb, trade.Symbol, t)
	}

	sb.mu.Lock()
	defer sb.mu.Unlock()

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
// Called once per symbol on first trade. s.mu must NOT be held.
func (a *Aggregator) recoverLocked(ctx context.Context, sb *symbolBuckets, symbol string, t time.Time) {
	for _, iv := range Intervals {
		startMs := iv.AlignFn(t).UnixMilli()
		k, err := a.repo.LoadKline(ctx, symbol, iv.Name, startMs)
		if err != nil {
			log.Printf("recover error: symbol=%s interval=%s err=%v", symbol, iv.Name, err)
			continue
		}
		if k != nil {
			sb.mu.Lock()
			sb.data[iv.Name] = k
			sb.mu.Unlock()
			log.Printf("recovered bucket: symbol=%s interval=%s start=%d trades=%d",
				symbol, iv.Name, startMs, k.TradeCount)
		}
	}
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

	// Iterate all shards, collecting non-empty buckets.
	type entry struct {
		symbol string
		klines []*model.Kline
	}
	var batch []entry

	for i := range a.shards {
		s := a.shards[i]
		s.mu.Lock()
		for sym, sb := range s.buckets {
			sb.mu.Lock()
			var toSave []*model.Kline
			for _, k := range sb.data {
				if k != nil && k.TradeCount > 0 {
					toSave = append(toSave, k)
				}
			}
			sb.mu.Unlock()
			if len(toSave) > 0 {
				batch = append(batch, entry{sym, toSave})
			}
		}
		s.mu.Unlock()
	}

	for _, e := range batch {
		if a.flush != nil {
			a.flush.Enqueue(e.symbol, e.klines)
		} else {
			if err := a.repo.BatchSave(ctx, e.symbol, e.klines); err != nil {
				log.Printf("checkpoint error: symbol=%s err=%v", e.symbol, err)
			}
		}
	}
}

// Snapshot returns all currently open klines for a symbol (debugging).
func (a *Aggregator) Snapshot(symbol string) map[string]*model.Kline {
	s := a.shards[shardKey(symbol)]
	s.mu.Lock()
	defer s.mu.Unlock()

	sb, ok := s.buckets[symbol]
	if !ok {
		return nil
	}
	sb.mu.Lock()
	defer sb.mu.Unlock()

	out := make(map[string]*model.Kline, len(sb.data))
	for k, v := range sb.data {
		out[k] = v
	}
	return out
}

// Close flushes all open klines before shutdown (only if leader).
func (a *Aggregator) Close(ctx context.Context) error {
	if a.checkpointCancel != nil {
		a.checkpointCancel()
	}
	if !a.shouldWrite() {
		return nil
	}

	for i := range a.shards {
		s := a.shards[i]
		s.mu.Lock()
		for sym, sb := range s.buckets {
			sb.mu.Lock()
			for _, k := range sb.data {
				if k != nil && k.TradeCount > 0 {
					if a.flush != nil {
						a.flush.Enqueue(sym, []*model.Kline{k})
					} else {
						if err := a.repo.BatchSave(ctx, sym, []*model.Kline{k}); err != nil {
							log.Printf("flush error on close: %v", err)
						}
					}
				}
			}
			sb.mu.Unlock()
		}
		s.mu.Unlock()
	}

	if a.flush != nil {
		// Give flush worker time to drain.
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}
