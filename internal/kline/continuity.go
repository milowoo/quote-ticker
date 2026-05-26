package kline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"quote-ticker/internal/model"
)

const continuityWorkers = 8 // parallel check goroutines

// ContinuityChecker periodically verifies kline data integrity and backfills
// missing longer-interval klines from 1m data.
//
// Optimizations:
//   - Only checks symbols that received trades since last check (DrainDirty)
//   - Parallel checking across symbols (8 workers)
//   - Scans only the last N klines, not full time ranges
type ContinuityChecker struct {
	repo        Repository
	drainDirty  func() []string
	isLeader    func() bool
	checkWindow int // number of recent klines to check per interval
}

// NewContinuityChecker creates a continuity checker.
//   - drainDirty: returns symbols that have new trades since last call
//   - checkWindow: number of recent klines to validate per interval (default 10)
func NewContinuityChecker(repo Repository, drainDirty func() []string, leaderFn func() bool) *ContinuityChecker {
	return &ContinuityChecker{
		repo:       repo,
		drainDirty: drainDirty,
		isLeader:   leaderFn,
		checkWindow: 10,
	}
}

// Run starts the periodic check loop. Blocks until ctx is cancelled.
func (c *ContinuityChecker) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	c.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runOnce(ctx)
		}
	}
}

func (c *ContinuityChecker) runOnce(ctx context.Context) {
	if c.isLeader != nil && !c.isLeader() {
		return
	}

	symbols := c.drainDirty()
	if len(symbols) == 0 {
		return
	}

	if c.checkWindow <= 0 {
		c.checkWindow = 10
	}

	log.Printf("continuity: checking %d active symbols (parallel=%d)", len(symbols), continuityWorkers)

	// Parallel check with bounded goroutines.
	symCh := make(chan string, len(symbols))
	for _, sym := range symbols {
		symCh <- sym
	}
	close(symCh)

	var wg sync.WaitGroup
	wg.Add(continuityWorkers)
	for i := 0; i < continuityWorkers; i++ {
		go c.checkWorker(ctx, symCh, &wg)
	}
	wg.Wait()
}

func (c *ContinuityChecker) checkWorker(ctx context.Context, symCh <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for sym := range symCh {
		select {
		case <-ctx.Done():
			return
		default:
		}
		c.checkSymbol(ctx, sym)
	}
}

func (c *ContinuityChecker) checkSymbol(ctx context.Context, symbol string) {
	// Check 1m klines — only the last checkWindow records.
	if err := c.checkInterval(ctx, symbol, "1m"); err != nil {
		log.Printf("continuity: %s 1m: %v", symbol, err)
	}

	// Longer intervals — check last checkWindow and backfill from 1m cache/DB.
	for _, iv := range []string{"10m", "30m", "1h"} {
		if err := c.checkAndBackfill(ctx, symbol, iv); err != nil {
			log.Printf("continuity: %s %s: %v", iv, symbol, err)
		}
	}
}

// checkInterval fetches the last checkWindow klines and verifies no gaps.
func (c *ContinuityChecker) checkInterval(ctx context.Context, symbol, interval string) error {
	// Fetch a bit more than checkWindow to account for partial windows.
	klines, err := c.repo.Query(ctx, symbol, interval, 0, time.Now().UnixMilli(), c.checkWindow*2)
	if err != nil {
		return err
	}
	if len(klines) < 2 {
		return nil
	}

	// Take only the last checkWindow.
	if len(klines) > c.checkWindow {
		klines = klines[len(klines)-c.checkWindow:]
	}

	ivDef, _ := Lookup(interval)
	var gapFound bool
	for i := 1; i < len(klines); i++ {
		expected := klines[i-1].StartTime + intervalMs(ivDef, klines[i-1].StartTime)
		if klines[i].StartTime != expected {
			log.Printf("continuity GAP: %s %s missing window start=%d (prev=%d, cur=%d)",
				symbol, interval, expected, klines[i-1].StartTime, klines[i].StartTime)
			gapFound = true
		}
	}
	if gapFound {
		log.Printf("continuity: %s %s has gaps — will attempt backfill", symbol, interval)
	}
	return nil
}

// checkAndBackfill verifies the last checkWindow klines and reconstructs
// any missing windows from 1m data.
func (c *ContinuityChecker) checkAndBackfill(ctx context.Context, symbol, interval string) error {
	now := time.Now().UnixMilli()

	klines, err := c.repo.Query(ctx, symbol, interval, 0, now, c.checkWindow*2)
	if err != nil {
		return err
	}

	ivDef, ok := Lookup(interval)
	if !ok {
		return fmt.Errorf("unknown interval: %s", interval)
	}

	// Build a set of existing start_times.
	existing := make(map[int64]bool, len(klines))
	for _, k := range klines {
		existing[k.StartTime] = true
	}

	// Scan backward from now, checking each expected window.
	var missing []int64
	t := time.UnixMilli(now).UTC()
	checked := 0

	for checked < c.checkWindow {
		startMs := ivDef.AlignFn(t).UnixMilli()
		if !existing[startMs] {
			missing = append(missing, startMs)
		}
		// Move backward.
		t = time.UnixMilli(startMs - 1).UTC()
		checked++
	}

	if len(missing) == 0 {
		return nil
	}

	log.Printf("continuity: backfilling %s %s missing %d windows", symbol, interval, len(missing))

	for _, ws := range missing {
		winEnd := ws + intervalMs(ivDef, ws)
		oneMKlines, err := c.repo.Query(ctx, symbol, "1m", ws, winEnd, 1000)
		if err != nil {
			return err
		}
		if len(oneMKlines) == 0 {
			continue
		}

		merged := mergeOneMKlines(oneMKlines, interval, ws, winEnd)
		if merged == nil {
			continue
		}
		if err := c.repo.BatchSave(ctx, symbol, []*model.Kline{merged}); err != nil {
			return err
		}
	}
	return nil
}

func mergeOneMKlines(klines []*model.Kline, interval string, windowStart, windowEnd int64) *model.Kline {
	if len(klines) == 0 {
		return nil
	}

	ivDef, _ := Lookup(interval)
	start := time.UnixMilli(windowStart).UTC()

	merged := &model.Kline{
		Interval:    interval,
		StartTime:   windowStart,
		CloseTime:   calcCloseTime(start, ivDef.Name, ivDef.Width),
		Open:        klines[0].Open,
		High:        klines[0].High,
		Low:         klines[0].Low,
		Close:       klines[len(klines)-1].Close,
		Volume:      0,
		Amount:      0,
		WeightedAvg: 0,
		CreatedAt:   time.Now().UnixMilli(),
		UpdatedAt:   time.Now().UnixMilli(),
	}

	for _, k := range klines {
		if k.High.Cmp(merged.High) > 0 {
			merged.High = k.High
		}
		if k.Low.Cmp(merged.Low) < 0 {
			merged.Low = k.Low
		}
		merged.Volume = merged.Volume.Add(k.Volume)
		merged.Amount = merged.Amount.Add(k.Amount)
		merged.TradeCount += k.TradeCount
		merged.BuyTakerVol = merged.BuyTakerVol.Add(k.BuyTakerVol)
		merged.BuyTakerAmt = merged.BuyTakerAmt.Add(k.BuyTakerAmt)
	}

	if merged.Volume.Sign() > 0 {
		merged.WeightedAvg = merged.Amount.Quo(merged.Volume)
	}
	return merged
}

func intervalMs(iv Interval, startTime int64) int64 {
	if iv.Width > 0 {
		return iv.Width.Milliseconds()
	}
	if iv.Name == "1mon" {
		t := time.UnixMilli(startTime).UTC()
		next := time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		return next.UnixMilli() - startTime
	}
	t := time.UnixMilli(startTime).UTC()
	next := time.Date(t.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
	return next.UnixMilli() - startTime
}
