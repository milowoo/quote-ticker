package kline

import (
	"context"
	"fmt"
	"log"
	"time"

	"quote-ticker/internal/model"
)

// ContinuityChecker periodically verifies kline data integrity and backfills
// missing longer-interval klines from 1m data.
type ContinuityChecker struct {
	repo     Repository
	symbols  SymbolLister
	isLeader func() bool
}

// SymbolLister discovers symbols that have kline tables.
type SymbolLister interface {
	ListSymbols(ctx context.Context) ([]string, error)
}

// NewContinuityChecker creates a continuity checker.
func NewContinuityChecker(repo Repository, symbols SymbolLister, leaderFn func() bool) *ContinuityChecker {
	return &ContinuityChecker{
		repo:     repo,
		symbols:  symbols,
		isLeader: leaderFn,
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

	symbols, err := c.symbols.ListSymbols(ctx)
	if err != nil {
		log.Printf("continuity: list symbols error: %v", err)
		return
	}
	log.Printf("continuity: checking %d symbols", len(symbols))

	now := time.Now().UnixMilli()
	for _, sym := range symbols {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.checkInterval(ctx, sym, "1m", now-3600_000, now); err != nil {
			log.Printf("continuity: %s 1m: %v", sym, err)
		}
		for _, iv := range []string{"10m", "30m", "1h"} {
			if err := c.checkAndBackfill(ctx, sym, iv, now-24*3600_000, now); err != nil {
				log.Printf("continuity: %s %s: %v", iv, sym, err)
			}
		}
	}
}

func (c *ContinuityChecker) checkInterval(ctx context.Context, symbol, interval string, from, to int64) error {
	klines, err := c.repo.Query(ctx, symbol, interval, from, to, 2000)
	if err != nil {
		return err
	}
	if len(klines) < 2 {
		return nil
	}

	var gapFound bool
	ivDef, _ := Lookup(interval)
	for i := 1; i < len(klines); i++ {
		expected := klines[i-1].StartTime + intervalMs(ivDef, klines[i-1].StartTime)
		if klines[i].StartTime != expected {
			log.Printf("continuity GAP: %s %s missing window start=%d", symbol, interval, expected)
			gapFound = true
		}
	}
	if gapFound {
		log.Printf("continuity: %s %s has gaps in [%d, %d]", symbol, interval, from, to)
	}
	return nil
}

func (c *ContinuityChecker) checkAndBackfill(ctx context.Context, symbol, interval string, from, to int64) error {
	klines, err := c.repo.Query(ctx, symbol, interval, from, to, 2000)
	if err != nil {
		return err
	}

	ivDef, ok := Lookup(interval)
	if !ok {
		return fmt.Errorf("unknown interval: %s", interval)
	}

	existing := make(map[int64]bool, len(klines))
	for _, k := range klines {
		existing[k.StartTime] = true
	}

	t := time.UnixMilli(from).UTC()
	end := time.UnixMilli(to).UTC()
	var missing []int64

	for t.Before(end) {
		startMs := ivDef.AlignFn(t).UnixMilli()
		if !existing[startMs] {
			missing = append(missing, startMs)
		}
		t = time.UnixMilli(startMs + intervalMs(ivDef, startMs)).UTC()
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
