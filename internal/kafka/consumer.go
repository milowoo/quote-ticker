package kafka

import (
	"context"
	"log"
	"sync"
	"time"

	"quote-ticker/internal/config"
	"quote-ticker/internal/metrics"
	"quote-ticker/internal/model"
	"quote-ticker/internal/model/pb"
	"google.golang.org/protobuf/proto"

	kafkago "github.com/segmentio/kafka-go"
)

// TradeHandler processes a single trade.
type TradeHandler func(ctx context.Context, trade model.Trade) error

// task carries a Kafka message through the pipeline.
type task struct {
	msg  kafkago.Message
	data []byte
}

// Consumer reads trade ticks from Kafka and dispatches them.
// Uses a centralized offset committer to guarantee that only the highest
// CONTIGUOUS processed offset is committed per partition, preventing data
// loss when workers finish out of order.
type Consumer struct {
	reader  *kafkago.Reader
	handler TradeHandler
	workers int
	deduper *Deduper
	wg      sync.WaitGroup

	completed chan kafkago.Message // workers signal completion here
}

func NewConsumer(cfg config.KafkaConfig, handler TradeHandler) *Consumer {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    1,
		MaxBytes:    10 * 1024 * 1024,
		StartOffset: kafkago.LastOffset,
	})
	w := cfg.Parallel
	if w < 1 {
		w = 1
	}
	return &Consumer{
		reader:    r,
		handler:   handler,
		workers:   w,
		deduper:   NewDeduper(5 * time.Minute),
		completed: make(chan kafkago.Message, 16384),
	}
}

// ── Offset tracker ──────────────────────────────────────────────────────

// partOffsets tracks processed offsets per Kafka partition and finds the
// highest contiguous sequence for safe commits.
type partOffsets struct {
	committed int64               // last committed offset (-1 = none)
	pending   map[int64]struct{}  // processed but not yet committed
}

// highestContiguous returns the highest offset that can be safely committed
// (i.e. all offsets from committed+1 up to this are processed).
func (po *partOffsets) highestContiguous() int64 {
	next := po.committed + 1
	for {
		if _, ok := po.pending[next]; !ok {
			return next - 1
		}
		next++
	}
}

// ── Centralized committer ───────────────────────────────────────────────

// committer collects completed offsets from workers and periodically
// commits the highest contiguous offset per partition. This eliminates
// the data-loss scenario where a fast worker commits a later offset while
// an earlier message is still being processed.
func (c *Consumer) committer(ctx context.Context) {
	const commitInterval = 500 * time.Millisecond
	ticker := time.NewTicker(commitInterval)
	defer ticker.Stop()

	parts := make(map[int]*partOffsets)

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown.
			c.flushCommits(context.Background(), parts)
			return

		case msg := <-c.completed:
			p := msg.Partition
			po, ok := parts[p]
			if !ok {
				po = &partOffsets{
					committed: -1,
					pending:   make(map[int64]struct{}),
				}
				parts[p] = po
			}
			po.pending[msg.Offset] = struct{}{}

		case <-ticker.C:
			c.flushCommits(ctx, parts)
		}
	}
}

func (c *Consumer) flushCommits(ctx context.Context, parts map[int]*partOffsets) {
	for part, po := range parts {
		commitTo := po.highestContiguous()
		if commitTo <= po.committed {
			continue
		}
		// Commit a minimal message — CommitMessages only uses Partition + Offset.
		if err := c.reader.CommitMessages(ctx, kafkago.Message{
			Partition: part,
			Offset:    commitTo,
		}); err != nil {
			log.Printf("commit error: partition=%d offset=%d err=%v", part, commitTo, err)
			continue
		}
		po.committed = commitTo
		// GC: remove committed offsets from pending map.
		for o := range po.pending {
			if o <= commitTo {
				delete(po.pending, o)
			}
		}
	}
}

// ── Run loop ────────────────────────────────────────────────────────────

func (c *Consumer) Run(ctx context.Context) error {
	log.Printf("kafka consumer started: topic=%s group=%s workers=%d",
		c.reader.Config().Topic, c.reader.Config().GroupID, c.workers)

	const chanBuf = 16384 // absorb bursts up to 16K messages without blocking
	tasks := make(chan task, chanBuf)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start committer goroutine (commits processed offsets).
	go c.committer(ctx)

	// Start worker pool.
	c.wg.Add(c.workers)
	for i := 0; i < c.workers; i++ {
		go c.worker(ctx, tasks, &c.wg)
	}

	// Reader goroutine: fetch messages only — no commit.
	fetchErr := make(chan error, 1)
	go func() {
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				fetchErr <- err
				return
			}
			// Non-blocking send with context guard: if the channel is full
			// (workers can't keep up), we block but remain interruptible.
			// The channel is sized to absorb bursts up to 16K messages,
			// so blocking is rare and only happens under sustained overload.
			select {
			case tasks <- task{msg: msg, data: msg.Value}:
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		cancel()
		c.wg.Wait()
		return nil
	case err := <-fetchErr:
		cancel()
		c.wg.Wait()
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
}

// worker processes a task and signals completion to the committer.
// It does NOT commit offsets — that's the committer's job.
func (c *Consumer) worker(ctx context.Context, tasks <-chan task, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-tasks:
			metrics.KafkaMessages.WithLabelValues("received").Inc()
			start := time.Now()

			err := c.process(t.data)
			metrics.KafkaProcessingDuration.Observe(time.Since(start).Seconds())

			if err != nil {
				metrics.KafkaMessages.WithLabelValues("error").Inc()
				log.Printf("process error: %v", err)
				continue
			}

			metrics.KafkaMessages.WithLabelValues("processed").Inc()

			// Signal completion — the committer will handle the commit.
			select {
			case c.completed <- t.msg:
			default:
				log.Printf("warning: completed channel full, offset %d may delay commit", t.msg.Offset)
			}
		}
	}
}

func (c *Consumer) process(data []byte) error {
	tick := &pb.PbTradeTick{}
	if err := proto.Unmarshal(data, tick); err != nil {
		return err
	}
	if c.deduper.Seen(tick.TradeId) {
		metrics.KafkaMessages.WithLabelValues("deduped").Inc()
		return nil
	}
	trade := model.NewTradeFromTick(tick)
	return c.handler(context.Background(), trade)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
