package kafka

import (
	"context"
	"log"
	"sync"

	"quote-ticker/internal/config"
	"quote-ticker/internal/model"
	"quote-ticker/internal/model/pb"
	"google.golang.org/protobuf/proto"

	kafkago "github.com/segmentio/kafka-go"
)

// TradeHandler processes a single trade.
type TradeHandler func(ctx context.Context, trade model.Trade) error

// Consumer reads trade ticks from Kafka and dispatches them.
// Uses at-least-once delivery + dedup for exactly-once semantics.
type Consumer struct {
	reader  *kafkago.Reader
	handler TradeHandler
	workers int
	deduper *Deduper
	wg      sync.WaitGroup
}

// task carries a message and its parsed form through the pipeline.
type task struct {
	msg   kafkago.Message
	data  []byte
}

// NewConsumer creates a Consumer.
//   - workers: number of goroutines sharing consumption.
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
		reader:  r,
		handler: handler,
		workers: w,
		deduper: NewDeduper(1_000_000), // ~16MB, ~5 min window
	}
}

// Run starts the consumer loop. Blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	log.Printf("kafka consumer started: topic=%s group=%s workers=%d",
		c.reader.Config().Topic, c.reader.Config().GroupID, c.workers)

	tasks := make(chan task, c.workers*64)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

// worker pulls tasks from the channel, processes them, and commits.
func (c *Consumer) worker(ctx context.Context, tasks <-chan task, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-tasks:
			shouldCommit := true

			if err := c.process(t.data); err != nil {
				log.Printf("process error: %v", err)
				shouldCommit = false
			}

			if shouldCommit {
				if err := c.reader.CommitMessages(ctx, t.msg); err != nil {
					log.Printf("commit error (will retry on rebalance): %v", err)
				}
			}
		}
	}
}

func (c *Consumer) process(data []byte) error {
	tick := &pb.PbTradeTick{}
	if err := proto.Unmarshal(data, tick); err != nil {
		return err
	}

	// Idempotent check: skip known trade IDs.
	if c.deduper.Seen(tick.TradeId) {
		return nil
	}

	trade := model.NewTradeFromTick(tick)
	return c.handler(context.Background(), trade)
}

// Close shuts down the consumer.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
