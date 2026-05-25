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
// Multiple goroutines share the same reader for partitioned topics.
type Consumer struct {
	reader  *kafkago.Reader
	handler TradeHandler
	workers int
	wg      sync.WaitGroup
}

// NewConsumer creates a Consumer.
//   - workers: number of goroutines sharing consumption (set >1 for partitioned topics).
//     Internally this controls a goroutine pool: 1 reader goroutine fans out to
//     N processor goroutines via a buffered channel.
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
	}
}

// Run starts the consumer loop. Blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	log.Printf("kafka consumer started: topic=%s group=%s workers=%d",
		c.reader.Config().Topic, c.reader.Config().GroupID, c.workers)

	tasks := make(chan []byte, c.workers*64)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start worker pool.
	c.wg.Add(c.workers)
	for i := 0; i < c.workers; i++ {
		go c.worker(ctx, tasks, &c.wg)
	}

	// Reader goroutine: fetch messages, send to channel.
	fetchErr := make(chan error, 1)
	go func() {
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				fetchErr <- err
				return
			}
			select {
			case tasks <- msg.Value:
			case <-ctx.Done():
				return
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit error: %v", err)
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

func (c *Consumer) worker(ctx context.Context, tasks <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-tasks:
			if err := c.process(data); err != nil {
				log.Printf("process error: %v", err)
			}
		}
	}
}

func (c *Consumer) process(data []byte) error {
	tick := &pb.PbTradeTick{}
	if err := proto.Unmarshal(data, tick); err != nil {
		return err
	}
	trade := model.NewTradeFromTick(tick)
	return c.handler(context.Background(), trade)
}

// Close shuts down the consumer.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
