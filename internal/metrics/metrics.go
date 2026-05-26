// Package metrics provides Prometheus instrumentation for quote-ticker.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const ns = "quote_ticker"

// ── Kafka ──────────────────────────────────────────────────────────────

var (
	KafkaMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "kafka_messages_total",
		Help:      "Kafka messages by status (received, processed, deduped, error).",
	}, []string{"status"})

	KafkaProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns,
		Name:      "kafka_processing_duration_seconds",
		Help:      "Time to process one Kafka message (unmarshal → handler).",
		Buckets:   prometheus.ExponentialBuckets(1e-6, 4, 8), // 1µs ~ 16ms
	})
)

// ── Trades / Klines ────────────────────────────────────────────────────

var (
	TradesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "trades_total",
		Help:      "Trades received by symbol.",
	}, []string{"symbol"})

	KlinesWrittenTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "klines_written_total",
		Help:      "Klines flushed to DB by interval.",
	}, []string{"interval"})

	ProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns,
		Name:      "processing_duration_seconds",
		Help:      "Time to update all intervals for one trade.",
		Buckets:   prometheus.ExponentialBuckets(1e-6, 4, 8),
	}, []string{"symbol"})

	SymbolBuckets = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "open_buckets",
		Help:      "Open kline buckets per symbol × interval.",
	}, []string{"symbol", "interval"})
)

// ── WebSocket ───────────────────────────────────────────────────────────

var (
	WSConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "ws_connections",
		Help:      "Active WebSocket connections.",
	})

	WSSubscriptions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "ws_subscriptions",
		Help:      "Active subscriptions per symbol.",
	}, []string{"symbol"})
)

// ── Leader ──────────────────────────────────────────────────────────────

var (
	Leader = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "leader",
		Help:      "1 if this instance is the leader, 0 otherwise.",
	})
)

// ── Checkpoint ──────────────────────────────────────────────────────────

var (
	CheckpointDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns,
		Name:      "checkpoint_duration_seconds",
		Help:      "Time to flush one checkpoint batch.",
		Buckets:   prometheus.ExponentialBuckets(1e-4, 4, 8), // 100µs ~ 1.6s
	})

	CheckpointKlines = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace: ns,
		Name:      "checkpoint_klines",
		Help:      "Number of klines flushed per checkpoint cycle.",
	})
)
