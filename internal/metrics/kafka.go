package metrics

import "github.com/prometheus/client_golang/prometheus"

// Kafka is the metric set for evm-sink-kafka, registered on a private registry so
// the suite controls exactly what is exposed. It mirrors the per-suite shape of
// the producer sets: record consumed/published/failed counters, a publish
// duration histogram, and retry/backoff/blocked gauges, with low-cardinality
// labels only (topic, coarse error_type) — no per-record key or secret-bearing
// value is ever a label.
type Kafka struct {
	reg *prometheus.Registry

	chainName string
	chainID   string

	// Process gauges.
	up      prometheus.Gauge
	workers prometheus.Gauge

	// Record counters.
	consumed  prometheus.Counter
	published *prometheus.CounterVec // by topic
	failed    *prometheus.CounterVec // by error_type
	retries   prometheus.Counter

	// Publish timing + backpressure.
	publishDuration prometheus.Histogram
	backoffSeconds  prometheus.Gauge
	blocked         prometheus.Gauge
	consecutiveFail prometheus.Gauge
}

// kafkaTopicLabel is the only sink-specific label; topic is operator-configured
// (bounded cardinality), unlike a per-record key.
const kafkaTopicLabel = "topic"

// NewKafka builds the Kafka sink metric set on a fresh registry. chainName and
// chainID are accepted for label parity with the producer sets, but a sink reads
// JSONL (it does not resolve a chain), so both are typically empty/"unknown" —
// the const labels are still attached so a scrape from a sink and a producer on
// the same dashboard line up.
func NewKafka(chainName, chainID string) *Kafka {
	if chainID == "" {
		chainID = "unknown"
	}
	reg := prometheus.NewRegistry()
	k := &Kafka{reg: reg, chainName: chainName, chainID: chainID}
	base := prometheus.Labels{labelBlockchain: chainName, labelChainID: k.chainID}
	registerCommon(reg, "evm_sink_kafka", base)

	g := func(name, help string) prometheus.Gauge {
		m := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: base})
		reg.MustRegister(m)
		return m
	}
	c := func(name, help string) prometheus.Counter {
		m := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: base})
		reg.MustRegister(m)
		return m
	}
	cv := func(name, help string, labels []string) *prometheus.CounterVec {
		m := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: base}, labels)
		reg.MustRegister(m)
		return m
	}

	k.up = g("evm_sink_kafka_up", "Whether the kafka sink process is available (1) or not (0).")
	k.workers = g("evm_sink_kafka_workers", "Active sink workers/goroutines.")

	k.consumed = c("evm_sink_kafka_records_consumed_total", "Records read from stdin.")
	k.published = cv("evm_sink_kafka_records_published_total", "Records confirmed published, by topic.", []string{kafkaTopicLabel})
	k.failed = cv("evm_sink_kafka_records_failed_total", "Publish failures, by coarse error type.", []string{labelErrorType})
	k.retries = c("evm_sink_kafka_publish_retries_total", "Publish retry attempts after a transient failure.")

	k.publishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "evm_sink_kafka_publish_duration_seconds",
		Help:        "Duration of each broker publish (confirmed write).",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(k.publishDuration)

	k.backoffSeconds = g("evm_sink_kafka_backoff_duration_seconds", "Current retry backoff duration after a transient publish failure, in seconds.")
	k.blocked = g("evm_sink_kafka_publish_blocked", "Whether the sink is currently blocked retrying a failing broker (1) or not (0).")
	k.consecutiveFail = g("evm_sink_kafka_consecutive_failures", "Current consecutive publish failure count.")

	return k
}

// Registry exposes the underlying registry (used by the HTTP handler and tests).
func (k *Kafka) Registry() *prometheus.Registry { return k.reg }
