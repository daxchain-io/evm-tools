package metrics

import "github.com/prometheus/client_golang/prometheus"

// Webhook is the metric set for evm-sink-webhook, registered on a private
// registry so the suite controls exactly what is exposed. It mirrors the
// per-suite shape of the other sink (records consumed/filtered/forwarded/failed
// counters, a POST duration histogram, and retry/backoff/blocked gauges) with
// low-cardinality labels only (record type, coarse error_type) — no per-record
// key, URL, or secret-bearing value is ever a label.
type Webhook struct {
	reg *prometheus.Registry

	chainName string
	chainID   string

	// Process gauges.
	up      prometheus.Gauge
	workers prometheus.Gauge

	// Record counters.
	consumed  prometheus.Counter
	filtered  prometheus.Counter
	forwarded *prometheus.CounterVec // by record type
	failed    *prometheus.CounterVec // by error_type
	retries   prometheus.Counter

	// POST timing + backpressure.
	postDuration    prometheus.Histogram
	backoffSeconds  prometheus.Gauge
	blocked         prometheus.Gauge
	consecutiveFail prometheus.Gauge
}

// webhookTypeLabel is the only sink-specific label; the record type is drawn
// from the contract's bounded discriminator vocabulary, unlike a per-record key.
const webhookTypeLabel = "record_type"

// NewWebhook builds the Webhook sink metric set on a fresh registry. chainName
// and chainID are accepted for label parity with the producer sets, but a sink
// reads JSONL (it does not resolve a chain), so both are typically
// empty/"unknown" — the const labels are still attached so a scrape from a sink
// and a producer on the same dashboard line up.
func NewWebhook(chainName, chainID string) *Webhook {
	if chainID == "" {
		chainID = "unknown"
	}
	reg := prometheus.NewRegistry()
	w := &Webhook{reg: reg, chainName: chainName, chainID: chainID}
	base := prometheus.Labels{labelBlockchain: chainName, labelChainID: w.chainID}

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

	w.up = g("evm_sink_webhook_up", "Whether the webhook sink process is available (1) or not (0).")
	w.workers = g("evm_sink_webhook_workers", "Active sink workers/goroutines.")

	w.consumed = c("evm_sink_webhook_records_consumed_total", "Records read from stdin.")
	w.filtered = c("evm_sink_webhook_records_filtered_total", "Records dropped by the configured filters (not forwarded).")
	w.forwarded = cv("evm_sink_webhook_records_forwarded_total", "Records confirmed forwarded (HTTP 2xx), by record type.", []string{webhookTypeLabel})
	w.failed = cv("evm_sink_webhook_records_failed_total", "POST failures, by coarse error type.", []string{labelErrorType})
	w.retries = c("evm_sink_webhook_post_retries_total", "POST retry attempts after a transient failure.")

	w.postDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "evm_sink_webhook_post_duration_seconds",
		Help:        "Duration of each HTTP POST (confirmed delivery).",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(w.postDuration)

	w.backoffSeconds = g("evm_sink_webhook_backoff_duration_seconds", "Current retry backoff duration after a transient POST failure, in seconds.")
	w.blocked = g("evm_sink_webhook_post_blocked", "Whether the sink is currently blocked retrying a failing endpoint (1) or not (0).")
	w.consecutiveFail = g("evm_sink_webhook_consecutive_failures", "Current consecutive POST failure count.")

	return w
}

// Registry exposes the underlying registry (used by the HTTP handler and tests).
func (w *Webhook) Registry() *prometheus.Registry { return w.reg }
