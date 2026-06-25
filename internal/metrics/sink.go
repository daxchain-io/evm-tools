package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SinkMetrics is a generic delivery-sink metric set, shared by the newer sinks
// (evm-sink-aws-sqs, evm-sink-aws-sns, evm-sink-postgres). It registers the common
// records-consumed/delivered/failed counters, a delivery-duration histogram, and
// retry/backoff/blocked gauges — plus the standard Go runtime/process collectors
// and a build_info gauge — on a private registry under the given namespace prefix
// (e.g. "evm_sink_aws_sqs"). The kafka/webhook/file sinks predate this and keep
// their bespoke sets. Labels stay low-cardinality (record type, coarse
// error_type) — no per-record key or secret value is ever a label.
type SinkMetrics struct {
	reg *prometheus.Registry

	up      prometheus.Gauge
	workers prometheus.Gauge

	consumed    prometheus.Counter
	delivered   *prometheus.CounterVec // by record type
	failed      *prometheus.CounterVec // by error_type
	retries     prometheus.Counter
	quarantined prometheus.Counter

	deliverDuration prometheus.Histogram
	backoffSeconds  prometheus.Gauge
	blocked         prometheus.Gauge
	consecutiveFail prometheus.Gauge
}

const sinkTypeLabel = "record_type"

// NewSinkMetrics builds a generic sink metric set on a fresh registry. namespace
// is the per-tool metric prefix; chainName/chainID are attached as const labels
// for dashboard parity (a sink reads JSONL and resolves no chain, so chainID is
// typically "unknown").
func NewSinkMetrics(namespace, chainName, chainID string) *SinkMetrics {
	if chainID == "" {
		chainID = "unknown"
	}
	reg := prometheus.NewRegistry()
	m := &SinkMetrics{reg: reg}
	base := prometheus.Labels{labelBlockchain: chainName, labelChainID: chainID}
	registerCommon(reg, namespace, base)

	g := func(name, help string) prometheus.Gauge {
		x := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: base})
		reg.MustRegister(x)
		return x
	}
	c := func(name, help string) prometheus.Counter {
		x := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: base})
		reg.MustRegister(x)
		return x
	}
	cv := func(name, help string, labels []string) *prometheus.CounterVec {
		x := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: base}, labels)
		reg.MustRegister(x)
		return x
	}

	m.up = g(namespace+"_up", "Whether the sink process is available (1) or not (0).")
	m.workers = g(namespace+"_workers", "Active sink workers/goroutines.")
	m.consumed = c(namespace+"_records_consumed_total", "Records read from stdin.")
	m.delivered = cv(namespace+"_records_delivered_total", "Records confirmed delivered downstream, by record type.", []string{sinkTypeLabel})
	m.failed = cv(namespace+"_records_failed_total", "Delivery failures, by coarse error type.", []string{labelErrorType})
	m.retries = c(namespace+"_delivery_retries_total", "Delivery retry attempts after a transient failure.")
	m.quarantined = c(namespace+"_records_quarantined_total", "Poison records (unparseable/unsupported) routed to the dead-letter file instead of halting.")

	m.deliverDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        namespace + "_delivery_duration_seconds",
		Help:        "Duration of each confirmed downstream delivery.",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(m.deliverDuration)

	m.backoffSeconds = g(namespace+"_backoff_duration_seconds", "Current retry backoff after a transient delivery failure, in seconds.")
	m.blocked = g(namespace+"_delivery_blocked", "Whether the sink is currently blocked retrying a failing destination (1) or not (0).")
	m.consecutiveFail = g(namespace+"_consecutive_failures", "Current consecutive delivery failure count.")

	return m
}

// Registry exposes the underlying registry (used by the HTTP handler and tests).
func (m *SinkMetrics) Registry() *prometheus.Registry { return m.reg }

// SetUp marks the sink process available (true) or not.
func (m *SinkMetrics) SetUp(up bool) { m.up.Set(b2f(up)) }

// SetWorkers records the active sink worker count.
func (m *SinkMetrics) SetWorkers(n int) { m.workers.Set(float64(n)) }

// IncConsumed counts one record read from stdin.
func (m *SinkMetrics) IncConsumed() { m.consumed.Inc() }

// IncDelivered counts one record confirmed delivered of the given type.
func (m *SinkMetrics) IncDelivered(recordType string) { m.delivered.WithLabelValues(recordType).Inc() }

// IncFailed counts one delivery failure of the given coarse error type.
func (m *SinkMetrics) IncFailed(errorType string) { m.failed.WithLabelValues(errorType).Inc() }

// IncRetry counts one delivery retry attempt after a transient failure.
func (m *SinkMetrics) IncRetry() { m.retries.Inc() }

// IncQuarantined counts one poison record routed to the dead-letter file.
func (m *SinkMetrics) IncQuarantined() { m.quarantined.Inc() }

// ObserveDeliver records one delivery duration.
func (m *SinkMetrics) ObserveDeliver(d time.Duration) { m.deliverDuration.Observe(d.Seconds()) }

// SetBackoffSeconds records the current retry backoff.
func (m *SinkMetrics) SetBackoffSeconds(d time.Duration) { m.backoffSeconds.Set(d.Seconds()) }

// SetBlocked records whether the sink is currently blocked retrying a destination.
func (m *SinkMetrics) SetBlocked(blocked bool) { m.blocked.Set(b2f(blocked)) }

// SetConsecutiveFailures records the current consecutive delivery failure count.
func (m *SinkMetrics) SetConsecutiveFailures(n int) { m.consecutiveFail.Set(float64(n)) }

// SinkHealth adapts the shared *Health to a delivery sink's readiness surface:
// destination reachability maps onto the RPC-reachable signal and delivery-blocked
// onto the emit-blocked signal, reusing the same /readyz logic and HTTP server.
// Build the backing Health with a delivery-blocked threshold and a zero lag
// threshold (lag disabled).
type SinkHealth struct{ h *Health }

// NewSinkHealth wraps a Health for a delivery sink, starting reachable so /readyz
// is ready until a delivery actually fails.
func NewSinkHealth(h *Health) *SinkHealth {
	h.SetRPCReachable(true)
	return &SinkHealth{h: h}
}

// SetReachable records the latest destination reachability for /readyz.
func (s *SinkHealth) SetReachable(v bool) { s.h.SetRPCReachable(v) }

// SetEmitBlocked records how long delivery has been blocked retrying a failing
// destination; beyond the Health threshold /readyz flips not-ready.
func (s *SinkHealth) SetEmitBlocked(d time.Duration) { s.h.SetEmitBlocked(d) }
