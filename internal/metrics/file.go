package metrics

import "github.com/prometheus/client_golang/prometheus"

// File is the metric set for evm-sink-file, registered on a private registry so
// the suite controls exactly what is exposed. It mirrors the per-sink shape
// (records consumed/filtered/written/failed counters, a write-duration histogram,
// and retry/backoff/blocked gauges) plus file-specific rotation count and current
// active-file size, with low-cardinality labels only (record type, coarse
// error_type) — no per-record key, path, or secret-bearing value is ever a label.
type File struct {
	reg *prometheus.Registry

	chainName string
	chainID   string

	// Process gauges.
	up      prometheus.Gauge
	workers prometheus.Gauge

	// Record counters.
	consumed prometheus.Counter
	filtered prometheus.Counter
	written  *prometheus.CounterVec // by record type
	failed   *prometheus.CounterVec // by error_type
	retries  prometheus.Counter

	// Write timing + backpressure.
	writeDuration   prometheus.Histogram
	backoffSeconds  prometheus.Gauge
	blocked         prometheus.Gauge
	consecutiveFail prometheus.Gauge

	// File-specific.
	rotations   prometheus.Counter
	currentSize prometheus.Gauge
}

// fileTypeLabel is the only sink-specific label; the record type is drawn from
// the contract's bounded discriminator vocabulary, unlike a per-record key.
const fileTypeLabel = "record_type"

// NewFile builds the File sink metric set on a fresh registry. chainName and
// chainID are accepted for label parity with the producer sets, but a sink reads
// JSONL (it does not resolve a chain), so both are typically empty/"unknown" —
// the const labels are still attached so a scrape from a sink and a producer on
// the same dashboard line up.
func NewFile(chainName, chainID string) *File {
	if chainID == "" {
		chainID = "unknown"
	}
	reg := prometheus.NewRegistry()
	f := &File{reg: reg, chainName: chainName, chainID: chainID}
	base := prometheus.Labels{labelBlockchain: chainName, labelChainID: f.chainID}

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

	f.up = g("evm_sink_file_up", "Whether the file sink process is available (1) or not (0).")
	f.workers = g("evm_sink_file_workers", "Active sink workers/goroutines.")

	f.consumed = c("evm_sink_file_records_consumed_total", "Records read from stdin.")
	f.filtered = c("evm_sink_file_records_filtered_total", "Records dropped by the configured filters (not written).")
	f.written = cv("evm_sink_file_records_written_total", "Records confirmed written (and fsync'd when enabled), by record type.", []string{fileTypeLabel})
	f.failed = cv("evm_sink_file_records_failed_total", "Write failures, by coarse error type.", []string{labelErrorType})
	f.retries = c("evm_sink_file_write_retries_total", "Write retry attempts after a transient (disk-full) failure.")

	f.writeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "evm_sink_file_write_duration_seconds",
		Help:        "Duration of each record write (confirmed append, including fsync when enabled).",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(f.writeDuration)

	f.backoffSeconds = g("evm_sink_file_backoff_duration_seconds", "Current retry backoff duration after a transient write failure, in seconds.")
	f.blocked = g("evm_sink_file_write_blocked", "Whether the sink is currently blocked retrying a failing disk (1) or not (0).")
	f.consecutiveFail = g("evm_sink_file_consecutive_failures", "Current consecutive write failure count.")

	f.rotations = c("evm_sink_file_rotations_total", "Number of times the active file has been rotated.")
	f.currentSize = g("evm_sink_file_current_size_bytes", "Current size of the active output file in bytes.")

	return f
}

// Registry exposes the underlying registry (used by the HTTP handler and tests).
func (f *File) Registry() *prometheus.Registry { return f.reg }
