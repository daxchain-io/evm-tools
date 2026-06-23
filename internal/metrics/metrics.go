// Package metrics holds the Prometheus registry, the stream metric set, the
// HTTP server that exposes them, and the /healthz + /readyz endpoints. The
// health endpoints are served independently of whether metrics scraping is
// enabled (see docs/design.md, "RPC Health Checks").
//
// Metric naming follows Prometheus convention and the project rules: counters
// end in _total, gauges carry no suffix, durations are _seconds histograms.
// Labels are drawn only from the enumerated low-cardinality vocabulary —
// per-transaction identifiers (tx_hash, log_index, transfer from/to) and any
// secret-bearing value (RPC URL, tokens, mTLS material) are never labels.
package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// ErrNotImplemented is retained for callers that referenced the scaffold.
var ErrNotImplemented = errors.New("metrics: not implemented")

// Label names — the shared low-cardinality vocabulary.
const (
	labelBlockchain   = "blockchain"
	labelChainID      = "chain_id"
	labelOperation    = "operation"
	labelErrorType    = "error_type"
	labelContractName = "contract_name"
	labelContractAddr = "contract_address"
	labelEventName    = "event_name"
	labelAccountName  = "account_name"
	labelAccountAddr  = "account_address"
	labelTokenName    = "token_name"
	labelTokenAddr    = "token_address"
)

// rpcDurationBuckets cover sub-millisecond health checks through multi-second
// chunked log queries.
var rpcDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
}

// logChunkBlockBuckets cover small head-following deltas through large backfill
// chunks.
var logChunkBlockBuckets = []float64{1, 10, 50, 100, 500, 1000, 2000, 5000, 10000}

// Stream is the metric set for evm-stream, registered on a private registry so
// the suite controls exactly what is exposed.
type Stream struct {
	reg *prometheus.Registry

	chainName string
	chainID   string // resolved chain ID or "unknown"

	// Process gauges.
	up                       prometheus.Gauge
	configuredContracts      prometheus.Gauge
	configuredNativeTransfer prometheus.Gauge
	workers                  prometheus.Gauge

	// Chain health.
	headBlock          prometheus.Gauge
	finalizedBlock     prometheus.Gauge
	headBlockTimestamp prometheus.Gauge
	timeSinceLastBlock prometheus.Gauge

	// Stream progress.
	lastProcessedBlock prometheus.Gauge
	lastEmittedBlock   prometheus.Gauge
	lagBlocks          prometheus.Gauge
	emitBlockedSeconds prometheus.Gauge

	// Record counters.
	recordsEmitted          prometheus.Counter
	eventRecordsEmitted     prometheus.Counter
	contractEventRecords    *prometheus.CounterVec
	nativeTransferRecords   prometheus.Counter
	internalTransferRecords prometheus.Counter
	internalTraceSkipped    prometheus.Counter
	internalDisabled        prometheus.Gauge
	skippedLogs             prometheus.Counter
	reorgsDetected          prometheus.Counter
	reconnects              prometheus.Counter
	configReloads           prometheus.Counter
	configReloadErrors      prometheus.Counter

	// RPC + loop.
	rpcCallDuration *prometheus.HistogramVec
	rpcError        *prometheus.CounterVec
	loopDuration    prometheus.Histogram
	consecutiveFail prometheus.Gauge
	backoffSeconds  prometheus.Gauge

	// Log query.
	logChunksCreated prometheus.Counter
	logChunkBlocks   prometheus.Histogram
	logChunkDuration prometheus.Histogram
}

// NewStream builds the stream metric set on a fresh registry. chainName is the
// configured chain label and chainID is the resolved EVM chain ID label, or
// "unknown" when the set is built before resolution. Because chain_id is a
// const label, build the set after [chain.Resolve] so the value is stable for
// the process lifetime (config is loaded once at startup — see docs/design.md,
// "Metrics"). Pass "unknown" only when intentionally exposing pre-resolution
// metrics.
func NewStream(chainName, chainID string) *Stream {
	if chainID == "" {
		chainID = "unknown"
	}
	reg := prometheus.NewRegistry()
	s := &Stream{
		reg:       reg,
		chainName: chainName,
		chainID:   chainID,
	}
	base := prometheus.Labels{labelBlockchain: chainName, labelChainID: s.chainID}
	registerCommon(reg, "evm_stream", base)

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

	s.up = g("evm_stream_up", "Whether the stream process is available (1) or not (0).")
	s.configuredContracts = g("evm_stream_configured_contracts", "Number of enabled stream contracts.")
	s.configuredNativeTransfer = g("evm_stream_configured_native_transfers", "Whether native transfer monitoring is enabled (1) or not (0).")
	s.workers = g("evm_stream_workers", "Active stream workers/goroutines owned by monitors.")

	s.headBlock = g("blockchain_chain_head_block_number", "Latest block number reported by RPC.")
	s.finalizedBlock = g("blockchain_chain_finalized_block_number", "Finalized block number when the RPC endpoint supports it (reserved: populated once finality signaling lands; see design Open Question 4).")
	s.headBlockTimestamp = g("blockchain_chain_head_block_timestamp_seconds", "Unix timestamp of the latest observed head block.")
	s.timeSinceLastBlock = g("blockchain_chain_time_since_last_block_seconds", "Wall-clock age of the latest head block, in seconds.")

	s.lastProcessedBlock = g("evm_stream_last_processed_block_number", "Highest block processed.")
	s.lastEmittedBlock = g("evm_stream_last_emitted_block_number", "Highest block that produced at least one emitted record.")
	s.lagBlocks = g("evm_stream_lag_blocks", "Difference between RPC head and last processed block.")
	s.emitBlockedSeconds = g("evm_stream_emit_blocked_seconds", "Time the current or last stdout write has been blocked by downstream backpressure.")

	s.recordsEmitted = c("evm_stream_records_emitted_total", "Total JSONL records emitted.")
	s.eventRecordsEmitted = c("evm_stream_event_records_emitted_total", "Contract event records emitted.")
	s.nativeTransferRecords = c("evm_stream_native_transfer_records_emitted_total", "Native transfer records emitted.")
	s.internalTransferRecords = c("evm_stream_internal_transfer_records_emitted_total", "Internal (trace-derived) native transfer records emitted.")
	s.internalTraceSkipped = c("evm_stream_internal_trace_blocks_skipped_total", "Blocks whose internal transfers were skipped after repeated trace failures (best-effort).")
	s.internalDisabled = g("evm_stream_internal_transfers_disabled", "Whether internal-transfer detection self-disabled because the node lacks trace RPC (1) or not (0).")
	s.skippedLogs = c("evm_stream_logs_skipped_total", "Logs matched by the filter but not decodable to the configured event ABI (skipped, not emitted) — a signal of an ABI/config mismatch.")
	s.reorgsDetected = c("evm_stream_reorgs_detected_total", "Detected chain reorganizations.")
	s.reconnects = c("evm_stream_reconnects_total", "RPC reconnects after transport errors.")
	s.configReloads = c("evm_stream_config_reloads_total", "Successful SIGHUP config reloads that re-applied the watched contract set.")
	s.configReloadErrors = c("evm_stream_config_reload_errors_total", "Failed SIGHUP config reloads (the previous configuration was kept).")

	s.contractEventRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "evm_stream_contract_event_records_emitted_total",
		Help:        "Contract event records by configured contract and event name.",
		ConstLabels: base,
	}, []string{labelContractName, labelContractAddr, labelEventName})
	reg.MustRegister(s.contractEventRecords)

	s.rpcCallDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "blockchain_rpc_call_duration_seconds",
		Help:        "RPC call duration by operation.",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	}, []string{labelOperation})
	reg.MustRegister(s.rpcCallDuration)

	s.rpcError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "blockchain_rpc_error_total",
		Help:        "RPC errors by operation and coarse error type.",
		ConstLabels: base,
	}, []string{labelOperation, labelErrorType})
	reg.MustRegister(s.rpcError)

	s.loopDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "evm_stream_loop_duration_seconds",
		Help:        "Duration of each poll loop.",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(s.loopDuration)

	s.consecutiveFail = g("evm_stream_consecutive_failures", "Current consecutive failure count.")
	s.backoffSeconds = g("evm_stream_backoff_duration_seconds", "Retry backoff duration after failures, in seconds.")

	s.logChunksCreated = c("blockchain_log_chunks_created_total", "Log query chunks created.")
	s.logChunkBlocks = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "blockchain_log_chunk_blocks",
		Help:        "Histogram of blocks covered per log chunk.",
		Buckets:     logChunkBlockBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(s.logChunkBlocks)
	s.logChunkDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "blockchain_log_chunk_duration_seconds",
		Help:        "Duration of each log chunk query.",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(s.logChunkDuration)

	return s
}

// Registry exposes the underlying registry (used by the HTTP handler and tests).
func (s *Stream) Registry() *prometheus.Registry { return s.reg }
