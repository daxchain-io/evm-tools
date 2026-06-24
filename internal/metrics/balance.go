package metrics

import "github.com/prometheus/client_golang/prometheus"

// Balance is the metric set for evm-balance, registered on a private registry so
// the suite controls exactly what is exposed. It reuses the shared chain/RPC
// metrics plus exporter-aligned account/contract gauges driven by the configured
// [balance] entries (see docs/design.md, "Balance metrics").
//
// Per the project rules, address/name labels (account_*, contract_*, token_*)
// are attached only to per-entry metrics, so cardinality is bounded by config
// size; no per-transaction or secret-bearing value is ever a label.
type Balance struct {
	reg *prometheus.Registry

	chainName string
	chainID   string

	// Process gauges.
	up                      prometheus.Gauge
	configuredNative        prometheus.Gauge
	configuredERC20         prometheus.Gauge
	configuredERC721Balance prometheus.Gauge
	configuredERC721Owner   prometheus.Gauge
	configuredContract      prometheus.Gauge
	workers                 prometheus.Gauge

	// Chain health.
	headBlock          prometheus.Gauge
	headBlockTimestamp prometheus.Gauge
	timeSinceLastBlock prometheus.Gauge

	// Poll progress.
	lastSampledBlock   prometheus.Gauge
	lagBlocks          prometheus.Gauge
	emitBlockedSeconds prometheus.Gauge

	// Record counters.
	recordsEmitted prometheus.Counter
	sampleRecords  prometheus.Counter
	changeRecords  prometheus.Counter
	reconnects     prometheus.Counter

	// Account/contract state gauges (exporter-aligned).
	accountBalanceWei     *prometheus.GaugeVec
	accountBalanceEth     *prometheus.GaugeVec
	accountTokenBalRaw    *prometheus.GaugeVec
	accountTokenBalance   *prometheus.GaugeVec
	contractBalanceWei    *prometheus.GaugeVec
	contractBalanceEth    *prometheus.GaugeVec
	contractTokenSupply   *prometheus.GaugeVec
	contractTransferCount *prometheus.GaugeVec

	// RPC + poll cycle.
	rpcCallDuration *prometheus.HistogramVec
	rpcError        *prometheus.CounterVec
	pollDuration    prometheus.Histogram
	pollSuccess     prometheus.Gauge
	pollTimestamp   prometheus.Gauge
	consecutiveFail prometheus.Gauge
	backoffSeconds  prometheus.Gauge

	// Config reload.
	configReloads      prometheus.Counter
	configReloadErrors prometheus.Counter
}

// NewBalance builds the balance metric set on a fresh registry. As with the
// stream set, build it after chain.Resolve so chain_id is a stable const label.
func NewBalance(chainName, chainID string) *Balance {
	if chainID == "" {
		chainID = "unknown"
	}
	reg := prometheus.NewRegistry()
	b := &Balance{reg: reg, chainName: chainName, chainID: chainID}
	base := prometheus.Labels{labelBlockchain: chainName, labelChainID: b.chainID}
	registerCommon(reg, "evm_balance", base)

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
	gv := func(name, help string, labels []string) *prometheus.GaugeVec {
		m := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: base}, labels)
		reg.MustRegister(m)
		return m
	}

	b.up = g("evm_balance_up", "Whether the balance process is available (1) or not (0).")
	b.configuredNative = g("evm_balance_configured_native_accounts", "Number of configured native balance accounts.")
	b.configuredERC20 = g("evm_balance_configured_erc20_accounts", "Number of configured ERC-20 balance accounts.")
	b.configuredERC721Balance = g("evm_balance_configured_erc721_balances", "Number of configured ERC-721 balance (balance_of) entries.")
	b.configuredERC721Owner = g("evm_balance_configured_erc721_ownership", "Number of configured ERC-721 ownership entries.")
	b.configuredContract = g("evm_balance_configured_contracts", "Number of configured contract-state entries.")
	b.workers = g("evm_balance_workers", "Active poller workers/goroutines.")

	b.headBlock = g("evm_chain_head_block_number", "Latest block number reported by RPC.")
	b.headBlockTimestamp = g("evm_chain_head_block_timestamp_seconds", "Unix timestamp of the latest observed head block.")
	b.timeSinceLastBlock = g("evm_chain_time_since_last_block_seconds", "Wall-clock age of the latest head block, in seconds.")

	b.lastSampledBlock = g("evm_balance_last_sampled_block_number", "Highest block at which a sample was taken.")
	b.lagBlocks = g("evm_balance_lag_blocks", "Blocks the RPC head has advanced since the last sample (sampling staleness; informational — not a /readyz signal for the poller).")
	b.emitBlockedSeconds = g("evm_balance_emit_blocked_seconds", "Time the current or last stdout write has been blocked by downstream backpressure.")

	b.recordsEmitted = c("evm_balance_records_emitted_total", "Total JSONL records emitted.")
	b.sampleRecords = c("evm_balance_sample_records_emitted_total", "Sample records (*_sample) emitted.")
	b.changeRecords = c("evm_balance_change_records_emitted_total", "Change records (*_change) emitted.")
	b.reconnects = c("evm_balance_reconnects_total", "RPC reconnects after transport errors.")

	accountLabels := []string{labelAccountName, labelAccountAddr}
	tokenLabels := []string{labelAccountName, labelAccountAddr, labelTokenName, labelTokenAddr}
	contractLabels := []string{labelContractName, labelContractAddr}

	b.accountBalanceWei = gv("evm_account_balance_wei", "Native account balance in wei.", accountLabels)
	b.accountBalanceEth = gv("evm_account_balance_eth", "Native account balance in ether.", accountLabels)
	b.accountTokenBalRaw = gv("evm_account_token_balance_raw", "ERC-20 account balance in raw token units.", tokenLabels)
	b.accountTokenBalance = gv("evm_account_token_balance", "ERC-20 account balance in whole tokens (decimals applied).", tokenLabels)
	b.contractBalanceWei = gv("evm_contract_balance_wei", "Native contract balance in wei.", contractLabels)
	b.contractBalanceEth = gv("evm_contract_balance_eth", "Native contract balance in ether.", contractLabels)
	b.contractTokenSupply = gv("evm_contract_token_total_supply", "Token total supply for an ERC-compatible contract.", contractLabels)
	b.contractTransferCount = gv("evm_contract_transfer_count_window", "Transfers observed in the configured block window, by contract.", []string{labelContractName, labelContractAddr, labelWindowBlocks})

	b.rpcCallDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "evm_rpc_call_duration_seconds",
		Help:        "RPC call duration by operation.",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	}, []string{labelOperation})
	reg.MustRegister(b.rpcCallDuration)

	b.rpcError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "evm_rpc_errors_total",
		Help:        "RPC errors by operation and coarse error type.",
		ConstLabels: base,
	}, []string{labelOperation, labelErrorType})
	reg.MustRegister(b.rpcError)

	b.pollDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "evm_balance_poll_duration_seconds",
		Help:        "Duration of each sampling poll cycle.",
		Buckets:     rpcDurationBuckets,
		ConstLabels: base,
	})
	reg.MustRegister(b.pollDuration)

	b.pollSuccess = g("evm_balance_poll_success", "Whether the most recent sampling poll cycle succeeded (1) or failed (0).")
	b.pollTimestamp = g("evm_balance_poll_timestamp_seconds", "Unix timestamp of the most recent successful sampling poll cycle.")
	b.consecutiveFail = g("evm_balance_consecutive_failures", "Current consecutive failure count.")
	b.backoffSeconds = g("evm_balance_backoff_duration_seconds", "Retry backoff duration after failures, in seconds.")

	b.configReloads = c("evm_balance_config_reloads_total", "Successful SIGHUP config reloads that re-applied the watched target set.")
	b.configReloadErrors = c("evm_balance_config_reload_errors_total", "Failed SIGHUP config reloads (the previous configuration was kept).")

	return b
}

// Registry exposes the underlying registry (used by the HTTP handler and tests).
func (b *Balance) Registry() *prometheus.Registry { return b.reg }
