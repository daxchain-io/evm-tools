package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// SetUp marks the process available (true) or not.
func (s *Stream) SetUp(up bool) { s.up.Set(b2f(up)) }

// SetConfiguredContracts records the number of enabled stream contracts.
func (s *Stream) SetConfiguredContracts(n int) { s.configuredContracts.Set(float64(n)) }

// SetConfiguredNativeTransfers records whether native transfer monitoring is on.
func (s *Stream) SetConfiguredNativeTransfers(enabled bool) {
	s.configuredNativeTransfer.Set(b2f(enabled))
}

// SetWorkers records the active monitor worker count.
func (s *Stream) SetWorkers(n int) { s.workers.Set(float64(n)) }

// SetHead records the latest RPC head block number.
func (s *Stream) SetHead(n uint64) { s.headBlock.Set(float64(n)) }

// SetFinalizedBlock records the chain's finalized block number. The poll loop
// fetches the "finalized" block tag each poll (best-effort) and publishes it
// here; on a chain without finality (some L2s, dev nodes) the tag is unsupported
// and the gauge stays at 0. The additive `finalized` envelope field remains
// deferred (design.md Open Question 4 — "Finality signaling").
func (s *Stream) SetFinalizedBlock(n uint64) { s.finalizedBlock.Set(float64(n)) }

// SetHeadBlockTime records the head block timestamp and its wall-clock age.
func (s *Stream) SetHeadBlockTime(t time.Time, now time.Time) {
	s.headBlockTimestamp.Set(float64(t.Unix()))
	age := now.Sub(t).Seconds()
	if age < 0 {
		age = 0
	}
	s.timeSinceLastBlock.Set(age)
}

// SetLastProcessedBlock records the highest processed block.
func (s *Stream) SetLastProcessedBlock(n uint64) { s.lastProcessedBlock.Set(float64(n)) }

// SetLastEmittedBlock records the highest block that emitted a record.
func (s *Stream) SetLastEmittedBlock(n uint64) { s.lastEmittedBlock.Set(float64(n)) }

// SetLagBlocks records head-minus-processed lag.
func (s *Stream) SetLagBlocks(lag uint64) { s.lagBlocks.Set(float64(lag)) }

// SetEmitBlockedSeconds records how long the current/last stdout write blocked.
func (s *Stream) SetEmitBlockedSeconds(sec float64) { s.emitBlockedSeconds.Set(sec) }

// IncEventRecord counts one emitted contract event record, both overall and by
// configured contract/event.
func (s *Stream) IncEventRecord(contractName, contractAddr, eventName string) {
	s.recordsEmitted.Inc()
	s.eventRecordsEmitted.Inc()
	s.contractEventRecords.WithLabelValues(contractName, contractAddr, eventName).Inc()
}

// IncSkippedLog counts one filter-matched log that could not be decoded to the
// configured event ABI (skipped, not emitted).
func (s *Stream) IncSkippedLog() { s.skippedLogs.Inc() }

// IncConfigReload counts one successful SIGHUP config reload.
func (s *Stream) IncConfigReload() { s.configReloads.Inc() }

// IncConfigReloadError counts one failed SIGHUP config reload (old config kept).
func (s *Stream) IncConfigReloadError() { s.configReloadErrors.Inc() }

// ResetContractSeries removes the per-contract event-record series for a contract
// that a reload removed/disabled, so a stale counter no longer lingers on the
// endpoint (design "Metrics": removed entries are reset). It deletes every
// (event) series under the contract via a partial label match.
func (s *Stream) ResetContractSeries(contractName, contractAddr string) {
	s.contractEventRecords.DeletePartialMatch(prometheus.Labels{
		labelContractName: contractName,
		labelContractAddr: contractAddr,
	})
}

// IncNativeTransferRecord counts one emitted native transfer record.
func (s *Stream) IncNativeTransferRecord() {
	s.recordsEmitted.Inc()
	s.nativeTransferRecords.Inc()
}

// IncInternalTransferRecord counts one emitted internal (trace-derived) transfer.
func (s *Stream) IncInternalTransferRecord() {
	s.recordsEmitted.Inc()
	s.internalTransferRecords.Inc()
}

// IncInternalTraceSkipped counts one block whose internal transfers were skipped
// after repeated trace failures (best-effort; the core stream still advanced).
func (s *Stream) IncInternalTraceSkipped() { s.internalTraceSkipped.Inc() }

// SetInternalTransfersDisabled records whether internal-transfer detection
// self-disabled because the node does not expose trace RPC.
func (s *Stream) SetInternalTransfersDisabled(disabled bool) { s.internalDisabled.Set(b2f(disabled)) }

// IncReorgsDetected counts a chain reorg detected near the head (emitted with a
// reorg marker and a re-scan of the canonical chain; see internal/stream/reorg.go).
func (s *Stream) IncReorgsDetected() { s.reorgsDetected.Inc() }

// IncReconnects counts an RPC reconnect after a transport error.
func (s *Stream) IncReconnects() { s.reconnects.Inc() }

// ObserveLoop records one poll-loop duration.
func (s *Stream) ObserveLoop(d time.Duration) { s.loopDuration.Observe(d.Seconds()) }

// SetConsecutiveFailures records the current consecutive failure count.
func (s *Stream) SetConsecutiveFailures(n int) { s.consecutiveFail.Set(float64(n)) }

// SetBackoffSeconds records the current retry backoff.
func (s *Stream) SetBackoffSeconds(d time.Duration) { s.backoffSeconds.Set(d.Seconds()) }

// ObserveLogChunk records one chunked eth_getLogs query: a chunk was created,
// covering blocks span, taking d.
func (s *Stream) ObserveLogChunk(blocks uint64, d time.Duration) {
	s.logChunksCreated.Inc()
	s.logChunkBlocks.Observe(float64(blocks))
	s.logChunkDuration.Observe(d.Seconds())
}

// RPCObserver returns an rpc.CallObserver that records call duration and, on
// failure, increments the coarse-typed error counter. Plug it into rpc.Options.
func (s *Stream) RPCObserver() rpc.CallObserver {
	return func(operation string, dur time.Duration, et rpc.ErrorType) {
		s.rpcCallDuration.WithLabelValues(operation).Observe(dur.Seconds())
		if et != rpc.ErrorNone {
			s.rpcError.WithLabelValues(operation, string(et)).Inc()
		}
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
