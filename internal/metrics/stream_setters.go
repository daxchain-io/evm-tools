package metrics

import (
	"time"

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

// SetFinalizedBlock records the finalized block number when known.
//
// Reserved: finalized-block tracking depends on finality signaling, which is
// deferred (design.md Open Question 4 — "Finality signaling"). The poll loop
// does not call this yet, so blockchain_chain_finalized_block_number stays at 0
// until finality lands. It is defined now so the metric name is stable and the
// wiring is a one-line addition when that decision is made.
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

// IncNativeTransferRecord counts one emitted native transfer record.
func (s *Stream) IncNativeTransferRecord() {
	s.recordsEmitted.Inc()
	s.nativeTransferRecords.Inc()
}

// IncReorgsDetected counts a detected reorg (reserved; reorg handling deferred).
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
