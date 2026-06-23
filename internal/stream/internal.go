package stream

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// maxTraceAttempts bounds how many times a single block's trace is retried before
// its internal transfers are skipped. Internal transfers are an opt-in, best-effort
// refinement: a persistent per-block trace failure (an oversized response, a tracer
// crash, a stubborn timeout) must never wedge the producer — the core log/native
// stream keeps advancing, and the skip is logged and counted.
const maxTraceAttempts = 4

// traceBackend identifies which trace RPC a node serves for internal transfers.
// The stream cascades through them once (selectBackend) and caches the winner.
type traceBackend int

const (
	backendUnknown  traceBackend = iota // not yet probed
	backendBlock                        // debug_traceBlockByNumber (geth, 1 call/block)
	backendParity                       // trace_block (Erigon/Nethermind/Besu/anvil, 1 call/block)
	backendPerTx                        // debug_traceTransaction (geth/anvil, N calls/block)
	backendDisabled                     // no supported trace method; internal transfers off
)

func (b traceBackend) String() string {
	switch b {
	case backendBlock:
		return rpc.OpDebugTraceBlock
	case backendParity:
		return rpc.OpTraceBlock
	case backendPerTx:
		return rpc.OpDebugTraceTx
	case backendDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// internalTransfer is a backend-agnostic value movement detected inside a tx,
// normalized from either the callTracer tree or a parity flat trace.
type internalTransfer struct {
	txHash           string
	traceAddress     []int
	from, to         string
	valueWei         *big.Int
	callType         string
	contractCreation bool
}

// emitInternalTransfers detects and emits internal (sub-call) native transfers for
// one block, using whichever trace backend the node supports. blk (already fetched
// by the native path) supplies the tx list for the per-tx backend and the
// timestamp; blkNum/ts avoid a re-fetch. The top-level transfer is owned by the
// native path, so the root frame is never emitted here. It never fails the poll:
// a node without any trace method self-disables for the run; a persistent per-block
// trace error skips that block's internal transfers (both logged + counted). Only
// a context cancellation is returned.
func (s *Stream) emitInternalTransfers(ctx context.Context, blk *rpc.Block, blkNum uint64, ts string) error {
	if s.traceBackend == backendDisabled {
		return nil
	}
	transfers, err := s.traceBlockInternal(ctx, blk, blkNum)
	if err != nil {
		return err // context cancellation only
	}
	for _, t := range transfers {
		if !s.opts.NativeFilter.matches(t.from, t.to) {
			continue
		}
		if eerr := s.emitInternal(t, blkNum, ts); eerr != nil {
			return eerr
		}
	}
	return nil
}

// traceBlockInternal fetches and normalizes a block's internal transfers, with a
// bounded retry. A capability gap (no supported trace method) self-disables and
// yields no transfers; after maxTraceAttempts of transient/persistent failure the
// block is skipped (logged + counted) so the producer never wedges.
func (s *Stream) traceBlockInternal(ctx context.Context, blk *rpc.Block, blkNum uint64) ([]internalTransfer, error) {
	for attempt := 1; ; attempt++ {
		transfers, err := s.fetchTraces(ctx, blk, blkNum)
		if err == nil {
			return transfers, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// A cached backend that stops being served mid-run (a provider plan change
		// or namespace revocation) must not retry-skip every block forever: clear the
		// selection so the next attempt re-runs the cascade — a sibling backend can
		// take over, or selectBackend disables cleanly if none remain.
		if s.traceBackend != backendUnknown && s.traceBackend != backendDisabled && rpc.IsMethodUnsupported(err) {
			s.log.Warn("internal-transfer trace backend stopped responding; re-selecting",
				"previous", s.traceBackend.String(), "error", err.Error())
			s.traceBackend = backendUnknown
			continue
		}
		if attempt >= maxTraceAttempts {
			s.opts.Metrics.IncInternalTraceSkipped()
			s.log.Warn("trace failed for block after retries; skipping its internal transfers (best-effort)",
				"block", blkNum, "attempts", attempt, "backend", s.traceBackend.String(),
				"error_type", string(rpc.Classify(err)), "error", err.Error())
			return nil, nil
		}
		if !sleepCtx(ctx, s.backoffFor(attempt)) {
			return nil, ctx.Err()
		}
	}
}

// fetchTraces dispatches to the selected backend, or selects one on first use.
func (s *Stream) fetchTraces(ctx context.Context, blk *rpc.Block, blkNum uint64) ([]internalTransfer, error) {
	switch s.traceBackend {
	case backendBlock:
		return s.blockLevelTraces(ctx, blkNum)
	case backendParity:
		return s.parityTraces(ctx, blkNum)
	case backendPerTx:
		return s.perTxTraces(ctx, blk)
	default:
		return s.selectBackend(ctx, blk, blkNum)
	}
}

// selectBackend cascades through the trace backends on first use, caching the
// first that responds. A backend that the node does not expose (capability gap) is
// skipped; a transient error aborts selection so it retries later. When no backend
// is supported, internal transfers are disabled for the run (logged once + metric).
func (s *Stream) selectBackend(ctx context.Context, blk *rpc.Block, blkNum uint64) ([]internalTransfer, error) {
	type candidate struct {
		backend traceBackend
		fetch   func() ([]internalTransfer, error)
	}
	// Block-level and parity are single-call probes that work on any block. The
	// per-tx backend makes one call per transaction, so it can only be probed on a
	// block that has transactions — on an empty block it would falsely "succeed"
	// with zero calls.
	candidates := []candidate{
		{backendBlock, func() ([]internalTransfer, error) { return s.blockLevelTraces(ctx, blkNum) }},
		{backendParity, func() ([]internalTransfer, error) { return s.parityTraces(ctx, blkNum) }},
	}
	hasTxs := blk != nil && len(blk.Transactions) > 0
	if hasTxs {
		candidates = append(candidates, candidate{backendPerTx, func() ([]internalTransfer, error) { return s.perTxTraces(ctx, blk) }})
	}
	for _, c := range candidates {
		transfers, err := c.fetch()
		if err == nil {
			s.traceBackend = c.backend
			s.log.Info("internal-transfer trace backend selected", "method", c.backend.String())
			return transfers, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !rpc.IsMethodUnsupported(err) {
			return nil, err // transient: leave backend unknown, retry later
		}
	}
	if !hasTxs {
		// Block-level and parity are unsupported, but the per-tx backend could not
		// be probed on this empty block; stay unknown and re-probe a block that has
		// transactions before concluding the node has no trace support.
		return nil, nil
	}
	s.traceBackend = backendDisabled
	s.opts.Metrics.SetInternalTransfersDisabled(true)
	s.log.Warn("node exposes no supported trace method " +
		"(debug_traceBlockByNumber / trace_block / debug_traceTransaction); " +
		"internal transfers disabled for this run (top-level transfers and logs continue)")
	return nil, nil
}

// blockLevelTraces uses debug_traceBlockByNumber (geth callTracer, one call).
func (s *Stream) blockLevelTraces(ctx context.Context, blkNum uint64) ([]internalTransfer, error) {
	traces, err := s.opts.Client.TraceBlockByNumber(ctx, blkNum)
	if err != nil {
		return nil, err
	}
	var out []internalTransfer
	for _, tt := range traces {
		if tt.Result == nil || tt.Result.Error != "" {
			continue // no trace, or the whole tx reverted
		}
		out, err = walkCallFrames(out, tt.TxHash, nil, tt.Result.Calls)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// perTxTraces uses debug_traceTransaction per transaction (geth/anvil fallback).
func (s *Stream) perTxTraces(ctx context.Context, blk *rpc.Block) ([]internalTransfer, error) {
	var out []internalTransfer
	for _, tx := range blk.Transactions {
		root, err := s.opts.Client.TraceTransaction(ctx, tx.Hash)
		if err != nil {
			return nil, err
		}
		if root == nil || root.Error != "" {
			continue
		}
		out, err = walkCallFrames(out, tx.Hash, nil, root.Calls)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// parityTraces uses trace_block (parity flat traces, one call).
func (s *Stream) parityTraces(ctx context.Context, blkNum uint64) ([]internalTransfer, error) {
	traces, err := s.opts.Client.TraceBlockParity(ctx, blkNum)
	if err != nil {
		return nil, err
	}
	return parseParityTraces(traces)
}

// walkCallFrames depth-first appends an internalTransfer for each value-moving
// sub-call frame, recursing into survivors. A frame with an error reverted: its
// value did not move and its subtree was rolled back, so it is pruned (skipped,
// not recursed). trace_address is the path from the top-level call (prefix+index).
func walkCallFrames(out []internalTransfer, txHash string, prefix []int, frames []rpc.CallFrame) ([]internalTransfer, error) {
	for i := range frames {
		f := frames[i]
		if f.Error != "" {
			continue // reverted frame: pruned with its subtree
		}
		path := append(append([]int(nil), prefix...), i) // fresh slice; never aliased
		if isValueMovingCall(f.Type) {
			val, err := f.ValueBig()
			if err != nil {
				return out, fmt.Errorf("internal transfer value (tx %s, trace %v): %w", txHash, path, err)
			}
			if val.Sign() > 0 {
				out = append(out, internalTransfer{
					txHash: txHash, traceAddress: path, from: f.From, to: f.To,
					valueWei: val, callType: strings.ToLower(f.Type), contractCreation: isCreate(f.Type),
				})
			}
		}
		var err error
		out, err = walkCallFrames(out, txHash, path, f.Calls)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// parseParityTraces normalizes a parity flat-trace list. The root (traceAddress
// []) is the top-level transfer (owned by the native path) and is skipped; a
// failed trace and any descendant of a failed trace are pruned (value rolled back).
func parseParityTraces(traces []rpc.ParityTrace) ([]internalTransfer, error) {
	var errored [][]int
	for _, t := range traces {
		if t.Error != "" {
			errored = append(errored, t.TraceAddress)
		}
	}
	var out []internalTransfer
	for _, t := range traces {
		if len(t.TraceAddress) == 0 || t.Error != "" || hasErroredAncestor(t.TraceAddress, errored) {
			continue
		}
		it, ok, err := parityToInternal(t)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, it)
		}
	}
	return out, nil
}

// parityToInternal maps one parity trace to an internalTransfer, reporting ok=false
// for non-value-moving traces (DELEGATECALL/STATICCALL/CALLCODE, zero value, reward).
func parityToInternal(t rpc.ParityTrace) (internalTransfer, bool, error) {
	it := internalTransfer{txHash: t.TxHash, traceAddress: append([]int(nil), t.TraceAddress...)}
	switch t.Type {
	case "call":
		if t.Action.CallType != "call" { // exclude callcode/delegatecall/staticcall
			return it, false, nil
		}
		it.from, it.to, it.callType = t.Action.From, t.Action.To, "call"
		v, err := parseHexValue(t.Action.Value)
		if err != nil {
			return it, false, err
		}
		it.valueWei = v
	case "create":
		it.from, it.callType, it.contractCreation = t.Action.From, "create", true
		if t.Result != nil {
			it.to = t.Result.Address
		}
		v, err := parseHexValue(t.Action.Value)
		if err != nil {
			return it, false, err
		}
		it.valueWei = v
	case "suicide":
		it.from, it.to, it.callType = t.Action.Address, t.Action.RefundAddress, "selfdestruct"
		v, err := parseHexValue(t.Action.Balance)
		if err != nil {
			return it, false, err
		}
		it.valueWei = v
	default:
		return it, false, nil // reward, etc.
	}
	if it.valueWei.Sign() <= 0 {
		return it, false, nil
	}
	return it, true, nil
}

// hasErroredAncestor reports whether any errored trace path is a strict prefix of
// path (so path is inside a reverted subtree).
func hasErroredAncestor(path []int, errored [][]int) bool {
	for _, e := range errored {
		if len(e) >= len(path) {
			continue
		}
		match := true
		for i := range e {
			if e[i] != path[i] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (s *Stream) emitInternal(t internalTransfer, blkNum uint64, ts string) error {
	data := record.InternalTransferData{
		From:             t.from,
		To:               t.to,
		ValueWei:         t.valueWei.String(),
		Value:            weiToEther(t.valueWei),
		CallType:         t.callType,
		ContractCreation: t.contractCreation,
	}
	env := record.Envelope{
		Type:         record.TypeInternalTransfer,
		Tool:         record.ToolStream,
		Name:         "native",
		Chain:        s.opts.ChainName,
		ChainID:      s.opts.ChainID,
		BlockNumber:  blkNum,
		TxHash:       t.txHash,
		TraceAddress: t.traceAddress,
		Timestamp:    ts,
		Finalized:    s.isFinalized(blkNum),
		Data:         data,
	}
	if err := s.opts.Emitter.Emit(env); err != nil {
		return &emitErr{err: err}
	}
	s.opts.Metrics.IncInternalTransferRecord()
	s.opts.Metrics.SetLastEmittedBlock(blkNum)
	return nil
}

// isValueMovingCall reports whether a callTracer frame type moves native ETH.
// CALL carries value; CREATE/CREATE2 endow the new contract; SELFDESTRUCT sweeps
// the dying contract's balance. DELEGATECALL/STATICCALL run in the caller's context
// and move no value, and CALLCODE's value is transferred to the caller itself (the
// `to` is borrowed code, not a recipient) — all excluded by TYPE so a borrowed
// frame's value field is never misattributed as a transfer.
func isValueMovingCall(frameType string) bool {
	switch strings.ToUpper(frameType) {
	case "CALL", "CREATE", "CREATE2", "SELFDESTRUCT", "SUICIDE":
		return true
	default:
		return false
	}
}

func isCreate(frameType string) bool {
	t := strings.ToUpper(frameType)
	return t == "CREATE" || t == "CREATE2"
}

func parseHexValue(s string) (*big.Int, error) {
	if s == "" || s == "0x" {
		return new(big.Int), nil
	}
	v, ok := new(big.Int).SetString(strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X"), 16)
	if !ok {
		return nil, fmt.Errorf("invalid hex value %q", s)
	}
	return v, nil
}
