// Package stream holds the evm-stream core logic: resolving configured event
// names to ABIs, matching logs by topic0 and decoding them to named params,
// the HTTP poll loop with chunked eth_getLogs backfill and gap-free handoff to
// head-following, success-gated native transfer detection, lossless emission
// through internal/record with an emit-blocked gauge, exponential-backoff
// retry, and graceful shutdown. See docs/design.md ("evm-stream").
package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// ErrNotImplemented is retained for callers that referenced the scaffold.
var ErrNotImplemented = errors.New("stream: not implemented")

// Client is the RPC surface the stream depends on. *rpc.Client satisfies it;
// tests substitute a fake.
type Client interface {
	ChainID(ctx context.Context) (int64, error)
	BlockNumber(ctx context.Context) (uint64, error)
	BlockByNumberUint(ctx context.Context, n uint64, full bool) (*rpc.Block, error)
	GetLogs(ctx context.Context, f rpc.LogFilter) ([]rpc.Log, error)
	TransactionReceipt(ctx context.Context, txHash string) (*rpc.Receipt, error)
}

// Metrics is the subset of *metrics.Stream the loop reports to. A nil Metrics
// is tolerated via the noopMetrics adapter so tests need not wire one.
type Metrics interface {
	SetHead(n uint64)
	SetHeadBlockTime(t time.Time, now time.Time)
	SetLastProcessedBlock(n uint64)
	SetLastEmittedBlock(n uint64)
	SetLagBlocks(lag uint64)
	SetEmitBlockedSeconds(sec float64)
	IncEventRecord(contractName, contractAddr, eventName string)
	IncNativeTransferRecord()
	IncReconnects()
	ObserveLoop(d time.Duration)
	SetConsecutiveFailures(n int)
	SetBackoffSeconds(d time.Duration)
	ObserveLogChunk(blocks uint64, d time.Duration)
}

// Healther is the readiness surface the loop updates.
type Healther interface {
	SetRPCReachable(v bool)
	SetEmitBlocked(d time.Duration)
	SetLag(n uint64)
}

// Emitter is the record sink (record.Writer satisfies it). It returns an error
// when the underlying stdout write blocks-then-fails, propagating backpressure.
type Emitter interface {
	Emit(env record.Envelope) error
}

// Options configures a Stream.
type Options struct {
	Client    Client
	Emitter   Emitter
	Metrics   Metrics
	Health    Healther
	Logger    *slog.Logger
	ChainName string
	ChainID   int64

	Contracts      []ResolvedContract
	NativeFilter   NativeFilter
	PollInterval   time.Duration
	LogChunkBlocks uint64
	FromBlock      string // "latest" or a decimal block number

	// Backoff parameters for transient RPC failures.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// now is injectable for deterministic tests; defaults to time.Now.
	now func() time.Time
}

// Stream is a configured, ready-to-run monitor.
type Stream struct {
	opts        Options
	log         *slog.Logger
	now         func() time.Time
	addresses   []string // lowercased contract addresses for the log filter
	topic0Set   []string // union of topic0s (any-of filter position 0)
	byAddrTopic map[string]map[string]eventABI
}

// New builds a Stream from resolved options. It validates the derived state but
// does not connect; call Run to begin.
func New(opts Options) (*Stream, error) {
	if opts.Client == nil {
		return nil, errors.New("stream: client is required")
	}
	if opts.Emitter == nil {
		return nil, errors.New("stream: emitter is required")
	}
	if opts.PollInterval <= 0 {
		return nil, errors.New("stream: poll_interval must be positive")
	}
	if opts.LogChunkBlocks == 0 {
		return nil, errors.New("stream: log_chunk_blocks must be positive")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	nowFn := opts.now
	if nowFn == nil {
		nowFn = time.Now
	}
	if opts.Metrics == nil {
		opts.Metrics = noopMetrics{}
	}
	if opts.Health == nil {
		opts.Health = noopHealth{}
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = 500 * time.Millisecond
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = 30 * time.Second
	}

	// Wrap the emitter so every stdout write's blocked duration feeds the
	// emit-blocked gauge and the readiness signal (lossless: measure only).
	opts.Emitter = newBlockTrackingEmitter(opts.Emitter, opts.Metrics, opts.Health, nowFn)

	s := &Stream{
		opts:        opts,
		log:         logger,
		now:         nowFn,
		byAddrTopic: map[string]map[string]eventABI{},
	}
	topicSeen := map[string]bool{}
	for _, c := range opts.Contracts {
		s.addresses = append(s.addresses, c.Address)
		s.byAddrTopic[c.Address] = c.byTopic0
		for t := range c.byTopic0 {
			if !topicSeen[t] {
				topicSeen[t] = true
				s.topic0Set = append(s.topic0Set, t)
			}
		}
	}
	return s, nil
}

// Run drives the monitor until ctx is cancelled. It resolves the starting
// block, then on each tick advances from the last processed block to the head,
// backfilling in chunks when behind and continuing seamlessly into
// head-following (no gap or duplicate at the boundary). Transient RPC failures
// are retried with exponential backoff plus jitter; the loop does not
// self-terminate on persistent failure. A cancelled ctx is a clean stop
// (returns nil).
func (s *Stream) Run(ctx context.Context) error {
	from, err := s.resolveStart(ctx)
	if err != nil {
		return err
	}
	// nextBlock is the next unprocessed block height.
	nextBlock := from
	s.log.Info("stream started",
		"endpoint", s.redactedEndpoint(),
		"chain", s.opts.ChainName,
		"chain_id", s.opts.ChainID,
		"from_block", nextBlock,
		"contracts", len(s.opts.Contracts),
		"native_transfers", s.opts.NativeFilter.enabled,
	)

	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()

	consecutiveFailures := 0
	// Run an initial poll immediately rather than waiting a full interval.
	for {
		loopStart := s.now()
		next, err := s.pollOnce(ctx, nextBlock)
		s.opts.Metrics.ObserveLoop(s.now().Sub(loopStart))
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown mid-poll
			}
			consecutiveFailures++
			s.opts.Health.SetRPCReachable(false)
			s.opts.Metrics.SetConsecutiveFailures(consecutiveFailures)
			backoff := s.backoffFor(consecutiveFailures)
			s.opts.Metrics.SetBackoffSeconds(backoff)
			s.opts.Metrics.IncReconnects()
			s.log.Warn("poll failed; backing off",
				"error_type", string(rpc.Classify(err)),
				"consecutive_failures", consecutiveFailures,
				"backoff", backoff.String(),
			)
			if !sleepCtx(ctx, backoff) {
				return nil
			}
			continue
		}
		// Success: reset failure state and advance.
		if consecutiveFailures > 0 {
			s.log.Info("rpc recovered", "after_failures", consecutiveFailures)
		}
		consecutiveFailures = 0
		s.opts.Health.SetRPCReachable(true)
		s.opts.Metrics.SetConsecutiveFailures(0)
		s.opts.Metrics.SetBackoffSeconds(0)
		nextBlock = next

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// pollOnce reads the head, processes [nextBlock, head] (chunked when behind),
// and returns the new nextBlock. When already at/after head it is a no-op that
// returns nextBlock unchanged.
func (s *Stream) pollOnce(ctx context.Context, nextBlock uint64) (uint64, error) {
	head, err := s.opts.Client.BlockNumber(ctx)
	if err != nil {
		return nextBlock, err
	}
	s.opts.Metrics.SetHead(head)
	s.updateHeadBlockTime(ctx, head)

	if nextBlock > head {
		// Ahead of head (e.g. from_block in the future, or a transient
		// head regression); nothing to do, no lag.
		s.opts.Metrics.SetLagBlocks(0)
		s.opts.Health.SetLag(0)
		return nextBlock, nil
	}

	lag := head - nextBlock
	s.opts.Metrics.SetLagBlocks(lag)
	s.opts.Health.SetLag(lag)

	// Process [nextBlock, head] in chunks of at most LogChunkBlocks.
	cur := nextBlock
	for cur <= head {
		end := cur + s.opts.LogChunkBlocks - 1
		if end > head {
			end = head
		}
		if err := s.processRange(ctx, cur, end); err != nil {
			// Return progress made so far so the next poll resumes at cur,
			// gap-free, rather than re-emitting completed chunks.
			return cur, err
		}
		s.opts.Metrics.SetLastProcessedBlock(end)
		cur = end + 1
	}
	return cur, nil
}

// updateHeadBlockTime fetches the head block header and publishes its timestamp
// and wall-clock age to the chain-health gauges
// (blockchain_chain_head_block_timestamp_seconds and
// blockchain_chain_time_since_last_block_seconds), so a stalled chain is
// detectable from metrics alone (design.md, "Chain health"). This is a
// best-effort health signal: a header fetch failure is logged at debug and does
// not fail the poll, since core progress does not depend on it.
func (s *Stream) updateHeadBlockTime(ctx context.Context, head uint64) {
	blk, err := s.opts.Client.BlockByNumberUint(ctx, head, false)
	if err != nil {
		s.log.Debug("head block header fetch failed; chain-health gauges not updated",
			"block", head, "error_type", string(rpc.Classify(err)))
		return
	}
	if bt, ok := chain.BlockTime(blk); ok {
		s.opts.Metrics.SetHeadBlockTime(bt, s.now())
	}
}

// processRange handles one inclusive [from,to] block range: it queries matching
// logs, then (when native transfers are enabled) the full blocks for value
// transfers, decoding and emitting each record. The range is one log chunk.
func (s *Stream) processRange(ctx context.Context, from, to uint64) error {
	if len(s.opts.Contracts) > 0 {
		if err := s.processLogs(ctx, from, to); err != nil {
			return err
		}
	}
	if s.opts.NativeFilter.enabled {
		if err := s.processNative(ctx, from, to); err != nil {
			return err
		}
	}
	return nil
}

// processLogs runs one chunked eth_getLogs query over [from,to] and emits a
// record per matched log.
func (s *Stream) processLogs(ctx context.Context, from, to uint64) error {
	filter := rpc.LogFilter{
		FromBlock: from,
		ToBlock:   to,
		Addresses: s.addresses,
	}
	if len(s.topic0Set) > 0 {
		// topics[0] any-of the configured topic0 union.
		filter.Topics = []any{s.topic0Set}
	}

	start := s.now()
	logs, err := s.opts.Client.GetLogs(ctx, filter)
	s.opts.Metrics.ObserveLogChunk(to-from+1, s.now().Sub(start))
	if err != nil {
		return err
	}

	for _, l := range logs {
		if err := s.emitLog(l); err != nil {
			return err
		}
	}
	return nil
}

// emitLog decodes and emits a single matched log. Logs whose address/topic0 are
// not configured are skipped (defensive; the filter already scopes the query).
func (s *Stream) emitLog(l rpc.Log) error {
	if len(l.Topics) == 0 {
		return nil
	}
	byTopic, ok := s.byAddrTopic[strings.ToLower(l.Address)]
	if !ok {
		return nil
	}
	ev, ok := byTopic[strings.ToLower(l.Topics[0])]
	if !ok {
		return nil
	}

	params, err := decodeLog(ev, l)
	if err != nil {
		s.log.Warn("skipping undecodable log",
			"contract", l.Address,
			"event", ev.Name,
			"block", l.BlockNumber,
			"error", err.Error(),
		)
		return nil // a single bad log must not stall the stream
	}

	blockNum, err := l.BlockNumberUint()
	if err != nil {
		return fmt.Errorf("parse log block number: %w", err)
	}
	logIndex, err := l.LogIndexUint()
	if err != nil {
		return fmt.Errorf("parse log index: %w", err)
	}

	cName := s.contractNameFor(l.Address)
	env := record.Envelope{
		Type:        record.TypeEvent,
		Tool:        record.ToolStream,
		Name:        cName,
		Chain:       s.opts.ChainName,
		ChainID:     s.opts.ChainID,
		BlockNumber: blockNum,
		BlockHash:   l.BlockHash,
		TxHash:      l.TxHash,
		LogIndex:    &logIndex,
		Data: record.EventData{
			Event:     ev.Name,
			Signature: ev.Signature,
			Contract:  l.Address,
			Params:    params,
		},
	}
	if err := s.opts.Emitter.Emit(env); err != nil {
		return err
	}
	s.opts.Metrics.IncEventRecord(cName, strings.ToLower(l.Address), ev.Name)
	s.opts.Metrics.SetLastEmittedBlock(blockNum)
	return nil
}

// processNative scans each block in [from,to] for success-gated value transfers
// and emits one native_transfer record per qualifying transaction.
func (s *Stream) processNative(ctx context.Context, from, to uint64) error {
	for n := from; n <= to; n++ {
		blk, err := s.opts.Client.BlockByNumberUint(ctx, n, true)
		if err != nil {
			return err
		}
		transfers, err := detectNativeTransfers(ctx, s.opts.Client, s.opts.NativeFilter, blk)
		if err != nil {
			return err
		}
		if len(transfers) == 0 {
			continue
		}
		blkNum, err := blk.NumberUint()
		if err != nil {
			return err
		}
		ts := ""
		if bt, ok := chain.BlockTime(blk); ok {
			ts = record.RFC3339(bt)
		}
		for _, t := range transfers {
			if err := s.emitNative(blk, blkNum, ts, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Stream) emitNative(blk *rpc.Block, blkNum uint64, ts string, t nativeTransfer) error {
	data := record.NativeTransferData{
		From:     t.tx.From,
		ValueWei: t.valueWei.String(),
		Value:    weiToEther(t.valueWei),
	}
	if t.contractCreation {
		data.ContractCreation = true
	} else {
		data.To = t.tx.To
	}
	env := record.Envelope{
		Type:        record.TypeNativeTransfer,
		Tool:        record.ToolStream,
		Name:        "native",
		Chain:       s.opts.ChainName,
		ChainID:     s.opts.ChainID,
		BlockNumber: blkNum,
		BlockHash:   blk.Hash,
		TxHash:      t.tx.Hash,
		Timestamp:   ts,
		Data:        data,
	}
	if err := s.opts.Emitter.Emit(env); err != nil {
		return err
	}
	s.opts.Metrics.IncNativeTransferRecord()
	s.opts.Metrics.SetLastEmittedBlock(blkNum)
	return nil
}

// resolveStart resolves the configured from_block into the first block to
// process. "latest" means "monitor only new activity" (design.md, "Command
// shape"), so it starts at head+1 — strictly the blocks mined after startup —
// rather than re-emitting the head block that already existed when the stream
// began. A numeric value replays inclusively from that block. The head+1 result
// can be one past the current head; the first poll then processes nothing until
// a new block arrives, which the loop already handles (nextBlock > head is a
// no-op).
func (s *Stream) resolveStart(ctx context.Context) (uint64, error) {
	fb := strings.TrimSpace(s.opts.FromBlock)
	if fb == "" || strings.EqualFold(fb, "latest") {
		head, err := s.opts.Client.BlockNumber(ctx)
		if err != nil {
			return 0, fmt.Errorf("resolve from_block=latest: %w", err)
		}
		return head + 1, nil
	}
	v, ok := new(big.Int).SetString(fb, 10)
	if !ok || v.Sign() < 0 || !v.IsUint64() {
		return 0, fmt.Errorf("invalid from_block %q (want \"latest\" or a non-negative block number)", fb)
	}
	return v.Uint64(), nil
}

func (s *Stream) contractNameFor(addr string) string {
	addr = strings.ToLower(addr)
	for _, c := range s.opts.Contracts {
		if c.Address == addr {
			return c.Name
		}
	}
	return addr
}

func (s *Stream) redactedEndpoint() string {
	if rc, ok := s.opts.Client.(interface{ RedactedURL() string }); ok {
		return rc.RedactedURL()
	}
	return ""
}

// backoffFor computes the exponential backoff (base * 2^(n-1), capped) with full
// jitter for the nth consecutive failure.
func (s *Stream) backoffFor(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	d := s.opts.BackoffBase
	for i := 1; i < n && d < s.opts.BackoffMax; i++ {
		d *= 2
	}
	if d > s.opts.BackoffMax {
		d = s.opts.BackoffMax
	}
	return jitter(d)
}
