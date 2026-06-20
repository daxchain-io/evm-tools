// Package balance holds the evm-balance core logic: native/ERC-20 balance
// polling, contract-state observation (native_balance, token_total_supply, and
// a transfer_count window), decimals resolution via eth_call decimals() with a
// config override, sampling cadence (interval XOR every_blocks), change
// detection, and emission of balance_* and contract_* records through
// internal/record. See docs/design.md ("evm-balance").
//
// Where evm-stream follows logs, evm-balance samples state: each cadence tick it
// reads every configured target at a single block, emits a *_sample record, and
// emits a *_change record when the polled value moved since the last tick.
// Emission goes through the same synchronized record.Writer, so backpressure is
// lossless (a blocked stdout write propagates upstream and throttles polling
// rather than dropping records).
package balance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/daxchain-io/evm-tools/internal/chain"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// ErrNotImplemented is retained for callers that referenced the scaffold.
var ErrNotImplemented = errors.New("balance: not implemented")

// Client is the RPC surface the poller depends on. *rpc.Client satisfies it;
// tests substitute a fake.
type Client interface {
	BlockNumber(ctx context.Context) (uint64, error)
	BlockByNumberUint(ctx context.Context, n uint64, full bool) (*rpc.Block, error)
	BalanceAt(ctx context.Context, address, blockTag string) (*big.Int, error)
	Call(ctx context.Context, msg rpc.CallMsg, blockTag string) (string, error)
	GetLogs(ctx context.Context, f rpc.LogFilter) ([]rpc.Log, error)
}

// Metrics is the subset of *metrics.Balance the loop reports to. A nil Metrics
// is tolerated via the noopMetrics adapter so tests need not wire one.
type Metrics interface {
	SetHead(n uint64)
	SetHeadBlockTime(t time.Time, now time.Time)
	SetLastSampledBlock(n uint64)
	SetLagBlocks(lag uint64)
	SetEmitBlockedSeconds(sec float64)

	IncSampleRecord()
	IncChangeRecord()

	SetAccountBalanceWei(accountName, accountAddr string, wei *big.Int)
	SetAccountBalanceEth(accountName, accountAddr string, eth float64)
	SetAccountTokenBalanceRaw(accountName, accountAddr, tokenName, tokenAddr string, raw *big.Int)
	SetAccountTokenBalance(accountName, accountAddr, tokenName, tokenAddr string, bal float64)
	SetContractBalanceWei(contractName, contractAddr string, wei *big.Int)
	SetContractBalanceEth(contractName, contractAddr string, eth float64)
	SetContractTokenTotalSupply(contractName, contractAddr string, supply float64)
	SetContractTransferCount(contractName, contractAddr string, count float64)

	IncReconnects()
	ObserveLoop(d time.Duration)
	SetConsecutiveFailures(n int)
	SetBackoffSeconds(d time.Duration)
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

// transferTopic0 is the keccak-256 of Transfer(address,address,uint256), shared
// by ERC-20 and ERC-721, used to count transfers in a contract's window.
const transferTopic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// Cadence selects how the poller samples: time-based (Interval) XOR block-based
// (EveryBlocks). Exactly one must be set; this is validated by the caller.
type Cadence struct {
	Interval    time.Duration
	EveryBlocks uint64
}

// Options configures a Poller.
type Options struct {
	Client    Client
	Emitter   Emitter
	Metrics   Metrics
	Health    Healther
	Logger    *slog.Logger
	ChainName string
	ChainID   int64

	Cadence         Cadence
	Native          []NativeTarget
	ERC20           []ERC20Target
	Contracts       []ContractTarget
	ERC721Balances  []ERC721BalanceTarget
	ERC721Ownership []ERC721OwnershipTarget

	// Backoff parameters for transient RPC failures.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// now is injectable for deterministic tests; defaults to time.Now.
	now func() time.Time
}

// NativeTarget is a resolved [[balance.native]] entry.
type NativeTarget struct {
	Name    string
	Address string // 0x-hex (lowercased for metric labels)
}

// ERC20Target is a resolved [[balance.erc20]] entry. Decimals is resolved at
// startup (config override, else eth_call decimals()); when it stays nil the
// token did not implement decimals() and none was configured, so only raw
// values are emitted.
type ERC20Target struct {
	Name     string
	Token    string // 0x-hex
	Address  string // holder, 0x-hex
	Decimals *int
}

// ContractTarget is a resolved [[balance.contracts]] entry: any combination of
// native balance, token total supply, and a transfer-count window.
type ContractTarget struct {
	Name                      string
	Address                   string // 0x-hex
	NativeBalance             bool
	TokenSupply               bool
	TransferCountWindowBlocks uint64
	Decimals                  *int // for token_total_supply formatting
}

// ERC721BalanceTarget is a resolved [[balance.erc721_balances]] entry (mode
// balance_of): it reads balanceOf(owner) on an ERC-721 token and emits a
// balance_sample/balance_change with kind erc721 and a token Count.
type ERC721BalanceTarget struct {
	Name  string
	Token string // 0x-hex ERC-721 contract
	Owner string // holder, 0x-hex
}

// ERC721OwnershipTarget is a resolved [[balance.erc721_ownership]] entry: it
// reads ownerOf(tokenID) on an ERC-721 token and emits an
// ownership_sample/ownership_change with the current owner.
type ERC721OwnershipTarget struct {
	Name    string
	Token   string // 0x-hex ERC-721 contract
	TokenID string // decimal or 0x-hex token ID, carried verbatim in the record
}

// Poller is a configured, ready-to-run state sampler.
type Poller struct {
	opts Options
	log  *slog.Logger
	now  func() time.Time

	// prior holds the last-observed value per target key for change detection.
	prior map[string]*big.Int
	// priorOwner holds the last-observed ERC-721 owner per ownership target key.
	priorOwner map[string]string
}

// New builds a Poller from resolved options. It validates the derived state but
// does not connect; call Run to begin. Decimals resolution happens in Run (it
// needs the client), so New stays offline-safe.
func New(opts Options) (*Poller, error) {
	if opts.Client == nil {
		return nil, errors.New("balance: client is required")
	}
	if opts.Emitter == nil {
		return nil, errors.New("balance: emitter is required")
	}
	if err := validateCadence(opts.Cadence); err != nil {
		return nil, err
	}
	if len(opts.Native) == 0 && len(opts.ERC20) == 0 && len(opts.Contracts) == 0 &&
		len(opts.ERC721Balances) == 0 && len(opts.ERC721Ownership) == 0 {
		return nil, errors.New("balance: nothing to poll (no native/erc20/erc721/contract targets)")
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

	return &Poller{
		opts:       opts,
		log:        logger,
		now:        nowFn,
		prior:      map[string]*big.Int{},
		priorOwner: map[string]string{},
	}, nil
}

// validateCadence enforces the interval-XOR-every_blocks rule.
func validateCadence(c Cadence) error {
	hasInterval := c.Interval > 0
	hasBlocks := c.EveryBlocks > 0
	switch {
	case hasInterval && hasBlocks:
		return errors.New("balance: set exactly one of interval or every_blocks, not both")
	case !hasInterval && !hasBlocks:
		return errors.New("balance: set exactly one of interval or every_blocks")
	}
	return nil
}

// Run drives the poller until ctx is cancelled. It first resolves token decimals
// (config override, else eth_call decimals()), then samples every cadence tick.
// Transient RPC failures are retried with exponential backoff plus jitter; the
// loop does not self-terminate on persistent failure. A cancelled ctx is a clean
// stop (returns nil).
func (p *Poller) Run(ctx context.Context) (err error) {
	// Convert a panic into a terminal error so the caller's graceful shutdown
	// (final flush, metrics server stop) still runs and the process exits non-zero
	// for a supervisor restart, rather than crashing abruptly.
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("recovered from panic in balance loop; stopping",
				"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("balance panic: %v", r)
		}
	}()

	p.resolveDecimals(ctx)

	p.log.Info("balance poller started",
		"endpoint", p.redactedEndpoint(),
		"chain", p.opts.ChainName,
		"chain_id", p.opts.ChainID,
		"native", len(p.opts.Native),
		"erc20", len(p.opts.ERC20),
		"erc721_balances", len(p.opts.ERC721Balances),
		"erc721_ownership", len(p.opts.ERC721Ownership),
		"contracts", len(p.opts.Contracts),
		"cadence", p.cadenceDesc(),
	)

	if p.opts.Cadence.EveryBlocks > 0 {
		return p.runBlockCadence(ctx)
	}
	return p.runIntervalCadence(ctx)
}

// errEmptyCallResult is returned by the decode helpers when an eth_call yields an
// empty ("0x") result — on most nodes that means the target is not a contract or
// the queried view method does not exist. It is wrapped in *permanentErr so the
// poll loop fails fast (Principle 7) with the target named, instead of retrying a
// misconfiguration forever and silently emitting no data for any target.
var errEmptyCallResult = errors.New("empty call result (target is not a contract, or the queried view method does not exist)")

// permanentErr marks a non-retryable sampling failure (a misconfigured target).
// The wrapping context from the sample function names the offending target.
type permanentErr struct{ err error }

func (e *permanentErr) Error() string { return e.err.Error() }
func (e *permanentErr) Unwrap() error { return e.err }

// emitErr marks a failure that originated from the output Emitter (a broken
// stdout pipe / dead downstream sink) rather than the RPC client — terminal, and
// never treated as a transient RPC fault.
type emitErr struct{ err error }

func (e *emitErr) Error() string { return "emit: " + e.err.Error() }
func (e *emitErr) Unwrap() error { return e.err }

// handleError decides how to react to a poll/sample error. It returns whether the
// loop should stop and the terminal error to return (nil for a clean stop). An
// emit error (broken output) or a permanent misconfiguration is terminal — the
// poller stops with a clear error rather than backing off forever; every other
// error is transient and backed off (blocking), with a ctx-cancel during backoff
// treated as a clean stop.
func (p *Poller) handleError(ctx context.Context, consecutiveFailures *int, err error) (stop bool, terminal error) {
	var ee *emitErr
	if errors.As(err, &ee) && (errors.Is(ee.err, syscall.EPIPE) || errors.Is(ee.err, syscall.EBADF)) {
		// Downstream output is gone (dead sink / closed pipe): terminal. Other,
		// recoverable output errors fall through to the transient backoff so a
		// record is never dropped.
		p.log.Error("output write failed; downstream gone, stopping", "error", ee.err.Error())
		return true, fmt.Errorf("emit to stdout failed: %w", ee.err)
	}
	var pe *permanentErr
	if errors.As(err, &pe) {
		p.log.Error("permanent sampling failure; fix configuration and restart", "error", err.Error())
		return true, fmt.Errorf("permanent sampling failure: %w", err)
	}
	if !p.handleFailure(ctx, consecutiveFailures, err) {
		return true, nil // ctx cancelled during backoff: clean stop
	}
	return false, nil
}

// runIntervalCadence samples once immediately, then on every Interval tick.
func (p *Poller) runIntervalCadence(ctx context.Context) error {
	ticker := time.NewTicker(p.opts.Cadence.Interval)
	defer ticker.Stop()

	consecutiveFailures := 0
	for {
		if stop, err := p.tick(ctx, &consecutiveFailures); stop {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// runBlockCadence polls the head and samples whenever the head has advanced at
// least EveryBlocks since the last sample. The head is polled on a short
// interval bounded by EveryBlocks-worth of expected block time, but to stay
// node-agnostic it simply polls on a fixed short interval and compares heights.
func (p *Poller) runBlockCadence(ctx context.Context) error {
	const headPoll = 2 * time.Second
	ticker := time.NewTicker(headPoll)
	defer ticker.Stop()

	consecutiveFailures := 0
	var lastSampled uint64
	haveSampled := false
	for {
		head, err := p.opts.Client.BlockNumber(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if stop, herr := p.handleError(ctx, &consecutiveFailures, err); stop {
				return herr
			}
			continue
		}
		p.opts.Metrics.SetHead(head)
		// A successful head poll means the RPC has recovered: reset failure state
		// and emit the recovery log on every poll, not only when a sample is due —
		// otherwise the "rpc recovered" log never fires and the consecutive-failure
		// /backoff gauges stay pinned at their failure values between samples.
		p.onSuccess(&consecutiveFailures)
		// Publish real lag — how far head has advanced since the last sample — so
		// evm_balance_lag_blocks reflects sampling staleness rather than a constant
		// 0. sampleAll resets it to 0 when a fresh sample is taken.
		if haveSampled && head > lastSampled {
			p.opts.Metrics.SetLagBlocks(head - lastSampled)
		}

		due := !haveSampled || head >= lastSampled+p.opts.Cadence.EveryBlocks
		if due {
			loopStart := p.now()
			if err := p.sampleAll(ctx, head); err != nil {
				p.opts.Metrics.ObserveLoop(p.now().Sub(loopStart))
				if ctx.Err() != nil {
					return nil
				}
				if stop, herr := p.handleError(ctx, &consecutiveFailures, err); stop {
					return herr
				}
				continue
			}
			p.opts.Metrics.ObserveLoop(p.now().Sub(loopStart))
			lastSampled = head
			haveSampled = true
			p.onSuccess(&consecutiveFailures)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// tick runs one interval-cadence sample at the current head. It returns
// (stop, terminal): stop=true with a nil error on a clean stop (ctx cancelled),
// stop=true with a non-nil error on a terminal failure (broken output or a
// permanent misconfiguration), and stop=false to continue after a transient
// failure was backed off.
func (p *Poller) tick(ctx context.Context, consecutiveFailures *int) (stop bool, terminal error) {
	loopStart := p.now()
	head, err := p.opts.Client.BlockNumber(ctx)
	if err == nil {
		p.opts.Metrics.SetHead(head)
		err = p.sampleAll(ctx, head)
	}
	p.opts.Metrics.ObserveLoop(p.now().Sub(loopStart))
	if err != nil {
		if ctx.Err() != nil {
			return true, nil
		}
		return p.handleError(ctx, consecutiveFailures, err)
	}
	p.onSuccess(consecutiveFailures)
	return false, nil
}

// onSuccess resets the failure/backoff state after a clean sample.
func (p *Poller) onSuccess(consecutiveFailures *int) {
	if *consecutiveFailures > 0 {
		p.log.Info("rpc recovered", "after_failures", *consecutiveFailures)
	}
	*consecutiveFailures = 0
	p.opts.Health.SetRPCReachable(true)
	p.opts.Metrics.SetConsecutiveFailures(0)
	p.opts.Metrics.SetBackoffSeconds(0)
}

// handleFailure records a transient failure and sleeps the computed backoff. It
// returns false when ctx was cancelled during the backoff (caller should stop).
func (p *Poller) handleFailure(ctx context.Context, consecutiveFailures *int, err error) bool {
	*consecutiveFailures++
	p.opts.Health.SetRPCReachable(false)
	p.opts.Metrics.SetConsecutiveFailures(*consecutiveFailures)
	backoff := p.backoffFor(*consecutiveFailures)
	p.opts.Metrics.SetBackoffSeconds(backoff)
	p.opts.Metrics.IncReconnects()
	p.log.Warn("poll failed; backing off",
		"error_type", string(rpc.Classify(err)),
		"consecutive_failures", *consecutiveFailures,
		"backoff", backoff.String(),
	)
	return sleepCtx(ctx, backoff)
}

// sampleAll samples every configured target at the given head block, emitting a
// *_sample for each and a *_change when the value moved. The block timestamp is
// fetched once and shared by every record from this tick.
func (p *Poller) sampleAll(ctx context.Context, head uint64) error {
	tag := rpc.BlockTag(head)

	ts := ""
	blockHash := ""
	if blk, err := p.opts.Client.BlockByNumberUint(ctx, head, false); err == nil {
		blockHash = blk.Hash
		if bt, ok := chain.BlockTime(blk); ok {
			ts = record.RFC3339(bt)
			p.opts.Metrics.SetHeadBlockTime(bt, p.now())
		}
	} else {
		p.log.Debug("head header fetch failed; record timestamp omitted this tick",
			"block", head, "error_type", string(rpc.Classify(err)))
	}

	for _, n := range p.opts.Native {
		if err := p.sampleNative(ctx, n, head, tag, ts, blockHash); err != nil {
			return err
		}
	}
	for _, e := range p.opts.ERC20 {
		if err := p.sampleERC20(ctx, e, head, tag, ts, blockHash); err != nil {
			return err
		}
	}
	for _, b := range p.opts.ERC721Balances {
		if err := p.sampleERC721Balance(ctx, b, head, tag, ts, blockHash); err != nil {
			return err
		}
	}
	for _, o := range p.opts.ERC721Ownership {
		if err := p.sampleERC721Ownership(ctx, o, head, tag, ts, blockHash); err != nil {
			return err
		}
	}
	for _, c := range p.opts.Contracts {
		if err := p.sampleContract(ctx, c, head, tag, ts, blockHash); err != nil {
			return err
		}
	}

	p.opts.Metrics.SetLastSampledBlock(head)
	// The sample was taken at the head just read, so lag is zero on success.
	p.opts.Metrics.SetLagBlocks(0)
	p.opts.Health.SetLag(0)
	return nil
}

// redactedEndpoint returns the log-safe RPC endpoint, or "" when the client does
// not expose one (a test fake).
func (p *Poller) redactedEndpoint() string {
	if rc, ok := p.opts.Client.(interface{ RedactedURL() string }); ok {
		return rc.RedactedURL()
	}
	return ""
}

func (p *Poller) cadenceDesc() string {
	if p.opts.Cadence.EveryBlocks > 0 {
		return fmt.Sprintf("every %d blocks", p.opts.Cadence.EveryBlocks)
	}
	return p.opts.Cadence.Interval.String()
}

// changed reports whether a target's value moved since the last tick and updates
// the stored prior. The first observation is never a change (there is nothing to
// compare against); it returns (false, nil) and seeds the prior. Subsequent
// observations return (moved, previousValue).
func (p *Poller) changed(key string, cur *big.Int) (bool, *big.Int) {
	prev, ok := p.prior[key]
	p.prior[key] = new(big.Int).Set(cur)
	if !ok {
		return false, nil
	}
	if prev.Cmp(cur) == 0 {
		return false, prev
	}
	return true, prev
}

// ownerChanged reports whether an ERC-721 token's owner moved since the last
// tick and updates the stored prior. Like changed, the first observation is
// never a change: it returns (false, "") and seeds the prior. Owners are
// compared case-insensitively so a checksum-vs-lowercase difference from the RPC
// is not mistaken for a transfer. Subsequent observations return
// (moved, previousOwner) with the previous owner in its originally observed form.
func (p *Poller) ownerChanged(key, cur string) (bool, string) {
	prev, ok := p.priorOwner[key]
	p.priorOwner[key] = cur
	if !ok {
		return false, ""
	}
	if strings.EqualFold(prev, cur) {
		return false, prev
	}
	return true, prev
}

// backoffFor computes the exponential backoff (base * 2^(n-1), capped) with full
// jitter for the nth consecutive failure.
func (p *Poller) backoffFor(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	d := p.opts.BackoffBase
	for i := 1; i < n && d < p.opts.BackoffMax; i++ {
		d *= 2
	}
	if d > p.opts.BackoffMax {
		d = p.opts.BackoffMax
	}
	return jitter(d)
}

// lower lowercases an address for metric labels and map keys.
func lower(s string) string { return strings.ToLower(s) }
