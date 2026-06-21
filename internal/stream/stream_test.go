package stream

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/daxchain-io/evm-tools/internal/config"
	"github.com/daxchain-io/evm-tools/internal/record"
	"github.com/daxchain-io/evm-tools/internal/rpc"
)

// fakeClient is a programmable RPC client for the poll loop tests.
type fakeClient struct {
	mu sync.Mutex

	chainID int64
	// heads is a queue of head block numbers returned by successive
	// BlockNumber calls; the last value repeats.
	heads   []uint64
	headIdx int

	// logsByRange records the GetLogs filters seen and returns the configured
	// logs for any range; a func lets tests scope logs to a range.
	getLogs func(f rpc.LogFilter) ([]rpc.Log, error)
	// blocks maps block number -> block (for native transfer tests).
	blocks   map[uint64]*rpc.Block
	receipts map[string]*rpc.Receipt
	// blockByNum, when set, takes precedence over blocks and lets a test return a
	// different block (hash/parentHash) per call — used to simulate a reorg where a
	// height's canonical hash changes between polls.
	blockByNum func(n uint64) *rpc.Block

	getLogsCalls []rpc.LogFilter
}

func (c *fakeClient) ChainID(context.Context) (int64, error) { return c.chainID, nil }

func (c *fakeClient) BlockNumber(context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.headIdx >= len(c.heads) {
		return c.heads[len(c.heads)-1], nil
	}
	h := c.heads[c.headIdx]
	c.headIdx++
	return h, nil
}

func (c *fakeClient) BlockByNumberUint(_ context.Context, n uint64, _ bool) (*rpc.Block, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blockByNum != nil {
		return c.blockByNum(n), nil
	}
	if b, ok := c.blocks[n]; ok {
		return b, nil
	}
	return &rpc.Block{Number: rpc.BlockTag(n), Hash: "0xblk"}, nil
}

func (c *fakeClient) GetLogs(_ context.Context, f rpc.LogFilter) ([]rpc.Log, error) {
	c.mu.Lock()
	c.getLogsCalls = append(c.getLogsCalls, f)
	c.mu.Unlock()
	if c.getLogs != nil {
		return c.getLogs(f)
	}
	return nil, nil
}

func (c *fakeClient) TransactionReceipt(_ context.Context, txHash string) (*rpc.Receipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.receipts[txHash], nil
}

// captureEmitter records emitted envelopes.
type captureEmitter struct {
	mu   sync.Mutex
	envs []record.Envelope
	// onEmit, when set, runs before recording (used to inject backpressure).
	onEmit func() error
}

func (e *captureEmitter) Emit(env record.Envelope) error {
	if e.onEmit != nil {
		if err := e.onEmit(); err != nil {
			return err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.envs = append(e.envs, env)
	return nil
}

func (e *captureEmitter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.envs)
}

func (e *captureEmitter) snapshot() []record.Envelope {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]record.Envelope, len(e.envs))
	copy(out, e.envs)
	return out
}

func resolvedUSDC(t *testing.T) []ResolvedContract {
	t.Helper()
	rcs, err := ResolveContracts([]config.StreamContract{{
		Name:    "usdc",
		Address: "0xToken",
		Events:  []string{"Transfer"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return rcs
}

func makeTransferLog(block uint64, idx string) rpc.Log {
	from := "0x000000000000000000000000" + "1111111111111111111111111111111111111111"
	to := "0x000000000000000000000000" + "2222222222222222222222222222222222222222"
	data := "0x" + "00000000000000000000000000000000000000000000000000000000001312d0"
	return rpc.Log{
		Address:     "0xtoken",
		Topics:      []string{transferTopic0, from, to},
		Data:        data,
		BlockNumber: rpc.BlockTag(block),
		BlockHash:   "0xblk",
		TxHash:      "0xtx" + idx,
		LogIndex:    "0x" + idx,
	}
}

// TestBackfillThenHeadFollowGapFree drives the loop from from_block=1 against a
// head that starts at 5 then advances to 7, verifying chunked backfill covers
// [1,5] and head-following continues at [6,7] with no gap or duplicate.
func TestBackfillThenHeadFollowGapFree(t *testing.T) {
	emitted := map[uint64]bool{}
	fc := &fakeClient{
		chainID: 4242,
		heads:   []uint64{5, 7}, // first poll head 5, then 7 thereafter
		getLogs: func(f rpc.LogFilter) ([]rpc.Log, error) {
			var out []rpc.Log
			for b := f.FromBlock; b <= f.ToBlock; b++ {
				if emitted[b] {
					return nil, errors.New("range overlap: block re-queried")
				}
				emitted[b] = true
				out = append(out, makeTransferLog(b, "1"))
			}
			return out, nil
		},
	}
	em := &captureEmitter{}

	s, err := New(Options{
		Client:         fc,
		Emitter:        em,
		ChainName:      "my-chain",
		ChainID:        4242,
		Contracts:      resolvedUSDC(t),
		PollInterval:   5 * time.Millisecond,
		LogChunkBlocks: 2, // force multiple chunks across [1,5]
		FromBlock:      "1",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Wait until blocks 1..7 have all been emitted exactly once.
	waitFor(t, func() bool { return em.count() >= 7 }, time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// Verify exactly one event per block 1..7, in order, no duplicates.
	envs := em.snapshot()
	if len(envs) != 7 {
		t.Fatalf("expected 7 events, got %d", len(envs))
	}
	for i, env := range envs {
		wantBlock := uint64(i + 1)
		if env.BlockNumber != wantBlock {
			t.Errorf("event %d: block = %d, want %d", i, env.BlockNumber, wantBlock)
		}
		if env.Type != record.TypeEvent {
			t.Errorf("event %d: type = %q", i, env.Type)
		}
	}
	// Verify chunked backfill: ranges were <= LogChunkBlocks wide.
	for _, f := range fc.getLogsCalls {
		if f.ToBlock-f.FromBlock+1 > 2 {
			t.Errorf("chunk wider than LogChunkBlocks: [%d,%d]", f.FromBlock, f.ToBlock)
		}
	}
}

// TestNativeTransferSuccessGating verifies only success-status value transfers
// are emitted; reverted ones are gated out, and contract-creation is flagged.
func TestNativeTransferSuccessGating(t *testing.T) {
	blk := &rpc.Block{
		Number:    rpc.BlockTag(10),
		Hash:      "0xblk",
		Timestamp: "0x0",
		Transactions: []rpc.Transaction{
			{Hash: "0xok", From: "0xa", To: "0xb", Value: "0xde0b6b3a7640000"}, // 1 ETH, success
			{Hash: "0xrevert", From: "0xc", To: "0xd", Value: "0x1"},           // reverted
			{Hash: "0xzero", From: "0xe", To: "0xf", Value: "0x0"},             // no value
			{Hash: "0xcreate", From: "0xg", To: "", Value: "0x2"},              // contract creation
		},
	}
	fc := &fakeClient{
		chainID: 4242,
		heads:   []uint64{10},
		blocks:  map[uint64]*rpc.Block{10: blk},
		receipts: map[string]*rpc.Receipt{
			"0xok":     {Status: "0x1"},
			"0xrevert": {Status: "0x0"},
			"0xcreate": {Status: "0x1"},
		},
	}
	em := &captureEmitter{}
	s, err := New(Options{
		Client:         fc,
		Emitter:        em,
		ChainName:      "my-chain",
		ChainID:        4242,
		NativeFilter:   NativeFilterFromConfig(config.NativeTransfersConfig{Enabled: true}),
		PollInterval:   5 * time.Millisecond,
		LogChunkBlocks: 100,
		FromBlock:      "10",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitFor(t, func() bool { return em.count() >= 2 }, time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	envs := em.snapshot()
	if len(envs) != 2 {
		t.Fatalf("expected 2 native transfers (ok + create), got %d: %+v", len(envs), envs)
	}
	byTx := map[string]record.NativeTransferData{}
	for _, e := range envs {
		byTx[e.TxHash] = e.Data.(record.NativeTransferData)
	}
	ok, hasOK := byTx["0xok"]
	if !hasOK {
		t.Fatal("missing successful transfer 0xok")
	}
	if ok.Value != "1.0" || ok.ValueWei != "1000000000000000000" {
		t.Errorf("ok transfer value = %q / %q", ok.Value, ok.ValueWei)
	}
	create, hasCreate := byTx["0xcreate"]
	if !hasCreate {
		t.Fatal("missing contract-creation transfer 0xcreate")
	}
	if !create.ContractCreation || create.To != "" {
		t.Errorf("contract creation flag/To wrong: %+v", create)
	}
}

// TestRetryOnTransientFailure verifies a transient BlockNumber error is retried
// (not fatal) and the loop recovers.
func TestRetryOnTransientFailure(t *testing.T) {
	calls := 0
	fc := &failHeadClient{
		failFirst: 2,
		onCall:    func() { calls++ },
		head:      3,
	}
	em := &captureEmitter{}
	s, err := New(Options{
		Client:         fc,
		Emitter:        em,
		ChainName:      "c",
		ChainID:        1,
		Contracts:      resolvedUSDC(t),
		PollInterval:   5 * time.Millisecond,
		LogChunkBlocks: 100,
		FromBlock:      "3",
		BackoffBase:    time.Millisecond,
		BackoffMax:     2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitFor(t, func() bool { return fc.headCalls() > 3 }, 2*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run should recover from transient failure, got: %v", err)
	}
}

// failHeadClient fails the first failFirst BlockNumber calls, then succeeds.
type failHeadClient struct {
	mu        sync.Mutex
	failFirst int
	n         int
	head      uint64
	onCall    func()
}

func (c *failHeadClient) headCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *failHeadClient) ChainID(context.Context) (int64, error) { return 1, nil }
func (c *failHeadClient) BlockNumber(context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	if c.onCall != nil {
		c.onCall()
	}
	if c.n <= c.failFirst {
		return 0, errors.New("transient")
	}
	return c.head, nil
}
func (c *failHeadClient) BlockByNumberUint(_ context.Context, n uint64, _ bool) (*rpc.Block, error) {
	return &rpc.Block{Number: rpc.BlockTag(n)}, nil
}
func (c *failHeadClient) GetLogs(context.Context, rpc.LogFilter) ([]rpc.Log, error) { return nil, nil }
func (c *failHeadClient) TransactionReceipt(context.Context, string) (*rpc.Receipt, error) {
	return nil, nil
}

// TestBackpressurePropagates verifies an emitter error (a failed stdout write)
// propagates as a poll failure rather than dropping the record. The loop should
// not crash; it retries.
// TestProcessLogsSplitsOnRangeCap verifies that when a provider rejects a
// getLogs window as too wide / too many results, the stream splits the range and
// retries the sub-ranges (down to small windows) instead of retrying the same
// oversized chunk forever, and still emits the records.
func TestProcessLogsSplitsOnRangeCap(t *testing.T) {
	fc := &fakeClient{
		chainID: 1,
		heads:   []uint64{100},
		getLogs: func(f rpc.LogFilter) ([]rpc.Log, error) {
			// Reject any window wider than 10 blocks, like a public provider's cap.
			if f.ToBlock-f.FromBlock+1 > 10 {
				return nil, errors.New("rpc error -32005: query returned more than 10000 results")
			}
			return []rpc.Log{makeTransferLog(f.FromBlock, "1")}, nil
		},
	}
	em := &captureEmitter{}
	s, err := New(Options{
		Client:         fc,
		Emitter:        em,
		ChainName:      "c",
		ChainID:        1,
		Contracts:      resolvedUSDC(t),
		PollInterval:   time.Hour, // a single backfill poll covers [1,100]
		LogChunkBlocks: 100,
		FromBlock:      "1",
		BackoffBase:    time.Millisecond,
		BackoffMax:     2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitFor(t, func() bool { return em.count() >= 1 }, 2*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if em.count() == 0 {
		t.Error("no records emitted; chunk split did not recover the oversized range")
	}
}

func TestBackpressurePropagates(t *testing.T) {
	emitErr := errors.New("pipe blocked then broke")
	fc := &fakeClient{
		chainID: 1,
		heads:   []uint64{1},
		getLogs: func(rpc.LogFilter) ([]rpc.Log, error) {
			return []rpc.Log{makeTransferLog(1, "1")}, nil
		},
	}
	var mu sync.Mutex
	attempts := 0
	em := &captureEmitter{onEmit: func() error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts >= 3 {
			return nil // recover after backpressure clears
		}
		return emitErr
	}}
	s, err := New(Options{
		Client:         fc,
		Emitter:        em,
		ChainName:      "c",
		ChainID:        1,
		Contracts:      resolvedUSDC(t),
		PollInterval:   2 * time.Millisecond,
		LogChunkBlocks: 100,
		FromBlock:      "1",
		BackoffBase:    time.Millisecond,
		BackoffMax:     2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitFor(t, func() bool { return em.count() >= 1 }, 2*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The record was eventually emitted (not dropped) once backpressure cleared.
	if em.count() < 1 {
		t.Error("record was dropped under backpressure")
	}
}

// TestResolveStartLatestIsStrictlyNew pins the "latest" semantics: it resolves
// to head+1 so only blocks mined after startup are processed, and the head block
// that already existed at startup is never re-emitted.
func TestResolveStartLatestIsStrictlyNew(t *testing.T) {
	t.Run("resolveStart returns head+1", func(t *testing.T) {
		fc := &fakeClient{chainID: 1, heads: []uint64{5}}
		s, err := New(Options{
			Client:         fc,
			Emitter:        &captureEmitter{},
			ChainName:      "c",
			ChainID:        1,
			Contracts:      resolvedUSDC(t),
			PollInterval:   time.Millisecond,
			LogChunkBlocks: 100,
			FromBlock:      "latest",
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.resolveStart(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got != 6 {
			t.Errorf("resolveStart(latest) with head 5 = %d, want 6 (head+1)", got)
		}
	})

	t.Run("head block not re-emitted; only strictly-new blocks emit", func(t *testing.T) {
		emitted := map[uint64]bool{}
		// First poll head 5 (start = 6, nothing to do), then head advances to 7.
		fc := &fakeClient{
			chainID: 1,
			heads:   []uint64{5, 7},
			getLogs: func(f rpc.LogFilter) ([]rpc.Log, error) {
				var out []rpc.Log
				for b := f.FromBlock; b <= f.ToBlock; b++ {
					emitted[b] = true
					out = append(out, makeTransferLog(b, "1"))
				}
				return out, nil
			},
		}
		em := &captureEmitter{}
		s, err := New(Options{
			Client:         fc,
			Emitter:        em,
			ChainName:      "c",
			ChainID:        1,
			Contracts:      resolvedUSDC(t),
			PollInterval:   2 * time.Millisecond,
			LogChunkBlocks: 100,
			FromBlock:      "latest",
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		// Blocks 6 and 7 are the only strictly-new blocks.
		waitFor(t, func() bool { return em.count() >= 2 }, time.Second)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		for _, env := range em.snapshot() {
			if env.BlockNumber == 5 {
				t.Fatal("head block 5 (pre-existing at startup) must not be re-emitted with from_block=latest")
			}
			if env.BlockNumber < 6 {
				t.Errorf("emitted block %d below head+1", env.BlockNumber)
			}
		}
	})
}

// headTimeMetrics records the head-block-time updates the loop publishes, so a
// test can assert the chain-health gauges are populated from a real poll rather
// than stubbed at zero.
type headTimeMetrics struct {
	noopMetrics
	mu       sync.Mutex
	lastTime time.Time
	calls    int
}

func (m *headTimeMetrics) SetHeadBlockTime(t time.Time, _ time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastTime = t
	m.calls++
}

func (m *headTimeMetrics) snapshot() (time.Time, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastTime, m.calls
}

// TestHeadBlockTimePublished verifies pollOnce fetches the head block header and
// publishes its timestamp to the chain-health gauges (so
// blockchain_chain_head_block_timestamp_seconds is non-zero after a poll, not
// stubbed at 0). See design.md, "Chain health".
func TestHeadBlockTimePublished(t *testing.T) {
	const headNum = uint64(10)
	wantTS := uint64(1_700_000_000) // a real, non-zero unix timestamp
	headBlk := &rpc.Block{
		Number:    rpc.BlockTag(headNum),
		Hash:      "0xhead",
		Timestamp: "0x" + bigHex(wantTS),
	}
	fc := &fakeClient{
		chainID: 4242,
		heads:   []uint64{headNum},
		blocks:  map[uint64]*rpc.Block{headNum: headBlk},
	}
	em := &captureEmitter{}
	hm := &headTimeMetrics{}
	s, err := New(Options{
		Client:         fc,
		Emitter:        em,
		Metrics:        hm,
		ChainName:      "my-chain",
		ChainID:        4242,
		Contracts:      resolvedUSDC(t),
		PollInterval:   5 * time.Millisecond,
		LogChunkBlocks: 100,
		FromBlock:      "10",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitFor(t, func() bool { _, c := hm.snapshot(); return c >= 1 }, time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, _ := hm.snapshot()
	if got.IsZero() {
		t.Fatal("head block time was never published (gauge would stay at 0)")
	}
	if uint64(got.Unix()) != wantTS {
		t.Errorf("head block time = %d, want %d", got.Unix(), wantTS)
	}
}

// bigHex renders n as a lowercase hex string with no 0x prefix.
func bigHex(n uint64) string {
	return new(big.Int).SetUint64(n).Text(16)
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
